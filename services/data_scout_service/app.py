import hashlib
import json
import re
import time
from datetime import datetime, timezone
from typing import Any

import schedule
import sqlalchemy
import yaml
from kafka import KafkaProducer
from sqlalchemy import text

from scout.impl.clickhouse_scout import ClickhouseScout
from scout.impl.pg_scout import PostgresScout
from scout.models import ColumnMetadata, Metadata, TableMetadata
from utils import parse_source


class App:
    def __init__(self, config="./config/config.yaml"):
        with open(config, "r", encoding="utf-8") as file:
            config_data = yaml.safe_load(file) or {}

        datasource = config_data.get("datasource", {}).get("main", {})
        username = datasource["username"]
        password = datasource["password"]
        host = datasource["host"]
        port = datasource["port"]
        database = datasource["database"]
        url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        self.engine = sqlalchemy.create_engine(url)

        storage_config = config_data.get("metadata_storage", {})
        self.metadata_table = self._validate_identifier(
            storage_config.get("table_name", "metadata_catalog")
        )
        self.outbox_table = self._validate_identifier(
            storage_config.get("outbox_table_name", "metadata_outbox")
        )

        kafka_config = config_data.get("kafka", {})
        self.kafka_topic = kafka_config.get("metadata_topic", "navigator.metadata")
        self.kafka_enabled = bool(kafka_config.get("enabled", True))
        self.outbox_batch_size = int(kafka_config.get("outbox_batch_size", 500))
        self.kafka_send_timeout_seconds = float(kafka_config.get("send_timeout_seconds", 10))
        self.kafka_flush_timeout_seconds = float(kafka_config.get("flush_timeout_seconds", 10))

        bootstrap_servers = kafka_config.get("bootstrap_servers", ["localhost:9092"])
        if isinstance(bootstrap_servers, str):
            bootstrap_servers = [bootstrap_servers]

        self.kafka_producer: KafkaProducer | None = None
        if self.kafka_enabled:
            try:
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    client_id=kafka_config.get("client_id", "data-scout-service"),
                    acks=kafka_config.get("acks", "all"),
                    retries=int(kafka_config.get("retries", 3)),
                    request_timeout_ms=int(kafka_config.get("request_timeout_ms", 10000)),
                    max_block_ms=int(kafka_config.get("max_block_ms", 10000)),
                    metadata_max_age_ms=int(kafka_config.get("metadata_max_age_ms", 300000)),
                    reconnect_backoff_ms=int(kafka_config.get("reconnect_backoff_ms", 1000)),
                    reconnect_backoff_max_ms=int(kafka_config.get("reconnect_backoff_max_ms", 10000)),
                    key_serializer=lambda value: value.encode("utf-8"),
                    value_serializer=lambda value: json.dumps(
                        value, sort_keys=True, ensure_ascii=False
                    ).encode("utf-8"),
                )
            except Exception as error:
                print(f"Kafka producer init failed: {error}")
                self.kafka_producer = None

        self._prepare_storage()
        self._prepare_queries()

    @staticmethod
    def _validate_identifier(identifier: str) -> str:
        if not isinstance(identifier, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError(f"Invalid sql identifier: {identifier}")
        return identifier

    def _prepare_storage(self):
        with self.engine.connect() as connection:
            print("Preparing storage...")
            connection.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.metadata_table} (
                        entity_id TEXT PRIMARY KEY,
                        entity_type TEXT NOT NULL CHECK (entity_type IN ('table', 'column')),
                        source TEXT NOT NULL,
                        database_name TEXT NOT NULL,
                        table_name TEXT NOT NULL,
                        column_name TEXT NULL,
                        data_type TEXT NULL,
                        is_not_null BOOLEAN NULL,
                        table_comment TEXT NULL,
                        column_comment TEXT NULL,
                        rag_text TEXT NOT NULL,
                        metadata_hash TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            )
            connection.execute(
                text(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_metadata_catalog_lookup
                    ON {self.metadata_table}(source, database_name, table_name, column_name)
                    """
                )
            )
            print("Metadata storage prepared")

            connection.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.outbox_table} (
                        event_id TEXT PRIMARY KEY,
                        entity_id TEXT NOT NULL,
                        metadata_hash TEXT NOT NULL,
                        topic TEXT NOT NULL,
                        event_key TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        published_at TIMESTAMPTZ NULL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        last_error TEXT NULL
                    )
                    """
                )
            )
            connection.execute(
                text(
                    f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_metadata_outbox_entity_hash
                    ON {self.outbox_table}(entity_id, metadata_hash)
                    """
                )
            )
            connection.execute(
                text(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_metadata_outbox_pending
                    ON {self.outbox_table}(published_at, created_at)
                    """
                )
            )
            print("Outbox storage prepared")
            connection.commit()

    def _prepare_queries(self):
        self.select_hash_query = text(
            f"SELECT metadata_hash FROM {self.metadata_table} WHERE entity_id = :entity_id"
        )
        self.upsert_metadata_query = text(
            f"""
            INSERT INTO {self.metadata_table} (
                entity_id,
                entity_type,
                source,
                database_name,
                table_name,
                column_name,
                data_type,
                is_not_null,
                table_comment,
                column_comment,
                rag_text,
                metadata_hash,
                payload
            ) VALUES (
                :entity_id,
                :entity_type,
                :source,
                :database_name,
                :table_name,
                :column_name,
                :data_type,
                :is_not_null,
                :table_comment,
                :column_comment,
                :rag_text,
                :metadata_hash,
                CAST(:payload AS JSONB)
            )
            ON CONFLICT (entity_id)
            DO UPDATE SET
                entity_type = EXCLUDED.entity_type,
                source = EXCLUDED.source,
                database_name = EXCLUDED.database_name,
                table_name = EXCLUDED.table_name,
                column_name = EXCLUDED.column_name,
                data_type = EXCLUDED.data_type,
                is_not_null = EXCLUDED.is_not_null,
                table_comment = EXCLUDED.table_comment,
                column_comment = EXCLUDED.column_comment,
                rag_text = EXCLUDED.rag_text,
                metadata_hash = EXCLUDED.metadata_hash,
                payload = EXCLUDED.payload,
                updated_at = CASE
                    WHEN {self.metadata_table}.metadata_hash IS DISTINCT FROM EXCLUDED.metadata_hash
                        THEN NOW()
                    ELSE {self.metadata_table}.updated_at
                END,
                last_seen_at = NOW()
            """
        )
        self.insert_outbox_query = text(
            f"""
            INSERT INTO {self.outbox_table} (
                event_id,
                entity_id,
                metadata_hash,
                topic,
                event_key,
                payload
            ) VALUES (
                :event_id,
                :entity_id,
                :metadata_hash,
                :topic,
                :event_key,
                CAST(:payload AS JSONB)
            )
            ON CONFLICT (event_id) DO NOTHING
            """
        )
        self.fetch_pending_outbox_query = text(
            f"""
            SELECT event_id, topic, event_key, payload
            FROM {self.outbox_table}
            WHERE published_at IS NULL
            ORDER BY created_at
            LIMIT :batch_size
            """
        )
        self.mark_outbox_success_query = text(
            f"""
            UPDATE {self.outbox_table}
            SET published_at = NOW(),
                attempts = attempts + 1,
                last_error = NULL
            WHERE event_id = :event_id
            """
        )
        self.mark_outbox_error_query = text(
            f"""
            UPDATE {self.outbox_table}
            SET attempts = attempts + 1,
                last_error = :last_error
            WHERE event_id = :event_id
            """
        )

    @staticmethod
    def _clean_text(value: Any) -> str | None:
        if value is None:
            return None
        text_value = str(value).strip()
        return text_value if text_value else None

    def _build_table_payload(self, table: TableMetadata) -> dict[str, Any]:
        payload = {
            "entity_type": "table",
            "source": self._clean_text(table.source),
            "database_name": self._clean_text(table.database_name),
            "table_name": self._clean_text(table.table_name),
            "column_name": None,
            "data_type": None,
            "is_not_null": None,
            "table_comment": self._clean_text(table.table_comment),
            "column_comment": None,
        }
        payload["rag_text"] = self._build_rag_text(payload)
        return payload

    def _build_column_payload(self, column: ColumnMetadata) -> dict[str, Any]:
        payload = {
            "entity_type": "column",
            "source": self._clean_text(column.source),
            "database_name": self._clean_text(column.database_name),
            "table_name": self._clean_text(column.table_name),
            "column_name": self._clean_text(column.column_name),
            "data_type": self._clean_text(column.data_type),
            "is_not_null": bool(column.is_not_null),
            "table_comment": self._clean_text(column.table_comment),
            "column_comment": self._clean_text(column.column_comment),
        }
        payload["rag_text"] = self._build_rag_text(payload)
        return payload

    @staticmethod
    def _build_rag_text(payload: dict[str, Any]) -> str:
        if payload["entity_type"] == "table":
            return (
                f"source={payload['source']}; database={payload['database_name']}; "
                f"table={payload['table_name']}; table_comment={payload['table_comment'] or ''}"
            )
        return (
            f"source={payload['source']}; database={payload['database_name']}; "
            f"table={payload['table_name']}; column={payload['column_name']}; "
            f"type={payload['data_type']}; not_null={payload['is_not_null']}; "
            f"table_comment={payload['table_comment'] or ''}; "
            f"column_comment={payload['column_comment'] or ''}"
        )

    @staticmethod
    def _hash_data(payload: dict[str, Any]) -> str:
        serialized = json.dumps(
            payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        )
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    @staticmethod
    def _build_entity_id(payload: dict[str, Any]) -> str:
        key_payload = {
            "entity_type": payload["entity_type"],
            "source": payload["source"],
            "database_name": payload["database_name"],
            "table_name": payload["table_name"],
            "column_name": payload["column_name"],
        }
        return App._hash_data(key_payload)

    @staticmethod
    def _build_event_id(entity_id: str, metadata_hash: str) -> str:
        return hashlib.sha256(f"{entity_id}:{metadata_hash}".encode("utf-8")).hexdigest()

    def _build_event(self, payload: dict[str, Any], entity_id: str, metadata_hash: str) -> dict[str, Any]:
        return {
            "event_id": self._build_event_id(entity_id, metadata_hash),
            "event_type": "metadata_upsert",
            "event_version": 1,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "entity_id": entity_id,
            "metadata_hash": metadata_hash,
            **payload,
        }

    def _iter_payloads(self, metadata: Metadata):
        for table in metadata.tables:
            yield self._build_table_payload(table)
        for column in metadata.columns:
            yield self._build_column_payload(column)

    def _publish_outbox(self):
        if self.kafka_producer is None:
            return

        with self.engine.connect() as connection:
            events = connection.execute(
                self.fetch_pending_outbox_query, {"batch_size": self.outbox_batch_size}
            ).mappings().all()

        if not events:
            return

        for event_row in events:
            print(event_row)
            payload = event_row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)

            try:
                self.kafka_producer.send(
                    topic=event_row["topic"],
                    key=event_row["event_key"],
                    value=payload,
                ).get(timeout=self.kafka_send_timeout_seconds)
                print(f"Event sent to Kafka: event_id={event_row['event_id']}, topic={event_row['topic']}")

                with self.engine.connect() as connection:
                    connection.execute(
                        self.mark_outbox_success_query,
                        {"event_id": event_row["event_id"]},
                    )
                    connection.commit()
            except Exception as error:
                with self.engine.connect() as connection:
                    connection.execute(
                        self.mark_outbox_error_query,
                        {
                            "event_id": event_row["event_id"],
                            "last_error": str(error)[:2000],
                        },
                    )
                    connection.commit()
                print(error)

        self.kafka_producer.flush(timeout=self.kafka_flush_timeout_seconds)

    def send_metadata(self, metadata: Metadata):
        with self.engine.connect() as connection:
            for payload in self._iter_payloads(metadata):
                entity_id = self._build_entity_id(payload)
                metadata_hash = self._hash_data(payload)
                event = self._build_event(payload, entity_id, metadata_hash)

                previous_hash = connection.execute(
                    self.select_hash_query, {"entity_id": entity_id}
                ).scalar_one_or_none()

                print(previous_hash)

                connection.execute(
                    self.upsert_metadata_query,
                    {
                        "entity_id": entity_id,
                        "entity_type": payload["entity_type"],
                        "source": payload["source"],
                        "database_name": payload["database_name"],
                        "table_name": payload["table_name"],
                        "column_name": payload["column_name"],
                        "data_type": payload["data_type"],
                        "is_not_null": payload["is_not_null"],
                        "table_comment": payload["table_comment"],
                        "column_comment": payload["column_comment"],
                        "rag_text": payload["rag_text"],
                        "metadata_hash": metadata_hash,
                        "payload": json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    },
                )



                print(payload)

                if previous_hash != metadata_hash:
                    connection.execute(
                        self.insert_outbox_query,
                        {
                            "event_id": event["event_id"],
                            "entity_id": entity_id,
                            "metadata_hash": metadata_hash,
                            "topic": self.kafka_topic,
                            "event_key": entity_id,
                            "payload": json.dumps(event, ensure_ascii=False, sort_keys=True),
                        },
                    )
            connection.commit()

        self._publish_outbox()

    def walk_sources(self):
        with open("config/sources.json", "r", encoding="utf-8") as file:
            sources = json.load(file)

        for source in sources:
            source_type = source.split(":")[0]
            print(source_type)
            match source_type:
                case "postgresql":
                    pg_scout = PostgresScout(source)
                    metadata = pg_scout.get_metadata()
                    print(metadata)
                    self.send_metadata(metadata)
                case "clickhouse":
                    source_params = parse_source(source)
                    source_params.pop("scheme")
                    ch_scout = ClickhouseScout(**source_params)
                    metadata = ch_scout.get_metadata()
                    self.send_metadata(metadata)


if __name__ == "__main__":
    app = App()
    schedule.every(30).seconds.do(app.walk_sources)

    while True:
        schedule.run_pending()
