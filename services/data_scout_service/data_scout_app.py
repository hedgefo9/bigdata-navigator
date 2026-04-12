import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

import httpx
import schedule
import sqlalchemy
import yaml
from kafka import KafkaProducer
from sqlalchemy import text

from scout.impl.clickhouse_scout import ClickhouseScout
from scout.impl.pg_scout import PostgresScout
from scout.models import ColumnMetadata, Metadata, TableMetadata
from utils import apply_password_to_uri, build_connection_uri, parse_source


class App:
    def __init__(self, config="./config/config.yaml"):
        config_path = Path(config)
        if not config_path.is_absolute():
            config_path = (Path(__file__).resolve().parent / config_path).resolve()
        else:
            config_path = config_path.resolve()
        self.base_dir = config_path.parent.parent
        with config_path.open("r", encoding="utf-8") as file:
            raw_config = yaml.safe_load(file) or {}
        self.config = self._resolve_config_secrets(raw_config)
        self._vault_cache: dict[str, Any] = {}

        datasource = self.config.get("datasource", {}).get("main", {})
        username = datasource["username"]
        password = datasource["password"]
        host = datasource["host"]
        port = datasource["port"]
        database = datasource["database"]
        url = (
            f"postgresql://{quote(str(username), safe='')}:{quote(str(password), safe='')}"
            f"@{host}:{port}/{database}"
        )
        self.engine = sqlalchemy.create_engine(url)

        storage_config = self.config.get("metadata_storage", {})
        self.metadata_table = self._validate_identifier(
            storage_config.get("table_name", "metadata_catalog")
        )
        self.outbox_table = self._validate_identifier(
            storage_config.get("outbox_table_name", "metadata_outbox")
        )

        kafka_config = self.config.get("kafka", {})
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

    def _get_vault_config(self) -> dict[str, Any]:
        return dict(self.config.get("vault") or {})

    def _resolve_vault_ref(
        self,
        value: str,
        cache: dict[str, Any],
        vault_config: dict[str, Any] | None = None,
    ) -> Any:
        if not value.startswith("vault://"):
            return value
        if value in cache:
            return cache[value]

        effective_vault_config = vault_config or self._get_vault_config()
        base_url = str(
            effective_vault_config.get("url")
            or os.getenv(str(effective_vault_config.get("url_env", "VAULT_ADDR")), "http://localhost:8200")
        ).rstrip("/")
        token = effective_vault_config.get("token") or os.getenv(
            str(effective_vault_config.get("token_env", "VAULT_TOKEN"))
        )
        timeout_seconds = float(effective_vault_config.get("timeout_seconds", 5))
        if not token:
            raise RuntimeError("Vault token is required to resolve vault:// references")

        parsed = urlsplit(value)
        mount = parsed.netloc.strip("/")
        secret_path = parsed.path.strip("/")
        secret_field = parsed.fragment
        if not mount or not secret_path or not secret_field:
            raise ValueError(
                f"Invalid vault reference: {value}. Use vault://<mount>/<secret_path>#<field>"
            )

        vault_endpoint = f"{base_url}/v1/{mount}/data/{secret_path}"
        with httpx.Client(timeout=timeout_seconds) as client:
            response = client.get(vault_endpoint, headers={"X-Vault-Token": str(token)})
            response.raise_for_status()
            payload = response.json()

        secret_data = dict(payload.get("data", {}).get("data", {}) or {})
        if secret_field not in secret_data:
            raise KeyError(f"Vault secret field not found: {value}")

        cache[value] = secret_data[secret_field]
        return cache[value]

    def _resolve_config_secrets(self, config_data: dict[str, Any]) -> dict[str, Any]:
        vault_config = dict(config_data.get("vault") or {})
        if not vault_config:
            return config_data

        cache: dict[str, Any] = {}

        def _resolve_recursive(node: Any) -> Any:
            if isinstance(node, dict):
                return {key: _resolve_recursive(value) for key, value in node.items()}
            if isinstance(node, list):
                return [_resolve_recursive(item) for item in node]
            if isinstance(node, str):
                return self._resolve_vault_ref(node, cache=cache, vault_config=vault_config)
            return node

        resolved: dict[str, Any] = {"vault": vault_config}
        for key, value in config_data.items():
            if key == "vault":
                continue
            resolved[key] = _resolve_recursive(value)
        return resolved

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
                        source_name TEXT NULL,
                        source_kind TEXT NULL,
                        source_dialect TEXT NULL,
                        connection_uri TEXT NULL,
                        vault_url TEXT NULL,
                        vault_secret_ref TEXT NULL,
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
            # Backward-compatible migrations for existing installations.
            connection.execute(
                text(f"ALTER TABLE {self.metadata_table} ADD COLUMN IF NOT EXISTS source_name TEXT NULL")
            )
            connection.execute(
                text(f"ALTER TABLE {self.metadata_table} ADD COLUMN IF NOT EXISTS source_kind TEXT NULL")
            )
            connection.execute(
                text(f"ALTER TABLE {self.metadata_table} ADD COLUMN IF NOT EXISTS source_dialect TEXT NULL")
            )
            connection.execute(
                text(f"ALTER TABLE {self.metadata_table} ADD COLUMN IF NOT EXISTS connection_uri TEXT NULL")
            )
            connection.execute(
                text(f"ALTER TABLE {self.metadata_table} ADD COLUMN IF NOT EXISTS vault_url TEXT NULL")
            )
            connection.execute(
                text(f"ALTER TABLE {self.metadata_table} ADD COLUMN IF NOT EXISTS vault_secret_ref TEXT NULL")
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
                source_name,
                source_kind,
                source_dialect,
                connection_uri,
                vault_url,
                vault_secret_ref,
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
                :source_name,
                :source_kind,
                :source_dialect,
                :connection_uri,
                :vault_url,
                :vault_secret_ref,
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
                source_name = EXCLUDED.source_name,
                source_kind = EXCLUDED.source_kind,
                source_dialect = EXCLUDED.source_dialect,
                connection_uri = EXCLUDED.connection_uri,
                vault_url = EXCLUDED.vault_url,
                vault_secret_ref = EXCLUDED.vault_secret_ref,
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
            "source_name": self._clean_text(table.source_name),
            "source_kind": self._clean_text(table.source_kind),
            "source_dialect": self._clean_text(table.source_dialect),
            "connection_uri": self._clean_text(table.connection_uri),
            "vault_url": self._clean_text(table.vault_url),
            "vault_secret_ref": self._clean_text(table.vault_secret_ref),
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
            "source_name": self._clean_text(column.source_name),
            "source_kind": self._clean_text(column.source_kind),
            "source_dialect": self._clean_text(column.source_dialect),
            "connection_uri": self._clean_text(column.connection_uri),
            "vault_url": self._clean_text(column.vault_url),
            "vault_secret_ref": self._clean_text(column.vault_secret_ref),
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
        # RAG text must focus on semantic metadata only; connection/security fields
        # are intentionally excluded to avoid embedding noise.
        entity_type = payload.get("entity_type") or "unknown"
        source_name = payload.get("source_name") or payload.get("source")
        source_dialect = payload.get("source_dialect")
        database_name = payload.get("database_name")
        table_name = payload.get("table_name")
        table_comment = payload.get("table_comment")

        rag_parts: list[str] = [
            f"entity={entity_type}",
            f"path={database_name}.{table_name}",
        ]
        if source_name:
            rag_parts.append(f"source={source_name}")
        if source_dialect:
            rag_parts.append(f"dialect={source_dialect}")
        if table_comment:
            rag_parts.append(f"table_description={table_comment}")

        if entity_type == "column":
            column_name = payload.get("column_name")
            data_type = payload.get("data_type")
            is_not_null = payload.get("is_not_null")
            column_comment = payload.get("column_comment")
            rag_parts.append(f"path={database_name}.{table_name}.{column_name}")
            if data_type:
                rag_parts.append(f"data_type={data_type}")
            rag_parts.append(f"nullability={'not_null' if is_not_null else 'nullable'}")
            if column_comment:
                rag_parts.append(f"column_description={column_comment}")

        return " | ".join(part for part in rag_parts if part and "=" in part)

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
            "source_name": payload.get("source_name"),
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
            "event_version": 2,
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
                        "source_name": payload["source_name"],
                        "source_kind": payload["source_kind"],
                        "source_dialect": payload["source_dialect"],
                        "connection_uri": payload["connection_uri"],
                        "vault_url": payload["vault_url"],
                        "vault_secret_ref": payload["vault_secret_ref"],
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

    @staticmethod
    def _default_source_kind(source_dialect: str | None) -> str:
        dialect = (source_dialect or "").lower()
        if dialect in {"postgresql", "postgres", "clickhouse", "mysql", "mariadb", "oracle", "mssql"}:
            return "sql"
        if dialect in {"mongodb", "cassandra", "redis", "elasticsearch"}:
            return "nosql"
        return "other"

    def _load_sources(self) -> list[dict[str, Any]]:
        sources_path = self.base_dir / "config" / "sources.json"
        with sources_path.open("r", encoding="utf-8") as file:
            raw_sources = json.load(file)

        if not isinstance(raw_sources, list):
            raise ValueError("config/sources.json must contain a list")
        return [self._normalize_source(source) for source in raw_sources]

    def _normalize_source(self, source_entry: Any) -> dict[str, Any]:
        if isinstance(source_entry, str):
            source_params = parse_source(source_entry)
            source_dialect = str(source_params.get("scheme") or "").lower()
            source_kind = self._default_source_kind(source_dialect)
            source_name = f"{source_dialect}-{source_params.get('database') or 'default'}"
            connection_uri = build_connection_uri(
                scheme=source_dialect,
                host=source_params.get("host"),
                port=source_params.get("port"),
                database=source_params.get("database"),
                user=source_params.get("user"),
                password=None,
            )
            return {
                "name": source_name,
                "source": source_dialect,
                "kind": source_kind,
                "dialect": source_dialect,
                "connection_uri": connection_uri,
                "vault_url": None,
                "vault_secret_ref": None,
                "password": source_params.get("password"),
            }

        if not isinstance(source_entry, dict):
            raise ValueError(f"Unsupported source entry type: {type(source_entry)}")

        connection_uri = str(source_entry.get("connection_uri") or source_entry.get("url") or "").strip()
        if not connection_uri:
            raise ValueError("Each source must provide `connection_uri`")

        parsed = parse_source(connection_uri)
        source_dialect = str(source_entry.get("dialect") or parsed.get("scheme") or "").lower()
        source_kind = str(source_entry.get("kind") or self._default_source_kind(source_dialect)).lower()
        source = str(source_entry.get("source") or source_entry.get("provider") or source_dialect).strip().lower()
        source_name = str(
            source_entry.get("name") or f"{source_dialect}-{parsed.get('database') or 'default'}"
        ).strip()
        sanitized_connection_uri = build_connection_uri(
            scheme=source_dialect,
            host=parsed.get("host"),
            port=parsed.get("port"),
            database=parsed.get("database"),
            user=parsed.get("user"),
            password=None,
        )

        if not source or not source_name:
            raise ValueError(f"Invalid source entry: {source_entry}")

        return {
            "name": source_name,
            "source": source,
            "kind": source_kind,
            "dialect": source_dialect,
            "connection_uri": sanitized_connection_uri,
            "vault_url": source_entry.get("vault_url"),
            "vault_secret_ref": source_entry.get("vault_secret_ref"),
            "password": source_entry.get("password"),
        }

    def _resolve_source_password(self, source_config: dict[str, Any]) -> str | None:
        vault_secret_ref = source_config.get("vault_secret_ref")
        if isinstance(vault_secret_ref, str) and vault_secret_ref.startswith("vault://"):
            resolved = self._resolve_vault_ref(
                value=vault_secret_ref,
                cache=self._vault_cache,
                vault_config=self._get_vault_config(),
            )
            return str(resolved) if resolved is not None else None

        direct_password = source_config.get("password")
        if direct_password is None:
            return None
        direct_text = str(direct_password).strip()
        return direct_text or None

    def _collect_sql_source(self, source_config: dict[str, Any]):
        source_dialect = str(source_config["dialect"]).lower()
        connection_uri = str(source_config["connection_uri"]).strip()
        password = self._resolve_source_password(source_config)
        runtime_connection_uri = apply_password_to_uri(connection_uri, password=password)
        runtime_source = parse_source(runtime_connection_uri)
        if source_dialect in {"postgres", "postgresql"} and runtime_source.get("port") is None:
            runtime_source["port"] = 5432
        if source_dialect == "clickhouse" and runtime_source.get("port") is None:
            runtime_source["port"] = 8123

        common_kwargs = {
            "source": source_config["source"],
            "source_name": source_config["name"],
            "source_kind": source_config["kind"],
            "source_dialect": source_dialect,
            "connection_uri": connection_uri,
            "vault_url": source_config.get("vault_url"),
            "vault_secret_ref": source_config.get("vault_secret_ref"),
        }

        if source_dialect in {"postgres", "postgresql"}:
            pg_scout = PostgresScout(url=runtime_connection_uri, **common_kwargs)
            metadata = pg_scout.get_metadata()
            self.send_metadata(metadata)
            return

        if source_dialect == "clickhouse":
            ch_scout = ClickhouseScout(
                user=runtime_source.get("user"),
                password=runtime_source.get("password"),
                host=runtime_source.get("host"),
                port=runtime_source.get("port"),
                database=runtime_source.get("database"),
                **common_kwargs,
            )
            metadata = ch_scout.get_metadata()
            self.send_metadata(metadata)
            return

        print(f"Unsupported SQL dialect `{source_dialect}` for source `{source_config['name']}`")

    def walk_sources(self):
        sources = self._load_sources()
        for source_config in sources:
            source_kind = str(source_config.get("kind") or "").lower()
            print(
                "Walking source: "
                f"name={source_config.get('name')} kind={source_kind} dialect={source_config.get('dialect')}"
            )
            if source_kind == "sql":
                self._collect_sql_source(source_config)
                continue

            print(
                f"Source `{source_config.get('name')}` has kind `{source_kind}` "
                "and is not supported yet by data_scout_service."
            )


if __name__ == "__main__":
    app = App()
    schedule.every(30).seconds.do(app.walk_sources)

    while True:
        schedule.run_pending()
