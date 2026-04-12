from pathlib import Path
from typing import Any

import yaml

from brokers.consumer import MetadataConsumer
from clients.model_client import EmbeddingClient
from repositories.vector_db import (
    create_qdrant_client,
    entity_id_to_point_id,
    upsert_metadata_embedding,
)

class MetadataIndexer:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.embedding_client = EmbeddingClient(config=config)
        self.qdrant_client = create_qdrant_client(config=config)

    @staticmethod
    def _build_vector_text(event: dict[str, Any]) -> str:
        event_text = event.get("rag_text")
        if isinstance(event_text, str) and event_text.strip():
            return event_text.strip()

        source = event.get("source")
        source_name = event.get("source_name")
        source_dialect = event.get("source_dialect")
        database_name = event.get("database_name")
        table_name = event.get("table_name")
        column_name = event.get("column_name")
        data_type = event.get("data_type")
        is_not_null = event.get("is_not_null")
        table_comment = event.get("table_comment")
        column_comment = event.get("column_comment")
        entity_type = event.get("entity_type")
        source_label = source_name or source
        rag_parts = [
            f"entity={entity_type}",
            f"path={database_name}.{table_name}",
        ]
        if source_label:
            rag_parts.append(f"source={source_label}")
        if source_dialect:
            rag_parts.append(f"dialect={source_dialect}")
        if table_comment:
            rag_parts.append(f"table_description={table_comment}")

        if entity_type == "column":
            rag_parts.append(f"path={database_name}.{table_name}.{column_name}")
            if data_type:
                rag_parts.append(f"data_type={data_type}")
            rag_parts.append(f"nullability={'not_null' if is_not_null else 'nullable'}")
            if column_comment:
                rag_parts.append(f"column_description={column_comment}")

        return " | ".join(part for part in rag_parts if part and "=" in part)

    @staticmethod
    def _validate_event(event: dict[str, Any]):
        event_type = event.get("event_type")
        if event_type and event_type != "metadata_upsert":
            raise ValueError(f"Unsupported event_type: {event_type}")

        required_fields = ["entity_id", "entity_type", "source", "database_name", "table_name"]
        missing_fields = [field for field in required_fields if not event.get(field)]
        if missing_fields:
            raise ValueError(f"Metadata event has missing fields: {missing_fields}")

        if event.get("entity_type") not in {"table", "column"}:
            raise ValueError(f"Unsupported entity_type: {event.get('entity_type')}")
        if event.get("entity_type") == "column" and not event.get("column_name"):
            raise ValueError("Metadata column event has no column_name")

    @staticmethod
    def _build_payload(event: dict[str, Any], vector_text: str, point_id: str) -> dict[str, Any]:
        return {
            "entity_id": event.get("entity_id"),
            "qdrant_point_id": point_id,
            "event_id": event.get("event_id"),
            "metadata_hash": event.get("metadata_hash"),
            "event_version": event.get("event_version"),
            "occurred_at": event.get("occurred_at"),
            "entity_type": event.get("entity_type"),
            "source": event.get("source"),
            "source_name": event.get("source_name"),
            "source_kind": event.get("source_kind"),
            "source_dialect": event.get("source_dialect"),
            "connection_uri": event.get("connection_uri"),
            "vault_url": event.get("vault_url"),
            "vault_secret_ref": event.get("vault_secret_ref"),
            "database_name": event.get("database_name"),
            "table_name": event.get("table_name"),
            "column_name": event.get("column_name"),
            "data_type": event.get("data_type"),
            "is_not_null": event.get("is_not_null"),
            "table_comment": event.get("table_comment"),
            "column_comment": event.get("column_comment"),
            "rag_text": event.get("rag_text"),
            "vector_text": vector_text,
        }

    def process_metadata_event(self, event: dict[str, Any]):
        self._validate_event(event)
        vector_text = self._build_vector_text(event)
        vector = self.embedding_client.embed(vector_text)

        point_id = entity_id_to_point_id(str(event["entity_id"]))
        payload = self._build_payload(event=event, vector_text=vector_text, point_id=point_id)
        upsert_metadata_embedding(
            client=self.qdrant_client,
            config=self.config,
            point_id=point_id,
            vector=vector,
            payload=payload,
        )
        print("Indexed metadata event with point_id: ", point_id)


def process_metadata_event(event: dict[str, Any]):
    if _indexer is None:
        raise RuntimeError("MetadataIndexer is not initialized")

    try:
        _indexer.process_metadata_event(event)
        print(
            "Indexed metadata event: "
            f"event_id={event.get('event_id')} entity_id={event.get('entity_id')} "
            f"type={event.get('entity_type')} "
            f"table={event.get('table_name')} column={event.get('column_name')}"
        )
    except Exception as error:
        print(
            "Metadata event processing failed: "
            f"event_id={event.get('event_id')} entity_id={event.get('entity_id')} error={error}"
        )
        raise


def _load_config() -> dict[str, Any]:
    config_path = Path(__file__).resolve().parent / "config" / "config.yaml"
    with config_path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


_indexer: MetadataIndexer | None = None


def main():
    global _indexer
    config = _load_config()
    _indexer = MetadataIndexer(config=config)
    consumer = MetadataConsumer(config=config)
    consumer.consume_forever(process_metadata_event)


if __name__ == "__main__":
    main()
