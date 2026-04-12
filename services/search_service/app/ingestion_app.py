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
        database_name = event.get("database_name")
        table_name = event.get("table_name")
        column_name = event.get("column_name")
        data_type = event.get("data_type")
        is_not_null = event.get("is_not_null")
        table_comment = event.get("table_comment")
        column_comment = event.get("column_comment")
        entity_type = event.get("entity_type")

        if entity_type == "table":
            return (
                f"source={source}; database={database_name}; table={table_name}; "
                f"table_comment={table_comment or ''}"
            )

        return (
            f"source={source}; database={database_name}; table={table_name}; "
            f"column={column_name}; type={data_type}; not_null={is_not_null}; "
            f"table_comment={table_comment or ''}; column_comment={column_comment or ''}"
        )

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
