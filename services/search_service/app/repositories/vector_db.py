from typing import Any
import uuid

from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

QDRANT_POINT_NAMESPACE = uuid.UUID("44e3094a-2bb4-4ff8-b09e-8f64f0d02692")


def create_qdrant_client(config: dict[str, Any]) -> QdrantClient:
    qdrant_config = config.get("qdrant", {})
    host = qdrant_config.get("host", "localhost")
    port = int(qdrant_config.get("port", 6333))
    api_key = qdrant_config.get("api_key")
    https = bool(qdrant_config.get("https", False))

    return QdrantClient(host=host, port=port, api_key=api_key, https=https)


def entity_id_to_point_id(entity_id: str) -> str:
    raw_entity_id = str(entity_id).strip()
    if not raw_entity_id:
        raise ValueError("entity_id is empty")

    try:
        return str(uuid.UUID(raw_entity_id))
    except ValueError:
        pass

    return str(uuid.uuid5(QDRANT_POINT_NAMESPACE, raw_entity_id))


def get_collection_name(config: dict[str, Any]) -> str:
    qdrant_config = config.get("qdrant", {})
    collection_name = qdrant_config.get("collection_name", "metadata_catalog")
    if not collection_name:
        raise ValueError("qdrant.collection_name is required")
    return collection_name


def _resolve_distance(config: dict[str, Any]) -> qmodels.Distance:
    distance_name = str(config.get("qdrant", {}).get("distance", "cosine")).lower()
    if distance_name == "dot":
        return qmodels.Distance.DOT
    if distance_name == "euclid":
        return qmodels.Distance.EUCLID
    if distance_name == "manhattan":
        return qmodels.Distance.MANHATTAN
    return qmodels.Distance.COSINE


def ensure_collection(
    client: QdrantClient,
    collection_name: str,
    vector_size: int,
    distance: qmodels.Distance,
):
    if vector_size <= 0:
        raise ValueError("vector_size must be > 0")

    exists = client.collection_exists(collection_name=collection_name)
    if exists:
        collection_info = client.get_collection(collection_name=collection_name)
        vectors_config = collection_info.config.params.vectors

        existing_size: int | None = None
        if isinstance(vectors_config, qmodels.VectorParams):
            existing_size = vectors_config.size
        elif isinstance(vectors_config, dict) and vectors_config:
            first_vector = next(iter(vectors_config.values()))
            if isinstance(first_vector, qmodels.VectorParams):
                existing_size = first_vector.size

        if existing_size is not None and existing_size != vector_size:
            raise ValueError(
                f"Qdrant collection '{collection_name}' has vector_size={existing_size}, "
                f"but embedding returned vector_size={vector_size}. "
                "Recreate collection or use compatible embedding model."
            )
        return

    client.create_collection(
        collection_name=collection_name,
        vectors_config=qmodels.VectorParams(size=vector_size, distance=distance),
    )


def upsert_metadata_embedding(
    client: QdrantClient,
    config: dict[str, Any],
    point_id: str,
    vector: list[float],
    payload: dict[str, Any],
):
    if not point_id:
        raise ValueError("point_id is required")
    if not vector:
        raise ValueError("vector is required")

    collection_name = get_collection_name(config)
    distance = _resolve_distance(config)
    ensure_collection(
        client=client,
        collection_name=collection_name,
        vector_size=len(vector),
        distance=distance,
    )

    point = qmodels.PointStruct(id=point_id, vector=vector, payload=payload)
    client.upsert(collection_name=collection_name, points=[point], wait=True)


def search_metadata(
    client: QdrantClient,
    config: dict[str, Any],
    query_vector: list[float],
    top_k: int = 8,
    score_threshold: float | None = None,
    source: str | None = None,
) -> list[dict[str, Any]]:
    if not query_vector:
        return []

    collection_name = get_collection_name(config)
    if not client.collection_exists(collection_name=collection_name):
        return []

    must_filters = []
    if source:
        must_filters.append(
            qmodels.FieldCondition(
                key="source",
                match=qmodels.MatchValue(value=source),
            )
        )

    query_filter = qmodels.Filter(must=must_filters) if must_filters else None
    # qdrant-client API differs across versions:
    # older -> client.search(..., query_vector=...)
    # newer -> client.query_points(..., query=...)
    if hasattr(client, "search"):
        points = client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            limit=top_k,
            score_threshold=score_threshold,
            query_filter=query_filter,
            with_payload=True,
            with_vectors=False,
        )
    elif hasattr(client, "query_points"):
        try:
            response = client.query_points(
                collection_name=collection_name,
                query=query_vector,
                limit=top_k,
                score_threshold=score_threshold,
                query_filter=query_filter,
                with_payload=True,
                with_vectors=False,
            )
        except TypeError:
            # Some client versions use `filter` instead of `query_filter`.
            response = client.query_points(
                collection_name=collection_name,
                query=query_vector,
                limit=top_k,
                score_threshold=score_threshold,
                filter=query_filter,
                with_payload=True,
                with_vectors=False,
            )

        if isinstance(response, list):
            points = response
        else:
            points = list(getattr(response, "points", []) or [])
    else:
        raise RuntimeError("Unsupported qdrant-client version: neither search() nor query_points() available")

    return [
        {
            "id": str(point.id),
            "point_id": str(point.id),
            "entity_id": (point.payload or {}).get("entity_id"),
            "score": float(point.score),
            "payload": dict(point.payload or {}),
        }
        for point in points
    ]


def get_metadata_by_point_id(
    client: QdrantClient,
    config: dict[str, Any],
    point_id: str,
) -> dict[str, Any] | None:
    collection_name = get_collection_name(config)
    if not client.collection_exists(collection_name=collection_name):
        return None

    points = client.retrieve(
        collection_name=collection_name,
        ids=[point_id],
        with_payload=True,
        with_vectors=False,
    )
    if not points:
        return None

    point = points[0]
    return {
        "id": str(point.id),
        "point_id": str(point.id),
        "entity_id": (point.payload or {}).get("entity_id"),
        "payload": dict(point.payload or {}),
    }


def get_metadata_by_entity_id(
    client: QdrantClient,
    config: dict[str, Any],
    entity_id: str,
) -> dict[str, Any] | None:
    collection_name = get_collection_name(config)
    if not client.collection_exists(collection_name=collection_name):
        return None

    must_filters = [
        qmodels.FieldCondition(
            key="entity_id",
            match=qmodels.MatchValue(value=entity_id),
        )
    ]
    try:
        points, _ = client.scroll(
            collection_name=collection_name,
            scroll_filter=qmodels.Filter(must=must_filters),
            limit=1,
            with_payload=True,
            with_vectors=False,
        )
    except TypeError:
        points, _ = client.scroll(
            collection_name=collection_name,
            filter=qmodels.Filter(must=must_filters),
            limit=1,
            with_payload=True,
            with_vectors=False,
        )
    if points:
        point = points[0]
        return {
            "id": str(point.id),
            "point_id": str(point.id),
            "entity_id": (point.payload or {}).get("entity_id"),
            "payload": dict(point.payload or {}),
        }

    # Fallback through deterministic mapping if payload filter did not find record.
    return get_metadata_by_point_id(
        client=client,
        config=config,
        point_id=entity_id_to_point_id(entity_id),
    )
