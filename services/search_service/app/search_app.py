import math
import re
import uuid
from pathlib import Path
from typing import Any

import uvicorn
import yaml
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from starlette import status
from starlette.responses import JSONResponse

try:
    from .clients.model_client import EmbeddingClient
    from .repositories.vector_db import (
        create_qdrant_client,
        get_metadata_by_entity_id,
        get_metadata_by_point_id,
        search_metadata,
    )
except ImportError:
    from clients.model_client import EmbeddingClient
    from repositories.vector_db import (
        create_qdrant_client,
        get_metadata_by_entity_id,
        get_metadata_by_point_id,
        search_metadata,
    )


class SearchRequest(BaseModel):
    query: str = Field(min_length=2, max_length=3000)
    top_k: int | None = Field(default=None, ge=1, le=50)
    score_threshold: float | None = Field(default=None, ge=0.0, le=1.0)
    source: str | None = Field(default=None)


class SearchResponse(BaseModel):
    query: str
    matches: list[dict[str, Any]]


def _load_config() -> dict[str, Any]:
    config_path = Path(__file__).resolve().parent / "config" / "config.yaml"
    with config_path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}

config = _load_config()
embedding_client = EmbeddingClient(config=config)
qdrant_client = create_qdrant_client(config=config)

search_api_config = config.get("search_api", {})
default_top_k = int(search_api_config.get("default_top_k", 8))
default_score_threshold = search_api_config.get("default_score_threshold")
if default_score_threshold is not None:
    default_score_threshold = float(default_score_threshold)

rerank_config = config.get("rerank", {})
rerank_enabled = bool(rerank_config.get("enabled", True))
rerank_candidate_multiplier = int(rerank_config.get("candidate_multiplier", 4))
rerank_max_candidates = int(rerank_config.get("max_candidates", 64))
rerank_dense_weight = float(rerank_config.get("dense_weight", 0.65))
bm25_k1 = float(rerank_config.get("bm25_k1", 1.2))
bm25_b = float(rerank_config.get("bm25_b", 0.75))

TOKEN_PATTERN = re.compile(r"[A-Za-zА-Яа-я0-9_]+", re.UNICODE)


def _tokenize(text: str) -> list[str]:
    if not text:
        return []
    return [token.lower() for token in TOKEN_PATTERN.findall(text)]


def _build_document_text(match: dict[str, Any]) -> str:
    payload = dict(match.get("payload") or {})
    fields = [
        payload.get("rag_text"),
        payload.get("vector_text"),
        payload.get("database_name"),
        payload.get("table_name"),
        payload.get("column_name"),
        payload.get("data_type"),
        payload.get("table_comment"),
        payload.get("column_comment"),
        payload.get("source_name"),
        payload.get("source_dialect"),
    ]
    return " ".join(str(value) for value in fields if value is not None and str(value).strip())


def _normalize_scores(values: list[float]) -> list[float]:
    if not values:
        return []
    min_value = min(values)
    max_value = max(values)
    if abs(max_value - min_value) < 1e-9:
        if max_value <= 0:
            return [0.0 for _ in values]
        return [1.0 for _ in values]
    return [(value - min_value) / (max_value - min_value) for value in values]


def _bm25_scores(query: str, documents: list[str], k1: float, b: float) -> list[float]:
    query_tokens = _tokenize(query)
    if not query_tokens or not documents:
        return [0.0 for _ in documents]

    tokenized_docs = [_tokenize(document) for document in documents]
    corpus_size = len(tokenized_docs)
    doc_lengths = [len(doc_tokens) for doc_tokens in tokenized_docs]
    avg_doc_len = sum(doc_lengths) / corpus_size if corpus_size else 0.0

    doc_freq: dict[str, int] = {}
    for tokens in tokenized_docs:
        for token in set(tokens):
            doc_freq[token] = doc_freq.get(token, 0) + 1

    idf: dict[str, float] = {}
    for token, freq in doc_freq.items():
        idf[token] = math.log(1.0 + (corpus_size - freq + 0.5) / (freq + 0.5))

    scores: list[float] = []
    for doc_index, tokens in enumerate(tokenized_docs):
        tf: dict[str, int] = {}
        for token in tokens:
            tf[token] = tf.get(token, 0) + 1

        score = 0.0
        doc_len = doc_lengths[doc_index]
        for token in query_tokens:
            token_freq = tf.get(token)
            if not token_freq:
                continue
            norm = 1.0 - b + b * (doc_len / avg_doc_len) if avg_doc_len > 0 else 1.0
            denom = token_freq + k1 * norm
            if denom <= 0:
                continue
            score += idf.get(token, 0.0) * token_freq * (k1 + 1.0) / denom
        scores.append(score)

    return scores


def _rerank_matches(query: str, matches: list[dict[str, Any]], final_top_k: int) -> list[dict[str, Any]]:
    if not matches:
        return []

    documents = [_build_document_text(match) for match in matches]
    lexical_scores = _bm25_scores(query=query, documents=documents, k1=bm25_k1, b=bm25_b)
    dense_scores = [float(match.get("score", 0.0)) for match in matches]

    lexical_norm = _normalize_scores(lexical_scores)
    dense_norm = _normalize_scores(dense_scores)

    reranked: list[dict[str, Any]] = []
    for index, match in enumerate(matches):
        combined_score = (
            rerank_dense_weight * dense_norm[index]
            + (1.0 - rerank_dense_weight) * lexical_norm[index]
        )
        enriched = dict(match)
        enriched["dense_score"] = dense_scores[index]
        enriched["lexical_score"] = lexical_scores[index]
        enriched["rerank_score"] = combined_score
        enriched["score"] = combined_score
        reranked.append(enriched)

    reranked.sort(
        key=lambda item: (
            float(item.get("rerank_score", 0.0)),
            float(item.get("dense_score", 0.0)),
        ),
        reverse=True,
    )
    return reranked[:final_top_k]

app = FastAPI(
    title="BigData Navigator Search Service",
    description="Semantic metadata search over Qdrant",
    version="0.1.0",
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    # Генерируем уникальный ID ошибки для связи лога и ответа клиенту
    error_id = str(uuid.uuid4())

    # Логируем детальное сообщение и стек вызовов
    print(
        f"ErrorID: {error_id} | Path: {request.url.path} | "
        f"Method: {request.method} | Error: {str(exc)}"
    )

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Internal Server Error",
            "error_id": error_id  # Клиент может сообщить этот ID админу
        },
    )


@app.post("/search", response_model=SearchResponse)
def search(request: SearchRequest) -> SearchResponse:
    try:
        requested_top_k = request.top_k or default_top_k
        candidate_top_k = requested_top_k
        if rerank_enabled:
            candidate_top_k = max(
                requested_top_k,
                min(requested_top_k * max(rerank_candidate_multiplier, 1), rerank_max_candidates),
            )

        query_vector = embedding_client.embed(request.query)
        matches = search_metadata(
            client=qdrant_client,
            config=config,
            query_vector=query_vector,
            top_k=candidate_top_k,
            score_threshold=(
                request.score_threshold
                if request.score_threshold is not None
                else default_score_threshold
            ),
            source=request.source,
        )
        if rerank_enabled:
            matches = _rerank_matches(query=request.query, matches=matches, final_top_k=requested_top_k)
        else:
            matches = matches[:requested_top_k]
        return SearchResponse(query=request.query, matches=matches)
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Search error: {error}") from error


@app.get("/metadata/by-entity/{entity_id}")
def metadata_by_entity(entity_id: str) -> dict[str, Any]:
    try:
        result = get_metadata_by_entity_id(
            client=qdrant_client,
            config=config,
            entity_id=entity_id,
        )
        if result is None:
            raise HTTPException(status_code=404, detail="Metadata not found")
        return result
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Metadata lookup error: {error}") from error


@app.get("/metadata/by-point/{point_id}")
def metadata_by_point(point_id: str) -> dict[str, Any]:
    try:
        result = get_metadata_by_point_id(
            client=qdrant_client,
            config=config,
            point_id=point_id,
        )
        if result is None:
            raise HTTPException(status_code=404, detail="Metadata not found")
        return result
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Metadata lookup error: {error}") from error


if __name__ == "__main__":
    host = str(search_api_config.get("host", "0.0.0.0"))
    port = int(search_api_config.get("port", 8001))
    uvicorn.run(app, host=host, port=port, reload=False)
