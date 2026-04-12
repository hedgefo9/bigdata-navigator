import logging
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
        query_vector = embedding_client.embed(request.query)
        matches = search_metadata(
            client=qdrant_client,
            config=config,
            query_vector=query_vector,
            top_k=request.top_k or default_top_k,
            score_threshold=(
                request.score_threshold
                if request.score_threshold is not None
                else default_score_threshold
            ),
            source=request.source,
        )
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
