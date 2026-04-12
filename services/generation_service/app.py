import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from auth import create_access_token, get_current_user_dependency, hash_password, verify_password
from configuration import load_config
from db import Base, engine, get_db
from models import Chat, Message, User
from default_pipeline import GenerationPipeline, UpstreamServiceError

config = load_config()
pipeline = GenerationPipeline(config=config)
get_current_user = get_current_user_dependency(config)

app = FastAPI(
    title="BigData Navigator Generation Service",
    description="Chat, auth and RAG orchestration service",
    version="0.1.0",
)

base_dir = Path(__file__).resolve().parent
static_dir = base_dir / "static"
templates_dir = base_dir / "templates"

app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
templates = Jinja2Templates(directory=str(templates_dir))


class RegisterRequest(BaseModel):
    username: str = Field(min_length=3, max_length=100)
    password: str = Field(min_length=6, max_length=255)


class LoginRequest(BaseModel):
    username: str = Field(min_length=3, max_length=100)
    password: str = Field(min_length=6, max_length=255)


class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: dict[str, Any]


class CreateChatRequest(BaseModel):
    title: str | None = Field(default=None, max_length=255)


class ChatResponse(BaseModel):
    id: str
    title: str
    created_at: datetime
    updated_at: datetime


class ChatPageResponse(BaseModel):
    items: list[ChatResponse]
    offset: int
    limit: int
    has_more: bool
    next_offset: int | None


class SendMessageRequest(BaseModel):
    content: str = Field(min_length=1, max_length=10000)
    mode: Literal["new_search", "continue"] = "new_search"
    source: str | None = None
    top_k: int | None = Field(default=None, ge=1, le=50)
    score_threshold: float | None = Field(default=None, ge=0.0, le=1.0)


class MessageResponse(BaseModel):
    id: str
    role: str
    mode: str | None
    content: str
    metadata: dict[str, Any]
    created_at: datetime


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    try:
        # Starlette>=0.37 expects request as a separate argument.
        return templates.TemplateResponse(request=request, name="index.html")
    except TypeError:
        # Backward compatibility for older Starlette signature.
        return templates.TemplateResponse("index.html", {"request": request})


def _normalize_username(username: str) -> str:
    return username.strip().lower()


def _user_to_dict(user: User) -> dict[str, Any]:
    return {
        "id": user.id,
        "username": user.username,
        "created_at": user.created_at.isoformat(),
    }


def _chat_to_dict(chat: Chat) -> dict[str, Any]:
    return {
        "id": chat.id,
        "title": chat.title,
        "created_at": chat.created_at,
        "updated_at": chat.updated_at,
    }


def _message_to_dict(message: Message) -> dict[str, Any]:
    return {
        "id": message.id,
        "role": message.role,
        "mode": message.mode,
        "content": message.content,
        "metadata": dict(message.extra_data or {}),
        "created_at": message.created_at,
    }


def _get_chat_or_404(db: Session, user_id: int, chat_id: str) -> Chat:
    chat = db.execute(
        select(Chat).where(Chat.id == chat_id, Chat.user_id == user_id)
    ).scalar_one_or_none()
    if chat is None:
        raise HTTPException(status_code=404, detail="Chat not found")
    return chat


def _get_history_messages(db: Session, chat_id: str, window: int) -> list[dict[str, str]]:
    messages = db.execute(
        select(Message)
        .where(Message.chat_id == chat_id)
        .order_by(desc(Message.created_at))
        .limit(window)
    ).scalars().all()
    messages.reverse()
    return [{"role": item.role, "content": item.content} for item in messages]


def _get_last_matches(db: Session, chat_id: str) -> list[dict[str, Any]]:
    assistant_messages = db.execute(
        select(Message)
        .where(Message.chat_id == chat_id, Message.role == "assistant")
        .order_by(desc(Message.created_at))
        .limit(1)
    ).scalars().all()
    if not assistant_messages:
        return []

    metadata = dict(assistant_messages[0].extra_data or {})
    matches = metadata.get("matches")
    return matches if isinstance(matches, list) else []


def _touch_chat(chat: Chat):
    chat.updated_at = datetime.now(timezone.utc)


def _entity_label_from_payload(payload: dict[str, Any]) -> str | None:
    source = payload.get("source")
    database_name = payload.get("database_name")
    table_name = payload.get("table_name")
    if not source or not database_name or not table_name:
        return None
    column_name = payload.get("column_name")
    if column_name:
        return f"{source}.{database_name}.{table_name}.{column_name}"
    return f"{source}.{database_name}.{table_name}"


def _extract_sources(result: dict[str, Any]) -> list[str]:
    sources: list[str] = []
    used_entities = result.get("used_entities")
    if isinstance(used_entities, list):
        for item in used_entities:
            text = str(item).strip()
            if text and text not in sources:
                sources.append(text)

    matches = result.get("matches")
    if isinstance(matches, list):
        for match in matches:
            payload = dict((match or {}).get("payload") or {})
            label = _entity_label_from_payload(payload)
            if label and label not in sources:
                sources.append(label)

    return sources[:12]


def _build_assistant_metadata(result: dict[str, Any], mode: str, top_k: int | None) -> dict[str, Any]:
    return {
        "sql_query": result.get("sql_query"),
        "used_entities": result.get("used_entities", []),
        "sources": _extract_sources(result),
        "limitations": result.get("limitations"),
        "matches": result.get("matches", []),
        "search_performed": result.get("search_performed", False),
        "mode": mode,
        "top_k": top_k,
    }


def _sse(event: str, data: dict[str, Any]) -> str:
    payload = jsonable_encoder(data)
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


@app.post("/api/auth/register", response_model=AuthResponse)
def register(request: RegisterRequest, db: Session = Depends(get_db)):
    username = _normalize_username(request.username)
    existing = db.execute(select(User).where(User.username == username)).scalar_one_or_none()
    if existing is not None:
        raise HTTPException(status_code=409, detail="Username already exists")

    user = User(username=username, password_hash=hash_password(request.password))
    db.add(user)
    db.commit()
    db.refresh(user)

    token = create_access_token(config=config, user_id=user.id)
    return AuthResponse(access_token=token, user=_user_to_dict(user))


@app.post("/api/auth/login", response_model=AuthResponse)
def login(request: LoginRequest, db: Session = Depends(get_db)):
    username = _normalize_username(request.username)
    user = db.execute(select(User).where(User.username == username)).scalar_one_or_none()
    if user is None or not verify_password(request.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )

    token = create_access_token(config=config, user_id=user.id)
    return AuthResponse(access_token=token, user=_user_to_dict(user))


@app.get("/api/me")
def me(current_user: User = Depends(get_current_user)):
    return _user_to_dict(current_user)


@app.get("/api/chats", response_model=list[ChatResponse])
def list_chats(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    chats = db.execute(
        select(Chat)
        .where(Chat.user_id == current_user.id)
        .order_by(desc(Chat.updated_at), desc(Chat.created_at))
    ).scalars().all()
    return [_chat_to_dict(chat) for chat in chats]


@app.get("/api/chats/page", response_model=ChatPageResponse)
def list_chats_page(
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        select(Chat)
        .where(Chat.user_id == current_user.id)
        .order_by(desc(Chat.updated_at), desc(Chat.created_at))
        .offset(offset)
        .limit(limit + 1)
    ).scalars().all()

    has_more = len(rows) > limit
    items = rows[:limit]
    next_offset = offset + len(items) if has_more else None
    return ChatPageResponse(
        items=[_chat_to_dict(chat) for chat in items],
        offset=offset,
        limit=limit,
        has_more=has_more,
        next_offset=next_offset,
    )


@app.post("/api/chats", response_model=ChatResponse)
def create_chat(
    request: CreateChatRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    title = (request.title or "").strip() or "Новый чат"
    chat = Chat(user_id=current_user.id, title=title)
    db.add(chat)
    db.commit()
    db.refresh(chat)
    return _chat_to_dict(chat)


@app.get("/api/chats/{chat_id}", response_model=ChatResponse)
def get_chat(
    chat_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    chat = _get_chat_or_404(db=db, user_id=current_user.id, chat_id=chat_id)
    return _chat_to_dict(chat)


@app.delete("/api/chats/{chat_id}", status_code=204)
def delete_chat(
    chat_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    chat = _get_chat_or_404(db=db, user_id=current_user.id, chat_id=chat_id)
    db.delete(chat)
    db.commit()


@app.get("/api/chats/{chat_id}/messages", response_model=list[MessageResponse])
def list_messages(
    chat_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _get_chat_or_404(db=db, user_id=current_user.id, chat_id=chat_id)
    messages = db.execute(
        select(Message)
        .where(Message.chat_id == chat_id)
        .order_by(Message.created_at)
    ).scalars().all()
    return [_message_to_dict(item) for item in messages]


@app.post("/api/chats/{chat_id}/messages", response_model=MessageResponse)
def send_message(
    chat_id: str,
    request: SendMessageRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    chat = _get_chat_or_404(db=db, user_id=current_user.id, chat_id=chat_id)
    content = request.content.strip()
    if not content:
        raise HTTPException(status_code=400, detail="Message content is empty")

    user_message = Message(
        chat_id=chat.id,
        role="user",
        mode=request.mode,
        content=content,
        extra_data={},
    )
    db.add(user_message)
    _touch_chat(chat)
    db.flush()

    api_config = config.get("generation_api", {})
    history_window = int(api_config.get("history_window", 12))
    history = _get_history_messages(db=db, chat_id=chat.id, window=history_window)
    previous_matches = _get_last_matches(db=db, chat_id=chat.id) if request.mode == "continue" else []

    try:
        result = pipeline.ask(
            question=content,
            mode=request.mode,
            history=history,
            previous_matches=previous_matches,
            source=request.source,
            top_k=request.top_k,
            score_threshold=request.score_threshold,
        )
    except UpstreamServiceError as error:
        raise HTTPException(
            status_code=502,
            detail=f"Upstream `{error.service}` error: {error}",
        ) from error
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Generation pipeline error: {error}") from error

    assistant_metadata = _build_assistant_metadata(result, mode=request.mode, top_k=request.top_k)
    assistant_message = Message(
        chat_id=chat.id,
        role="assistant",
        mode=request.mode,
        content=result.get("answer", ""),
        extra_data=assistant_metadata,
    )
    db.add(assistant_message)

    if chat.title == "Новый чат":
        chat.title = content[:120]
    _touch_chat(chat)

    db.commit()
    db.refresh(assistant_message)
    return _message_to_dict(assistant_message)


@app.post("/api/chats/{chat_id}/messages/stream")
def send_message_stream(
    chat_id: str,
    request: SendMessageRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    chat = _get_chat_or_404(db=db, user_id=current_user.id, chat_id=chat_id)
    content = request.content.strip()
    if not content:
        raise HTTPException(status_code=400, detail="Message content is empty")

    user_message = Message(
        chat_id=chat.id,
        role="user",
        mode=request.mode,
        content=content,
        extra_data={},
    )
    db.add(user_message)
    _touch_chat(chat)
    db.commit()
    db.refresh(user_message)

    api_config = config.get("generation_api", {})
    history_window = int(api_config.get("history_window", 12))
    history = _get_history_messages(db=db, chat_id=chat.id, window=history_window)
    previous_matches = _get_last_matches(db=db, chat_id=chat.id) if request.mode == "continue" else []

    def stream_events():
        yield _sse("ack", {"message": _message_to_dict(user_message)})
        try:
            for event in pipeline.ask_stream(
                question=content,
                mode=request.mode,
                history=history,
                previous_matches=previous_matches,
                source=request.source,
                top_k=request.top_k,
                score_threshold=request.score_threshold,
            ):
                event_type = event.get("type")
                if event_type == "delta":
                    delta_text = str(event.get("text") or "")
                    if delta_text:
                        yield _sse("delta", {"text": delta_text})
                    continue

                if event_type != "final":
                    continue

                result = dict(event.get("result") or {})
                assistant_metadata = _build_assistant_metadata(
                    result=result,
                    mode=request.mode,
                    top_k=request.top_k,
                )
                assistant_message = Message(
                    chat_id=chat.id,
                    role="assistant",
                    mode=request.mode,
                    content=result.get("answer", ""),
                    extra_data=assistant_metadata,
                )
                db.add(assistant_message)
                if chat.title == "Новый чат":
                    chat.title = content[:120]
                _touch_chat(chat)
                db.commit()
                db.refresh(assistant_message)
                yield _sse("done", {"message": _message_to_dict(assistant_message)})
        except UpstreamServiceError as error:
            db.rollback()
            yield _sse("error", {"detail": f"Upstream `{error.service}` error: {error}"})
        except Exception as error:
            db.rollback()
            yield _sse("error", {"detail": f"Generation pipeline error: {error}"})

    return StreamingResponse(
        stream_events(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    api_config = config.get("generation_api", {})
    host = str(api_config.get("host", "0.0.0.0"))
    port = int(api_config.get("port", 8010))
    uvicorn.run("app:app", host=host, port=port, reload=False)
