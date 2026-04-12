import json
import re
from collections.abc import Iterator
from typing import Any

import httpx


SYSTEM_PROMPT = """
Ты — интеллектуальный помощник BigData Navigator.
Твоя задача: по вопросу пользователя находить релевантные метаданные таблиц/колонок и помогать строить SQL.

Правила ответа:
1) Используй только предоставленный контекст и историю диалога.
2) Не придумывай таблицы, поля и связи, которых нет в контексте.
3) Если данных достаточно — предложи SQL (PostgreSQL-диалект по умолчанию).
4) Если данных недостаточно — явно опиши, каких метаданных не хватает.
5) Отвечай на русском языке, кратко и по делу.

Верни JSON без markdown:
{
  "answer": "человеко-понятный ответ",
  "sql_query": "SQL или null",
  "used_entities": ["source.database.table[.column]", "..."],
  "limitations": "ограничения/нехватка контекста или null"
}
"""


class UpstreamServiceError(RuntimeError):
    def __init__(self, service: str, message: str):
        super().__init__(message)
        self.service = service


class SearchServiceClient:
    def __init__(self, config: dict[str, Any]):
        search_config = config.get("search_service", {})
        self.base_url = str(search_config.get("base_url", "http://localhost:8001")).rstrip("/")
        self.timeout_seconds = float(search_config.get("timeout_seconds", 30))

    def search(
        self,
        query: str,
        top_k: int | None = None,
        score_threshold: float | None = None,
        source: str | None = None,
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {"query": query}
        if top_k is not None:
            payload["top_k"] = top_k
        if score_threshold is not None:
            payload["score_threshold"] = score_threshold
        if source:
            payload["source"] = source

        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.post(f"{self.base_url}/search", json=payload)
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPStatusError as error:
            raise UpstreamServiceError(
                service="search_service",
                message=(
                    f"HTTP {error.response.status_code} from {error.request.url}; "
                    f"body={error.response.text[:500]}"
                ),
            ) from error
        except httpx.HTTPError as error:
            raise UpstreamServiceError(
                service="search_service",
                message=f"Network error while calling {self.base_url}/search: {error}",
            ) from error

        return list(data.get("matches") or [])


class ChatClient:
    def __init__(self, config: dict[str, Any]):
        llm_config = config.get("llm", {})
        self.provider = str(llm_config.get("provider", "openai_compatible")).lower()
        self.base_url = str(llm_config.get("base_url", "http://localhost:8000/v1")).rstrip("/")
        self.api_key = llm_config.get("api_key")
        self.model = llm_config.get("model", "Qwen/Qwen2.5-14B-Instruct")
        self.temperature = float(llm_config.get("temperature", 0.1))
        self.max_tokens = int(llm_config.get("max_tokens", 900))
        self.timeout_seconds = float(llm_config.get("timeout_seconds", 90))

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def generate(self, system_prompt: str, user_prompt: str) -> str:
        if self.provider == "ollama":
            return self._generate_ollama(system_prompt=system_prompt, user_prompt=user_prompt)
        return self._generate_openai_compatible(system_prompt=system_prompt, user_prompt=user_prompt)

    def generate_stream(self, system_prompt: str, user_prompt: str) -> Iterator[str]:
        if self.provider == "ollama":
            yield from self._generate_stream_ollama(system_prompt=system_prompt, user_prompt=user_prompt)
            return
        yield from self._generate_stream_openai_compatible(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )

    def _generate_openai_compatible(self, system_prompt: str, user_prompt: str) -> str:
        payload = {
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.post(
                    f"{self.base_url}/chat/completions",
                    headers=self._headers(),
                    json=payload,
                )
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPStatusError as error:
            raise UpstreamServiceError(
                service="llm",
                message=(
                    f"HTTP {error.response.status_code} from {error.request.url}; "
                    f"body={error.response.text[:500]}"
                ),
            ) from error
        except httpx.HTTPError as error:
            raise UpstreamServiceError(
                service="llm",
                message=f"Network error while calling {self.base_url}/chat/completions: {error}",
            ) from error

        choices = data.get("choices") or []
        if not choices:
            raise RuntimeError(f"Invalid LLM response: {data}")

        content = (choices[0].get("message") or {}).get("content")
        if not content:
            raise RuntimeError(f"Empty LLM response: {data}")
        return str(content).strip()

    @staticmethod
    def _extract_stream_content(choice: dict[str, Any]) -> str:
        delta = dict(choice.get("delta") or {})
        content = delta.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            text_parts: list[str] = []
            for part in content:
                if isinstance(part, dict):
                    text_value = part.get("text")
                    if isinstance(text_value, str):
                        text_parts.append(text_value)
            return "".join(text_parts)
        return ""

    def _generate_stream_openai_compatible(self, system_prompt: str, user_prompt: str) -> Iterator[str]:
        payload = {
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "stream": True,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                with client.stream(
                    "POST",
                    f"{self.base_url}/chat/completions",
                    headers=self._headers(),
                    json=payload,
                ) as response:
                    response.raise_for_status()
                    for raw_line in response.iter_lines():
                        if not raw_line:
                            continue
                        line = raw_line.strip()
                        if not line.startswith("data:"):
                            continue
                        chunk_raw = line[5:].strip()
                        if chunk_raw == "[DONE]":
                            break
                        try:
                            chunk = json.loads(chunk_raw)
                        except json.JSONDecodeError:
                            continue
                        choices = chunk.get("choices") or []
                        if not choices:
                            continue
                        text_delta = self._extract_stream_content(dict(choices[0]))
                        if text_delta:
                            yield text_delta
        except httpx.HTTPStatusError as error:
            raise UpstreamServiceError(
                service="llm",
                message=(
                    f"HTTP {error.response.status_code} from {error.request.url}; "
                    f"body={error.response.text[:500]}"
                ),
            ) from error
        except httpx.HTTPError as error:
            raise UpstreamServiceError(
                service="llm",
                message=f"Network error while calling {self.base_url}/chat/completions: {error}",
            ) from error

    def _generate_ollama(self, system_prompt: str, user_prompt: str) -> str:
        payload = {
            "model": self.model,
            "prompt": f"{system_prompt}\n\n{user_prompt}",
            "stream": False,
            "options": {
                "temperature": self.temperature,
                "num_predict": self.max_tokens,
            },
        }
        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.post(
                    f"{self.base_url}/api/generate",
                    headers=self._headers(),
                    json=payload,
                )
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPStatusError as error:
            raise UpstreamServiceError(
                service="llm",
                message=(
                    f"HTTP {error.response.status_code} from {error.request.url}; "
                    f"body={error.response.text[:500]}"
                ),
            ) from error
        except httpx.HTTPError as error:
            raise UpstreamServiceError(
                service="llm",
                message=f"Network error while calling {self.base_url}/api/generate: {error}",
            ) from error

        content = data.get("response")
        if not content:
            raise RuntimeError(f"Empty LLM response: {data}")
        return str(content).strip()

    def _generate_stream_ollama(self, system_prompt: str, user_prompt: str) -> Iterator[str]:
        payload = {
            "model": self.model,
            "prompt": f"{system_prompt}\n\n{user_prompt}",
            "stream": True,
            "options": {
                "temperature": self.temperature,
                "num_predict": self.max_tokens,
            },
        }
        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                with client.stream(
                    "POST",
                    f"{self.base_url}/api/generate",
                    headers=self._headers(),
                    json=payload,
                ) as response:
                    response.raise_for_status()
                    for raw_line in response.iter_lines():
                        if not raw_line:
                            continue
                        try:
                            chunk = json.loads(raw_line)
                        except json.JSONDecodeError:
                            continue
                        text_delta = chunk.get("response")
                        if isinstance(text_delta, str) and text_delta:
                            yield text_delta
                        if chunk.get("done"):
                            break
        except httpx.HTTPStatusError as error:
            raise UpstreamServiceError(
                service="llm",
                message=(
                    f"HTTP {error.response.status_code} from {error.request.url}; "
                    f"body={error.response.text[:500]}"
                ),
            ) from error
        except httpx.HTTPError as error:
            raise UpstreamServiceError(
                service="llm",
                message=f"Network error while calling {self.base_url}/api/generate: {error}",
            ) from error


class GenerationPipeline:
    def __init__(self, config: dict[str, Any]):
        self.search_client = SearchServiceClient(config=config)
        self.chat_client = ChatClient(config=config)

    @staticmethod
    def _entity_label(payload: dict[str, Any]) -> str:
        source = payload.get("source") or "unknown_source"
        database_name = payload.get("database_name") or "unknown_db"
        table_name = payload.get("table_name") or "unknown_table"
        column_name = payload.get("column_name")
        if column_name:
            return f"{source}.{database_name}.{table_name}.{column_name}"
        return f"{source}.{database_name}.{table_name}"

    def _build_context(self, matches: list[dict[str, Any]]) -> str:
        if not matches:
            return "Контекст не найден."

        rows: list[str] = []
        for index, match in enumerate(matches, start=1):
            payload = dict(match.get("payload") or {})
            rows.append(
                f"[{index}] score={match.get('score', 0):.4f}; "
                f"entity={self._entity_label(payload)}; "
                f"entity_type={payload.get('entity_type')}; "
                f"data_type={payload.get('data_type')}; "
                f"not_null={payload.get('is_not_null')}; "
                f"table_comment={payload.get('table_comment')}; "
                f"column_comment={payload.get('column_comment')}; "
                f"rag_text={payload.get('rag_text') or payload.get('vector_text')}"
            )
        return "\n".join(rows)

    @staticmethod
    def _build_history(history: list[dict[str, str]]) -> str:
        if not history:
            return "История диалога отсутствует."

        rows: list[str] = []
        for item in history:
            role = item.get("role", "unknown")
            content = item.get("content", "")
            rows.append(f"{role}: {content}")
        return "\n".join(rows)

    @staticmethod
    def _try_parse_json(raw_text: str) -> dict[str, Any] | None:
        candidate = raw_text.strip()
        if not candidate:
            return None

        try:
            loaded = json.loads(candidate)
            if isinstance(loaded, dict):
                return loaded
            if isinstance(loaded, str):
                nested = loaded.strip()
                if nested.startswith("{") and nested.endswith("}"):
                    nested_loaded = json.loads(nested)
                    if isinstance(nested_loaded, dict):
                        return nested_loaded
        except json.JSONDecodeError:
            pass

        if '\\"' in candidate:
            try:
                loaded = json.loads(candidate.replace('\\"', '"'))
                if isinstance(loaded, dict):
                    return loaded
            except json.JSONDecodeError:
                pass
        return None

    @classmethod
    def _extract_json(cls, text: str) -> dict[str, Any]:
        raw = text.strip()
        if not raw:
            return {
                "answer": "",
                "sql_query": None,
                "used_entities": [],
                "limitations": "LLM вернула пустой ответ",
            }

        parsed = cls._try_parse_json(raw)
        if parsed is not None:
            return parsed

        fenced_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw, re.IGNORECASE)
        if fenced_match:
            parsed = cls._try_parse_json(fenced_match.group(1))
            if parsed is not None:
                return parsed

        brace_match = re.search(r"\{[\s\S]*\}", raw)
        if brace_match:
            parsed = cls._try_parse_json(brace_match.group(0))
            if parsed is not None:
                return parsed

        return {
            "answer": raw,
            "sql_query": None,
            "used_entities": [],
            "limitations": "Не удалось распарсить JSON-ответ LLM",
        }

    @staticmethod
    def _normalize_result(result: dict[str, Any], fallback_answer: str) -> dict[str, Any]:
        answer = result.get("answer") or fallback_answer
        sql_query = result.get("sql_query")
        if sql_query in {"", "null", "None"}:
            sql_query = None

        used_entities = result.get("used_entities")
        if not isinstance(used_entities, list):
            used_entities = []

        limitations = result.get("limitations")
        if limitations in {"", "null", "None"}:
            limitations = None

        return {
            "answer": str(answer).strip(),
            "sql_query": str(sql_query).strip() if isinstance(sql_query, str) else None,
            "used_entities": [str(item) for item in used_entities],
            "limitations": str(limitations).strip() if isinstance(limitations, str) else limitations,
        }

    def ask(
        self,
        question: str,
        mode: str,
        history: list[dict[str, str]],
        previous_matches: list[dict[str, Any]] | None = None,
        source: str | None = None,
        top_k: int | None = None,
        score_threshold: float | None = None,
    ) -> dict[str, Any]:
        matches, search_performed, user_prompt = self._prepare_prompt(
            question=question,
            mode=mode,
            history=history,
            previous_matches=previous_matches,
            source=source,
            top_k=top_k,
            score_threshold=score_threshold,
        )
        llm_text = self.chat_client.generate(SYSTEM_PROMPT, user_prompt)
        raw_result = self._extract_json(llm_text)
        result = self._normalize_result(raw_result, fallback_answer=llm_text)
        result["matches"] = matches
        result["search_performed"] = search_performed
        return result

    def _prepare_prompt(
        self,
        question: str,
        mode: str,
        history: list[dict[str, str]],
        previous_matches: list[dict[str, Any]] | None = None,
        source: str | None = None,
        top_k: int | None = None,
        score_threshold: float | None = None,
    ) -> tuple[list[dict[str, Any]], bool, str]:
        matches = previous_matches or []
        search_performed = False

        if mode == "new_search" or not matches:
            matches = self.search_client.search(
                query=question,
                top_k=top_k,
                score_threshold=score_threshold,
                source=source,
            )
            search_performed = True

        context = self._build_context(matches)
        history_text = self._build_history(history)
        user_prompt = (
            f"Режим: {mode}\n\n"
            f"История диалога:\n{history_text}\n\n"
            f"Вопрос пользователя:\n{question}\n\n"
            f"Контекст метаданных:\n{context}\n\n"
            "Сформируй ответ строго по правилам."
        )
        return matches, search_performed, user_prompt

    def ask_stream(
        self,
        question: str,
        mode: str,
        history: list[dict[str, str]],
        previous_matches: list[dict[str, Any]] | None = None,
        source: str | None = None,
        top_k: int | None = None,
        score_threshold: float | None = None,
    ) -> Iterator[dict[str, Any]]:
        matches, search_performed, user_prompt = self._prepare_prompt(
            question=question,
            mode=mode,
            history=history,
            previous_matches=previous_matches,
            source=source,
            top_k=top_k,
            score_threshold=score_threshold,
        )
        llm_parts: list[str] = []
        for delta in self.chat_client.generate_stream(SYSTEM_PROMPT, user_prompt):
            llm_parts.append(delta)
            yield {"type": "delta", "text": delta}

        llm_text = "".join(llm_parts).strip()
        raw_result = self._extract_json(llm_text)
        result = self._normalize_result(raw_result, fallback_answer=llm_text)
        result["matches"] = matches
        result["search_performed"] = search_performed
        yield {"type": "final", "result": result}
