import json
import re
from collections.abc import Iterator
from typing import Any

import httpx


SYSTEM_PROMPT = """
Ты — интеллектуальный помощник BigData Navigator.
Твоя задача: по вопросу пользователя находить релевантные метаданные сущностей данных
(SQL, NoSQL, файловые и другие источники), объяснять данные и при возможности строить запросы.

Правила ответа:
1) Используй только предоставленный контекст и историю диалога.
2) Не придумывай таблицы, поля и связи, которых нет в контексте.
3) Если источник в контексте SQL и данных достаточно — предложи SQL.
4) Если источник не SQL — тоже сформируй query в нативном формате источника (например MongoDB filter/pipeline, Elasticsearch DSL, Cypher, Gremlin, KQL и т.д.) при достаточном контексте.
5) Если данных недостаточно — явно опиши, каких метаданных не хватает.
6) Отвечай на русском языке, кратко и по делу.
7) Для non-SQL источников в used_entities используй тот же формат database.table[.column], где table = контейнер/коллекция/индекс/топик.
8) Если пользователь спрашивает про подключение, опирайся только на connection_uri и vault_url из контекста.

Верни JSON без markdown:
{
  "answer": "человеко-понятный ответ",
  "query": "запрос на языке источника или null",
  "used_entities": ["database.table[.column]", "..."],
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
        database_name = payload.get("database_name") or "unknown_db"
        table_name = payload.get("table_name") or "unknown_table"
        column_name = payload.get("column_name")
        if column_name:
            return f"{database_name}.{table_name}.{column_name}"
        return f"{database_name}.{table_name}"

    @staticmethod
    def _normalize_entity_label(value: str) -> str:
        text = str(value).strip()
        if not text:
            return ""

        parts = [part.strip() for part in text.split(".") if part.strip()]
        if not parts:
            return ""

        source_prefixes = {
            "postgres",
            "postgresql",
            "clickhouse",
            "mysql",
            "mariadb",
            "oracle",
            "mssql",
            "mongodb",
            "cassandra",
            "redis",
            "kafka",
            "s3",
            "minio",
            "bigquery",
            "snowflake",
        }

        if len(parts) >= 4 and parts[0].lower() in source_prefixes:
            parts = parts[1:]
        elif len(parts) > 3:
            parts = parts[-3:]

        return ".".join(parts)

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
                f"source_kind={payload.get('source_kind')}; "
                f"source_dialect={payload.get('source_dialect')}; "
                f"source_name={payload.get('source_name')}; "
                f"data_type={payload.get('data_type')}; "
                f"not_null={payload.get('is_not_null')}; "
                f"table_comment={payload.get('table_comment')}; "
                f"column_comment={payload.get('column_comment')}; "
                f"connection_uri={payload.get('connection_uri')}; "
                f"vault_url={payload.get('vault_url')}; "
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
                "query": None,
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
            "query": None,
            "used_entities": [],
            "limitations": "Не удалось распарсить JSON-ответ LLM",
        }

    @staticmethod
    def _resolve_auto_mode(question: str, previous_matches: list[dict[str, Any]]) -> str:
        if not previous_matches:
            return "new_search"

        question_lc = question.lower()
        discovery_markers = [
            "найди",
            "поищи",
            "покажи таблиц",
            "какие таблицы",
            "какие колонки",
            "какие есть",
            "новый поиск",
            "новые данные",
            "метаданные",
            "schema",
            "find",
            "search",
        ]
        followup_markers = [
            "добавь",
            "измени",
            "перепиши",
            "уточни",
            "а если",
            "тогда",
            "еще",
            "ещё",
            "продолж",
            "объясни",
            "оптимизируй",
            "оптимизируй запрос",
            "дальше",
        ]

        if any(marker in question_lc for marker in discovery_markers):
            return "new_search"
        if any(marker in question_lc for marker in followup_markers):
            return "continue"
        if len(question_lc.split()) <= 6:
            return "continue"
        return "new_search"

    @classmethod
    def _resolve_mode(
        cls,
        mode: str,
        question: str,
        previous_matches: list[dict[str, Any]] | None,
    ) -> str:
        if mode != "auto":
            return mode
        return cls._resolve_auto_mode(question=question, previous_matches=previous_matches or [])

    @classmethod
    def _normalize_result(cls, result: dict[str, Any], fallback_answer: str) -> dict[str, Any]:
        answer = result.get("answer") or fallback_answer
        query = result.get("query")
        if query is None:
            query = result.get("sql_query")
        if query in {"", "null", "None"}:
            query = None

        used_entities = result.get("used_entities")
        if not isinstance(used_entities, list):
            used_entities = []

        normalized_used_entities: list[str] = []
        for item in used_entities:
            label = cls._normalize_entity_label(str(item))
            if label and label not in normalized_used_entities:
                normalized_used_entities.append(label)

        limitations = result.get("limitations")
        if limitations in {"", "null", "None"}:
            limitations = None

        return {
            "answer": str(answer).strip(),
            "query": str(query).strip() if isinstance(query, str) else None,
            "used_entities": normalized_used_entities,
            "limitations": str(limitations).strip() if isinstance(limitations, str) else limitations,
        }

    @staticmethod
    def _decode_json_escape(raw: str, escape_pos: int) -> tuple[str | None, int]:
        current = raw[escape_pos]
        mapping = {
            '"': '"',
            "\\": "\\",
            "/": "/",
            "b": "\b",
            "f": "\f",
            "n": "\n",
            "r": "\r",
            "t": "\t",
        }
        if current in mapping:
            return mapping[current], escape_pos + 1

        if current == "u":
            unicode_end = escape_pos + 5
            if unicode_end > len(raw):
                return None, escape_pos
            hex_value = raw[escape_pos + 1:unicode_end]
            if not re.fullmatch(r"[0-9a-fA-F]{4}", hex_value):
                return "u", escape_pos + 1
            return chr(int(hex_value, 16)), unicode_end

        return current, escape_pos + 1

    @classmethod
    def _extract_answer_prefix(cls, raw_text: str) -> str | None:
        answer_key_pos = raw_text.find('"answer"')
        if answer_key_pos < 0:
            return None

        colon_pos = raw_text.find(":", answer_key_pos + len('"answer"'))
        if colon_pos < 0:
            return None

        quote_pos = colon_pos + 1
        while quote_pos < len(raw_text) and raw_text[quote_pos].isspace():
            quote_pos += 1
        if quote_pos >= len(raw_text) or raw_text[quote_pos] != '"':
            return None
        quote_pos += 1

        chars: list[str] = []
        index = quote_pos
        while index < len(raw_text):
            symbol = raw_text[index]
            if symbol == '"':
                return "".join(chars)
            if symbol == "\\":
                index += 1
                if index >= len(raw_text):
                    break
                decoded, next_index = cls._decode_json_escape(raw_text, index)
                if decoded is None:
                    break
                chars.append(decoded)
                index = next_index
                continue
            chars.append(symbol)
            index += 1

        return "".join(chars)

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
        matches, search_performed, user_prompt, resolved_mode = self._prepare_prompt(
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
        result["resolved_mode"] = resolved_mode
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
    ) -> tuple[list[dict[str, Any]], bool, str, str]:
        matches = previous_matches or []
        resolved_mode = self._resolve_mode(mode=mode, question=question, previous_matches=matches)
        search_performed = False

        if resolved_mode == "new_search" or not matches:
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
            f"Режим пользователя: {mode}\n"
            f"Режим обработки: {resolved_mode}\n\n"
            f"История диалога:\n{history_text}\n\n"
            f"Вопрос пользователя:\n{question}\n\n"
            f"Контекст метаданных:\n{context}\n\n"
            "Сформируй ответ строго по правилам."
        )
        return matches, search_performed, user_prompt, resolved_mode

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
        matches, search_performed, user_prompt, resolved_mode = self._prepare_prompt(
            question=question,
            mode=mode,
            history=history,
            previous_matches=previous_matches,
            source=source,
            top_k=top_k,
            score_threshold=score_threshold,
        )

        llm_parts: list[str] = []
        streamed_answer = ""
        for delta in self.chat_client.generate_stream(SYSTEM_PROMPT, user_prompt):
            llm_parts.append(delta)
            partial_text = "".join(llm_parts)
            answer_prefix = self._extract_answer_prefix(partial_text)
            if answer_prefix is None or len(answer_prefix) <= len(streamed_answer):
                continue

            text_delta = answer_prefix[len(streamed_answer):]
            streamed_answer = answer_prefix
            if text_delta:
                yield {"type": "delta", "text": text_delta}

        llm_text = "".join(llm_parts).strip()
        raw_result = self._extract_json(llm_text)
        result = self._normalize_result(raw_result, fallback_answer=llm_text)
        result["matches"] = matches
        result["search_performed"] = search_performed
        result["resolved_mode"] = resolved_mode

        final_answer = str(result.get("answer") or "")
        if final_answer and final_answer != streamed_answer:
            if final_answer.startswith(streamed_answer):
                tail = final_answer[len(streamed_answer):]
            else:
                tail = final_answer
            if tail:
                yield {"type": "delta", "text": tail}

        yield {"type": "final", "result": result}
