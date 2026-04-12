from typing import Any

import httpx


class EmbeddingClient:
    def __init__(self, config: dict[str, Any]):
        embedding_config = config.get("embedding", {})
        self.provider = str(embedding_config.get("provider", "openai_compatible")).lower()
        self.base_url = str(embedding_config.get("base_url", "http://localhost:8000/v1")).rstrip("/")
        self.api_key = embedding_config.get("api_key")
        self.model = embedding_config.get("model", "Qwen/Qwen3-Embedding-8B")
        self.timeout_seconds = float(embedding_config.get("timeout_seconds", 30))

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def embed(self, text: str) -> list[float]:
        if not text or not text.strip():
            raise ValueError("text for embedding is empty")

        if self.provider == "ollama":
            return self._embed_ollama(text)
        return self._embed_openai_compatible(text)

    def _embed_openai_compatible(self, text: str) -> list[float]:
        url = f"{self.base_url}/embeddings"
        body = {"model": self.model, "input": text}

        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.post(url, headers=self._headers(), json=body)
            response.raise_for_status()
            payload = response.json()

        data = payload.get("data") or []
        if not data or "embedding" not in data[0]:
            raise RuntimeError(f"Invalid embedding response: {payload}")

        embedding = data[0]["embedding"]
        if not isinstance(embedding, list) or not embedding:
            raise RuntimeError("Embedding vector is empty")
        return [float(value) for value in embedding]

    def _embed_ollama(self, text: str) -> list[float]:
        url = f"{self.base_url}/api/embeddings"
        body = {"model": self.model, "prompt": text}

        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.post(url, headers=self._headers(), json=body)
            response.raise_for_status()
            payload = response.json()

        embedding = payload.get("embedding")
        if not isinstance(embedding, list) or not embedding:
            raise RuntimeError(f"Invalid embedding response: {payload}")
        return [float(value) for value in embedding]


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

    def _generate_openai_compatible(self, system_prompt: str, user_prompt: str) -> str:
        url = f"{self.base_url}/chat/completions"
        body = {
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }

        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.post(url, headers=self._headers(), json=body)
            response.raise_for_status()
            payload = response.json()

        choices = payload.get("choices") or []
        if not choices:
            raise RuntimeError(f"Invalid chat response: {payload}")

        message = choices[0].get("message") or {}
        content = message.get("content")
        if not content:
            raise RuntimeError(f"Empty chat response: {payload}")
        return str(content).strip()

    def _generate_ollama(self, system_prompt: str, user_prompt: str) -> str:
        url = f"{self.base_url}/api/generate"
        prompt = f"{system_prompt}\n\n{user_prompt}"
        body = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": self.temperature,
                "num_predict": self.max_tokens,
            },
        }

        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.post(url, headers=self._headers(), json=body)
            response.raise_for_status()
            payload = response.json()

        text = payload.get("response")
        if not text:
            raise RuntimeError(f"Invalid ollama response: {payload}")
        return str(text).strip()
