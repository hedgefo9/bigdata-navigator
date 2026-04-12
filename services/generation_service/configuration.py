import os
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

import httpx
import yaml


def _resolve_vault_ref(value: str, vault_config: dict[str, Any], cache: dict[str, Any]) -> Any:
    if not value.startswith("vault://"):
        return value

    if value in cache:
        return cache[value]

    base_url = str(
        vault_config.get("url")
        or os.getenv(str(vault_config.get("url_env", "VAULT_ADDR")), "http://localhost:8200")
    ).rstrip("/")
    token = vault_config.get("token") or os.getenv(str(vault_config.get("token_env", "VAULT_TOKEN")))
    timeout_seconds = float(vault_config.get("timeout_seconds", 5))

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


def _resolve_recursive(node: Any, vault_config: dict[str, Any], cache: dict[str, Any]) -> Any:
    if isinstance(node, dict):
        return {key: _resolve_recursive(value, vault_config, cache) for key, value in node.items()}
    if isinstance(node, list):
        return [_resolve_recursive(item, vault_config, cache) for item in node]
    if isinstance(node, str):
        return _resolve_vault_ref(node, vault_config, cache)
    return node


def load_config() -> dict[str, Any]:
    config_path = Path(__file__).resolve().parent / "config" / "config.yaml"
    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    vault_config = dict(config.get("vault") or {})
    if not vault_config:
        return config

    resolved: dict[str, Any] = {"vault": vault_config}
    cache: dict[str, Any] = {}
    for key, value in config.items():
        if key == "vault":
            continue
        resolved[key] = _resolve_recursive(value, vault_config=vault_config, cache=cache)
    return resolved


def build_database_url(config: dict[str, Any]) -> str:
    database = config.get("database", {})
    username = quote(str(database["username"]), safe="")
    password = quote(str(database["password"]), safe="")
    host = database["host"]
    port = database["port"]
    db_name = database["database"]
    return f"postgresql://{username}:{password}@{host}:{port}/{db_name}"
