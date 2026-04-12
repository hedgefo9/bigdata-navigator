from pathlib import Path
from typing import Any

import yaml


def load_config() -> dict[str, Any]:
    config_path = Path(__file__).resolve().parent / "config" / "config.yaml"
    with config_path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def build_database_url(config: dict[str, Any]) -> str:
    database = config.get("database", {})
    username = database["username"]
    password = database["password"]
    host = database["host"]
    port = database["port"]
    db_name = database["database"]
    return f"postgresql://{username}:{password}@{host}:{port}/{db_name}"
