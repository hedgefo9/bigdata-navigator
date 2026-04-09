from urllib.parse import urlparse, unquote


def parse_source(url: str) -> dict:
    raw_url = url.strip()
    if raw_url.startswith("jdbc:"):
        raw_url = raw_url[len("jdbc:"):]

    parsed = urlparse(raw_url)

    if not parsed.scheme or not parsed.hostname:
        raise ValueError(f"Invalid source URL: {url}")

    return {
        "scheme": parsed.scheme.lower(),
        "user": unquote(parsed.username) if parsed.username else None,
        "password": unquote(parsed.password) if parsed.password else None,
        "host": parsed.hostname,
        "port": parsed.port,
        "database": parsed.path.lstrip("/") or None,
    }
