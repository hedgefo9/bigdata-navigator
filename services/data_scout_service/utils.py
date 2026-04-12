from urllib.parse import quote, unquote, urlparse, urlunparse


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


def build_connection_uri(
    scheme: str,
    host: str | None,
    port: int | None,
    database: str | None,
    user: str | None = None,
    password: str | None = None,
) -> str:
    if not scheme:
        raise ValueError("scheme is required")
    if not host:
        raise ValueError("host is required")
    if not database:
        raise ValueError("database is required")

    credentials = ""
    if user:
        credentials = quote(user, safe="")
        if password:
            credentials += f":{quote(password, safe='')}"
        credentials += "@"

    host_port = host
    if port:
        host_port = f"{host}:{int(port)}"
    netloc = f"{credentials}{host_port}"

    return urlunparse((scheme, netloc, f"/{database}", "", "", ""))


def apply_password_to_uri(uri: str, password: str | None) -> str:
    if not password:
        return uri

    parsed = urlparse(uri)
    username = parsed.username
    if not username:
        return uri

    userinfo = f"{quote(username, safe='')}:{quote(password, safe='')}"
    host_part = parsed.hostname or ""
    if parsed.port:
        host_part += f":{parsed.port}"
    netloc = f"{userinfo}@{host_part}"
    return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
