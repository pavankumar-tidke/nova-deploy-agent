from urllib.parse import urlparse


def normalize_ws_url(server_url: str) -> str:
    """Build ws(s) URL for the node agent from an http(s) or ws(s) base URL."""
    trimmed = server_url.strip().rstrip("/")
    parsed = urlparse(trimmed)

    if parsed.scheme in {"ws", "wss"}:
        return f"{trimmed}/ws/node"
    if parsed.scheme in {"http", "https"}:
        ws_scheme = "wss" if parsed.scheme == "https" else "ws"
        netloc = parsed.netloc
        path = parsed.path.rstrip("/")
        return f"{ws_scheme}://{netloc}{path}/ws/node"
    raise ValueError("server_url must start with http(s):// or ws(s)://")
