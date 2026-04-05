import json
import os
import socket
from dataclasses import dataclass
from pathlib import Path


CONFIG_FILE_ENV = "NODEHOST_AGENT_CONFIG"
DEFAULT_HOME = Path.home() / ".nodehost"


@dataclass
class AgentConfig:
    node_id: str
    token: str
    backend_url: str
    api_prefix: str
    log_retention_hours: int
    # Optional UI label (datacenter / city). Override with NODEHOST_REGION env in main.
    region: str | None = None


class ConfigError(Exception):
    pass


def nodehost_home() -> Path:
    raw = os.getenv("NODEHOST_HOME", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return DEFAULT_HOME.expanduser().resolve()


def default_config_path() -> Path:
    return nodehost_home() / "config" / "config.json"


def get_config_path() -> Path:
    env_path = os.getenv(CONFIG_FILE_ENV, "").strip()
    if env_path:
        return Path(env_path).expanduser().resolve()
    return default_config_path()


def load_config(config_path: Path | None = None) -> AgentConfig:
    path = config_path or get_config_path()

    if not path.exists():
        raise ConfigError(f"Missing config file at {path}")

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Invalid config JSON at {path}") from exc

    if not isinstance(raw, dict):
        raise ConfigError("Config root must be a JSON object")

    backend_url = str(
        raw.get("backendUrl") or raw.get("server_url") or raw.get("serverUrl") or "",
    ).strip().rstrip("/")
    token = str(raw.get("token", "")).strip()
    node_id = str(raw.get("nodeId") or raw.get("node_id", "") or socket.gethostname()).strip()
    api_prefix = str(raw.get("apiPrefix", raw.get("api_prefix", ""))).strip()
    if api_prefix and not api_prefix.startswith("/"):
        api_prefix = f"/{api_prefix}"
    log_retention_hours = int(raw.get("logRetentionHours", raw.get("log_retention_hours", 5)))
    region_raw = raw.get("region")
    region = str(region_raw).strip() if region_raw is not None and str(region_raw).strip() else None

    if not node_id:
        raise ConfigError("nodeId is empty and hostname could not be resolved")
    if not token:
        raise ConfigError("token is required")
    if not backend_url:
        raise ConfigError("backendUrl is required")

    return AgentConfig(
        node_id=node_id,
        token=token,
        backend_url=backend_url,
        api_prefix=api_prefix,
        log_retention_hours=max(1, min(168, log_retention_hours)),
        region=region,
    )


def save_token(config_path: Path | None, new_token: str) -> Path:
    path = config_path or get_config_path()
    if not path.exists():
        raise ConfigError(f"Cannot update missing config at {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Invalid config JSON at {path}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("Config root must be a JSON object")
    raw["token"] = new_token.strip()
    path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return path
