"""Clone → detect → Dockerfile → docker build/run for Railway-style deployments."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from docker_cli import check_docker_cli, resolve_docker_executable
from http_client import NodeHttpClient


def _internal_base(backend_url: str, api_prefix: str) -> str:
    b = backend_url.rstrip("/")
    p = (api_prefix or "").strip()
    if p and not p.startswith("/"):
        p = f"/{p}"
    return f"{b}{p}/internal"


def _post(
    http: NodeHttpClient,
    cfg: Any,
    path: str,
    body: dict[str, Any],
    *,
    timeout: float = 60.0,
) -> None:
    url = f"{_internal_base(cfg.backend_url, cfg.api_prefix)}{path}"
    code, _ = http.post_json(url, body, bearer=cfg.token, timeout=timeout)
    if code != 200:
        logging.getLogger("nodehost-agent").warning("deploy POST %s -> %s", path, code)


def _append_log(http: NodeHttpClient, cfg: Any, deployment_id: str, chunk: str, phase: str = "build") -> None:
    if not chunk:
        return
    _post(
        http,
        cfg,
        f"/deployments/{deployment_id}/logs",
        {"phase": phase, "chunk": chunk},
        timeout=30.0,
    )


def _step(
    http: NodeHttpClient,
    cfg: Any,
    deployment_id: str,
    status: str,
    *,
    step: str | None = None,
    step_status: str | None = None,
    error_message: str | None = None,
    commit_sha: str | None = None,
) -> None:
    body: dict[str, Any] = {"status": status}
    if step:
        body["step"] = step
    if step_status:
        body["step_status"] = step_status
    if error_message:
        body["error_message"] = error_message
    if commit_sha:
        body["commit_sha"] = commit_sha
    _post(http, cfg, f"/deployments/{deployment_id}/step", body, timeout=30.0)


def _complete(
    http: NodeHttpClient,
    cfg: Any,
    deployment_id: str,
    *,
    status: str,
    container_id: str | None,
    image_tag: str | None,
    commit_sha: str | None,
    error_message: str | None,
) -> None:
    log = logging.getLogger("nodehost-agent")
    body = {
        "status": status,
        "container_id": container_id,
        "image_tag": image_tag,
        "commit_sha": commit_sha,
        "error_message": error_message,
    }
    url = f"{_internal_base(cfg.backend_url, cfg.api_prefix)}/deployments/{deployment_id}/complete"
    last_code = 0
    for attempt in range(4):
        code, _ = http.post_json(url, body, bearer=cfg.token, timeout=60.0)
        last_code = code
        if code == 200:
            log.info("Deployment %s complete posted (%s)", deployment_id, status)
            return
        log.warning("complete POST failed (attempt %s) -> %s", attempt + 1, code)
        time.sleep(0.4 * (attempt + 1))
    log.error("complete POST exhausted retries (last=%s) for deployment %s", last_code, deployment_id)


_VITE_CONFIG_NAMES = frozenset(
    {
        "vite.config.ts",
        "vite.config.js",
        "vite.config.mjs",
        "vite.config.mts",
        "vite.config.cjs",
    }
)


def _package_json_has_vite(root: Path) -> bool:
    pkg = root / "package.json"
    if not pkg.is_file():
        return False
    try:
        data = json.loads(pkg.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    dep = {**(data.get("dependencies") or {}), **(data.get("devDependencies") or {})}
    return "vite" in dep


def _detect_local(root: Path) -> tuple[str, bool]:
    files = {p.name.lower() for p in root.iterdir() if p.is_file()}
    if "dockerfile" in files:
        return "docker", True
    has_vite_cfg = bool(_VITE_CONFIG_NAMES & files)
    if "package.json" in files and (has_vite_cfg or _package_json_has_vite(root)):
        return "vite", False
    if "package.json" in files:
        return "node", False
    if "requirements.txt" in files or "pyproject.toml" in files:
        return "python", False
    if "index.html" in files:
        return "static", False
    return "node", False


def _write_dockerfile(pt: str, root: Path, internal_port: int) -> None:
    if pt == "vite":
        body = """FROM node:18-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build
FROM nginx:1.25-alpine
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
"""
    elif pt == "node":
        body = f"""FROM node:18-bookworm-slim
WORKDIR /app
ENV NODE_ENV=production
ENV PORT={internal_port}
COPY . .
RUN npm install
RUN npm run build || true
EXPOSE {internal_port}
CMD ["sh", "-c", "npm start"]
"""
    elif pt == "python":
        body = f"""FROM python:3.11-slim
WORKDIR /app
ENV PORT={internal_port}
COPY . .
RUN pip install --no-cache-dir -r requirements.txt || pip install --no-cache-dir .
EXPOSE {internal_port}
CMD ["sh", "-c", "python app.py"]
"""
    elif pt == "static":
        body = """FROM nginx:1.25-alpine
COPY . /usr/share/nginx/html
EXPOSE 80
"""
    else:
        body = f"""FROM alpine:3.19
WORKDIR /app
COPY . .
EXPOSE {internal_port}
CMD ["echo", "ok"]
"""
    (root / "Dockerfile").write_text(body, encoding="utf-8")


def _log_docker_invocation(logger: logging.Logger, cmd: list[str]) -> None:
    logger.info("Executing Docker command: %s", " ".join(cmd))


def _run_stream(
    cmd: list[str],
    cwd: Path,
    on_line: Any,
) -> int:
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        on_line(line)
    proc.wait()
    return int(proc.returncode or 0)


def _require_docker(logger: logging.Logger) -> str:
    ok, err = check_docker_cli()
    if not ok:
        raise RuntimeError(err or "Docker is not installed or not accessible on this node")
    exe = resolve_docker_executable()
    if not exe:
        raise RuntimeError("Docker is not installed or not accessible on this node")
    return exe


def run_deploy_job(logger: logging.Logger, http: NodeHttpClient, cfg: Any, job: dict[str, Any]) -> None:
    deployment_id = str(job.get("deployment_id") or "")
    if not deployment_id:
        logger.error("deploy job payload missing deployment_id")
        return
    clone_url = str(job.get("clone_url") or "")
    branch = str(job.get("branch") or "main")
    host_port = int(job.get("host_port") or 8080)
    internal_port = int(job.get("internal_port") or host_port)
    pt = str(job.get("project_type") or "node")
    image_name = str(job.get("image_name") or f"nodehost-{deployment_id[:12]}")
    env_vars = job.get("env") if isinstance(job.get("env"), dict) else {}

    base = Path.home() / ".nodehost" / "deployments" / deployment_id
    if base.exists():
        shutil.rmtree(base, ignore_errors=True)
    base.parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("Deployment pipeline started for %s", deployment_id)
        docker_exe = _require_docker(logger)

        _step(http, cfg, deployment_id, "cloning", step="clone", step_status="running")
        _append_log(http, cfg, deployment_id, f"Cloning {branch} …\n")
        rc = _run_stream(
            ["git", "clone", "--depth", "1", "--branch", branch, clone_url, str(base)],
            cwd=base.parent,
            on_line=lambda ln: _append_log(http, cfg, deployment_id, ln),
        )
        if rc != 0:
            raise RuntimeError(f"git clone failed with exit {rc}")
        _step(http, cfg, deployment_id, "building", step="clone", step_status="done")

        sha = ""
        try:
            out = subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=str(base),
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
            sha = out[:40]
        except subprocess.CalledProcessError:
            pass

        gen_raw = job.get("generated_dockerfile")
        df_name = str(job.get("dockerfile_path") or "generated.Dockerfile").strip() or "generated.Dockerfile"
        uses_repo = bool(job.get("uses_repo_dockerfile"))
        fw = str(job.get("framework") or pt or "node")

        used_control_plane_dockerfile = bool(
            gen_raw and str(gen_raw).strip() and not uses_repo,
        )
        if used_control_plane_dockerfile:
            (base / df_name).write_text(str(gen_raw), encoding="utf-8")
            _append_log(
                http,
                cfg,
                deployment_id,
                f"Detected framework: {fw}\nUsing generated Dockerfile ({df_name})\nDocker build started…\n",
            )

        pt_local, has_df = _detect_local(base)
        use_type = pt_local
        if not used_control_plane_dockerfile:
            if has_df or uses_repo:
                _append_log(http, cfg, deployment_id, "Using Dockerfile from repository.\n")
            if not has_df:
                gen_pt = "node" if use_type == "docker" else use_type
                _write_dockerfile(gen_pt, base, internal_port if gen_pt not in ("static", "vite") else 80)
                _append_log(http, cfg, deployment_id, "Generated Dockerfile (local fallback).\n")

        _step(http, cfg, deployment_id, "building", step="dockerize", step_status="running")
        if used_control_plane_dockerfile:
            run_internal = internal_port
        elif use_type in ("static", "vite"):
            run_internal = 80
        else:
            run_internal = internal_port
        tag = f"{re.sub(r'[^a-z0-9._-]', '-', image_name.lower())}:latest"
        if used_control_plane_dockerfile:
            build_cmd = [docker_exe, "build", "-f", df_name, "-t", tag, "."]
            _append_log(http, cfg, deployment_id, f"docker build -f {df_name} -t {tag} …\n")
        else:
            build_cmd = [docker_exe, "build", "-t", tag, "."]
            _append_log(http, cfg, deployment_id, f"docker build -t {tag} …\n")
        _log_docker_invocation(logger, build_cmd)

        rc = -1
        last_err = ""
        for attempt in range(2):
            rc = _run_stream(
                build_cmd,
                cwd=base,
                on_line=lambda ln: _append_log(http, cfg, deployment_id, ln),
            )
            if rc == 0:
                break
            last_err = f"docker build failed ({rc})"
            if attempt == 0:
                _append_log(http, cfg, deployment_id, f"\nRetrying docker build (attempt 2)…\n")
                time.sleep(2)
        if rc != 0:
            raise RuntimeError(last_err or f"docker build failed ({rc})")
        _step(http, cfg, deployment_id, "deploying", step="build", step_status="done")

        name = f"nodehost-dep-{deployment_id[:12]}"
        rm_cmd = [docker_exe, "rm", "-f", name]
        _log_docker_invocation(logger, rm_cmd)
        subprocess.run(rm_cmd, capture_output=True, text=True)

        run_port = f"{host_port}:{run_internal}"
        _append_log(http, cfg, deployment_id, f"Publish {run_port} → container port {run_internal}\n")
        cmd = [
            docker_exe,
            "run",
            "-d",
            "--name",
            name,
            "-p",
            run_port,
        ]
        for k, v in env_vars.items():
            if k:
                cmd.extend(["-e", f"{k}={v}"])
        cmd.extend(["-e", f"PORT={run_internal}"])
        cmd.append(tag)

        _append_log(http, cfg, deployment_id, f"Starting container {name} ({run_port})…\n")
        _step(http, cfg, deployment_id, "deploying", step="run", step_status="running")
        _log_docker_invocation(logger, cmd)
        proc: subprocess.CompletedProcess[str] | None = None
        err = ""
        for attempt in range(2):
            proc = subprocess.run(cmd, cwd=str(base), capture_output=True, text=True)
            if proc.returncode == 0:
                break
            err = (proc.stderr or proc.stdout or "").strip()
            if attempt == 0:
                _append_log(http, cfg, deployment_id, f"\ndocker run failed; retrying once…\n{err}\n")
                time.sleep(2)
        assert proc is not None
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or "").strip()
            if "port is already allocated" in err.lower() or "bind" in err.lower():
                raise RuntimeError(f"port_conflict:{host_port} {err}")
            raise RuntimeError(err or "docker run failed")

        cid = (proc.stdout or "").strip()[:12]
        _step(http, cfg, deployment_id, "running", step="run", step_status="done", commit_sha=sha or None)
        _complete(
            http,
            cfg,
            deployment_id,
            status="running",
            container_id=cid,
            image_tag=tag,
            commit_sha=sha or None,
            error_message=None,
        )
        logger.info("Deployment %s running as %s", deployment_id, cid)
    except Exception as exc:  # noqa: BLE001
        logger.exception("deploy failed")
        msg = str(exc)
        _append_log(http, cfg, deployment_id, f"\nFAILED: {msg}\n", phase="build")
        try:
            _step(
                http,
                cfg,
                deployment_id,
                "failed",
                error_message=msg[:4000],
            )
        except Exception:
            logger.exception("failed to post failed status")
        _complete(
            http,
            cfg,
            deployment_id,
            status="failed",
            container_id=None,
            image_tag=None,
            commit_sha=None,
            error_message=msg[:4000],
        )


def poll_one_job(logger: logging.Logger, http: NodeHttpClient, cfg: Any) -> bool:
    url = f"{_internal_base(cfg.backend_url, cfg.api_prefix)}/deployments/jobs/next"
    code, body = http.get_json(url, bearer=cfg.token, timeout=45.0)
    if code != 200:
        return False
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict):
        return False
    job = data.get("job")
    if not isinstance(job, dict):
        return False
    try:
        run_deploy_job(logger, http, cfg, job)
    except Exception:
        logger.exception("run_deploy_job crashed (unexpected); backend stale sweep will recover stuck deployments")
    return True
