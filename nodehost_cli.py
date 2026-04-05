#!/usr/bin/env python3
"""NodeHost CLI: start | stop | status | logs | config | uninstall"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path


def _home() -> Path:
    raw = os.environ.get("NODEHOST_HOME", "").strip()
    return Path(raw).expanduser() if raw else Path.home() / ".nodehost"


def _venv_python() -> Path:
    return _home() / "agent" / ".venv" / "bin" / "python"


def _main_py() -> Path:
    return _home() / "agent" / "main.py"


def _watchdog_main_py() -> Path:
    return _home() / "agent" / "watchdog" / "main.py"


def _config_path() -> Path:
    return _home() / "config" / "config.json"


def _log_path() -> Path:
    return _home() / "logs" / "agent.log"


def _global_cli_path() -> Path:
    return Path("/usr/local/bin/nodehost")


def _systemd_unit_path() -> Path:
    return Path("/etc/systemd/system/nodehost-agent.service")


def _plist_path() -> Path:
    return _home() / "bin" / "nodehost-agent.plist"


def _launchctl_domain() -> str:
    return f"gui/{os.getuid()}"


def _launchctl_label() -> str:
    return "nodehost.agent"


def _is_darwin() -> bool:
    return sys.platform == "darwin"


def _read_config_snippet() -> tuple[str | None, str | None]:
    """backend URL and node id from config, if present."""
    try:
        p = _config_path()
        if not p.exists():
            return None, None
        data = json.loads(p.read_text(encoding="utf-8"))
        url = data.get("backendUrl") or data.get("server_url") or data.get("serverUrl")
        nid = data.get("nodeId") or data.get("node_id")
        return (str(url).strip() if url else None, str(nid).strip() if nid else None)
    except (OSError, json.JSONDecodeError, TypeError):
        return None, None


def _term_width() -> int:
    try:
        return max(48, min(100, os.get_terminal_size().columns))
    except OSError:
        return 72


def _wrap_value(text: str, width: int) -> list[str]:
    """Wrap for display; prefer splitting at '/' so file paths stay readable."""
    if width < 12:
        width = 12
    if len(text) <= width:
        return [text]
    lines: list[str] = []
    rest = text
    while rest:
        if len(rest) <= width:
            lines.append(rest)
            break
        window = rest[: width + 1]
        slash = window.rfind("/")
        if slash >= width // 4:
            lines.append(rest[: slash + 1].rstrip())
            rest = rest[slash + 1 :]
        else:
            lines.append(rest[:width])
            rest = rest[width:]
    return lines


def _print_status_pretty(title: str, rows: list[tuple[str, str]]) -> None:
    """Boxed summary: label column + wrapped value column."""
    label_w = min(max(max(len(r[0]) for r in rows), 10), 16)
    tw = _term_width()
    inner = max(52, min(110, tw - 6))
    line = "─" * inner
    val_w = max(36, inner - label_w - 7)

    def emit_row(label: str, value: str) -> None:
        if not value:
            value = "—"
        chunks = _wrap_value(value, val_w) or ["—"]
        pad = " " * label_w
        first = f"  │ {label:<{label_w}} │ {chunks[0]}"
        pad_to = 2 + inner
        if len(first) < pad_to:
            first += " " * (pad_to - len(first))
        print(first + " │")
        for c in chunks[1:]:
            cont = f"  │ {pad} │ {c}"
            if len(cont) < pad_to:
                cont += " " * (pad_to - len(cont))
            print(cont + " │")

    print()
    print(f"  ╭{line}╮")
    title_pad = max(0, inner - len(title))
    print(f"  │ {title}{' ' * title_pad} │")
    print(f"  ├{line}┤")
    for lab, val in rows:
        emit_row(lab, val)
    print(f"  ╰{line}╯")
    print()


def _parse_launchctl_print(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in text.splitlines():
        t = line.strip()
        if re.match(r"^state\s*=", t):
            out["state"] = t.split("=", 1)[1].strip()
        elif re.match(r"^pid\s*=", t):
            out["pid"] = t.split("=", 1)[1].strip()
        elif re.match(r"^program\s*=", t) and "program" not in out:
            out["program"] = t.split("=", 1)[1].strip()
        elif re.match(r"^path\s*=", t) and ".plist" in t:
            out["plist_path"] = t.split("=", 1)[1].strip()
        elif re.match(r"^runs\s*=", t):
            out["runs"] = t.split("=", 1)[1].strip()
        elif re.match(r"^last exit code\s*=", t):
            out["last_exit"] = t.split("=", 1)[1].strip()
        elif re.match(r"^active count\s*=", t):
            out["active_count"] = t.split("=", 1)[1].strip()
        elif "NODEHOST_HOME" in t and "=>" in t:
            out["env_home"] = t.split("=>", 1)[1].strip()
        elif "NODEHOST_AGENT_CONFIG" in t and "=>" in t:
            out["env_config"] = t.split("=>", 1)[1].strip()
        elif re.match(r"^stdout path\s*=", t):
            out["stdout_log"] = t.split("=", 1)[1].strip()
        elif re.match(r"^stderr path\s*=", t):
            out["stderr_log"] = t.split("=", 1)[1].strip()
    return out


def _status_darwin(args: argparse.Namespace) -> int:
    target = f"{_launchctl_domain()}/{_launchctl_label()}"
    if args.plain:
        return subprocess.call(["launchctl", "print", target])

    proc = subprocess.run(
        ["launchctl", "print", target],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        print(proc.stderr.strip() or "Could not read launchd job (is the agent installed?)", file=sys.stderr)
        return proc.returncode

    parsed = _parse_launchctl_print(proc.stdout)
    backend, node_id = _read_config_snippet()
    state = parsed.get("state", "unknown")
    state_icon = "●" if state in ("running", "active") else "○"
    state_line = f"{state_icon} {state}"

    rows: list[tuple[str, str]] = [
        ("Scheduler", "launchd (LaunchAgent)"),
        ("Job label", _launchctl_label()),
        ("State", state_line),
    ]
    if parsed.get("pid"):
        rows.append(("PID", parsed["pid"]))
    if parsed.get("active_count") is not None:
        rows.append(("Active", parsed["active_count"]))
    if parsed.get("runs"):
        rows.append(("Runs", parsed["runs"]))
    if parsed.get("last_exit"):
        rows.append(("Last exit", parsed["last_exit"]))
    if parsed.get("program"):
        rows.append(("Program", parsed["program"]))
    if parsed.get("plist_path"):
        rows.append(("Plist", parsed["plist_path"]))
    if backend:
        rows.append(("Backend URL", backend))
    if node_id:
        rows.append(("Node ID", node_id))
    cfg = parsed.get("env_config") or str(_config_path())
    rows.append(("Config file", cfg))
    nh = parsed.get("env_home") or str(_home())
    rows.append(("NODEHOST_HOME", nh))
    if parsed.get("stdout_log"):
        rows.append(("stdout log", parsed["stdout_log"]))
    if parsed.get("stderr_log"):
        rows.append(("stderr log", parsed["stderr_log"]))
    rows.append(("Agent log", str(_log_path())))

    _print_status_pretty("NodeHost watchdog", rows)

    hint = "Tip: nodehost logs   ·   nodehost logs -f   ·   nodehost status --plain"
    print(f"  {hint}")
    print()
    return 0


def _status_linux(args: argparse.Namespace) -> int:
    unit = "nodehost-agent.service"
    if args.plain:
        return subprocess.call(["systemctl", "status", unit, "--no-pager"])

    show = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "-p",
            "ActiveState",
            "-p",
            "SubState",
            "-p",
            "MainPID",
            "-p",
            "ExecMainStatus",
            "-p",
            "ExecMainCode",
            "-p",
            "FragmentPath",
            "--no-pager",
        ],
        capture_output=True,
        text=True,
    )
    if show.returncode != 0:
        print(show.stderr.strip() or f"systemctl show {unit} failed (is the unit installed?)", file=sys.stderr)
        return show.returncode

    kv: dict[str, str] = {}
    for line in show.stdout.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            kv[k.strip()] = v.strip()

    active = kv.get("ActiveState", "?")
    sub = kv.get("SubState", "?")
    pid = kv.get("MainPID", "0")
    frag = kv.get("FragmentPath", "—")
    exit_status = kv.get("ExecMainStatus", "—")
    exit_code = kv.get("ExecMainCode", "—")

    running = active == "active" and sub in ("running", "active")
    state_line = f"{'●' if running else '○'} {active} ({sub})"
    backend, node_id = _read_config_snippet()

    rows: list[tuple[str, str]] = [
        ("Init", "systemd"),
        ("Unit", unit),
        ("State", state_line),
    ]
    if pid and pid != "0":
        rows.append(("Main PID", pid))
    rows.append(("Exit status", exit_status))
    rows.append(("Exit code", exit_code))
    if backend:
        rows.append(("Backend URL", backend))
    if node_id:
        rows.append(("Node ID", node_id))
    rows.append(("Unit file", frag))
    rows.append(("Config file", str(_config_path())))
    rows.append(("NODEHOST_HOME", str(_home())))
    rows.append(("Agent log", str(_log_path())))

    _print_status_pretty("NodeHost watchdog", rows)
    hint = "Tip: nodehost logs   ·   journalctl -u nodehost-agent -f   ·   nodehost status --plain"
    print(f"  {hint}")
    print()
    return 0


def cmd_start(args: argparse.Namespace) -> int:
    if args.foreground:
        py = _venv_python()
        main = _watchdog_main_py()
        if not py.exists() or not main.exists():
            print("NodeHost not installed. Run the installer first.", file=sys.stderr)
            return 1
        os.environ.setdefault("NODEHOST_HOME", str(_home()))
        os.execv(str(py), [str(py), str(main)])
        return 0
    if _is_darwin():
        plist = _plist_path()
        if not plist.exists():
            print(f"Missing LaunchAgent plist: {plist}", file=sys.stderr)
            return 1
        boot = subprocess.call(["launchctl", "bootstrap", _launchctl_domain(), str(plist)])
        if boot != 0:
            return subprocess.call(["launchctl", "kickstart", "-k", f"{_launchctl_domain()}/{_launchctl_label()}"])
        return 0
    return subprocess.call(["sudo", "systemctl", "start", "nodehost-agent.service"])


def cmd_stop(_: argparse.Namespace) -> int:
    if _is_darwin():
        plist = _plist_path()
        if plist.exists():
            return subprocess.call(["launchctl", "bootout", _launchctl_domain(), str(plist)])
        return subprocess.call(["launchctl", "bootout", _launchctl_domain(), _launchctl_label()])
    return subprocess.call(["sudo", "systemctl", "stop", "nodehost-agent.service"])


def cmd_status(args: argparse.Namespace) -> int:
    if _is_darwin():
        return _status_darwin(args)
    return _status_linux(args)


def cmd_logs(args: argparse.Namespace) -> int:
    log_dir = _home() / "logs"
    candidates = [
        log_dir / "watchdog.log",
        _log_path(),
        log_dir / "watchdog.err.log",
        log_dir / "watchdog.out.log",
        log_dir / "launchd.err.log",
        log_dir / "launchd.out.log",
    ]
    p = next((c for c in candidates if c.exists()), None)
    if p is None:
        print(
            "No log files yet. Expected one of:\n  "
            + "\n  ".join(str(c) for c in candidates),
            file=sys.stderr,
        )
        return 1
    tail_cmd = ["tail", "-n", str(max(10, args.lines))]
    if args.follow:
        tail_cmd.append("-f")
    tail_cmd.append(str(p))
    return subprocess.call(tail_cmd)


def cmd_config(_: argparse.Namespace) -> int:
    ed = os.environ.get("EDITOR", "vi")
    p = _config_path()
    if not p.exists():
        print(f"Missing {p}", file=sys.stderr)
        return 1
    return subprocess.call([ed, str(p)])


def cmd_uninstall(args: argparse.Namespace) -> int:
    """Stop watchdog, remove launchd/systemd service, global CLI; optionally delete ~/.nodehost."""
    if not _is_darwin() and not sys.platform.startswith("linux"):
        print("nodehost uninstall is only supported on macOS and Linux.", file=sys.stderr)
        return 1

    home = _home()
    if not args.yes:
        print("This will:")
        print("  • Stop the NodeHost watchdog (watchdog + agent processes)")
        print("  • Unregister the launchd / systemd service")
        print("  • Remove the global command:", _global_cli_path())
        if args.purge:
            print("  • Delete all local data under:", home)
        try:
            ans = input("Continue? [y/N]: ").strip().lower()
        except EOFError:
            ans = "n"
        if ans not in ("y", "yes"):
            print("Aborted.")
            return 1

    # 1) Stop and unregister service
    if _is_darwin():
        plist = _plist_path()
        if plist.exists():
            subprocess.run(
                ["launchctl", "bootout", _launchctl_domain(), str(plist)],
                capture_output=True,
            )
        else:
            subprocess.run(
                ["launchctl", "bootout", _launchctl_domain(), _launchctl_label()],
                capture_output=True,
            )
        pp = _plist_path()
        if pp.exists():
            try:
                pp.unlink()
            except OSError as exc:
                print(f"Note: could not remove {pp}: {exc}", file=sys.stderr)
    else:
        unit = _systemd_unit_path()
        subprocess.run(
            ["sudo", "systemctl", "disable", "--now", "nodehost-agent.service"],
            capture_output=True,
            text=True,
        )
        subprocess.run(["sudo", "rm", "-f", str(unit)], capture_output=True)
        subprocess.run(["sudo", "systemctl", "daemon-reload"], capture_output=True)

    # 2) Remove global nodehost wrapper
    cli = _global_cli_path()
    if cli.is_file():
        r = subprocess.run(["sudo", "rm", "-f", str(cli)], capture_output=True, text=True)
        if r.returncode != 0:
            print(f"Remove the CLI manually: sudo rm {cli}", file=sys.stderr)

    if args.purge:
        if home.exists():
            # Defer deletion so this Python process is not running from inside the tree.
            quoted = shlex.quote(h)
            subprocess.Popen(
                ["/bin/sh", "-c", f"sleep 1; rm -rf {quoted}"],
                start_new_session=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            print(f"Scheduled removal of {home} (completes in ~1s).")
        else:
            print(f"Data directory not found: {home}")
    else:
        print()
        print("Service and global CLI removed.")
        print(f"Local files are still at: {home}")
        print(f"To delete them: rm -rf {shlex.quote(str(home))}")
        print()

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="nodehost", description="NodeHost agent control")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_start = sub.add_parser("start", help="Start the watchdog service (or run in foreground)")
    p_start.add_argument("-f", "--foreground", action="store_true", help="Run watchdog in foreground (no launchd/systemd)")
    p_start.set_defaults(func=cmd_start)

    sub.add_parser("stop", help="Stop the agent service").set_defaults(func=cmd_stop)

    p_status = sub.add_parser("status", help="Show service status (pretty summary)")
    p_status.add_argument(
        "--plain",
        action="store_true",
        help="Raw launchctl/systemctl output (for debugging)",
    )
    p_status.set_defaults(func=cmd_status)

    p_logs = sub.add_parser("logs", help="View agent log file")
    p_logs.add_argument("-n", "--lines", type=int, default=200)
    p_logs.add_argument("-f", "--follow", action="store_true")
    p_logs.set_defaults(func=cmd_logs)

    sub.add_parser("config", help="Open config in $EDITOR").set_defaults(func=cmd_config)

    p_un = sub.add_parser(
        "uninstall",
        help="Remove NodeHost service, global CLI, and optionally all local data (for a clean reinstall)",
    )
    p_un.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Do not prompt for confirmation",
    )
    p_un.add_argument(
        "--purge",
        action="store_true",
        help=f"Also delete {_home()} (agent, venv, config, logs)",
    )
    p_un.set_defaults(func=cmd_uninstall)

    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
