import asyncio
import os
import socket
import sys
import time
from datetime import UTC, datetime
from typing import Any

import psutil

from version import AGENT_VERSION


def _primary_ipv4() -> str | None:
    """Best non-loopback IPv4 for display (same host may have many addresses)."""
    candidates: list[tuple[int, str, str]] = []
    try:
        for name, addrs in psutil.net_if_addrs().items():
            nl = name.lower()
            if nl.startswith("lo") or nl == "loopback pseudo-interface 1":
                continue
            for addr in addrs:
                if addr.family != socket.AF_INET:
                    continue
                ip = (addr.address or "").strip()
                if not ip or ip.startswith("127."):
                    continue
                pri = 5
                if nl.startswith("en") or nl.startswith("eth"):
                    pri = 0
                elif nl.startswith("wl") or nl.startswith("wifi"):
                    pri = 1
                elif nl.startswith("enx") or "ethernet" in nl:
                    pri = 2
                candidates.append((pri, name, ip))
    except (OSError, AttributeError):
        return None
    if not candidates:
        return None
    candidates.sort(key=lambda t: (t[0], t[1]))
    return candidates[0][2]


def _disk_entries() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for part in psutil.disk_partitions(all=False):
        if part.fstype in ("squashfs", "tmpfs", "devtmpfs", "overlay"):
            continue
        try:
            du = psutil.disk_usage(part.mountpoint)
        except (PermissionError, OSError):
            continue
        out.append(
            {
                "total": round(du.total / (1024 * 1024), 1),
                "used": round(du.used / (1024 * 1024), 1),
                "free": round(du.free / (1024 * 1024), 1),
                "usagePercent": round(du.percent, 1),
                "mountPath": part.mountpoint,
                "fstype": part.fstype,
            }
        )
    return out


def _iface_entries() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    counters = psutil.net_io_counters(pernic=True)
    if not counters:
        c = psutil.net_io_counters()
        counters = {"total": c}
    for name, c in counters.items():
        out.append(
            {
                "name": name,
                "rxBytes": int(c.bytes_recv),
                "txBytes": int(c.bytes_sent),
                "rxPackets": int(c.packets_recv),
                "txPackets": int(c.packets_sent),
                "rxErrors": int(c.errin),
                "txErrors": int(c.errout),
            }
        )
    return out


class SystemMonitor:
    def __init__(self, region: str | None = None) -> None:
        self._region = (region or "").strip() or None

    def collect(self) -> dict[str, Any]:
        # One blocking sample for meaningful per-core and overall CPU %.
        per_raw = psutil.cpu_percent(interval=0.1, percpu=True)
        if isinstance(per_raw, list):
            per_core = [round(float(x), 1) for x in per_raw]
            usage = round(sum(per_core) / max(len(per_core), 1), 1)
        else:
            per_core = []
            usage = round(float(per_raw), 1)

        vm = psutil.virtual_memory()
        cached_mb = round(vm.cached / (1024 * 1024), 1) if hasattr(vm, "cached") else None
        buffers_mb = round(vm.buffers / (1024 * 1024), 1) if hasattr(vm, "buffers") else None

        load_avg: list[float] = [0.0, 0.0, 0.0]
        try:
            load_avg = [float(x) for x in psutil.getloadavg()]
        except (AttributeError, OSError):
            pass

        disks = _disk_entries()
        if not disks:
            try:
                du = psutil.disk_usage("/")
                disks = [
                    {
                        "total": round(du.total / (1024 * 1024), 1),
                        "used": round(du.used / (1024 * 1024), 1),
                        "free": round(du.free / (1024 * 1024), 1),
                        "usagePercent": round(du.percent, 1),
                        "mountPath": "/",
                        "fstype": "unknown",
                    }
                ]
            except OSError:
                disks = []
        boot = psutil.boot_time()
        uptime = int(time.time() - boot)

        mem_payload: dict[str, Any] = {
            "total": round(vm.total / (1024 * 1024), 1),
            "used": round(vm.used / (1024 * 1024), 1),
            "free": round(vm.free / (1024 * 1024), 1),
            "usagePercent": round(vm.percent, 1),
        }
        if cached_mb is not None:
            mem_payload["cached"] = cached_mb
        if buffers_mb is not None:
            mem_payload["buffers"] = buffers_mb

        system: dict[str, Any] = {
            "cpu": {
                "usage": usage,
                "cores": psutil.cpu_count() or len(per_core) or 1,
                "loadAvg": load_avg,
                "perCore": per_core,
            },
            "memory": mem_payload,
            "disks": disks,
            "network": {
                "interfaces": _iface_entries(),
            },
            "uptime": uptime,
        }

        pip = _primary_ipv4()
        meta: dict[str, Any] = {
            "os": sys.platform,
            "hostname": socket.gethostname(),
            "agent_version": AGENT_VERSION,
            "agentVersion": AGENT_VERSION,
        }
        if pip:
            meta["primary_ip"] = pip
            meta["primaryIp"] = pip
        env_region = (os.environ.get("NODEHOST_REGION") or "").strip()
        loc = self._region or env_region or None
        if loc:
            meta["region"] = loc

        return {
            "type": "heartbeat",
            "timestamp": datetime.now(UTC).isoformat(),
            "system": system,
            "meta": meta,
        }

    async def next_interval(self) -> float:
        return 5.0

    async def wait_next(self) -> None:
        await asyncio.sleep(await self.next_interval())
