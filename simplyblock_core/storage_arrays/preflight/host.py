"""Host-side helpers of the array pre-check: facts for the bundle, block
device I/O with a deadline, and the host's SAN setup."""
from __future__ import annotations

import io
import mmap
import os
import shutil
import socket
import ssl
import subprocess
import threading
from typing import Any

PATTERN_BYTES = 4 << 20
# Linux only (the checks of a host run there); 0 keeps the module importable.
O_DIRECT = getattr(os, "O_DIRECT", 0)
O_SYNC = getattr(os, "O_SYNC", 0)


def sh(*cmd: str, timeout: float = 30.0) -> tuple[int, str]:
    """Run a command; (returncode, stdout+stderr). A missing tool is rc 127."""
    if not shutil.which(cmd[0]):
        return 127, f"{cmd[0]}: not installed"
    try:
        res = subprocess.run(list(cmd), capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 124, f"{' '.join(cmd)}: timed out after {timeout:.0f} s"
    return res.returncode, (res.stdout + res.stderr).strip()


def host_file(path: str) -> str:
    """A host configuration file: /host<path> (container with the host's
    /etc under /host/etc) first, then <path>."""
    for candidate in (f"/host{path}", path):
        try:
            with open(candidate) as f:
                return f.read()
        except OSError:
            continue
    return ""


#: What goes into the bundle about the host: (file name, command or file).
FACTS: list[tuple[str, tuple[str, ...]]] = [
    ("uname.txt", ("uname", "-a")),
    ("os-release.txt", ("@file", "/etc/os-release")),
    ("lsblk.json", ("lsblk", "-J", "-o", "NAME,KNAME,TYPE,SIZE,WWN,SERIAL,MODEL,VENDOR,TRAN,RO,STATE")),
    ("ip-addr.txt", ("ip", "-br", "addr")),
    ("modules.txt", ("sh", "-c", "lsmod | grep -E 'nvme|iscsi|scsi_|dm_|multipath' || true")),
    ("iscsi-initiatorname.txt", ("@file", "/etc/iscsi/initiatorname.iscsi")),
    ("iscsi-sessions.txt", ("iscsiadm", "-m", "session", "-P", "1")),
    ("nvme-hostnqn.txt", ("@file", "/etc/nvme/hostnqn")),
    ("nvme-discovery.conf", ("@file", "/etc/nvme/discovery.conf")),
    ("nvme-subsys.json", ("nvme", "list-subsys", "-o", "json")),
    ("multipath.conf", ("@file", "/etc/multipath.conf")),
    ("multipath-ll.txt", ("multipath", "-ll")),
    ("multipathd-config.txt", ("multipathd", "show", "config")),
    ("fc-hosts.txt", ("sh", "-c", ("for h in /sys/class/fc_host/host*; do [ -e $h ] && echo $h "
                                   "$(cat $h/port_name $h/port_state 2>/dev/null); done; true"))),
    ("dmesg-san.txt", ("sh", "-c", ("dmesg 2>/dev/null | grep -Ei 'scsi|iscsi|nvme|multipath|sd[a-z]+|alua'"
                                    " | tail -n 400; true"))),
]


def collect_facts(out_dir: str) -> list[str]:
    os.makedirs(out_dir, exist_ok=True)
    written = []
    for name, cmd in FACTS:
        if cmd[0] == "@file":
            text = host_file(cmd[1]) or f"({cmd[1]} not present)"
        else:
            _, text = sh(*cmd)
        with open(os.path.join(out_dir, name), "w") as f:
            f.write(text + "\n")
        written.append(name)
    return written


def tcp_reachable(host: str, port: int, timeout: float = 5.0) -> str | None:
    """None when a TCP connection opens; else the reason."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return None
    except OSError as e:
        return f"{type(e).__name__}: {e}"


def tls_probe(host: str, port: int, cafile: str | None, verify: bool, timeout: float = 10.0) -> dict[str, Any]:
    """The endpoint's certificate and whether it verifies as configured."""
    out: dict[str, Any] = {}
    ctx = ssl.create_default_context(cafile=cafile) if verify else ssl._create_unverified_context()
    if verify:
        ctx.check_hostname = True
    try:
        with socket.create_connection((host, port), timeout=timeout) as raw, \
                ctx.wrap_socket(raw, server_hostname=host) as s:
            out["tls_version"] = s.version()
            out["verified"] = verify
            cert = s.getpeercert() or {}
            subject: dict[str, str] = {}
            for rdn in cert.get("subject", ()):
                for pair in rdn:
                    if isinstance(pair, tuple) and len(pair) == 2:
                        subject[str(pair[0])] = str(pair[1])
            out["subject"] = subject or None
            out["not_after"] = cert.get("notAfter") if cert else None
    except ssl.SSLCertVerificationError as e:
        out["error"] = f"certificate does not verify: {e.verify_message}"
    except OSError as e:
        out["error"] = f"{type(e).__name__}: {e}"
    return out


def size_of(dev: str) -> int:
    rc, out = sh("blockdev", "--getsize64", dev)
    return int(out) if rc == 0 and out.isdigit() else -1


def write_pattern(dev: str) -> bytes:
    data = os.urandom(PATTERN_BYTES)
    buf = mmap.mmap(-1, PATTERN_BYTES)
    buf.write(data)
    fd = os.open(dev, os.O_WRONLY | O_DIRECT | O_SYNC)
    try:
        os.write(fd, buf)
    finally:
        os.close(fd)
    return data


def read_pattern(dev: str) -> bytes:
    buf = mmap.mmap(-1, PATTERN_BYTES)
    with io.FileIO(os.open(dev, os.O_RDONLY | O_DIRECT), "rb", closefd=True) as f:
        n = f.readinto(buf)
    if n != PATTERN_BYTES:
        raise OSError(f"short read of {dev}: {n} bytes")
    return buf[:]


def read_within(dev: str, seconds: float) -> tuple[str, Any]:
    """("ok", data), ("error", exception) or ("hung", None): a read that has
    not returned after ``seconds`` (I/O queued with no path to the LUN)."""
    box: dict[str, Any] = {}

    def run() -> None:
        try:
            box["data"] = read_pattern(dev)
        except Exception as e:  # noqa: BLE001
            box["error"] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(seconds)
    if t.is_alive():
        return "hung", None
    if "error" in box:
        return "error", box["error"]
    return "ok", box["data"]


def release_queued_io(map_name: str) -> None:
    """Fail the I/O a multipath map queues with no path left (after a hung
    read), so the check can go on and clean up."""
    sh("dmsetup", "message", map_name, "0", "fail_if_no_path")


def drop_devices(devs: list[dict]) -> None:
    """Remove the host's SCSI devices of a LUN the array no longer presents
    (NVMe namespaces go away by themselves)."""
    for d in devs:
        if d.get("type") == "mpath" or d.get("name", "").startswith("dm-"):
            sh("multipath", "-f", d["name"])
            continue
        path = f"/sys/block/{d['name']}/device/delete"
        if os.path.exists(path):
            try:
                with open(path, "w") as f:
                    f.write("1")
            except OSError:
                pass


def iscsi_sessions() -> list[str]:
    """Portals (``ip:port``) with a logged-in iSCSI session."""
    rc, out = sh("iscsiadm", "-m", "session")
    if rc != 0:
        return []
    portals = []
    for line in out.splitlines():
        # tcp: [1] 10.0.0.5:3260,1030 iqn.... (non-flash)
        parts = line.split()
        if len(parts) >= 3:
            portals.append(parts[2].split(",", 1)[0])
    return portals


def multipath_settings() -> dict[str, str] | None:
    """``no_path_retry`` and ``polling_interval`` of the effective multipath
    configuration's ``defaults`` and ``overrides`` (``overrides.<key>`` /
    ``defaults.<key>``); None without multipathd."""
    rc, out = sh("multipathd", "show", "config")
    if rc != 0:
        return None
    found: dict[str, str] = {}
    stack: list[str] = []
    for line in out.splitlines():
        s = line.strip()
        if s.endswith("{"):
            stack.append(s[:-1].strip())
        elif s == "}":
            if stack:
                stack.pop()
        elif len(stack) == 1 and stack[0] in ("defaults", "overrides"):
            key, _, value = s.partition(" ")
            if key in ("no_path_retry", "polling_interval"):
                found[f"{stack[0]}.{key}"] = value.strip().strip('"')
    return found
