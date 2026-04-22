#!/usr/bin/env python3
"""Copy operator scripts, SSH key and soak scripts to a simplyblock mgmt node.

Uploads:
  * scripts/stop_cluster_run.py, scripts/extract_spdk_critical.py  -> <dest>/scripts/
  * scripts/*soak*.py                                              -> <dest>/perf/
  * tests/perf/cluster_metadata*.json                              -> <dest>/perf/
  * The SSH key                                                    -> ~/.ssh/<basename>  (chmod 600)

The mgmt IP is the required positional argument. Key path and SSH user default
to the values in a metadata JSON (e.g. tests/perf/cluster_metadata_base.json)
when one is passed with --metadata, otherwise use --key / --user.

Usage:
  python upload_to_mgmt.py 50.17.149.3 \
      --key C:\\ssh\\mtes01.pem \
      --user ec2-user \
      --metadata tests\\perf\\cluster_metadata_base.json
"""
from __future__ import annotations

import argparse
import json
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT  = Path(__file__).resolve().parent.parent
LOCAL_OPS  = [REPO_ROOT / "scripts" / "stop_cluster_run.py",
              REPO_ROOT / "scripts" / "extract_spdk_critical.py"]
SOAK_GLOB  = "scripts/*soak*.py"
META_GLOB  = "tests/perf/cluster_metadata*.json"


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def need(tool: str) -> None:
    if shutil.which(tool) is None:
        sys.exit(f"ERROR: '{tool}' not on PATH; on Windows install OpenSSH client feature.")


VERBOSE = False


def ssh_args(key: str) -> list[str]:
    # Note: no UserKnownHostsFile=/dev/null — on Windows OpenSSH this sometimes
    # triggers silent hangs. StrictHostKeyChecking=no is enough to skip the
    # interactive host-key prompt on first connect.
    args = [
        "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=30",
        "-o", "ServerAliveInterval=15",
        "-o", "ServerAliveCountMax=4",
        "-i", key,
    ]
    if VERBOSE:
        args.insert(0, "-v")
    return args


def ssh_run(user: str, host: str, key: str, command: str, timeout: int = 60) -> tuple[int, str, str]:
    argv = ["ssh", *ssh_args(key), f"{user}@{host}", command]
    log(f"ssh {user}@{host}: {command}")
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        log(f"WARNING: ssh timed out after {timeout}s: {command}")
        return 124, exc.stdout or "", exc.stderr or "timeout"
    if proc.stdout.strip():
        print(proc.stdout.rstrip())
    if proc.stderr.strip():
        print(proc.stderr.rstrip(), file=sys.stderr)
    return proc.returncode, proc.stdout, proc.stderr


def scp_upload(local: Path, user: str, host: str, key: str, remote: str, timeout: int = 300) -> None:
    if not local.exists():
        log(f"skip (missing): {local}")
        return
    argv = ["scp", *ssh_args(key), str(local), f"{user}@{host}:{remote}"]
    log(f"scp {local.name} -> {user}@{host}:{remote}")
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(f"scp failed rc={proc.returncode}: {proc.stderr.strip()}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mgmt_ip", help="mgmt node IP (public or reachable)")
    ap.add_argument("--key", default=None, help="SSH private key (to auth AND to copy up)")
    ap.add_argument("--user", default=None, help="SSH user on mgmt (default: metadata['user'] or ec2-user)")
    ap.add_argument("--metadata", type=Path, default=None, help="cluster metadata JSON to pull defaults from")
    ap.add_argument("--dest", default=None, help="remote base dir (default: /home/<user>)")
    ap.add_argument("--no-key-upload", action="store_true", help="do not copy the SSH key to the mgmt node")
    ap.add_argument("-v", "--verbose", action="store_true", help="pass -v to ssh/scp for diagnostics")
    args = ap.parse_args()

    global VERBOSE
    VERBOSE = args.verbose

    need("ssh")
    need("scp")

    meta: dict = {}
    if args.metadata:
        meta = json.loads(args.metadata.read_text(encoding="utf-8"))

    user = args.user or meta.get("user") or "ec2-user"
    key  = args.key  or meta.get("key_path")
    if not key:
        sys.exit("ERROR: no SSH key. Pass --key or --metadata pointing at a JSON with 'key_path'.")
    key_path = Path(key).expanduser()
    if not key_path.exists():
        sys.exit(f"ERROR: SSH key not found: {key_path}")

    dest = args.dest or f"/home/{user}"
    host = args.mgmt_ip

    # 0) preflight: prove we can SSH at all before doing real work.
    rc, _, err = ssh_run(user, host, str(key_path), "echo ok", timeout=120)
    if rc != 0:
        sys.exit(f"ERROR: preflight ssh to {user}@{host} failed rc={rc}\n{err}")

    # 1) create remote layout
    rc, _, _ = ssh_run(user, host, str(key_path),
                       f"mkdir -p {shlex.quote(dest)}/scripts {shlex.quote(dest)}/perf .ssh "
                       f"&& chmod 700 .ssh",
                       timeout=60)
    if rc != 0:
        sys.exit("ERROR: mkdir on mgmt failed (see stderr above)")

    # 2) upload ops scripts
    for p in LOCAL_OPS:
        scp_upload(p, user, host, str(key_path), f"{dest}/scripts/")

    # 3) upload soak scripts
    soaks = sorted(REPO_ROOT.glob(SOAK_GLOB))
    if not soaks:
        log(f"WARNING: no files matched {SOAK_GLOB}")
    for p in soaks:
        scp_upload(p, user, host, str(key_path), f"{dest}/perf/")

    # 3b) upload cluster metadata JSONs (used as stop_cluster_run.py input)
    metas = sorted(REPO_ROOT.glob(META_GLOB))
    if not metas:
        log(f"WARNING: no files matched {META_GLOB}")
    for p in metas:
        scp_upload(p, user, host, str(key_path), f"{dest}/perf/")

    # 4) upload the key itself and tighten perms.
    # Use a home-relative path (no leading ~) so we don't rely on shell tilde
    # expansion — some shells / chmod invocations won't expand it.
    if not args.no_key_upload:
        remote_key = f".ssh/{key_path.name}"
        scp_upload(key_path, user, host, str(key_path), remote_key)
        rc, _, _ = ssh_run(user, host, str(key_path),
                           f"chmod 600 {remote_key}", timeout=60)
        if rc != 0:
            log("WARNING: chmod 600 on remote key failed")

    # 5) make scripts executable
    ssh_run(user, host, str(key_path),
            f"chmod +x {shlex.quote(dest)}/scripts/*.py {shlex.quote(dest)}/perf/*.py",
            timeout=60)

    log(f"done. on mgmt: ls {dest}/scripts {dest}/perf")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
