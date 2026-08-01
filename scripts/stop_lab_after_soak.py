#!/usr/bin/env python3
"""Wait for an outage soak process to exit, then stop the lab's storage
nodes and clients via AWS EC2 to stop accruing instance hours.

Usage (typical):
    nohup python3 scripts/aws_dual_node_outage_soak_mixed_churn.py \\
        --metadata cluster_metadata_mp.json ... > soak.out 2>&1 &
    SOAK_PID=$!
    nohup python3 scripts/stop_lab_after_soak.py \\
        --metadata cluster_metadata_mp.json \\
        --soak-pid "$SOAK_PID" > stop_lab.out 2>&1 &

The monitor polls the PID until it exits. Exit code 0 -> the soak
finished its configured iteration budget; non-zero -> the soak failed
permanently (fio fault, RemoteCommandError, KeyboardInterrupt, etc.).
Either outcome triggers the same shutdown: every instance listed under
``storage_nodes[*].instance_id`` and ``clients[*].instance_id`` in the
metadata file is sent ``StopInstances``. ``mgmt.instance_id`` is
deliberately left running so the user can SSH back in to retrieve logs
without paying for the storage / client tier.

AWS calls are best-effort: each instance is attempted independently,
errors are logged, and the monitor exits non-zero if any stop failed
while still attempting the rest.
"""

import argparse
import errno
import json
import os
import sys
import time
from pathlib import Path
from typing import List, Tuple


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} {msg}", flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--metadata", required=True,
                   help="Path to cluster_metadata*.json (same file the soak script uses).")
    p.add_argument("--soak-pid", type=int, required=True,
                   help="PID of the soak process to wait on. Cross-process "
                        "(does NOT have to be a direct child of this monitor).")
    p.add_argument("--region", default="us-east-1",
                   help="AWS region for boto3 / ec2 calls. Default: us-east-1.")
    p.add_argument("--poll-interval", type=float, default=5.0,
                   help="Seconds between PID liveness checks. Default 5s. "
                        "Lower = quicker stop after soak exit; higher = less load.")
    p.add_argument("--dry-run", action="store_true",
                   help="Log every action but do not call AWS StopInstances. "
                        "Useful for testing in a non-AWS environment.")
    p.add_argument("--stop-mgmt", action="store_true",
                   help="Also stop the mgmt instance. Default: leave it running "
                        "so the user can SSH back to retrieve soak logs.")
    return p.parse_args()


def _pid_is_running(pid: int) -> bool:
    """True iff process ``pid`` exists. Works for non-child processes —
    sends signal 0 (no-op) and reads ESRCH/EPERM to classify."""
    try:
        os.kill(pid, 0)
    except OSError as e:
        if e.errno == errno.ESRCH:
            return False
        if e.errno == errno.EPERM:
            # Process exists but is owned by someone else. Still "running"
            # for our purposes — the soak might have been started by root
            # while this monitor runs as a user.
            return True
        raise
    return True


def _pid_exit_code(pid: int) -> int:
    """Reap a child PID and return its exit code; return -1 if the PID is
    not our child (signal-0 poll is the best we can do, but we cannot
    read the exit code of a non-descendant). The monitor still triggers
    the stop sequence regardless — for sibling/foreign PIDs we just
    can't distinguish "completed" vs "failed permanently" in the log."""
    try:
        _, status = os.waitpid(pid, os.WNOHANG)
        if os.WIFEXITED(status):
            return os.WEXITSTATUS(status)
        if os.WIFSIGNALED(status):
            return 128 + os.WTERMSIG(status)
        return -1
    except ChildProcessError:
        # Not our child — exit code is unavailable on POSIX without
        # ptrace / process accounting. Caller treats -1 as "unknown".
        return -1
    except OSError:
        return -1


def wait_for_soak_exit(pid: int, poll_interval: float) -> int:
    log(f"Waiting for soak PID {pid} to exit (poll every {poll_interval}s)…")
    while _pid_is_running(pid):
        time.sleep(poll_interval)
    rc = _pid_exit_code(pid)
    if rc == -1:
        log(f"Soak PID {pid} has exited. Exit code is unknown (not a child of this monitor).")
    else:
        log(f"Soak PID {pid} has exited with code {rc}.")
    return rc


def _collect_instance_ids(metadata: dict, stop_mgmt: bool) -> Tuple[List[str], List[str], List[str]]:
    """Return (storage_node_ids, client_ids, mgmt_ids).

    mgmt_ids is non-empty only when --stop-mgmt was passed.
    """
    sn_ids = [
        sn["instance_id"]
        for sn in metadata.get("storage_nodes", [])
        if sn.get("instance_id")
    ]
    client_ids = [
        c["instance_id"]
        for c in metadata.get("clients", [])
        if c.get("instance_id")
    ]
    mgmt_block = metadata.get("mgmt") or {}
    mgmt_id = mgmt_block.get("instance_id")
    mgmt_ids = [mgmt_id] if (stop_mgmt and mgmt_id) else []
    return sn_ids, client_ids, mgmt_ids


def _stop_one(ec2_client, instance_id: str, dry_run: bool) -> Tuple[str, bool, str]:
    """Stop a single instance. Returns (instance_id, ok, message). The
    monitor catches every exception per-instance so one ENI tied up by
    a stuck VPC endpoint or a misconfigured IAM doesn't prevent the
    other instances from stopping."""
    if dry_run:
        return instance_id, True, "dry-run (no API call)"
    try:
        resp = ec2_client.stop_instances(InstanceIds=[instance_id])
        states = [s.get("CurrentState", {}).get("Name", "?")
                  for s in resp.get("StoppingInstances", [])]
        return instance_id, True, f"requested stop; current state: {','.join(states) or '?'}"
    except Exception as e:  # noqa: BLE001 — every failure mode reported uniformly
        return instance_id, False, f"{type(e).__name__}: {e}"


def stop_instances(region: str, ids: List[str], label: str, dry_run: bool) -> Tuple[int, int]:
    """Stop every id in ``ids``. Returns (ok_count, fail_count). Each
    instance is attempted independently — a failure on one does not
    short-circuit the rest."""
    if not ids:
        log(f"No {label} instance ids in metadata; skipping.")
        return 0, 0

    log(f"Stopping {len(ids)} {label} instance(s): {', '.join(ids)}"
        + (" [DRY-RUN]" if dry_run else ""))

    if dry_run:
        for inst in ids:
            log(f"  [dry-run] would stop {inst}")
        return len(ids), 0

    # Defer the boto3 import until we actually need it so --dry-run works
    # on hosts where boto3 isn't installed.
    try:
        import boto3  # type: ignore
    except ImportError:
        log("ERROR: boto3 is required to call AWS EC2. Install with: "
            "`pip install boto3`. To preview without AWS, re-run with --dry-run.")
        return 0, len(ids)

    ec2 = boto3.client("ec2", region_name=region)
    ok = 0
    fail = 0
    for inst in ids:
        _, success, msg = _stop_one(ec2, inst, dry_run=False)
        if success:
            log(f"  OK  {inst}: {msg}")
            ok += 1
        else:
            log(f"  FAIL {inst}: {msg}")
            fail += 1
    return ok, fail


def main() -> int:
    args = parse_args()

    metadata_path = Path(args.metadata)
    if not metadata_path.is_file():
        log(f"ERROR: metadata file not found: {metadata_path}")
        return 2

    with open(metadata_path) as fh:
        metadata = json.load(fh)

    sn_ids, client_ids, mgmt_ids = _collect_instance_ids(metadata, args.stop_mgmt)
    if not sn_ids and not client_ids and not mgmt_ids:
        log(f"ERROR: metadata at {metadata_path} has no instance ids under "
            f"storage_nodes / clients / mgmt; nothing to stop.")
        return 2

    log(f"Lab inventory: storage_nodes={len(sn_ids)}, clients={len(client_ids)}, "
        f"mgmt={'KEEP RUNNING' if not args.stop_mgmt else 'STOP'} "
        f"(mgmt.instance_id={(metadata.get('mgmt') or {}).get('instance_id', '?')})")

    # Block until the soak's PID exits. Exit code is informational only —
    # both success and permanent failure trigger the same shutdown.
    soak_rc = wait_for_soak_exit(args.soak_pid, args.poll_interval)
    log("Proceeding to stop instances regardless of soak outcome "
        "(both completion and permanent failure should leave the cluster idle).")

    total_ok = 0
    total_fail = 0
    for ids, label in (
        (client_ids, "client"),
        (sn_ids, "storage_node"),
        (mgmt_ids, "mgmt"),  # only populated when --stop-mgmt passed
    ):
        ok, fail = stop_instances(args.region, ids, label, dry_run=args.dry_run)
        total_ok += ok
        total_fail += fail

    log(f"Done. Stopped OK: {total_ok}, failed: {total_fail}. "
        f"Soak exit code was {soak_rc}.")

    # Non-zero exit if any individual stop failed (best-effort policy
    # already attempted every instance, but the user should see the
    # signal in their wrapping shell / monitoring).
    if total_fail > 0:
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("Interrupted before completion; instances may still be running.")
        sys.exit(130)
