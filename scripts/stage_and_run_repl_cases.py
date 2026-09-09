"""stage_and_run_repl_cases.py — put the replication driver on the mgmt node and start it.

The driver has to run ON the mgmt node (it talks to the CP and to every storage
node over the private network), so the metadata it reads there must address
everything by PRIVATE ip and point at the key as it lands in the mgmt node's
home directory. Doing that by hand is how runs got lost between steps.

Usage:  python stage_and_run_repl_cases.py [cases]
        cases defaults to `all_c3_last` (case1,2,4,5,6 then 3) and may be any
        group or comma-separated case list the driver accepts.
"""
import json
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
KEY = "C:/Users/Michael/.ssh/mtes01.pem"
REMOTE_KEY = "/home/ec2-user/mtes01.pem"
SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "LogLevel=ERROR",
            "-o", "ConnectTimeout=30", "-i", KEY]


def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def run(cmd, timeout=600, check=True):
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed rc={proc.returncode}: {' '.join(cmd[:6])}...\n"
                           f"stdout: {proc.stdout[-2000:]}\nstderr: {proc.stderr[-2000:]}")
    return proc.stdout


def ssh(host, remote_cmd, timeout=600, check=True):
    return run(["ssh", *SSH_OPTS, f"ec2-user@{host}", remote_cmd], timeout=timeout, check=check)


def scp(paths, host, dest, timeout=900):
    return run(["scp", *SSH_OPTS, *paths, f"ec2-user@{host}:{dest}"], timeout=timeout)


def remote_metadata(meta):
    """The same lab, addressed from inside the VPC."""
    out = json.loads(json.dumps(meta))
    out["key_path"] = REMOTE_KEY
    out["mgmt"]["public_ip"] = out["mgmt"]["private_ip"]
    for client in out.get("clients", []):
        client["public_ip"] = client["private_ip"]
    for cluster in out["clusters"].values():
        cluster["storage_public_ips"] = list(cluster["storage_private_ips"])
    return out


def main():
    cases = sys.argv[1] if len(sys.argv) > 1 else "all_c3_last"
    # Any further NAME=VALUE arguments are exported for the remote driver, so
    # a run can be tuned (CHAOS_EVENTS, PRESSURE_CYCLES, ...) without editing
    # the script on the box.
    env_args = [a for a in sys.argv[2:] if "=" in a]
    env_prefix = "".join(f"{a} " for a in env_args)
    meta = json.loads((HERE / "cluster_metadata_repl.json").read_text())
    mgmt = meta["mgmt"]["public_ip"]
    log(f"mgmt={mgmt} cases={cases}" + (f" env={env_args}" if env_args else ""))

    remote_meta = HERE / "cluster_metadata_repl_remote.json"
    remote_meta.write_text(json.dumps(remote_metadata(meta), indent=4))

    log("staging driver, metadata and key")
    scp([KEY], mgmt, REMOTE_KEY)
    ssh(mgmt, f"chmod 600 {REMOTE_KEY}")
    scp([str(HERE / "test_async_replication.py")], mgmt, "~/test_async_replication.py")
    scp([str(remote_meta)], mgmt, "~/cluster_metadata_repl.json")

    ts = time.strftime("%Y%m%d_%H%M%S")
    remote_log = f"~/repl_cases_{ts}.log"
    log(f"starting driver -> {remote_log}")
    # `ssh -f` backgrounds the client itself once authenticated, so this returns
    # immediately no matter what the remote shell keeps open. Redirecting every
    # fd and using setsid is still needed so the driver survives the client
    # going away; without -f, ssh sat on the channel until it timed out (twice:
    # 2026-08-19 with nohup, 2026-08-20 with setsid) while the driver ran fine.
    # The 60s budget is for the ssh HANDSHAKE, not for the driver -- but a busy
    # management node can exceed it while the driver has already started, and
    # treating that as a launch failure reported a healthy run as dead twice
    # (runs 20260826_205051 and _214011, both progressing normally while the
    # wrapper exited non-zero). A timeout here is inconclusive, so ask the node
    # what actually happened instead of guessing.
    try:
        run(["ssh", "-f", *SSH_OPTS, f"ec2-user@{mgmt}",
             f"cd ~ && setsid env {env_prefix}python3 -u test_async_replication.py {cases} "
             f"> {remote_log} 2>&1 < /dev/null & echo $! > ~/repl_pid; "
             f"echo ~/repl_cases_{ts}.log > ~/repl_log"], timeout=60)
    except subprocess.TimeoutExpired:
        log("ssh -f exceeded its 60s budget; checking whether the driver started")
        started = False
        for _ in range(10):
            time.sleep(15)
            probe = ssh(mgmt,
                        f"test -f ~/repl_log && grep -q {ts} ~/repl_log && "
                        f"pgrep -f '[t]est_async_replication.py' >/dev/null "
                        f"&& echo STARTED || echo NOT_YET", check=False)
            if "STARTED" in probe:
                started = True
                break
        if not started:
            raise RuntimeError(
                f"ssh -f timed out AND no driver for {ts} is running on {mgmt}")
        log("driver is running despite the ssh timeout")
    time.sleep(45)
    status = ssh(mgmt, "P=$(cat ~/repl_pid); L=$(cat ~/repl_log); "
                       "echo \"pid=$P etime=$(ps -p $P -o etime= | tr -d ' ')\"; "
                       "echo \"log=$L\"; tail -12 $L", check=False)
    log(f"after 45s:\n{status.strip()}")
    log(f"DONE — driver running on {mgmt}. Follow it with:\n"
        f"  ssh -i {KEY} ec2-user@{mgmt} \"tail -f \\$(cat ~/repl_log)\"")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"FAILED: {exc}")
        sys.exit(1)
