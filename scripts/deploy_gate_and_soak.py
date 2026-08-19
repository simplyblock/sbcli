"""
deploy_gate_and_soak.py — deploy an MP cluster, verify the SPDK image really
carries the instrumentation, then start the soak. One process, no human in the
middle.

Written because the hand-driven sequence kept losing runs between steps: on
2026-08-18 the deploy finished at 22:16 and the soak was never started, so 8
instances sat idle until the 12-hour auto-stop. The gate is not optional — three
earlier runs looked instrumented and were not (ultra:main-latest's manifest list
points amd64 at a five-day-old image), so a quiet soak log proves nothing unless
the binary was checked first.

Usage:  python deploy_gate_and_soak.py [--skip-deploy]
        --skip-deploy reuses the existing cluster_metadata_mp.json (gate + soak
        only), for when a cluster is already up.
"""
import json
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
KEY = "C:/Users/Michael/.ssh/mtes01.pem"
SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "LogLevel=ERROR",
            "-o", "ConnectTimeout=20", "-i", KEY]
#: The binary that must be running. See the SPDK_IMAGE pin in
#: setup_perf_test_multipath.py for why this is checked rather than assumed.
EXPECT_MTIME = "2026-08-17 09:15:25"
REQUIRED_MARKERS = ("fault tolerance degraded", "anti-affinity dropped")


def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def run(cmd, timeout=600, check=True):
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"command failed rc={proc.returncode}: {' '.join(cmd[:6])}...\n"
            f"stdout: {proc.stdout[-2000:]}\nstderr: {proc.stderr[-2000:]}")
    return proc.stdout


def ssh(host, remote_cmd, timeout=600, check=True):
    return run(["ssh", *SSH_OPTS, f"ec2-user@{host}", remote_cmd],
               timeout=timeout, check=check)


def scp(paths, host, dest, timeout=900):
    return run(["scp", *SSH_OPTS, *paths, f"ec2-user@{host}:{dest}"], timeout=timeout)


def deploy():
    """Run the deployer, streaming BOTH streams to their own log file.

    capture_output() with only a stdout tail logged made a failure
    undiagnosable (2026-08-19 13:15: rc=1 with the traceback sitting in the
    discarded stderr), so stderr is merged into a real deployer log.
    """
    log("=== deploying MP cluster ===")
    dlog = HERE / f"mp_deploy_{time.strftime('%Y%m%d_%H%M%S')}.log"
    log(f"deployer output -> {dlog.name}")
    with open(dlog, "w", encoding="utf-8") as fh:
        proc = subprocess.run([sys.executable, "setup_perf_test_multipath.py"],
                              cwd=str(HERE), stdout=fh,
                              stderr=subprocess.STDOUT, timeout=5400)
    tail = "\n".join(
        dlog.read_text(encoding="utf-8", errors="replace").splitlines()[-30:])
    log(f"deployer rc={proc.returncode}; tail:\n{tail}")
    if proc.returncode != 0:
        raise RuntimeError(
            f"deployment failed (rc={proc.returncode}); full output in "
            f"{dlog.name}. Instances are left running for inspection — "
            f"terminate them by tag before retrying.")


def stage(mgmt):
    log(f"staging soak + gate on mgmt {mgmt}")
    scp([KEY], mgmt, "~/.ssh/mtes01.pem")
    ssh(mgmt, "chmod 600 ~/.ssh/mtes01.pem")
    scp([str(HERE / f) for f in (
        "aws_dual_node_outage_soak_multipath.py", "start_soak_mp.sh",
        "cluster_metadata_mp.json", "verify_spdk_image.sh", "probe_bin.sh",
        "collect_logs.py")], mgmt, "~/")


def gate(mgmt, sn):
    """Abort unless the SPDK container really runs the instrumented binary."""
    log(f"=== image provenance gate on {sn} ===")
    ssh(mgmt, f"scp -o StrictHostKeyChecking=no -o LogLevel=ERROR "
              f"-i ~/.ssh/mtes01.pem ~/verify_spdk_image.sh ~/probe_bin.sh "
              f"ec2-user@{sn}:/tmp/")
    out = ssh(mgmt, f"ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "
                    f"-i ~/.ssh/mtes01.pem ec2-user@{sn} "
                    f"'bash /tmp/verify_spdk_image.sh'")
    log(out.strip())
    if EXPECT_MTIME not in out:
        raise RuntimeError(
            f"GATE FAILED: running binary is not the expected build "
            f"(no mtime {EXPECT_MTIME}). The node is on a stale image — check "
            f"the SPDK_IMAGE pin.")
    for marker in REQUIRED_MARKERS:
        # probe_bin.sh prints "  [N] <marker>"; N==0 means absent.
        if f"[0] {marker}" in out or marker not in out:
            raise RuntimeError(
                f"GATE FAILED: marker {marker!r} missing from the running "
                f"binary — the run would be blind to placement degradation.")
    log("GATE PASSED: instrumented binary confirmed")


def start_soak(mgmt):
    log("=== starting soak ===")
    out = ssh(mgmt, "bash ~/start_soak_mp.sh")
    log(out.strip())
    time.sleep(120)
    status = ssh(mgmt, "TS=$(cat ~/soak_ts); P=$(cat ~/soak_pid); "
                       "echo \"pid=$P etime=$(ps -p $P -o etime= | tr -d ' ')\"; "
                       "grep -E 'loop:|Connected|fio running|##########' "
                       "~/soak_mp_${TS}.out | tail -8", check=False)
    log(f"soak after 2 min:\n{status.strip()}")


def main():
    if "--skip-deploy" not in sys.argv:
        deploy()
    meta = json.loads((HERE / "cluster_metadata_mp.json").read_text())
    mgmt = meta["mgmt"]["public_ip"]
    sn = meta["storage_nodes"][0]["private_ip"]
    log(f"cluster={meta['cluster_uuid']} mgmt={mgmt} sn={sn}")
    stage(mgmt)
    gate(mgmt, sn)
    start_soak(mgmt)
    log(f"DONE — soak running on {mgmt}. Check with:\n"
        f"  ssh -i {KEY} ec2-user@{mgmt} "
        f"\"grep -E '####|PASS|FAIL|ERROR|healed after|degraded' "
        f"~/soak_mp_\\$(cat ~/soak_ts).out | tail -20\"")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"FAILED: {exc}")
        sys.exit(1)
