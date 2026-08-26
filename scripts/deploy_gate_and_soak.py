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

Usage:  python deploy_gate_and_soak.py [--skip-deploy] [--mgmt-boot-100]
                                       [--placement-dumps]

--mgmt-boot-100 gives the mgmt node a 100GB boot disk (default 80).
--placement-dumps turns on per-outage placement-map dumps in the soak.
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
#: The build that must be running. Identity is checked by commit rather than
#: mtime: the ultra Dockerfile bakes `git log` of the spdk repo into
#: /root/spdk/git_log.txt, which pins the image to a commit. See the SPDK_IMAGE
#: pin in setup_perf_test_multipath.py for why this is checked, not assumed.
EXPECT_SPDK_COMMIT = "554c80f11"
#: The ultra commit that must be running. b44de698 defers the JC leadership
#: signal until the parity-desynchronisation check completes. Before it, a
#: reactively promoted distrib announced leadership while parity was still
#: desynchronised, so reads served in that window came off desynchronised
#: parity and returned arbitrary bytes -- the 2026-08-24 iteration-12 "bad
#: magic" failures, whose received magics were random rather than fio's
#: 0xacca, ruling out stale-but-valid data. Without this pin the run would
#: re-test the build that already failed.
EXPECT_ULTRA_COMMIT = "b44de698"
#: Lines probe_bin.sh must print, with what their absence would mean. These
#: distinguish upstream d528e1a67 (zeroes retry state at the submission entry
#: point) from the superseded local attempt that zeroed it at completion and so
#: missed any bdev_io whose previous occupant was a different bdev module.
REQUIRED_FIX_LINES = (
    ("[1] bdev_nvme_submit_request_initial",
     "retry state is not initialised at submit (spdk/spdk#3686 unfixed)"),
    ("[1] fn_table .submit_request wired to it",
     "the initialising entry point is not wired into nvmelib_fn_table"),
    ("[0] bio->retry_count zeroed at completion",
     "the superseded completion-side reset is still in this build"),
)
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
        deploy_cmd = [sys.executable, "setup_perf_test_multipath.py"]
        if "--mgmt-boot-100" in sys.argv:
            deploy_cmd += ["--mgmt-boot-gb", "100"]
        proc = subprocess.run(deploy_cmd,
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
    if EXPECT_SPDK_COMMIT not in out:
        raise RuntimeError(
            f"GATE FAILED: image was not built from spdk {EXPECT_SPDK_COMMIT} "
            f"— /root/spdk/git_log.txt names a different commit, so the node is "
            f"on a stale image. Check the SPDK_IMAGE pin and that the "
            f"spdk-core R26.3 tags finished rebuilding (amd64 included).")
    if EXPECT_ULTRA_COMMIT not in out:
        raise RuntimeError(
            f"GATE FAILED: image was not built from ultra "
            f"{EXPECT_ULTRA_COMMIT} — the parity-desync leadership fix under "
            f"test is absent, so the run would prove nothing.")
    for fragment, why in REQUIRED_FIX_LINES:
        if fragment not in out:
            raise RuntimeError(f"GATE FAILED: {why} ({fragment!r} absent)")
    for marker in REQUIRED_MARKERS:
        # probe_bin.sh prints "  [N] <marker>"; N==0 means absent.
        if f"[0] {marker}" in out or marker not in out:
            raise RuntimeError(
                f"GATE FAILED: marker {marker!r} missing from the running "
                f"binary — the run would be blind to placement degradation.")
    log("GATE PASSED: instrumented binary confirmed")


def start_soak(mgmt):
    log("=== starting soak ===")
    env_prefix = "PLACEMENT_DUMPS=1 " if "--placement-dumps" in sys.argv else ""
    out = ssh(mgmt, f"{env_prefix}bash ~/start_soak_mp.sh")
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
