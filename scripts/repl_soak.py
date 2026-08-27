#!/usr/bin/env python3
"""One-liner for the async-replication soak: deploy a lab from a branch, run cases, report.

    python scripts/repl_soak.py --cases case7,case9 --env CHAOS_EVENTS=100
    python scripts/repl_soak.py --cases case9 --env CHAOS_EVENTS=100 --teardown on-pass
    python scripts/repl_soak.py --cases case10,case11 --branch main --spdk-image public.ecr.aws/simply-block/ultra:main-latest-amd64
    python scripts/repl_soak.py --cases case3 --reuse-lab          # existing lab, no redeploy

What --branch means: the sbcli checkout that is (1) pip-installed on every
lab node by the deployer, (2) the source of the control-plane hotfix mounted
over the running services, and (3) the copy of the test driver that is staged
to the management node. So a case runs against exactly that branch's code,
whatever the pre-built CP image lags behind. The SPDK image is independent
(--spdk-image, default: the digest pinned in setup_repl_test_2clusters.py);
--cp-image overrides the control-plane image (SIMPLYBLOCK_DOCKER_IMAGE).

Steps: clone/refresh the branch -> deploy (unless --reuse-lab) -> hotfix
(unless --no-hotfix) -> stage + run the cases -> wait for the driver ->
print the SUMMARY -> optional teardown. Exit code 0 only when every case
passed.
"""
import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

KEY = os.environ.get("REPL_KEY", "C:/Users/Michael/.ssh/mtes01.pem")
REPO_URL = "github.com/simplyblock/sbcli.git"
WORK_ROOT = Path(os.environ.get("REPL_SOAK_ROOT",
                                Path(__file__).resolve().parent.parent.parent / "soak-labs"))
SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "LogLevel=ERROR",
            "-o", "ConnectTimeout=30", "-i", KEY]


# Any credential that reaches a log line is leaked: these logs are pasted into
# tickets and chat, and the lab copies keep them on disk for the life of the
# instance. The fetch URL carries a GitHub token, so redact before printing
# rather than trusting each call site to remember.
_SECRET_RE = re.compile(r"(x-access-token:)[^@\s]+(@)")


def redact(msg):
    return _SECRET_RE.sub(r"\1***\2", str(msg))


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {redact(msg)}", flush=True)


def sh(cmd, cwd=None, env=None, check=True, capture=False):
    log("$ " + (cmd if isinstance(cmd, str) else " ".join(shlex.quote(c) for c in cmd)))
    r = subprocess.run(cmd, cwd=cwd, env=env, shell=isinstance(cmd, str),
                       text=True, capture_output=capture)
    if check and r.returncode != 0:
        if capture:
            print(redact(r.stdout[-2000:]), redact(r.stderr[-2000:]))
        raise SystemExit(f"step failed (rc={r.returncode}): {redact(cmd)}")
    return r


def gh_token():
    r = subprocess.run(["C:/Users/Michael/.local/bin/gh.exe", "auth", "token"],
                       text=True, capture_output=True, check=True)
    return r.stdout.strip()


def ssh(mgmt, remote_cmd, check=True):
    r = subprocess.run(["ssh", *SSH_OPTS, f"ec2-user@{mgmt}", remote_cmd],
                       text=True, capture_output=True)
    if check and r.returncode != 0:
        raise SystemExit(f"ssh failed on {mgmt}: {r.stderr.strip()[-500:]}")
    return r.stdout


def checkout(branch):
    """A dedicated clone per branch under WORK_ROOT (never the developer's tree)."""
    WORK_ROOT.mkdir(parents=True, exist_ok=True)
    dest = WORK_ROOT / f"sbcli-{re.sub(r'[^A-Za-z0-9._-]', '_', branch)}"
    url = f"https://x-access-token:{gh_token()}@{REPO_URL}"
    if not (dest / ".git").exists():
        sh(["git", "clone", "-q", "--branch", branch, url, str(dest)])
    else:
        sh(["git", "-C", str(dest), "fetch", "-q", url, branch])
        sh(["git", "-C", str(dest), "checkout", "-q", "-B", branch, "FETCH_HEAD"])
    head = sh(["git", "-C", str(dest), "log", "--oneline", "-1"], capture=True).stdout.strip()
    log(f"branch {branch} @ {head}")
    return dest


def load_meta(scripts_dir):
    p = scripts_dir / "cluster_metadata_repl.json"
    if not p.exists():
        raise SystemExit(f"no lab metadata at {p} (deploy first, or drop --reuse-lab)")
    return json.loads(p.read_text())


def lab_is_reachable(mgmt, timeout=20):
    r = subprocess.run(["ssh", "-o", "StrictHostKeyChecking=no", "-o", "LogLevel=ERROR",
                        "-o", f"ConnectTimeout={timeout}", "-i", KEY,
                        f"ec2-user@{mgmt}", "true"], capture_output=True, text=True)
    return r.returncode == 0


def require_reachable(mgmt, reused):
    """A stale metadata file is the normal case, not the exception: the repo
    TRACKS scripts/cluster_metadata_repl.json, so every fresh clone arrives
    carrying whichever lab was alive when it was last committed. Probe before
    doing anything that would otherwise fail deep inside the hotfix."""
    if lab_is_reachable(mgmt):
        return
    hint = ("that metadata is the copy committed in the repo, pointing at a lab "
            "that no longer exists -- drop --reuse-lab to deploy a fresh one"
            if reused else
            "the deploy reported success but the management node is unreachable")
    raise SystemExit(f"management node {mgmt} is not reachable: {hint}")


def deploy(scripts_dir, branch, spdk_image, cp_image):
    env = dict(os.environ, SBCLI_BRANCH=branch)
    if spdk_image:
        env["SPDK_IMAGE"] = spdk_image
    if cp_image:
        env["SIMPLYBLOCK_DOCKER_IMAGE"] = cp_image
    sh([sys.executable, "-u", "setup_repl_test_2clusters.py", "-d"], cwd=scripts_dir, env=env)


def hotfix(scripts_dir):
    sh([sys.executable, "hotfix_repl_lab.py"], cwd=scripts_dir)


def run_cases(scripts_dir, cases, env_kv):
    sh([sys.executable, "stage_and_run_repl_cases.py", cases, *env_kv], cwd=scripts_dir)


def wait_for_driver(mgmt, poll=120):
    """Follow the remote driver to === DONE ===, then return (summary, passed)."""
    logfile = ""
    for _ in range(20):
        logfile = ssh(mgmt, "cat ~/repl_log 2>/dev/null", check=False).strip()
        if logfile:
            break
        time.sleep(15)
    if not logfile:
        raise SystemExit("driver never registered a log file on the management node")
    log(f"following {logfile}")
    while True:
        out = ssh(mgmt,
                  f"if grep -q '=== DONE ===' {logfile}; then echo FINISHED; "
                  f"grep -A12 '=== SUMMARY ===' {logfile}; "
                  f"elif ! pgrep -f '[t]est_async_replication.py' >/dev/null; then echo DIED; "
                  f"tail -15 {logfile}; else echo RUNNING; "
                  f"grep -E '^==========' {logfile} | tail -1; fi", check=False)
        if out.startswith("FINISHED"):
            summary = out.split("\n", 1)[1]
            passed = ("FAIL" not in summary) and ("PASS" in summary)
            return summary, passed
        if out.startswith("DIED"):
            return out, False
        log("running: " + (out.split("\n", 1)[1].strip() if "\n" in out else "..."))
        time.sleep(poll)


def teardown(meta):
    ips = [meta["mgmt"]["private_ip"]] + [c["private_ip"] for c in meta.get("clients", [])]
    for cl in meta["clusters"].values():
        ips += cl["storage_private_ips"]
    r = sh(["aws", "ec2", "describe-instances", "--filters",
            f"Name=private-ip-address,Values={','.join(ips)}",
            "Name=instance-state-name,Values=running,stopped,pending",
            "--query", "Reservations[].Instances[].InstanceId", "--output", "text"],
           capture=True)
    ids = r.stdout.split()
    if ids:
        sh(["aws", "ec2", "terminate-instances", "--instance-ids", *ids], capture=True)
        log(f"terminated {len(ids)} lab instances")
    else:
        log("no lab instances found to terminate")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cases", required=True, nargs="+", metavar="CASE",
                    help="cases or a group name: 'case6,case7', 'case6 case7', "
                         "'case6, case7' and 'all9' all work")
    ap.add_argument("--branch", default="replication-features",
                    help="sbcli branch to deploy/hotfix/stage (default replication-features)")
    ap.add_argument("--spdk-image", default="", help="ultra image ref; default = digest pinned in the deployer")
    ap.add_argument("--cp-image", default="", help="control-plane docker image (SIMPLYBLOCK_DOCKER_IMAGE)")
    ap.add_argument("--env", action="append", default=[], metavar="KEY=VAL",
                    help="driver knobs, e.g. CHAOS_EVENTS=100, CASE11_RUNTIME_MIN=30 (repeatable)")
    ap.add_argument("--reuse-lab", action="store_true", help="skip deploy; use the branch clone's existing metadata")
    ap.add_argument("--no-hotfix", action="store_true", help="skip mounting the branch's python over the CP services")
    ap.add_argument("--teardown", choices=["never", "on-pass", "always"], default="never")
    args = ap.parse_args()
    # Accept every shape a shell hands us: "a,b", "a, b" (the space makes the
    # shell pass two argv entries) and "a b" all mean the same list.
    args.cases = ",".join(c for tok in args.cases for c in tok.split(",") if c)
    if not args.cases:
        ap.error("--cases got no case names")
    for kv in args.env:
        if "=" not in kv:
            ap.error(f"--env expects KEY=VAL, got {kv!r}")

    clone = checkout(args.branch)
    scripts_dir = clone / "scripts"

    if not args.reuse_lab:
        deploy(scripts_dir, args.branch, args.spdk_image, args.cp_image)
    meta = load_meta(scripts_dir)
    mgmt = meta["mgmt"]["public_ip"]
    shape = ", ".join("{}={}({}n)".format(k, v["cluster_uuid"][:8], v["nodes"])
                      for k, v in meta["clusters"].items())
    log(f"lab mgmt {mgmt}: {shape}")
    require_reachable(mgmt, args.reuse_lab)

    if not args.no_hotfix:
        hotfix(scripts_dir)

    run_cases(scripts_dir, args.cases, args.env)
    summary, passed = wait_for_driver(mgmt)
    print("\n" + summary.strip() + "\n")
    log("RESULT: " + ("ALL PASSED" if passed else "FAILED"))

    if args.teardown == "always" or (args.teardown == "on-pass" and passed):
        teardown(meta)
    elif not passed:
        log(f"lab kept for diagnosis: ssh -i {KEY} ec2-user@{mgmt}  (log: $(cat ~/repl_log))")
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()
