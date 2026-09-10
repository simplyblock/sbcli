"""hotfix_repl_lab.py — run local simplyblock_core changes on a deployed lab.

The CP services run the CI-built image, so code that is only on a branch never
reaches a lab. This bind-mounts the working tree's modules into the running
services and updates the mgmt node's installed copy, then PROVES the running
code has them.

Two things make this less obvious than it looks:

  * The services are started as `python3 simplyblock_core/services/<svc>.py`
    from /app, so sys.path[0] is the SCRIPT directory -- `import
    simplyblock_core` resolves to the container's site-packages, NOT /app.
    Mounting over /app alone changes nothing the service sees (2026-08-20:
    11,562 AttributeErrors while /app held the fixed file).
  * `docker exec python3 -c ...` runs with sys.path[0] = cwd = /app, so it
    reports the mounted file and gives a false green. Verification has to
    reproduce the service's own import path.

docker cp does not survive task recreation; a bind mount does.

Usage:  python hotfix_repl_lab.py [--verify-only]
"""
import json
import shlex
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
REPO = HERE.parent
KEY = "C:/Users/Michael/.ssh/mtes01.pem"
SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "LogLevel=ERROR",
            "-o", "ConnectTimeout=30", "-i", KEY]

HOTFIX_DIR = "/opt/sb-hotfix"
#: Both copies matter, for different reasons:
#:  * /app -- the service is started as `python3
#:    simplyblock_core/services/<svc>.py` from /app, so ITS OWN module is
#:    executed from here as __main__;
#:  * site-packages -- sys.path[0] is then the SCRIPT directory, so every
#:    `import simplyblock_core...` resolves there instead.
#: Mounting only one of them patches half the process. On 2026-08-20 the
#: chaining fix was mounted over site-packages while the entry-point script
#: ran the image's original from /app, and the bug's signature kept
#: appearing (49 in 6 minutes) on a lab that had just been "verified".
CONTAINER_PATHS = ("/app/simplyblock_core",
                   "/usr/local/lib/python3.12/site-packages/simplyblock_core")
HOST_SP = "/usr/local/lib/python3.9/site-packages"

#: local path -> path under simplyblock_core
SHARED = {
    "simplyblock_core/controllers/lvol_controller.py": "controllers/lvol_controller.py",
    "simplyblock_core/controllers/snapshot_controller.py": "controllers/snapshot_controller.py",
    "simplyblock_core/db_controller.py": "db_controller.py",
    # Modules the mounted files IMPORT but the deployed image predates. main
    # moves while a lab stays pinned to the release it was built from, so a
    # hotfixed file can reference something that simply is not there: on
    # 2026-08-20 lvol_controller pulled in ops_gate and every service — and
    # the mgmt host, which the harness queries — died with
    # "ImportError: cannot import name 'ops_gate'", mid-run. Ship the
    # dependency alongside the file that needs it.
    "simplyblock_core/controllers/ops_gate.py": "controllers/ops_gate.py",
    "simplyblock_core/controllers/tasks_controller.py": "controllers/tasks_controller.py",
    # The JC dual-node flag lives here: apply_jc_dual_node() is called on
    # node add AND on every bring-up, and case 6 restarts a node mid-run --
    # a restarted node comes back with the flag off unless the CP re-applies
    # it, which is exactly when the survivor would abort.
    "simplyblock_core/storage_node_ops.py": "storage_node_ops.py",
    "simplyblock_core/rpc_client.py": "rpc_client.py",
}
#: service -> its own module (mounted on top of the shared set)
SERVICES = {
    "app_SnapshotReplication": {
        "simplyblock_core/services/snapshot_replication.py": "services/snapshot_replication.py"},
    "app_SnapshotMonitor": {
        "simplyblock_core/services/snapshot_monitor.py": "services/snapshot_monitor.py"},
    "app_TasksRunnerReplicationFinal": {
        "simplyblock_core/services/tasks_runner_replication_final.py":
            "services/tasks_runner_replication_final.py"},
    "app_LVolMonitor": {
        "simplyblock_core/services/lvol_monitor.py": "services/lvol_monitor.py"},
}
#: also refreshed on the mgmt HOST: the harness runs `sudo python3 -c
#: "...lvol_controller.get_replication_info..."` there, and sbctl imports it.
HOST_FILES = dict(SHARED)
HOST_FILES.update({p: v for s in SERVICES.values() for p, v in s.items()})
HOST_CLI = "simplyblock_cli/clibase.py"

CRLF, LF = bytes([13, 10]), bytes([10])

#: filled in by discover_drift() at run time: repo path -> path under
#: simplyblock_core, for every module that differs from the deployed image
DRIFT = {}


def discover_drift(mgmt):
    """Every simplyblock_core module that differs from the one in the IMAGE.

    A hand-maintained mount list is wrong by construction: it must be extended
    for every new module a fix happens to touch. Forgetting one fails loudly
    (ImportError: cannot import name 'ConsistencyGroup' -- the service
    crash-looping, lab 2026-08-26) or, worse, quietly: the fix mounted while a
    module it depends on stays at the image's version. Ask the image what it
    has and mount everything that does not match.
    """
    import hashlib
    ref = ssh(mgmt, "sudo docker ps --format '{{.Names}}' | grep -m1 "
                    "TasksRunnerReplicationFinal", check=False).strip()
    if not ref:
        log("no reference container; falling back to the static mount list")
        return {}
    probe = ("cd /usr/local/lib/python3.12/site-packages && "
             'find simplyblock_core -name "*.py" | sort | xargs md5sum')
    listing = ssh(mgmt, f"sudo docker exec {ref} sh -c {shlex.quote(probe)}",
                  check=False)
    image = {}
    for line in listing.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].endswith(".py"):
            image[parts[1]] = parts[0]
    if not image:
        log("could not read the image's module hashes; using the static list")
        return {}

    drift, core = {}, REPO / "simplyblock_core"
    for path in core.rglob("*.py"):
        rel = path.relative_to(REPO).as_posix()
        if "__pycache__" in rel or rel.startswith("simplyblock_core/test"):
            continue
        raw = path.read_bytes().replace(CRLF, LF)   # image files are LF
        digest = hashlib.md5(raw).hexdigest()
        if image.get(rel) != digest:
            drift[rel] = path.relative_to(core).as_posix()
    log(f"image drift: {len(drift)} module(s) differ from the deployed image")
    for rel in sorted(drift):
        log(f"    {rel}{'' if rel in image else '  (new)'}")
    return drift


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run(cmd, timeout=600, check=True):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError(f"rc={p.returncode}: {' '.join(cmd[:5])}...\n"
                           f"{p.stdout[-1500:]}\n{p.stderr[-1500:]}")
    return p.stdout


def ssh(host, cmd, timeout=600, check=True):
    return run(["ssh", *SSH_OPTS, f"ec2-user@{host}", cmd], timeout=timeout, check=check)


def stage(mgmt):
    log(f"staging {len(HOST_FILES) + 1} files in {HOTFIX_DIR}")
    names = [Path(x).name for x in HOST_FILES]
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise RuntimeError(f"modules share a basename and would collide in the "
                           f"flat staging dir: {dupes}")
    ssh(mgmt, f"sudo mkdir -p {HOTFIX_DIR}/backup && sudo chown ec2-user {HOTFIX_DIR}")
    locals_ = [str(REPO / p) for p in HOST_FILES] + [str(REPO / HOST_CLI)]
    run(["scp", *SSH_OPTS, *locals_, f"ec2-user@{mgmt}:{HOTFIX_DIR}/"], timeout=900)


def mount_services(mgmt):
    for service, own in SERVICES.items():
        mounts = []
        for local, rel in {**SHARED, **own}.items():
            name = Path(local).name
            for base in CONTAINER_PATHS:
                mounts.append(
                    f"--mount-add type=bind,source={HOTFIX_DIR}/{name},"
                    f"target={base}/{rel}")
        # --force matters: a --mount-add whose target is ALREADY mounted is a
        # no-op and does NOT recreate the task, so the service keeps running
        # the module it imported at startup. On 2026-08-20 the chaining fix sat
        # mounted and unused for an hour while the verification passed, because
        # an import-based probe reads the file from DISK, not from the running
        # process's memory. Always recreate, then verify.
        log(f"mounting {len(mounts)} files into {service} (forcing a restart)")
        ssh(mgmt, f"sudo docker service update --quiet {' '.join(mounts)} "
                  f"--force {service}", timeout=900)


def patch_host(mgmt):
    """Refresh the mgmt node's installed copy (originals kept, first copy wins)."""
    log("patching the mgmt host's installed copy")
    cmds = []
    for local, rel in HOST_FILES.items():
        name = Path(local).name
        cmds.append(f"sudo cp -n {HOST_SP}/simplyblock_core/{rel} {HOTFIX_DIR}/backup/{name} || true")
        cmds.append(f"sudo cp {HOTFIX_DIR}/{name} {HOST_SP}/simplyblock_core/{rel}")
    cmds.append(f"sudo cp -n {HOST_SP}/simplyblock_cli/clibase.py {HOTFIX_DIR}/backup/clibase.py || true")
    cmds.append(f"sudo cp {HOTFIX_DIR}/clibase.py {HOST_SP}/simplyblock_cli/clibase.py")
    cmds.append(f"sudo find {HOST_SP}/simplyblock_core {HOST_SP}/simplyblock_cli "
                f"-name __pycache__ -type d -exec rm -rf {{}} + 2>/dev/null || true")
    ssh(mgmt, " ; ".join(cmds), timeout=600)


#: Import the way a service does -- sys.path[0] is the script directory, so
#: site-packages wins. Checking any other way reports the wrong file.
PROBE = (
    'import sys; sys.path[0] = "/app/simplyblock_core/services"; '
    'from simplyblock_core.controllers import snapshot_controller as sc, lvol_controller as lc; '
    'from simplyblock_core.services import snapshot_monitor as sm; '
    'print(sc.__file__); '
    'print("delete_bdev_absent_ok", hasattr(sc, "delete_bdev_absent_ok")); '
    'print("resolve_replication_destination", hasattr(lc, "resolve_replication_destination")); '
    'print("outstanding_internal_snapshot", hasattr(sm, "_outstanding_internal_snapshot"))'
)
REQUIRED = ("delete_bdev_absent_ok True", "resolve_replication_destination True")


def verify(mgmt):
    failures = []
    for service in SERVICES:
        out = ssh(mgmt,
                  f'C=$(sudo docker ps --filter name={service} --format "{{{{.Names}}}}" | head -1); '
                  f'sudo docker exec "$C" python3 -c \'{PROBE}\'', check=False)
        ok = all(r in out for r in REQUIRED)
        log(f"{service}: {'OK' if ok else 'FAILED'}\n{out.strip()}")
        if not ok:
            failures.append(service)
    if failures:
        raise RuntimeError(f"hotfix not live in: {', '.join(failures)}")
    log("VERIFIED: every service imports the hotfixed modules")


def main():
    global DRIFT, HOST_FILES
    meta = json.loads((HERE / "cluster_metadata_repl.json").read_text())
    mgmt = meta["mgmt"]["public_ip"]
    log(f"mgmt={mgmt}")
    if "--verify-only" not in sys.argv:
        # Mount everything that differs from the image, not a hand-kept list.
        DRIFT = discover_drift(mgmt)
        SHARED.update(DRIFT)
        HOST_FILES = dict(SHARED)
        HOST_FILES.update({p: v for sv in SERVICES.values()
                           for p, v in sv.items()})
        stage(mgmt)
        mount_services(mgmt)
        patch_host(mgmt)
        time.sleep(60)          # let the recreated tasks come up
    verify(mgmt)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"FAILED: {exc}")
        sys.exit(1)
