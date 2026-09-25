#!/usr/bin/env python3
"""Bring up a simplyblock cluster through the operator's deployment config.

The operator stopped reconciling StorageNodeSet. Nothing registers a reconciler
for it in operator/cmd/main.go, and the only code that creates a StorageNode is
deployment/expansion.go: a StorageNodeSet now applies cleanly, is never acted
on, and the cluster waits forever for storage nodes that were never asked for.
That is the "no spdk pod came up so the cluster never activated" failure, and it
is not something a longer timeout fixes.

The supported path is a ClusterDeploymentConfig -- a draft describing the whole
deployment, which does nothing until somebody approves it:

    OperatorOps(action: Discover)   inspects the workers and writes a draft
    ClusterDeploymentConfig         the draft, in phase Draft
    spec.approved = true            the gate; the operator then builds
    phase: Expanded                 StorageCluster + one StorageNode per slot

Two properties of that flow decide the shape of this script.

*The draft is immutable once approved* ("an approved deployment config is
immutable", and "approval cannot be withdrawn"). Every value we care about has
to be written before the gate opens, not patched afterwards -- so this reads the
draft back, edits it as one object, replaces it, and only then approves.

*The device class is read off the groups*, not declared. DeviceClassOf() in
expansion.go looks at whether a group lists devices.nvme or devices.block, and
sets the cluster's immutable deviceClass from that. So lblk is not a flag we
set on the cluster: it is a discovery that scanned block devices instead of
NVMe ones, which is deviceFilter.enableLogicalBlockDevices. There is no
deviceMode and no enableLblk in v1alpha2; both were removed.

We raise our own discovery rather than using the one a fresh install performs by
itself. That automatic run is guarded to fire only when no OperatorOps, no
draft and no StorageCluster exist (bootstrap.go), so it is absent exactly when a
job reruns against a half-built namespace; and it runs with default filters,
which report every device including the one the worker boots from.

Environment variables, all optional unless marked:

    NAMESPACE            default simplyblock
    CLUSTER_NAME         default simplyblock-cluster
    ENVIRONMENT          Vanilla|OpenShift|Rancher|K3s|Talos; discovery
                         concludes this itself, and this only overrides it
    WORKER_NODES         whitespace- or comma-separated; empty inspects every
                         schedulable worker
    DEVICE_MODE          nvme (default) or lblk
    NDCS / NPCS          erasure coding stripe
    VCPU_COUNT           required by the CRD
    MAX_SUBSYS           required by the CRD
    MGMT_IFC             management interface name
    DATA_NICS            comma-separated data interfaces
    JM_COUNT             journal managers per node
    JM_PERCENT           percent of each device given to the journal
    ENABLE_JOURNAL_DEVICE  true|false
    FORCE_DRIVE_FORMAT   true|false; 4K reformat on NVMe, wipefs on block
    DRIVE_SIZE_RANGE     e.g. 1.7T-2T
    PCIE_MODEL           NVMe only
    BLOCK_DENY_LIST      comma-separated paths, lblk only; keeps the root disk out
    BLOCK_ALLOW_LIST     comma-separated paths, lblk only
    ENABLE_PARTITIONED   true|false; report devices carrying a partition table
    NODES_PER_SOCKET / SOCKETS_TO_USE
    TIMEOUT_DISCOVERY    seconds, default 900
    TIMEOUT_EXPAND       seconds, default 3600
    DRY_RUN              1 prints what it would do and touches nothing
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

NS = os.environ.get("NAMESPACE", "simplyblock")
DRY_RUN = os.environ.get("DRY_RUN", "") in ("1", "true", "yes")

API = "storage.simplyblock.io/v1alpha2"


def log(msg: str) -> None:
    print(f"[bringup] {msg}", flush=True)


def kubectl(*args: str, check: bool = True, stdin: str | None = None) -> str:
    cmd = ["kubectl", "-n", NS, *args]
    if DRY_RUN:
        # A dry run must not touch the cluster at all, reads included: it is
        # run to check the document this script would build, often from a
        # machine that has no kubectl and no kubeconfig. An empty read reads
        # as "not there", which is the state a dry run is describing anyway.
        log(f"DRY_RUN would run: {' '.join(cmd)}")
        if stdin:
            print(stdin)
        return ""
    try:
        proc = subprocess.run(
            cmd, input=stdin, capture_output=True, text=True,
        )
    except FileNotFoundError:
        raise RuntimeError(
            "kubectl is not on PATH; this script drives the cluster through "
            "it and cannot run without one") from None
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"{' '.join(cmd)} failed ({proc.returncode})\n"
            f"stdout: {proc.stdout.strip()}\nstderr: {proc.stderr.strip()}"
        )
    return proc.stdout


def env_list(name: str) -> list[str]:
    """Split a comma- or whitespace-separated variable, dropping blanks."""
    raw = os.environ.get(name, "") or ""
    return [p for p in raw.replace(",", " ").split() if p]


def env_bool(name: str, default: bool | None = None) -> bool | None:
    raw = (os.environ.get(name, "") or "").strip().lower()
    if raw in ("true", "1", "yes"):
        return True
    if raw in ("false", "0", "no"):
        return False
    return default


def env_int(name: str) -> int | None:
    raw = (os.environ.get(name, "") or "").strip()
    try:
        return int(raw)
    except ValueError:
        return None


def is_lblk() -> bool:
    return (os.environ.get("DEVICE_MODE", "nvme") or "nvme").lower() in (
        "lblk", "logicalblock", "block")


# ── step 1: discovery ────────────────────────────────────────────────────


def build_discovery(name: str) -> dict:
    """The OperatorOps that inspects the workers and writes the draft."""
    lblk = is_lblk()
    device_filter: dict = {}

    # The class is chosen here and nowhere else. Scanning block devices is what
    # makes the draft's groups carry devices.block, which is what makes the
    # cluster LogicalBlock.
    if lblk:
        device_filter["enableLogicalBlockDevices"] = True

    size_range = (os.environ.get("DRIVE_SIZE_RANGE", "") or "").strip()
    if size_range:
        device_filter["driveSizeRange"] = size_range

    # The per-class filters are mutually exclusive with the other class's, so
    # each is only ever sent for the class actually being scanned.
    if lblk:
        allow = env_list("BLOCK_ALLOW_LIST")
        deny = env_list("BLOCK_DENY_LIST")
        if allow:
            device_filter["blockAllowList"] = allow
        if deny:
            device_filter["blockDenyList"] = deny
    else:
        model = (os.environ.get("PCIE_MODEL", "") or "").strip()
        if model:
            device_filter["pcieModel"] = model

    partitioned = env_bool("ENABLE_PARTITIONED")
    if partitioned is not None:
        device_filter["enablePartitionedDevices"] = partitioned

    discover: dict = {
        "configName": name,
        # A storage node is a data path; the default already declines
        # control-plane machines, and this states it so a single-node
        # development cluster fails loudly rather than quietly enrolling etcd.
        "enableControlPlaneNodes": False,
    }
    if device_filter:
        discover["deviceFilter"] = device_filter

    workers = env_list("WORKER_NODES")
    if workers:
        # Named workers rather than a selector: a selector's entries are ANDed,
        # so two hostnames in one selector match nothing at all.
        discover["workers"] = workers

    # Naming an existing cluster turns the run from "create a cluster" into
    # "grow this one". It is how a node is added now: the draft it writes
    # carries the same clusterRef, and the nodes it creates are marked as an
    # expansion, which the control plane reads as a request to rebalance onto
    # them rather than to treat them as part of an initial layout.
    cluster_ref = (os.environ.get("CLUSTER_REF", "") or "").strip()
    if cluster_ref:
        discover["clusterRef"] = cluster_ref

    return {
        "apiVersion": API,
        "kind": "OperatorOps",
        "metadata": {"name": f"e2e-discover-{name}", "namespace": NS},
        "spec": {"action": "Discover", "discover": discover},
    }


def run_discovery(config_name: str, timeout: int) -> str:
    op = build_discovery(config_name)
    op_name = op["metadata"]["name"]

    existing = kubectl("get", "operatorops", op_name, "-o", "json", check=False)
    if existing.strip():
        log(f"discovery {op_name} already exists; reusing it")
    else:
        log(f"raising discovery {op_name}")
        log(json.dumps(op["spec"], indent=2))
        kubectl("apply", "-f", "-", stdin=json.dumps(op))

    if DRY_RUN:
        return config_name

    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        out = kubectl("get", "operatorops", op_name, "-o", "json", check=False)
        if out.strip():
            st = json.loads(out).get("status", {})
            phase = st.get("phase", "")
            msg = st.get("message", "")
            if (phase, msg) != last:
                log(f"discovery phase={phase or '-'} {msg}")
                last = (phase, msg)
            ref = st.get("configRef")
            if ref:
                log(f"discovery wrote ClusterDeploymentConfig {ref}")
                return ref
            if phase == "Failed":
                raise RuntimeError(f"discovery failed: {msg}")
        time.sleep(10)
    raise RuntimeError(
        f"discovery did not produce a config within {timeout}s (phase={last})")


# ── step 2: edit the draft ───────────────────────────────────────────────


def shape_draft(cfg: dict) -> dict:
    """Write our parameters into the draft, before anyone approves it."""
    spec = cfg.setdefault("spec", {})

    # A growth document names the cluster it grows and describes no new one.
    # Writing a cluster template into it as well is how a draft ends up both
    # creating and joining, which the expansion refuses.
    if spec.get("clusterRef"):
        return shape_growth(spec, cfg)

    cluster = spec.setdefault("cluster", {})
    cluster["name"] = os.environ.get("CLUSTER_NAME", "simplyblock-cluster")

    for key, var in (("vcpuCount", "VCPU_COUNT"),
                     ("maxSubsystemCount", "MAX_SUBSYS"),
                     ("nodesPerSocket", "NODES_PER_SOCKET")):
        val = env_int(var)
        if val is not None:
            cluster[key] = val

    ndcs, npcs = env_int("NDCS"), env_int("NPCS")
    if ndcs is not None or npcs is not None:
        stripe = cluster.setdefault("stripe", {})
        if ndcs is not None:
            stripe["dataChunks"] = ndcs
        if npcs is not None:
            stripe["parityChunks"] = npcs

    sockets = env_list("SOCKETS_TO_USE")
    if sockets:
        cluster["socketsToUse"] = sockets

    jd = env_bool("ENABLE_JOURNAL_DEVICE")
    if jd is not None:
        cluster["enableJournalDevice"] = jd

    # One field, two operations. buildWorkload resolves it to enableFormat4K on
    # an NVMe cluster and enableBlockFormat on a block one, because reformatting
    # a namespace and wiping a partition table are not the same act.
    fmt = env_bool("FORCE_DRIVE_FORMAT")
    if fmt is not None:
        cluster["enableDriveFormat"] = fmt

    env_name = (os.environ.get("ENVIRONMENT", "") or "").strip()
    if env_name:
        # Discovery concludes this itself; overriding is for the case where it
        # guessed a distribution we know better than.
        spec["environment"] = env_name

    shape_groups(spec)

    return cfg


def shape_groups(spec: dict) -> None:
    """Write the interfaces and journal layout into every group.

    Shared by both paths: a growth document has groups too, and its nodes need
    the same interfaces as the ones already in the cluster.
    """
    # Interfaces are stated per group, and buildWorkload carries the first
    # group's onto the cluster -- a DaemonSet is one object and cannot differ
    # per group. Writing them into every group is how discovery writes a draft,
    # and keeps the document meaning what it looks like.
    mgmt = (os.environ.get("MGMT_IFC", "") or "").strip()
    data = env_list("DATA_NICS")
    jm_count, jm_pct = env_int("JM_COUNT"), env_int("JM_PERCENT")

    groups = 0
    for node_set in spec.get("nodeSets") or []:
        for group in node_set.get("groups") or []:
            groups += 1
            if mgmt:
                group["mgmtInterface"] = mgmt
            if data:
                group["dataInterfaces"] = data
            if jm_count is not None or jm_pct is not None:
                jm = group.setdefault("journalManager", {})
                if jm_count is not None:
                    jm["count"] = jm_count
                if jm_pct is not None:
                    jm["percentPerDevice"] = jm_pct

    if not groups:
        raise RuntimeError(
            "the draft has no groups: discovery found no worker with a usable "
            "device. Check the device filter -- a driveSizeRange or pcieModel "
            "that matches nothing produces exactly this.")



def shape_growth(spec: dict, cfg: dict) -> dict:
    """Edit a growth document: only the groups are ours to set."""
    log(f"growth document for existing cluster {spec['clusterRef']}")
    shape_groups(spec)
    return cfg


def describe(cfg: dict) -> None:
    """Say what is about to be approved, in the terms that decide the cluster."""
    spec = cfg.get("spec", {})
    cluster = spec.get("cluster") or {}
    nvme = block = 0
    workers: list[str] = []
    for node_set in spec.get("nodeSets") or []:
        for group in node_set.get("groups") or []:
            devs = group.get("devices") or {}
            per = len(devs.get("nvme") or []) or len(devs.get("block") or [])
            n = len(group.get("workers") or [])
            workers += group.get("workers") or []
            if devs.get("block"):
                block += per * n
            else:
                nvme += per * n
    stripe = cluster.get("stripe") or {}
    log("draft to approve:")
    log(f"  cluster        {cluster.get('name')}")
    log(f"  environment    {spec.get('environment')}")
    log(f"  stripe         {stripe.get('dataChunks')}+{stripe.get('parityChunks')}")
    log(f"  vcpu/subsys    {cluster.get('vcpuCount')}/{cluster.get('maxSubsystemCount')}")
    log(f"  device class   {'LogicalBlock' if block else 'NVMe'} "
        f"({block or nvme} device(s) across {len(workers)} worker slot(s))")
    log(f"  workers        {', '.join(sorted(set(workers)))}")


# ── step 3: approve and wait ─────────────────────────────────────────────


def approve_and_wait(name: str, timeout: int) -> None:
    log(f"approving {name}")
    kubectl("patch", "clusterdeploymentconfig", name, "--type=merge",
            "-p", json.dumps({"spec": {"approved": True}}))
    if DRY_RUN:
        return

    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        out = kubectl("get", "clusterdeploymentconfig", name, "-o", "json",
                      check=False)
        if out.strip():
            st = json.loads(out).get("status", {})
            phase = st.get("phase", "")
            step = (st.get("step") or {}).get("state", "")
            msg = st.get("message", "")
            cur = f"{phase}/{step}: {msg}"
            if cur != last:
                log(f"  {cur}")
                last = cur
            if phase == "Expanded":
                log(f"cluster {st.get('clusterRef')} deployed; "
                    f"{len(st.get('nodeRefs') or [])} storage node(s)")
                return
            if phase == "Failed":
                raise RuntimeError(f"deployment failed: {msg}")
        time.sleep(15)
    raise RuntimeError(f"deployment did not finish within {timeout}s ({last})")


def main() -> int:
    config_name = os.environ.get(
        "CDC_NAME", f"e2e-{os.environ.get('CLUSTER_NAME', 'simplyblock-cluster')}")

    ref = run_discovery(config_name,
                        int(os.environ.get("TIMEOUT_DISCOVERY", "900")))

    if DRY_RUN:
        log("DRY_RUN: no draft to read back")
        describe(shape_draft({"spec": {"cluster": {}, "nodeSets": [
            {"name": "dry", "groups": [{"name": "g", "workers": ["w"],
                                        "devices": {"nvme": ["0000:01:00.0"]}}]}]}}))
        return 0

    raw = kubectl("get", "clusterdeploymentconfig", ref, "-o", "json")
    cfg = json.loads(raw)

    if (cfg.get("spec") or {}).get("approved"):
        log(f"{ref} is already approved; waiting on it rather than editing "
            f"(an approved document is immutable)")
        approve_and_wait(ref, int(os.environ.get("TIMEOUT_EXPAND", "3600")))
        return 0

    cfg = shape_draft(cfg)
    describe(cfg)

    # Replace rather than patch: a merge patch replaces whole lists anyway, and
    # sending the object we just read keeps the groups discovery found intact.
    cfg.get("metadata", {}).pop("managedFields", None)
    cfg.pop("status", None)
    log(f"writing the edited draft back to {ref}")
    kubectl("replace", "-f", "-", stdin=json.dumps(cfg))

    approve_and_wait(ref, int(os.environ.get("TIMEOUT_EXPAND", "3600")))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:                            # noqa: BLE001
        log(f"FAILED: {exc}")
        sys.exit(1)
