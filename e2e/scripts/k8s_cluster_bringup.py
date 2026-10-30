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
NVMe ones, which is discover.enableLogicalBlockDevices. There is no
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
    BLOCK_DENY_LIST      comma-separated paths, lblk only. Unset by default:
                         the root disk needs no help being excluded (it probes
                         as Mounted/Busy/Partitioned), and a fixed name like
                         /dev/sda is a data disk on some workers and the OS
                         disk on others
    BLOCK_DEVICES        comma-separated paths to use as block devices, e.g.
                         /dev/nvme0n1,/dev/nvme1n1. Setting it SKIPS discovery
                         and authors the document directly, for the case where
                         discovery will not propose the hardware the lab has --
                         see the RCA of 2026-09-25. Needs WORKER_NODES.
    BLOCK_ALLOW_LIST     comma-separated paths, lblk only
    ENABLE_PARTITIONED   true|false; report devices carrying a partition table
    NODES_PER_SOCKET / SOCKETS_TO_USE
    TIMEOUT_DISCOVERY    seconds, default 900
    TIMEOUT_EXPAND       seconds, default 1800 (30 min)
    TIMEOUT_CP_READY     seconds to wait for ControlPlane Available
                         before approving, default 1800
    DRY_RUN              1 prints what it would do and touches nothing
    DRAFT_ONLY           1 discovers and writes the draft, but does not
                         approve it, so it can be reviewed or hand-edited
    APPROVE_ONLY         name of an existing draft: approve that and wait,
                         changing none of its contents
    KUBECTL_ATTEMPTS     tries per kubectl call (default 4)
    DISCOVERY_ATTEMPTS   discovery runs before giving up (default 3)
    FABRIC_TYPE          default tcp; no CRD default and immutable once set
    SPDK_IMAGE           SPDK image every node should start. The deployment
                         config has no field for it, so this is applied by
                         setting SIMPLY_BLOCK_SPDK_ULTRA_IMAGE on the control
                         plane before approval, and verified afterwards.
                         Empty means no pin: the images are reported, not
                         enforced.
    CP_API_DEPLOYMENT    deployment serving add_node, default
                         simplyblock-webappapi; only used to apply SPDK_IMAGE
    CP_NAMESPACE         where the ControlPlane lives, default NAMESPACE.
                         One operator and one control plane serve a whole
                         Kubernetes cluster, so a second StorageCluster in a
                         second namespace has none of its own to wait for --
                         see operator/internal/upgrade/scope.go. Only the
                         readiness wait and its dump use this; everything
                         else is genuinely namespaced.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time

NS = os.environ.get("NAMESPACE", "simplyblock")

#: The namespace holding the ControlPlane, which is not always this one. The
#: operator's manager caches no namespace and its RBAC is a ClusterRole, so
#: one installation reconciles every namespace; a two-cluster topology is one
#: control plane and two StorageClusters, not two of each.
CP_NS = os.environ.get("CP_NAMESPACE", "").strip() or NS
DRY_RUN = os.environ.get("DRY_RUN", "") in ("1", "true", "yes")

API = "storage.simplyblock.io/v1alpha2"

#: How many times one kubectl call is tried before giving up.
KUBECTL_ATTEMPTS = int(os.environ.get("KUBECTL_ATTEMPTS", "4"))

#: How many times discovery is raised before giving up. Each attempt writes a
#: fresh document, because a failed run's own document is its record.
DISCOVERY_ATTEMPTS = int(os.environ.get("DISCOVERY_ATTEMPTS", "3"))

#: Errors worth trying again. Everything else is the answer, not a hiccup.
_TRANSIENT = (
    "connection refused",
    "timeout",
    "timed out",
    "temporarily unavailable",
    "too many requests",
    "etcdserver",
    "the server is currently unable",
    "unable to connect to the server",
    "no route to host",
    "eof",
    "tls handshake",
    "webhook",            # the conversion webhook may not be serving yet
)

#: ...but a webhook that ANSWERED and said no is not transient, and retrying
#: it three times buries the one sentence that explains the run. The
#: approval denial on 2026-10-19 ("spec.cluster.stripe is 2+2, which needs at
#: least 6 storage nodes ... and this document produces 2") was reported as a
#: "transient kubectl failure" four times over 30s before the reason showed.
_DENIED = ("denied the request",)


def _transient(stderr: str) -> bool:
    low = (stderr or "").lower()
    if any(m in low for m in _DENIED):
        return False
    return any(m in low for m in _TRANSIENT)



def log(msg: str) -> None:
    print(f"[bringup] {msg}", flush=True)


def kubectl(*args: str, check: bool = True, stdin: str | None = None,
            ns: str | None = None) -> str:
    cmd = ["kubectl", "-n", ns or NS, *args]
    if DRY_RUN:
        # A dry run must not touch the cluster at all, reads included: it is
        # run to check the document this script would build, often from a
        # machine that has no kubectl and no kubeconfig. An empty read reads
        # as "not there", which is the state a dry run is describing anyway.
        log(f"DRY_RUN would run: {' '.join(cmd)}")
        if stdin:
            print(stdin)
        return ""
    last = None
    for attempt in range(1, KUBECTL_ATTEMPTS + 1):
        try:
            proc = subprocess.run(
                cmd, input=stdin, capture_output=True, text=True,
            )
        except FileNotFoundError:
            raise RuntimeError(
                "kubectl is not on PATH; this script drives the cluster "
                "through it and cannot run without one") from None

        if proc.returncode == 0:
            return proc.stdout

        last = (f"{' '.join(cmd)} failed ({proc.returncode})\n"
                f"stdout: {proc.stdout.strip()}\n"
                f"stderr: {proc.stderr.strip()}")

        # Retry only what a retry can fix. A rejected document is rejected on
        # every attempt, and repeating it buries the reason under identical
        # noise; an apiserver that is rolling, throttling or briefly
        # unreachable is the case this exists for.
        if not _transient(proc.stderr) or attempt == KUBECTL_ATTEMPTS:
            break
        wait = 5 * attempt
        log(f"transient kubectl failure (attempt {attempt}/"
            f"{KUBECTL_ATTEMPTS}), retrying in {wait}s: "
            f"{proc.stderr.strip()[:120]}")
        time.sleep(wait)

    if check:
        raise RuntimeError(last)
    return ""


def kubectl_cp(*args):
    """kubectl_out against CP_NS rather than NS.

    Identical to kubectl_out when the two are the same, which is every
    single-cluster run.
    """
    cmd = ["kubectl", "-n", CP_NS, *args]
    if DRY_RUN:
        return "", ""
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        return "", "kubectl is not on PATH"
    return proc.stdout.strip(), proc.stderr


def kubectl_out(*args):
    """kubectl read that returns (stdout, stderr) instead of swallowing both.

    kubectl() returns "" for both "no such object" and "the call failed", which
    is fine where the caller only wants the happy path and hides the cause
    where it does not.
    """
    cmd = ["kubectl", "-n", NS, *args]
    if DRY_RUN:
        return "", ""
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        return "", "kubectl is not on PATH"
    return proc.stdout.strip(), proc.stderr


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


def resolve_workers(names):
    """Map the names we were given onto node names the cluster actually has.

    On OpenShift a node is named by its FQDN -- worker-1.ocp.simplyblock.ai --
    and both metadata.name and the kubernetes.io/hostname label carry it. A
    worker_nodes input of "worker-1" therefore names nothing.

    Discovery accepts that list and writes it into the draft. The expansion
    then creates StorageNodes with workerNode: worker-1, whose pods can never
    be scheduled, and the document sits in Activating until its hour is up.
    The run fails after 60 minutes with a message about the control plane,
    which is not the problem -- the problem is a worker that does not exist,
    and it was knowable in the first second.

    So: exact match wins; failing that a unique name that starts with
    "<given>." is taken as the same machine written short. Anything left over
    is fatal here rather than an hour from now.
    """
    if not names:
        return names

    out = kubectl("get", "nodes", "-o",
                  "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
                  check=False)
    actual = [n.strip() for n in (out or "").splitlines() if n.strip()]
    if not actual:
        log("WARNING: could not list nodes, so worker names go through "
            "unchecked")
        return names

    resolved, unknown = [], []
    for name in names:
        if name in actual:
            resolved.append(name)
            continue
        matches = [a for a in actual if a.startswith(name + ".")]
        if len(matches) == 1:
            log(f"worker {name!r} -> {matches[0]!r}")
            resolved.append(matches[0])
        elif matches:
            unknown.append(f"{name} (ambiguous: {', '.join(sorted(matches))})")
        else:
            unknown.append(name)

    if unknown:
        raise RuntimeError(
            "these workers do not exist on this cluster: "
            + ", ".join(unknown)
            + f".\nNodes present: {', '.join(sorted(actual))}.\n"
            "Discovery would accept the names and write them into the draft, "
            "and the expansion would then wait out its full hour on "
            "StorageNodes whose pods can never be scheduled.")
    return resolved


def is_lblk() -> bool:
    return (os.environ.get("DEVICE_MODE", "nvme") or "nvme").lower() in (
        "lblk", "logicalblock", "block")


# ── step 1: discovery ────────────────────────────────────────────────────


def build_discovery(name: str) -> dict:
    """The OperatorOps that inspects the workers and writes the draft."""
    lblk = is_lblk()
    device_filter: dict = {}

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

    discover: dict = {
        "configName": name,
        # A storage node is a data path; the default already declines
        # control-plane machines, and this states it so a single-node
        # development cluster fails loudly rather than quietly enrolling etcd.
        "enableControlPlaneNodes": False,
    }

    # The class is chosen here and nowhere else. Scanning block devices is what
    # makes the draft's groups carry devices.block, which is what makes the
    # cluster LogicalBlock.
    #
    # On spec.discover, NOT under deviceFilter. It was written one level too
    # deep and the apiserver rejects the document outright, so every lblk k8s
    # bring-up failed all three discovery attempts with
    #   strict decoding error: unknown field
    #   "spec.discover.deviceFilter.enableLogicalBlockDevices"
    # Strict decoding is doing us a favour here: a tolerant apiserver would
    # have dropped the field and silently given us an NVMe cluster under an
    # lblk name.
    if lblk:
        discover["enableLogicalBlockDevices"] = True

    # The same one-level-too-deep bug as above, and it stayed dormant
    # because nothing set ENABLE_PARTITIONED until a k8s run needed it.
    # Run 38224849247 failed all three discovery attempts in 0.13s each:
    #   strict decoding error: unknown field
    #   "spec.discover.deviceFilter.enablePartitionedDevices"
    #
    # It belongs on spec.discover for the reason the operator's own type
    # gives: it "is a statement about the run rather than a member of
    # DeviceFilter [...] it waives an availability condition for whichever
    # class is scanned, and so widens what is reported, where every member
    # of the filter narrows it" (api/v1alpha2/operatorops_types.go).
    partitioned = env_bool("ENABLE_PARTITIONED")
    if partitioned is not None:
        discover["enablePartitionedDevices"] = partitioned

    if device_filter:
        discover["deviceFilter"] = device_filter

    workers = resolve_workers(env_list("WORKER_NODES"))
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


def uncordon_leftovers() -> None:
    """Uncordon workers a previous run left SchedulingDisabled.

    The operator declines a cordoned worker outright -- "is not used: it is
    cordoned" -- so one left over from a run that died between its cordon
    and its uncordon removes a node from discovery. The bring-up then fails
    on stripe width ("produces 5 storage node(s) and the 2+2 stripe needs
    6"), which says nothing about the real cause.

    This is bring-up for a cluster that is about to be built from scratch,
    so no earlier run's cordon is worth honouring. Masters are left alone.

    custom-columns, not jsonpath. The jsonpath this used to carry embedded a
    real newline where kubectl wanted the two characters backslash-n, so
    kubectl answered "unterminated quoted string" and returned nothing on
    every call -- and the result was never checked, so this helper silently
    did nothing from the day it was written. It was added to prevent exactly
    the bring-up failure it then failed to prevent, in run 41037615762.
    custom-columns needs no quoting and cannot fail that way.
    """
    def _cordoned_workers():
        out, err = kubectl_out(
            "get", "nodes", "--no-headers", "-o",
            "custom-columns=:metadata.name,:spec.unschedulable")
        if err and err.strip():
            return None, err.strip()
        names = []
        for line in (out or "").splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1] == "true"                     and "master" not in parts[0]:
                names.append(parts[0])
        return names, ""

    cordoned, err = _cordoned_workers()
    if cordoned is None:
        # Loud. A cleanup helper that cannot see is worse than none: it
        # reports success by saying nothing, and the cost lands on the next
        # run as an unrelated-looking error.
        log(f"WARNING: could not list nodes to check for leftover cordons: "
            f"{err[:200]}")
        return
    if not cordoned:
        log("no leftover cordons")
        return

    for name in cordoned:
        log(f"uncordoning {name}: a previous run left it cordoned, and "
            f"discovery declines a cordoned worker")
        kubectl("uncordon", name, check=False)

    # Say whether it worked rather than assume. A cordon that survives this
    # is the difference between a bring-up and a confusing stripe-width
    # refusal several minutes later.
    still, _ = _cordoned_workers()
    if still:
        log(f"WARNING: still cordoned after uncordon: {', '.join(still)}. "
            f"Discovery will decline these and the stripe check will then "
            f"fail on node count.")
    else:
        log(f"uncordoned {len(cordoned)} leftover worker(s)")


def run_discovery(config_name: str, timeout: int) -> str:
    uncordon_leftovers()
    op = build_discovery(config_name)
    op_name = op["metadata"]["name"]

    existing = kubectl("get", "operatorops", op_name, "-o", "json", check=False)
    if existing.strip():
        prior = json.loads(existing).get("status", {}) or {}
        if prior.get("configRef"):
            log(f"discovery {op_name} already produced "
                f"{prior['configRef']}; reusing it")
        elif prior.get("phase") == "Failed":
            # Reusing a failed run just waits out the timeout on a result that
            # is already in. Clear it so this attempt is a real one.
            log(f"discovery {op_name} previously failed "
                f"({prior.get('message', 'no message')}); deleting and "
                f"raising it again")
            kubectl("delete", "operatorops", op_name, "--ignore-not-found",
                    check=False)
            kubectl("apply", "-f", "-", stdin=json.dumps(op))
        else:
            log(f"discovery {op_name} is still running; waiting on it")
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


#: Registries the CRD's ImageSpec pattern admits. Anything else is refused by
#: the apiserver with a message naming the regex rather than the value, so this
#: is checked here where the offending variable can be named.
_IMAGE_RE = re.compile(
    r"^(quay\.io/simplyblock-io|docker\.io/simplyblock|public\.ecr\.aws/simply-block)"
    r"/[a-z0-9][a-z0-9._-]*:[a-zA-Z0-9][a-zA-Z0-9._-]*(@sha256:[a-f0-9]{64})?$")


def shape_images(spec: dict) -> None:
    """Pin the images the deployment runs, in spec.images.

    This is what replaced StorageNodeSet.spdkImage, and it landed on
    2026-09-27. Before it there was no field at all: a document could not carry
    an SPDK image, the StorageNodes the expansion created came out with
    spec.config.spdkImage empty, and storage_node_ops.py fell back to
    constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE. A run that named a build got the
    default and, until the check below existed, said nothing about it.

    The expansion spends these on two kinds: nodeAgent goes to
    StorageCluster.spec.storageNodes, spdk and spdkProxy onto every
    StorageNode.spec.config, which is why they are per node and can be rolled
    one machine at a time later.

    An earlier version of this script set SIMPLY_BLOCK_SPDK_ULTRA_IMAGE on the
    control-plane Deployment instead, on the reasoning that get_config_var reads
    the environment first. The variable applied and survived, and the nodes
    still came up on the default -- so the add does not read it on this path.
    That workaround is gone; this is the supported field.
    """
    wanted = {
        "spdk": (os.environ.get("SPDK_IMAGE", "") or "").strip(),
        "spdkProxy": (os.environ.get("SPDK_PROXY_IMAGE", "") or "").strip(),
        "nodeAgent": (os.environ.get("NODE_AGENT_IMAGE", "") or "").strip(),
    }
    named = {k: v for k, v in wanted.items() if v}
    if not named:
        return

    bad = [f"{k}={v}" for k, v in named.items() if not _IMAGE_RE.match(v)]
    if bad:
        raise RuntimeError(
            "these images name a registry the CRD does not admit, so the "
            "document would be refused: " + ", ".join(bad) + ".\n"
            "    It takes quay.io/simplyblock-io, docker.io/simplyblock or "
            "public.ecr.aws/simply-block, each with an explicit tag -- a bare "
            "repository like simplyblock/spdk:main-latest is rejected, so the "
            "registry has to be written out.")

    images = spec.setdefault("images", {})
    for slot, ref in named.items():
        # Always, matching the CRD default: every tag this product ships by
        # default is a moving one, and a node added later otherwise runs
        # whatever its kubelet already held.
        images[slot] = {"image": ref, "imagePullPolicy": "Always"}
        log(f"  pinning {slot} -> {ref}")


def strip_orphan_failure_domains(spec: dict) -> None:
    """Drop group failure-domain labels the cluster has no feature for.

    simplyblock-operator f155e686 made discovery seed every group's
    failureDomain from the worker's topology.kubernetes.io/zone. On a fleet
    that is all one zone -- which every OpenShift lab here is, all six
    workers carrying zone=default -- that labels every group and then
    declines to enable failure-domain mode, because the control plane only
    activates it with at least parityChunks+2 distinct domains. The labels
    stay behind.

    Downstream, nothing rechecks the flag. The deployment controller indexes
    the labels into StorageCluster.status.failureDomains, the node controller
    sends that index as failure_domain on every node add, and the control
    plane refuses an explicit domain on a cluster created without the
    feature::

        --failure-domain was given but this cluster does not have the
        failure-domain feature enabled.

    So every node_add returns False, retries eleven times and gives up; no
    node is ever added; and the deployment reports nothing but "Activating"
    until it times out. That is run 39675850279 attempts 3 and 4, 1h20m, and
    a lab left unusable for nine hours.

    Removing the label here is the whole fix from this side: with no label on
    any group, failureDomainsOf() is empty, mapFailureDomains() returns
    early, no index exists, and the node add carries no failure_domain --
    which is exactly the shape every bring-up had before f155e686.

    A draft that really does enable the feature is left alone, labels and
    all: there the domains are the point.
    """
    if ((spec.get("cluster") or {}).get("enableFailureDomains")) is True:
        return
    stripped = {}
    for node_set in spec.get("nodeSets") or []:
        for group in node_set.get("groups") or []:
            domain = (group.pop("failureDomain", "") or "").strip()
            if domain:
                stripped.setdefault(domain, []).extend(
                    group.get("workers") or [])
    if not stripped:
        return
    for domain, workers in sorted(stripped.items()):
        log(f"dropping failureDomain {domain!r} from {len(workers)} worker(s): "
            f"the draft does not enable failure-domain mode, and the control "
            f"plane refuses a node add that names a domain on a cluster "
            f"without the feature")
    log("  (operator-side defect: discovery labels the groups from "
        "topology.kubernetes.io/zone and the index is sent regardless of "
        "spec.cluster.enableFailureDomains)")


def shape_draft(cfg: dict) -> dict:
    """Write our parameters into the draft, before anyone approves it."""
    spec = cfg.setdefault("spec", {})

    # Before the growth branch: spdk and spdkProxy are written per StorageNode,
    # so a document that only adds nodes still pins what those nodes run.
    shape_images(spec)

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

    # How many workers may be added at once. The CRD defaults this to 1, which
    # adds nodes strictly serially -- four nodes then take four node-adds
    # end to end, and every minute of that is another minute for a worker to
    # reboot or a controller to drop underneath the deployment. The dev's own
    # GCP pipelines pin it to the worker count for the same reason.
    budget = env_int("NODE_PROVISIONING_BUDGET")
    if budget is None:
        budget = len(env_list("WORKER_NODES")) or None
    if budget:
        cluster["nodeProvisioningBudget"] = budget

    jd = env_bool("ENABLE_JOURNAL_DEVICE")
    if jd is not None:
        cluster["enableJournalDevice"] = jd

    # No default in the CRD and immutable on the cluster, so an unset fabric is
    # not a value the cluster can be corrected to later -- it is a cluster that
    # serves volumes over nothing, permanently. Discovery does not fill it in.
    cluster["fabricType"] = (
        os.environ.get("FABRIC_TYPE", "") or "tcp").strip()

    # One field, two operations. buildWorkload resolves it to enableFormat4K on
    # an NVMe cluster and enableBlockFormat on a block one, because reformatting
    # a namespace and wiping a partition table are not the same act.
    fmt = env_bool("FORCE_DRIVE_FORMAT")
    if fmt is not None:
        cluster["enableDriveFormat"] = fmt

    # Where backups go. Stated on the document, not patched onto the cluster
    # after approval, because a store that arrives late never reaches the
    # control plane at all: the operator sends backup_config only from
    # creationParams, on the cluster-add call, and the control-plane API
    # exposes no setter for it -- /clusters/{id}/backup_config is GET only.
    # The CR then looks entirely correct while every backup fails with
    # "Cluster <uuid> has no backup configuration", which is how the
    # 2026-10-16 backup lane lost all 26 of its classes.
    #
    # The CRD says the same in its own words: the field is here so that "a
    # store stated on the document is present when the cluster is created
    # rather than patched in afterward by whoever remembers".
    #
    # The Secret is deliberately not resolved here. It is a core object a
    # deployment legitimately creates alongside the document or after it, and
    # the cluster's own creation is where its absence is reported.
    bucket = (os.environ.get("BACKUP_BUCKET", "") or "").strip()
    endpoint = (os.environ.get("BACKUP_ENDPOINT", "") or "").strip()
    secret = (os.environ.get("BACKUP_CREDENTIALS_SECRET", "") or "").strip()
    if bucket or endpoint or secret:
        missing = [name for name, val in (("BACKUP_BUCKET", bucket),
                                          ("BACKUP_ENDPOINT", endpoint),
                                          ("BACKUP_CREDENTIALS_SECRET", secret))
                   if not val]
        if missing:
            # The CRD requires all three. Refusing here beats a draft rejected
            # at admission, and beats a cluster that comes up perfectly and
            # cannot back anything up.
            raise SystemExit("backup store is half-stated: "
                             + ", ".join(missing)
                             + " unset. Set all three, or none.")
        cluster["backup"] = {
            "bucket": bucket,
            "endpoint": endpoint,
            "credentialsSecretRef": {"name": secret},
        }
        region = (os.environ.get("BACKUP_REGION", "") or "").strip()
        if region:
            cluster["backup"]["region"] = region

    env_name = (os.environ.get("ENVIRONMENT", "") or "").strip()
    if env_name:
        # Discovery concludes this itself; overriding is for the case where it
        # guessed a distribution we know better than.
        spec["environment"] = env_name

    shape_groups(spec)
    strip_orphan_failure_domains(spec)

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

    groups = devices = 0
    for node_set in spec.get("nodeSets") or []:
        for group in node_set.get("groups") or []:
            groups += 1
            devs = group.get("devices") or {}
            devices += len(devs.get("nvme") or []) + len(devs.get("block") or [])
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

    # Groups can exist and still name nothing. Approving that builds nodes that
    # hand over no storage, and the failure surfaces much later as a cluster
    # that will not activate.
    if devices == 0:
        raise RuntimeError(
            f"the draft has {groups} group(s) but names no devices at all. "
            f"The nodes it describes would hand over nothing. Check what the "
            f"probe refused: kubectl -n {NS} describe operatorops <run>")
    log(f"draft names {devices} device(s) across {groups} group(s)")

    # lblk needs two units per node, and the draft already says whether it has
    # them. LBLK_MIN_DEVICES_PER_NODE is 2 (constants.py), enforced by
    # node_configure.py inside the storage-node init container:
    #
    #     lblk mode requires at least 2 partitions or SSDs per node;
    #     only 1 eligible unit(s) selected: ['sdd']
    #
    # That container then crash-loops, so the storage-node API never starts,
    # so every StorageNode sits at CheckingHost reporting HostUnreachable until
    # its 30-minute deadline expires. The run on 2026-09-28 spent half an hour
    # arriving at a conclusion the draft above had already stated: one device
    # per group.
    #
    # Checked per group rather than per node because a group is the unit that
    # carries devices, and every worker in it gets that same list.
    thin = [(g.get("name") or "?", g.get("workers") or [], blk)
            for ns_ in spec.get("nodeSets") or []
            for g in ns_.get("groups") or []
            if len(blk := ((g.get("devices") or {}).get("block") or [])) == 1]
    if thin:
        detail = "\n    ".join(
            f"{name}: {len(dev)} device {dev} for worker(s) {', '.join(wrk)}"
            for name, wrk, dev in thin)
        raise RuntimeError(
            f"{len(thin)} group(s) name only one block device, and lblk "
            f"requires two per node:\n    " + detail + "\n"
            f"    Approving this builds nodes whose init container refuses to "
            f"configure them, and they fail at CheckingHost 30 minutes later.\n"
            f"    What the probe refused, and why, is in the node reports:\n"
            f"      kubectl -n {NS} get cm -l "
            f"storage.simplyblock.io/component=nodeprobe -o name\n"
            f"      kubectl -n {NS} get cm <name> -o "
            f"jsonpath='{{.data.report\\.json}}'"
            f" | python3 -m json.tool | grep -A3 rejections\n"
            f"    A disk rejected as NotBlank carries a filesystem or foreign "
            f"signature; discovery has no override for that, so it has to be "
            f"wiped before it can be offered.")



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
    # Printed even when there is none: a backup lane that reaches approval
    # with "<none>" here is the failure, and this is the last point before
    # the document becomes immutable.
    backup = cluster.get("backup") or {}
    log(f"  backup store   {backup.get('bucket') or '<none>'}"
        f"{' @ ' + backup['endpoint'] if backup.get('endpoint') else ''}")


# ── step 3: approve and wait ─────────────────────────────────────────────


# The Deployment the task runners share, the container of it that runs
# node_add, and how much of that container's log is worth reading: one line
# per attempt plus the control plane's own reason above it, and a node retried
# eleven times produces a few dozen.
TASKS_DEPLOY = "simplyblock-tasks"
NODE_ADD_CONTAINER = "tasks-node-add-runner"
NODE_ADD_LOG_LINES = 400


def node_add_failures(limit: int = 8) -> list[str]:
    """Why the control plane refused the node_add, in its own words.

    This is the gap 2026-10-23 exposed. Five StorageNodes sat in AwaitingSlot
    behind one in Resolving for the full 1800s; the operator emits nothing
    while a node_add is still in the control plane's task window, so the
    bring-up log had thirty minutes of silence and then a phase name. The
    reason was in this container the whole time, one line per attempt::

        ERROR: --failure-domain was given but this cluster does not have the
        failure-domain feature enabled. Redeploy the cluster with
        --enable-failure-domain to use failure domains.
        INFO: Node add result: False

    "Node add result: False" is all the operator ever sees -- add_node returns
    a bool -- so the line *above* it is the diagnosis and the only place it
    exists is here. Read it rather than leaving it on a lab the next run wipes.

    Returns the newest distinct reasons, oldest first, each with how many
    adds it failed and how many it exhausted. Best-effort: this runs on a
    path that is already reporting a problem.
    """
    out = kubectl("logs", f"deploy/{TASKS_DEPLOY}", "-c", NODE_ADD_CONTAINER,
                  f"--tail={NODE_ADD_LOG_LINES}", check=False)
    reason = ""
    attempts: dict[str, int] = {}
    gave_up: dict[str, set[str]] = {}
    order: list[str] = []
    for line in (out or "").splitlines():
        body = line.split(": ", 3)[-1].strip()
        if ": ERROR: " in line and "tasks-runner-node-add:" not in line:
            # The control plane's own complaint, logged just before add_node
            # returns False. It stays current until the next one: the "gave
            # up" line comes several attempts later and has none of its own.
            reason = body
            continue
        if "tasks-runner-node-add:" not in line:
            continue
        if " failed: " not in line and " gave up: " not in line:
            continue
        key = reason or body
        if key not in attempts:
            order.append(key)
            gave_up[key] = set()
        attempts[key] = attempts.get(key, 0) + 1
        if " gave up: " in line:
            task = body.split("task ", 1)[-1].split(" ", 1)[0]
            gave_up[key].add(task)

    lines = []
    for key in order[-limit:]:
        tally = f"{attempts[key]} attempt(s)"
        if gave_up[key]:
            tally += f", {len(gave_up[key])} add(s) gave up after 11 retries"
        lines.append(f"{key}  [{tally}]")
    return lines


def node_steps() -> list[tuple[str, str, str, str]]:
    """(worker, phase, step, message) for every StorageNode, worker order.

    The STEP column is where a stalled deployment says what it is doing, and
    it was the one thing the wait never printed. One node in Resolving with
    five in AwaitingSlot is a queue behind a single failing add; six in
    CheckingHost is a fleet that cannot be reached; and the two look identical
    from the deployment config's phase, which says Activating for both.
    """
    out = kubectl("get", "storagenode", "-o",
                  "jsonpath={range .items[*]}{.spec.workerNode}{\"\\t\"}"
                  "{.status.phase}{\"\\t\"}{.status.step.state}{\"\\t\"}"
                  "{.status.message}{\"\\n\"}{end}", check=False)
    rows = []
    for line in (out or "").splitlines():
        if not line.strip():
            continue
        parts = (line.split("\t") + ["", "", "", ""])[:4]
        rows.append(tuple(p.strip() for p in parts))
    return sorted(rows)


def steps_signature(rows) -> str:
    """What has to change for the wait to call the deployment "moving"."""
    return "|".join(f"{w}={p}/{s}" for w, p, s, _ in rows)


def failed_storage_nodes():
    """StorageNodes the operator has given up on, with the reason.

    A node that reaches Failed is not retried -- on 2026-09-25 one sat there
    for three hours while the deployment config waited out its full deadline
    and then reported that the step had timed out, which says nothing about
    which node or why. Its events did: the worker was cordoned and NotReady
    because OpenShift's machine-config operator was rebooting it to apply the
    KubeletConfig the cluster's own creation had just triggered.

    So the wait ends here instead, naming the node. Waiting longer cannot help
    once the operator has stopped trying.
    """
    out = kubectl("get", "storagenode", "-o",
                  "jsonpath={range .items[*]}{.metadata.name}\t"
                  "{.spec.workerNode}\t{.status.phase}\t"
                  "{.status.message}{\"\\n\"}{end}", check=False)
    bad = []
    for line in (out or "").splitlines():
        parts = line.split("\t")
        if len(parts) >= 3 and parts[2].strip() == "Failed":
            bad.append((parts[0], parts[1], (parts[3] if len(parts) > 3
                                             else "").strip()))
    return bad


def worker_state(worker):
    """Ready/schedulable summary for a worker, for the failure message."""
    out = kubectl("get", "node", worker, "-o",
                  "jsonpath={.spec.unschedulable}|"
                  "{range .status.conditions[?(@.type=='Ready')]}{.status}{end}",
                  check=False)
    cordoned, _, ready = (out or "").partition("|")
    return (f"Ready={ready.strip() or '?'}"
            f"{' CORDONED' if cordoned.strip() == 'true' else ''}")


def wait_control_plane_ready(timeout=1800):
    """Block until the ControlPlane reports Available.

    The manual OpenShift deploy gates on this explicitly -- install the chart,
    watch for "the control plane's readiness probe passed", and only then edit
    and approve the draft.

    The first version of this read phase and step through one jsonpath with a
    tab in it and treated any empty result as "not ready yet". When the read
    came back empty it waited the full 1800s and then said

        the control plane did not become Available within 1800s (last )

    which names neither what it saw nor why it saw nothing -- the reading was
    empty every time and the failure could not be told apart from a control
    plane that genuinely never came up. So: read one field at a time, keep the
    stderr, and say plainly whether the object is missing, unreadable, or
    simply not ready.
    """
    deadline = time.time() + timeout
    last = None
    complained = False

    while time.time() < deadline:
        phase, err = kubectl_cp("get", "controlplane", "-o",
                                "jsonpath={.items[0].status.phase}")
        if phase:
            step, _ = kubectl_cp("get", "controlplane", "-o",
                                 "jsonpath={.items[0].status.step.state}")
            reading = f"{phase}/{step or '-'}"
            if reading != last:
                log(f"control plane: {reading}")
                last = reading
            if phase == "Available":
                return
            if phase == "Degraded":
                # Degraded means up-but-something-is-unhealthy, not down. On a
                # namespace whose previous runs failed, tasks-runner-sync-lvol-del
                # crash-loops with "No clusters found!" because there is no
                # cluster yet -- a consequence of the thing we are here to fix,
                # and it would gate us out of ever fixing it. The pod is 16/17
                # ready and the management API answers; node-add lives in a
                # different container.
                #
                # So this proceeds, loudly. Waiting on Available would deadlock
                # against a condition only a successful deploy can clear.
                msg, _ = kubectl_cp("get", "controlplane", "-o",
                                    "jsonpath={.items[0].status.message}")
                log(f"WARNING: control plane is Degraded, proceeding anyway: "
                    f"{msg or 'no message'}")
                return
        elif not complained:
            # Say it once, with whatever the API actually said, rather than
            # silently retrying for half an hour.
            names, _ = kubectl_cp("get", "controlplane", "-o",
                                  "jsonpath={.items[*].metadata.name}")
            if names.strip():
                log(f"control plane {names.strip()} exists but reports no "
                    f"phase yet")
            else:
                log(f"no ControlPlane object in {CP_NS} yet"
                    + (f" (kubectl said: {err.strip()[:160]})" if err.strip()
                       else ""))
            complained = True
        time.sleep(10)

    dump_control_plane_state()
    raise RuntimeError(
        f"the control plane did not reach Available within {timeout}s "
        f"(last reading: {last or 'none -- nothing was ever read'}). "
        f"Approving now would deploy storage nodes against a control plane "
        f"that cannot answer them. The state above was read at the moment "
        f"this gave up; AwaitingFoundationDB holds until the "
        f"FoundationDBCluster reports status.health.available, so start "
        f"with its health line and the pods and claims under it.")


#: How long the deadline may be pushed out in total while OpenShift is
#: rebooting workers underneath us. Generous because a rolling MachineConfig
#: across four workers is drain + reboot + rejoin, one at a time.
MCO_GRACE_TOTAL = int(os.environ.get("TIMEOUT_MCO_GRACE", "3600"))

#: How long every storage node may sit on the same step before the
#: wait says so and goes looking for the reason. Short enough that a
#: 30 minute deadline produces several reports, long enough that the
#: ordinary one-node-at-a-time march through the fleet is not one.
STALL_REPORT_SEC = int(os.environ.get("STALL_REPORT_SEC", "300"))


def mco_updating():
    """MachineConfigPools OpenShift is currently applying, by name.

    Creating a StorageCluster writes a KubeletConfig and a MachineConfigPool,
    and the machine-config operator then drains, reboots and rejoins every
    worker in that pool one at a time. Storage nodes cannot come up while
    their worker is cordoned, so the deployment sits still through all of
    it -- and reports only the phase it is stuck in.

    That is what happened on 2026-10-07: "worker worker-1 is cordoned" at
    20:06, the 1800s deadline fired at 20:33, and the SPDK pods came up at
    21:00 -- the bring-up was still making progress when we gave up on it.

    Columns, not jsonpath: NAME CONFIG UPDATED UPDATING DEGRADED ...
    """
    out = kubectl("get", "machineconfigpool", "--no-headers", check=False)
    rolling = []
    for line in (out or "").splitlines():
        parts = line.split()
        if len(parts) >= 4 and parts[3] == "True":
            rolling.append(parts[0])
    return rolling


def dump_control_plane_state() -> None:
    """Everything worth seeing when the control plane will not come up.

    Written after run 38224849247 spent its full 1800s in
    Installing/AwaitingFoundationDB and failed with one phase string and
    "Check: kubectl -n simplyblock get controlplane" -- against a lab the
    next run wipes, so by the time anyone looks there is nothing to check.
    dump_stall_state already existed for exactly this reason on the
    deployment wait; this is its counterpart on the step before.

    AwaitingFoundationDB holds until the FoundationDBCluster reports
    status.health.available (controlplane/foundationdb.go), so what matters
    is that object, the operator that drives it, and the pods and claims it
    needs -- in that order. A FoundationDB stuck without coordinators looks
    identical from the ControlPlane's phase whether its pods are pending on
    storage, crash-looping on a bad image, or waiting for an operator that
    is not installed.

    Reads only, each independent and best-effort: this runs on a failure
    path and a dump that raises would replace the real error with its own.
    """
    def show(title, *args, limit=40):
        log(f"---- {title}")
        try:
            # CP_NS, not NS: every read below is about the control plane, and
            # on a second cluster that is someone else's namespace.
            out = kubectl(*args, check=False, ns=CP_NS)
        except Exception as exc:                       # noqa: BLE001
            log(f"     (could not read: {str(exc)[:120]})")
            return
        lines = (out or "").strip().splitlines()
        if not lines:
            log("     (nothing)")
            return
        for line in lines[:limit]:
            log(f"     {line}")
        if len(lines) > limit:
            log(f"     ... {len(lines) - limit} more line(s)")

    log("")
    log(f"state at the point the control plane stopped advancing "
        f"(namespace {CP_NS}):")
    show("control plane", "get", "controlplane", "-o", "wide")
    show("control plane status", "get", "controlplane", "-o",
         "jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}"
         "{.status.phase}{\"\\t\"}{.status.step.state}{\"\\t\"}"
         "{.status.message}{\"\\n\"}{end}")
    # The object the step is actually waiting on. Its health block is the
    # condition: available is what releases the wait, fullReplication only
    # downgrades it to a warning.
    show("foundationdb cluster", "get", "foundationdbcluster", "-o", "wide")
    show("foundationdb health", "get", "foundationdbcluster", "-o",
         "jsonpath={range .items[*]}{.metadata.name}"
         "{\"\\tavailable=\"}{.status.health.available}"
         "{\"\\tfullReplication=\"}{.status.health.fullReplication}"
         "{\"\\tdesired=\"}{.status.desiredProcessGroups}"
         "{\"\\treconciled=\"}{.status.reconciledProcessGroups}{\"\\n\"}{end}")
    # Without its operator the cluster object is inert and reports nothing,
    # which from the ControlPlane's side is indistinguishable from slow.
    show("foundationdb operator", "get", "deploy", "-A", "-l",
         "app=fdb-kubernetes-operator", "-o", "wide", limit=10)
    show("pods that are not Running", "get", "pods",
         "--field-selector=status.phase!=Running", "-o", "wide")
    show("pod restarts", "get", "pods", "--no-headers", "-o",
         "custom-columns=NAME:.metadata.name,READY:.status.containerStatuses"
         "[*].ready,RESTARTS:.status.containerStatuses[*].restartCount,"
         "NODE:.spec.nodeName")
    # A classic FoundationDB stall: process groups pending on storage that
    # no StorageClass will bind.
    show("claims that are not Bound", "get", "pvc",
         "--field-selector=status.phase!=Bound", "-o", "wide", limit=20)
    show("recent warnings", "get", "events", "--field-selector=type=Warning",
         "--sort-by=.lastTimestamp", limit=30)
    log("")


def not_ready_row(line: str) -> bool:
    """True for a custom-columns row whose READY column holds a false.

    Running is not Ready, and the field selector only knows about phase. On
    2026-10-23 simplyblock-tasks was Running and 0/1 ready for the whole
    stall, so the "not Running" list -- the only pod view the dump had --
    was empty while the control plane was degraded.
    """
    parts = line.split()
    return len(parts) > 1 and "false" in parts[1]


def dump_stall_state(name: str) -> None:
    """Everything worth seeing when the deployment stops advancing.

    Written after a run sat in Expanding/Activating for the full 1800s and
    then failed with nothing but the phase name. No node had reached Failed,
    so the per-node warning above never fired; the test step never ran, so no
    logs were collected; and the cluster was gone by the time anyone looked.
    That left one phase string as the only evidence a 30 minute wait
    produced. Not enough to act on.

    Reads only, each independent and best-effort: this runs on the failure
    path, and a dump that raises would replace the real error with its own.

    It also writes itself to a file, because the step that collects logs runs
    after the test step and the test step never runs when bring-up fails:
    run 39675850279 ended "RUN_DIR_FILE not found -- skipping" and kept
    nothing at all. save_stall_dump puts the same text somewhere that
    outlives the runner.
    """
    captured: list[str] = []

    def emit(line):
        captured.append(line)
        log(line)

    def show(title, *args, limit=50, keep=None):
        """One kubectl read, printed and captured. keep() drops the rows a
        server-side selector cannot: custom-columns has no field selector, so
        "the pods that are not Ready" is a filter this end or nothing."""
        emit(f"---- {title}")
        try:
            out = kubectl(*args, check=False)
        except Exception as exc:                       # noqa: BLE001
            emit(f"     (could not read: {str(exc)[:120]})")
            return
        lines = (out or "").strip().splitlines()
        if keep:
            lines = [line for line in lines if keep(line)]
        if not lines:
            emit("     (nothing)")
            return
        for line in lines[:limit]:
            emit(f"     {line}")
        if len(lines) > limit:
            emit(f"     ... {len(lines) - limit} more line(s)")

    emit("")
    emit("state at the point the deployment stopped advancing:")
    show("machine config pools (OpenShift reboots workers here)",
         "get", "machineconfigpool", "--no-headers", limit=40)
    show("storage nodes", "get", "storagenode", "-o", "wide")
    # -o wide stops at the step name. The message and the step's own deadline
    # are the two fields that say whether the operator is still trying and
    # how long it has left, and neither is in any column.
    show("storage node steps", "get", "storagenode", "-o",
         "jsonpath={range .items[*]}{.spec.workerNode}{\"\\t\"}"
         "{.status.phase}{\"/\"}{.status.step.state}"
         "{\"\\tdeadline=\"}{.status.step.deadline}"
         "{\"\\ttask=\"}{.status.nodeAddTaskID}"
         "{\"\\t\"}{.status.message}{\"\\n\"}{end}")
    show("storage cluster", "get", "storagecluster", "-o", "wide")
    # status.failureDomains is the label-to-index mapping the operator sends
    # as failure_domain on every node add. A mapping on a cluster whose
    # spec.enableFailureDomains is unset is the 2026-10-23 deadlock, and it
    # is invisible in every other view.
    show("failure-domain mapping vs the feature flag", "get",
         "storagecluster", "-o",
         "jsonpath={range .items[*]}{.metadata.name}"
         "{\"\\tenableFailureDomains=\"}{.spec.enableFailureDomains}"
         "{\"\\tstatus.failureDomains=\"}{.status.failureDomains}"
         "{\"\\n\"}{end}")
    show("control plane", "get", "controlplane", "-o",
         "jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}"
         "{.status.phase}{\"/\"}{.status.step.state}{\"\\t\"}"
         "{.status.message}{\"\\n\"}{end}")
    show("pods that are not Running", "get", "pods",
         "--field-selector=status.phase!=Running", "-o", "wide")
    # Running is not Ready. simplyblock-tasks was Running and 0/1 ready for
    # the whole of the 2026-10-23 stall, so it never appeared above.
    show("pods that are not Ready", "get", "pods", "--no-headers", "-o",
         "custom-columns=NAME:.metadata.name,READY:.status.containerStatuses"
         "[*].ready,RESTARTS:.status.containerStatuses[*].restartCount,"
         "NODE:.spec.nodeName",
         keep=not_ready_row)
    show("recent warnings", "get", "events", "--field-selector=type=Warning",
         "--sort-by=.lastTimestamp", limit=30)
    show(f"clusterdeploymentconfig {name}", "describe",
         "clusterdeploymentconfig", name, limit=80)

    # Last and most important: the only place the control plane's reason for
    # refusing a node_add is written down. The operator reduces it to
    # "Node add result: False" and a deployment that never gets a node
    # reports nothing but its phase.
    emit("---- why node_add failed (control plane, "
         f"{TASKS_DEPLOY}/{NODE_ADD_CONTAINER})")
    try:
        failures = node_add_failures(limit=12)
    except Exception as exc:                           # noqa: BLE001
        failures = []
        emit(f"     (could not read: {str(exc)[:120]})")
    for failure in failures:
        emit(f"     {failure}")
    if not failures:
        emit("     (no node_add failure in the runner's recent log)")
    emit("")
    save_stall_dump(name, captured)


def save_stall_dump(name: str, lines: list[str]) -> None:
    """Keep the dump somewhere the runner's workspace is not.

    The workflow collects logs in a later step keyed on RUN_DIR_FILE, which
    the test step writes -- so a bring-up failure, which is exactly when
    nobody can look at the cluster afterwards, collects nothing. The NFS
    share every lab already mounts is where the rest of a run's evidence
    lives, so the dump goes next to it; the workspace is the fallback, which
    at least survives long enough to be uploaded as an artifact.

    Best-effort by construction: a dump that cannot be saved must not replace
    the failure it was describing.
    """
    stamp = time.strftime("%Y%m%d-%H%M%S")
    run = os.environ.get("GITHUB_RUN_ID", "local")
    base = os.environ.get("NFS_MOUNTPOINT") or os.environ.get(
        "GITHUB_WORKSPACE") or "."
    path = os.path.join(base, f"bringup-stall-{run}-{stamp}.log")
    try:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(f"bring-up stall dump for {name}, run {run}\n")
            handle.write("\n".join(lines) + "\n")
    except Exception as exc:                           # noqa: BLE001
        log(f"could not save the stall dump ({str(exc)[:120]}); it is above")
        return
    log(f"stall dump saved to {path}")


def check_stripe_fits(name: str) -> None:
    """Refuse a draft that cannot satisfy its own stripe, and say why.

    Mirrors the admission rule in vclusterdeploymentconfig: a d+p stripe
    needs d+p nodes to place it across plus 2 spares to rebuild onto. The
    webhook enforces this and the message is a good one -- but it arrives
    at approval, which is the LAST step, after discovery, the control-plane
    wait and every edit. Reading it here turns the same refusal into a
    diagnosis, and names what is missing while the draft is still readable.

    The usual cause is not the stripe. It is the lab: a worker whose NVMe
    devices are no longer bound to the kernel driver reports them as 0B,
    every device rule declines them on size, and the worker silently drops
    out of the draft. The node stays Ready in kubectl the whole time --
    kubelet is healthy, which says nothing about what the disks are bound
    to -- so the cluster looks fine and only the device count shows it is
    not. That is how 2026-10-19 produced 2 slots out of 6 workers.
    """
    raw = kubectl("get", "clusterdeploymentconfig", name, "-o", "json")
    if not raw.strip():
        return
    spec = (json.loads(raw).get("spec") or {})
    cluster = spec.get("cluster") or {}
    stripe = cluster.get("stripe") or {}
    ndcs, npcs = stripe.get("dataChunks"), stripe.get("parityChunks")
    if ndcs is None or npcs is None:
        return

    workers, devices = [], 0
    for node_set in spec.get("nodeSets") or []:
        for group in node_set.get("groups") or []:
            ws = group.get("workers") or []
            workers += ws
            devs = group.get("devices") or {}
            devices += len(devs.get("nvme") or devs.get("block") or []) * len(ws)

    needed = ndcs + npcs + 2
    if len(workers) >= needed:
        return

    log("")
    log(f"REFUSING to approve {name}: the stripe does not fit the draft.")
    log(f"  stripe            {ndcs}+{npcs}, which needs {needed} storage "
        f"nodes ({ndcs + npcs} to place it across, 2 spares to rebuild onto)")
    log(f"  draft produces    {len(workers)} ({devices} device(s))")
    log(f"  workers in draft  {', '.join(sorted(set(workers))) or 'none'}")
    log("")
    log("  Every worker the filter accepted is listed above; the rest were")
    log("  dropped by discovery, NOT by this script. A node being Ready says")
    log("  nothing about its disks. To see the verdict per device:")
    log("")
    log(f"    kubectl -n {NS} logs deploy/simplyblock-operator "
        "| grep -E 'declined by|survived'")
    log("")
    log("  'declined by size range because it is 0B' on a disk that is not")
    log("  0B means the device is not bound to the kernel nvme driver any")
    log("  more, so it exposes no block device. Rebind it on that worker:")
    log("")
    log("    echo <pci-addr> > /sys/bus/pci/drivers/vfio-pci/unbind")
    log("    echo <pci-addr> > /sys/bus/pci/drivers/nvme/bind")
    log("")
    raise SystemExit(
        f"draft {name} produces {len(workers)} storage node(s) and the "
        f"{ndcs}+{npcs} stripe needs {needed}")


def check_failure_domains(name: str) -> None:
    """Refuse a draft that labels fault groups on a cluster that has none.

    This is what cost run 39675850279 its last two attempts, 1h20m, and left
    a lab unusable for nine hours.

    Discovery seeds every group's failureDomain from the worker's
    topology.kubernetes.io/zone (operator internal/discovery/failuredomain.go,
    new in simplyblock-operator f155e686). Every worker of this OpenShift lab
    carries zone=default, so all six were labelled and the draft went to
    enable failure-domain mode -- and then declined to, because the control
    plane only activates that mode with at least parity+2 distinct domains
    and one "default" is one. The decline leaves enableFailureDomains unset
    and the per-group labels in place, which is the state this checks for.

    What follows from it is not recoverable and not obvious. The deployment
    controller indexes the labels into StorageCluster.status.failureDomains
    without consulting enableFailureDomains, the node controller sends that
    index as failure_domain on every node add, and the control plane refuses
    an explicit domain on a cluster created without the feature:

        --failure-domain was given but this cluster does not have the
        failure-domain feature enabled.

    So every node_add returns False, retries eleven times and gives up; no
    node is ever added; the deployment waits out its deadline reporting
    "Activating". Nothing in the deployment config's own phase says why.

    The ordinary path never reaches this: strip_orphan_failure_domains
    removes the labels during the edit pass, before the draft is written
    back. This is the backstop for the paths that do not edit a draft --
    APPROVE_ONLY, and a draft that arrives already approved -- where
    refusing is all that is left, because approval is irreversible.
    """
    raw = kubectl("get", "clusterdeploymentconfig", name, "-o", "json")
    if not raw.strip():
        return
    spec = (json.loads(raw).get("spec") or {})
    enabled = (spec.get("cluster") or {}).get("enableFailureDomains")

    labelled = {}
    for node_set in spec.get("nodeSets") or []:
        for group in node_set.get("groups") or []:
            domain = (group.get("failureDomain") or "").strip()
            if domain:
                labelled.setdefault(domain, []).extend(
                    group.get("workers") or [])
    if not labelled or enabled is True:
        return

    log("")
    log(f"REFUSING to approve {name}: the draft names failure domains on a "
        f"cluster that does not have the feature.")
    log(f"  enableFailureDomains   {enabled!r} (the feature is off)")
    for domain, workers in sorted(labelled.items()):
        log(f"  failureDomain {domain!r}      {len(workers)} worker(s): "
            f"{', '.join(sorted(set(workers)))}")
    log("")
    log("  The operator will still send these as failure_domain on every")
    log("  node add, and the control plane refuses an explicit domain on a")
    log("  cluster created without the feature -- so no node is added, and")
    log("  the deployment reports only 'Activating' until it times out.")
    log("")
    log("  Clear the labels, or give the fleet real domains:")
    log("")
    log(f"    kubectl -n {NS} patch clusterdeploymentconfig {name} "
        "--type=json \\")
    log("      -p '[{\"op\":\"remove\",\"path\":"
        "\"/spec/nodeSets/0/groups/<i>/failureDomain\"}]'")
    log("")
    log("  or relabel the workers so topology.kubernetes.io/zone names at")
    log("  least parityChunks+2 domains holding an equal number of workers,")
    log("  and run discovery again.")
    log("")
    raise SystemExit(
        f"draft {name} labels failure domain(s) "
        f"{', '.join(sorted(labelled))} but enableFailureDomains is not set; "
        f"every node_add would be refused by the control plane")


def approve_and_wait(name: str, timeout: int) -> None:
    # The gate the manual deploy waits on before it edits the draft.
    wait_control_plane_ready(int(os.environ.get("TIMEOUT_CP_READY", "1800")))

    # Before the gate, not after: approval is irreversible and the document
    # becomes immutable, so a draft that cannot work is worth catching while
    # it can still be rebuilt.
    check_stripe_fits(name)
    check_failure_domains(name)

    log(f"approving {name}")
    kubectl("patch", "clusterdeploymentconfig", name, "--type=merge",
            "-p", json.dumps({"spec": {"approved": True}}))
    if DRY_RUN:
        return

    deadline = time.time() + timeout
    last = ""
    reported_failed = set()
    mco_granted = 0.0
    mco_seen = set()
    # The per-node view, which the deployment config's own phase does not
    # carry. Run 39675850279 spent its whole 1800s on one line -- "waiting on
    # Activating" -- while five nodes queued behind a sixth whose add was
    # being refused. The signature is what counts as movement; when it stops
    # changing, say what is holding and read the reason out of the task
    # runner rather than leaving it on the lab.
    last_steps = None
    steps_since = time.time()
    stall_reported = set()
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

        rows = node_steps()
        signature = steps_signature(rows)
        if rows and signature != last_steps:
            last_steps, steps_since = signature, time.time()
            for worker, phase, step, message in rows:
                detail = f"  {message}" if message else ""
                log(f"  node {worker}: {phase}/{step or '-'}{detail}")
        elif rows and time.time() - steps_since >= STALL_REPORT_SEC:
            held = sorted(f"{w} in {s_}" for w, _, s_, _ in rows
                          if s_ not in ("", "AwaitingSlot"))
            waiting = sum(1 for _, _, s_, _ in rows if s_ == "AwaitingSlot")
            mins = int((time.time() - steps_since) // 60)
            log(f"NOTE: no storage node has changed step in {mins}m. "
                f"Holding: {', '.join(held) or 'nothing'}"
                + (f"; {waiting} queued on AwaitingSlot behind it" if waiting
                   else ""))
            # The operator stays silent while the control plane still counts
            # the add as in flight, so its reason only exists in the task
            # runner. Print each one once, as it appears.
            for failure in node_add_failures():
                if failure in stall_reported:
                    continue
                stall_reported.add(failure)
                log(f"      node_add: {failure}")
            steps_since = time.time()

        # The document stays Expanding while a node underneath it has already
        # failed, so watch the nodes too -- but report rather than give up.
        #
        # Failed does look terminal: the handler that sets it releases the
        # node's provisioning slot ("a node that has given up holds nothing")
        # and nothing re-enters provisioning from there. Even so, aborting on
        # it would turn a run that recovers by some other route -- an operator
        # restart, a StorageNodeOps, somebody uncordoning a worker -- into a
        # failure we caused. Saying which node and why, the moment it happens,
        # is the part worth having; the deadline can still decide the outcome.
        for node, worker, why in failed_storage_nodes():
            if node in reported_failed:
                continue
            reported_failed.add(node)
            log(f"WARNING: storage node {node} on worker {worker} has Failed: "
                f"{why or 'no message'}")
            log(f"         worker now: {worker_state(worker)}")
            log(f"         kubectl -n {NS} get events "
                f"--field-selector involvedObject.name={node}")
            log("         still waiting -- usually terminal, but the deadline "
                "decides, not this check")
        # OpenShift is rebooting the workers this cluster needs, because
        # creating the cluster is what asked it to. Nothing can progress
        # until it finishes, so do not spend the deadline on it -- but cap
        # the generosity, or a genuinely stuck pool buys unlimited time.
        rolling = mco_updating()
        if rolling and mco_granted < MCO_GRACE_TOTAL:
            grant = min(15.0, MCO_GRACE_TOTAL - mco_granted)
            deadline += grant
            mco_granted += grant
            for pool in rolling:
                if pool in mco_seen:
                    continue
                mco_seen.add(pool)
                log(f"NOTE: OpenShift is rolling MachineConfigPool {pool}. "
                    f"It drains, reboots and rejoins each worker in turn, and "
                    f"a cordoned worker cannot bring a storage node up. "
                    f"Holding the deadline open for up to {MCO_GRACE_TOTAL}s "
                    f"while it works.")
        elif rolling:
            joined = ", ".join(rolling)
            log(f"WARNING: MachineConfigPool(s) {joined} are still rolling "
                f"after {MCO_GRACE_TOTAL}s of grace; the deadline is running "
                f"again.")
        time.sleep(15)
    dump_stall_state(name)
    raise RuntimeError(
        f"deployment did not finish within {timeout}s ({last}).\n"
        f"This one is not retryable in place: an approved document is "
        f"immutable and approval cannot be withdrawn, so there is nothing to "
        f"edit and re-approve. To start over, delete the "
        f"ClusterDeploymentConfig and the StorageCluster it created, then run "
        f"the bring-up again -- cleanup_k8s.sh does both.")


def verify_spdk_image(wanted: str, timeout: int = 600) -> None:
    """Fail if the SPDK pods are not running the image that was asked for.

    spdkImage lives on the StorageNode (spec.config.spdkImage in v1alpha2,
    spec.overrides.spdkImage in v1alpha1) and not on the deployment config, so
    the document cannot carry it. It is read once, in addParams, at the moment
    postNode adds the node to the control plane -- not re-read on restart. The
    config creates the node objects and an independent reconciler posts them,
    with no pause hook and a provisioning budget whose floor is 1, so there is
    no point after approval at which a patch reliably lands first.

    What is left is to check. A run that asked for a particular SPDK build and
    silently got the default is a wrong green -- the whole point of pinning it
    is that the build under test is the variable -- so this is an error rather
    than a warning.
    """
    # Select the SPDK pods, which are not the node-agent DaemonSet.
    #
    # This asked for "app=storage-node" and got
    # simplyblock-storage-node-ds-<cluster>-<hash>, the DaemonSet that runs the
    # node agent. Those legitimately run simplyblock/simplyblock:main, so every
    # one of them was reported as carrying the wrong SPDK image and a cluster
    # that had deployed correctly failed the gate.
    #
    # storage_deploy_spdk.yaml.j2 names the real pods
    # snode-spdk-pod-<rpc_port>-<cluster_id> and labels them
    # "role: simplyblock-storage-node" plus "app: spdk-app-<rpc_port>". The role
    # is the stable one -- app carries the port, so it differs per node.
    selector = "role=simplyblock-storage-node"

    # Wait for them, rather than reading an empty list as an answer.
    #
    # The SPDK pod is created at node-add, so it appears while the nodes are
    # Activating. approve_and_wait returns at phase Expanded, and the step line
    # at that point is routinely "Expanded/Activating" -- the document is done,
    # the nodes are not. Checking immediately can find nothing.
    #
    # An empty result used to WARN and return, which is the wrong green this
    # function exists to prevent: a run that pinned an image, never checked it,
    # and passed. So the absence of pods is a failure once the wait is spent.
    expected = len(kubectl("get", "storagenode", "-o",
                           "jsonpath={.items[*].metadata.name}",
                           check=False).split())
    deadline = time.time() + timeout
    lines: list[str] = []
    while True:
        out = kubectl(
            "get", "pods", "-l", selector, "-o",
            "jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}"
            "{range .spec.containers[*]}{.image}{\" \"}{end}{\"\\n\"}{end}",
            check=False)
        lines = [ln for ln in out.strip().splitlines() if ln.strip()]
        if lines and len(lines) >= expected:
            break
        if time.time() >= deadline:
            break
        log(f"  waiting for SPDK pods: {len(lines)}/{expected or '?'} up")
        time.sleep(15)

    if not lines:
        raise RuntimeError(
            f"no SPDK pods (-l {selector}) exist, so the requested image "
            f"{wanted} could not be checked after {timeout}s. The cluster "
            f"reported {expected} StorageNode(s); a node whose SPDK pod never "
            f"started has not finished activating.\n    "
            f"kubectl -n {NS} get pods -l {selector}\n    "
            f"kubectl -n {NS} get storagenode")

    if len(lines) < expected:
        log(f"WARNING: only {len(lines)} of {expected} SPDK pods are up; "
            f"checking the ones that are")

    # No pin asked for: say what the nodes actually came up on and stop.
    #
    # Worth printing even when nothing is being enforced. Which SPDK build a
    # run exercised is the first thing anyone reading a failure wants, and
    # until now it appeared in no log at all -- the only way to find it was to
    # go to the cluster, which is gone by the time the run is read.
    if not wanted:
        for line in lines:
            name, _, images = line.partition("\t")
            log(f"  {name}: {images.strip()}")
        return

    wrong = []
    for line in lines:
        name, _, images = line.partition("\t")
        if wanted not in images:
            wrong.append(f"{name}: {images.strip()}")

    if wrong:
        raise RuntimeError(
            f"{len(wrong)} storage node(s) are not running the requested SPDK "
            f"image {wanted}. The deployment config cannot set spdkImage, and "
            f"it is consumed at node-add, so the pin did not take:\n    "
            + "\n    ".join(wrong[:8]))
    log(f"all {len(lines)} SPDK pod(s) run the requested image {wanted}")


def author_draft(name: str) -> str:
    """Write the deployment config ourselves, naming the devices directly.

    Discovery refuses to propose an NVMe-transport disk for a logical-block
    cluster: ClassRule (discovery/rules.go:190) excludes the whole class, and
    it is a PreFilter, so the two 1.92T disks on each of this lab's workers are
    dropped before the failure is even explained. lblk on NVMe hardware is
    exactly how every lblk run here has worked -- an AIO bdev over /dev/nvme0n1
    is a block device by path -- so discovery cannot deliver what the hardware
    plainly supports.

    The data model has no such objection. groups.devices.block takes any
    ^/dev/... path, /dev/nvme0n1 included, and DeviceClassOf then reads the
    cluster as LogicalBlock. So the document is written by hand and the
    expansion acts on it unchanged.

    This is a workaround for a discovery rule, not a second way to deploy. It
    states the devices instead of finding them, which means it cannot notice a
    worker whose disks differ -- the reason discovery exists. Prefer discovery
    wherever it will answer; see the RCA of 2026-09-25.
    """
    devices = env_list("BLOCK_DEVICES")
    workers = resolve_workers(env_list("WORKER_NODES"))

    # The CRD takes ^/dev/[A-Za-z0-9._/-]+$ and rejects anything else, with an
    # admission error naming the pattern rather than the value. Checking here
    # says which entry is wrong. It also catches the shell having rewritten the
    # path: a POSIX-emulating shell on Windows turns /dev/nvme0n1 into a
    # C:/... path before this process ever sees it.
    bad = [d for d in devices if not re.match(r"^/dev/[A-Za-z0-9._/-]+$", d)]
    if bad:
        raise RuntimeError(
            f"BLOCK_DEVICES entries are not device paths: {bad}. Expected "
            f"paths like /dev/nvme0n1. If these look like Windows paths, the "
            f"shell rewrote them -- run this from the CI host or a real POSIX "
            f"shell.")
    if not workers:
        raise RuntimeError(
            "BLOCK_DEVICES names the devices but not the machines: set "
            "WORKER_NODES too. Authoring a document means stating both, "
            "because nothing is being discovered.")

    log(f"authoring {name} directly: {len(devices)} device(s) on "
        f"{len(workers)} worker(s), bypassing discovery")

    doc = {
        "apiVersion": API,
        "kind": "ClusterDeploymentConfig",
        "metadata": {"name": name, "namespace": NS},
        "spec": {
            "approved": False,
            "cluster": {},
            "nodeSets": [{
                "name": "authored",
                "groups": [{
                    "name": "group-1",
                    "workers": workers,
                    "devices": {"block": devices},
                }],
            }],
        },
    }
    env_name = (os.environ.get("ENVIRONMENT", "") or "").strip()
    if env_name:
        # Nothing inspected the fleet, so nothing concluded a distribution.
        # Unstated, the workload flags it decides are all left at their
        # defaults, which on OpenShift is the wrong deployment.
        doc["spec"]["environment"] = env_name

    existing = kubectl("get", "clusterdeploymentconfig", name, "-o", "json",
                       check=False)
    if existing.strip() and (json.loads(existing).get("spec") or {}).get("approved"):
        log(f"{name} exists and is already approved; waiting on it")
        return name
    kubectl("apply", "-f", "-", stdin=json.dumps(doc))
    return name


def discover_with_retries(base_name: str, timeout: int) -> str:
    """Raise discovery until it produces a draft, or give up saying why.

    Retried at this level rather than inside the wait because a discovery that
    failed has already written its own record: the OperatorOps holds the
    reason, and a second run under the same name would be refused as existing.
    Each attempt therefore gets its own name, and the failed ones are left
    behind on purpose -- they are the evidence for why the first two did not
    work.

    Only the draft is produced here. Nothing has been approved yet, so every
    attempt is free: discovery is read-only against the control plane and the
    document it writes is inert until somebody approves it.
    """
    last = None
    for attempt in range(1, DISCOVERY_ATTEMPTS + 1):
        name = base_name if attempt == 1 else f"{base_name}-try{attempt}"
        try:
            return run_discovery(name, timeout)
        except Exception as exc:                        # noqa: BLE001
            last = exc
            log(f"discovery attempt {attempt}/{DISCOVERY_ATTEMPTS} failed: "
                f"{str(exc)[:200]}")
            if attempt < DISCOVERY_ATTEMPTS:
                log("retrying with a fresh document")
                time.sleep(20)
    raise RuntimeError(
        f"discovery did not produce a usable draft in {DISCOVERY_ATTEMPTS} "
        f"attempts. Last failure: {last}")


def main() -> int:
    config_name = os.environ.get(
        "CDC_NAME", f"e2e-{os.environ.get('CLUSTER_NAME', 'simplyblock-cluster')}")

    # APPROVE_ONLY skips discovery and shaping and approves a draft that is
    # already sitting there. It is the second half of a review: somebody ran
    # DRAFT_ONLY, read or edited the document, and now wants it deployed
    # without this script touching the contents again.
    approve_only = (os.environ.get("APPROVE_ONLY", "") or "").strip()
    if approve_only:
        log(f"approving the existing draft {approve_only} as it stands")
        approve_and_wait(approve_only,
                         int(os.environ.get("TIMEOUT_EXPAND", "1800")))
        verify_spdk_image((os.environ.get("SPDK_IMAGE", "") or "").strip())
        return 0

    if env_list("BLOCK_DEVICES"):
        ref = author_draft(config_name)
    else:
        ref = discover_with_retries(
            config_name, int(os.environ.get("TIMEOUT_DISCOVERY", "900")))

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
        approve_and_wait(ref, int(os.environ.get("TIMEOUT_EXPAND", "1800")))
        return 0

    cfg = shape_draft(cfg)
    describe(cfg)

    # Replace rather than patch: a merge patch replaces whole lists anyway, and
    # sending the object we just read keeps the groups discovery found intact.
    cfg.get("metadata", {}).pop("managedFields", None)
    cfg.pop("status", None)
    log(f"writing the edited draft back to {ref}")
    kubectl("replace", "-f", "-", stdin=json.dumps(cfg))

    if env_bool("DRAFT_ONLY", False):
        log(f"DRAFT_ONLY: {ref} is written and NOT approved. Review it, edit "
            f"anything else you need, then approve:")
        log(f"    kubectl -n {NS} get clusterdeploymentconfig {ref} -o yaml")
        log(f"    APPROVE_ONLY={ref} python3 {os.path.basename(__file__)}")
        log("Nothing is deployed until it is approved, and after that the "
            "document cannot be changed.")
        return 0

    approve_and_wait(ref, int(os.environ.get("TIMEOUT_EXPAND", "1800")))

    verify_spdk_image((os.environ.get("SPDK_IMAGE", "") or "").strip())
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:                            # noqa: BLE001
        log(f"FAILED: {exc}")
        sys.exit(1)
