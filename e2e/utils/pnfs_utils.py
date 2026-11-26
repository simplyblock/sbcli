"""pNFS evidence collection — F-7 of the pNFS RWX test plan.

Everything in here answers one of two questions:

**Did the client actually take a layout?** A pNFS mount and a plain NFS mount
look identical from userspace. Both serve the filesystem, both pass a write
and a read, and a suite that ran for eight hours against plain NFS while
believing it tested pNFS is the worst outcome available. The only honest
answer is in ``/proc/self/mountstats``: a layout-taking mount reports a
``pnfs=`` layout type and a non-zero ``LAYOUTGET`` count, and once layouts are
in use the *server* sees no READ or WRITE operations at all, because the data
path bypasses it.

**Did the bytes reach the devices?** The other end of the same claim. The NVMe
namespace counters on each node grow for the length of the run, or the client
was talking to something other than our storage.

Nothing here imports ``simplyblock_core``, ``simplyblock_cli`` or
``simplyblock_web``. See the black-box invariant in ``e2e/AGENTS.md``.

The ``mountstats`` parser mirrors the shape of the one in
``atlas-lib/nfsclient``; it is deliberately a mirror and not an import, and
the two must move together if the kernel's format changes.
"""
import json
import re
import time

#: Operations whose server-side count must stay at zero once a layout is held.
#: If the client is doing READ/WRITE against the metadata server then it is
#: proxying data through it, which is plain NFS wearing a pNFS mount option.
DATA_OPS = ("READ", "WRITE")

#: The layout types the kernel spells in the ``pnfs=`` field.
#:
#: These are the ``pnfs_layoutdriver_type.name`` strings the NFS client
#: prints, not the driver's module name. This list used to hold
#:     ("flexfiles", "filelayout", "blocklayout", "scsilayout")
#: which is the module spelling, and the kernel emits none of them -- so
#: the B-03 check could not pass for any layout type, including the one it
#: was written for. Run 41572279257 is where that surfaced: six pods all
#: reported ``pnfs=LAYOUT_SCSI`` with LAYOUTGET=4098 and zero server-side
#: READ/WRITE, which is a layout being taken and used, and the assertion
#: called it "unexpected layout type".
#:
#: Upstream: fs/nfs/filelayout/filelayout.c, fs/nfs/flexfilelayout/
#: flexfilelayout.c and fs/nfs/blocklayout/blocklayout.c (which registers
#: both the block and the SCSI driver). These must move together with the
#: kernel the lab's nodes run.
KNOWN_LAYOUT_TYPES = (
    "LAYOUT_NFSV4_1_FILES",
    "LAYOUT_FLEX_FILES",
    "LAYOUT_BLOCK_VOLUME",
    "LAYOUT_SCSI",
)


class PnfsEvidenceError(AssertionError):
    """A pNFS claim could not be proven, or was disproven."""


class PnfsProbeError(PnfsEvidenceError):
    """The evidence could not be collected at all.

    Separate from its parent on purpose. "I looked and the mount is not
    pNFS" is a finding about the product; "I could not look" is a finding
    about the harness, and reporting the second as the first is how a test
    invents a defect.

    Callers must catch this *before* ``PnfsEvidenceError``, which it
    inherits from. Catching only the parent undoes the whole distinction:
    run 42803434177 failed with "a mount is no longer holding a pNFS
    layout ... the pNFS evidence could not be collected", one sentence
    asserting and disclaiming the same thing, because
    ``pnfs_stress._assert_still_pnfs`` caught the parent.
    """


#: Why a pod could not be probed. ``churn`` is a pod between lifecycle
#: generations -- the main container has not started, or has already gone --
#: which this soak produces constantly by design and which means there is no
#: mount to look at, not that the look failed. ``wedged`` is a read that
#: never came back, which on a proc file means the mount itself is stuck.
#: The two want opposite responses, so they are told apart rather than
#: counted together.
PROBE_CHURN, PROBE_WEDGED, PROBE_OTHER = "churn", "wedged", "other"


def probe_failure_kind(message):
    """Classify a :class:`PnfsProbeError` message. See the constants above."""
    m = str(message).lower()
    if "container not found" in m or "container is not" in m \
            or "podinitializing" in m or "containercreating" in m:
        return PROBE_CHURN
    if "timed out" in m or "timeout" in m:
        return PROBE_WEDGED
    return PROBE_OTHER


# ── /proc/self/mountstats ────────────────────────────────────────────────

def parse_mountstats(text, mountpoint=None):
    """Parse ``/proc/self/mountstats`` into ``{mountpoint: {...}}``.

    Each entry carries:

    ``device``      the server export, e.g. ``10.0.0.9:/export/pvc-abc``
    ``fstype``      ``nfs4`` and the option blob the kernel echoes back
    ``layout_type`` the value of ``pnfs=``, or ``None`` on a plain mount
    ``lease_time``  the server's lease in seconds, or ``None``
    ``ops``         ``{OPNAME: count}`` from the per-op table

    *mountpoint* narrows the result to one mount and returns that dict
    directly, or ``None`` when the path is not mounted.

    The format is stable but not a protocol, so this is defensive: an entry
    we cannot read contributes nothing rather than raising, and the caller's
    assertion is what reports the absence.
    """
    entries, cur = {}, None
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("device "):
            # device <src> mounted on <dst> with fstype <type> [opts]
            m = re.match(r"device (\S+) mounted on (\S+) with fstype (.*)$", line)
            if not m:
                cur = None
                continue
            src, dst, fstype = m.groups()
            cur = {"device": src, "fstype": fstype,
                   "layout_type": None, "lease_time": None, "ops": {}}
            entries[dst] = cur
            # The kernel puts pnfs= on the device line for flexfiles.
            lm = re.search(r"\bpnfs=(\w+)", fstype)
            if lm:
                cur["layout_type"] = lm.group(1)
            continue
        if cur is None:
            continue
        # Some kernels carry the layout on its own line instead.
        if "pnfs=" in line and cur["layout_type"] is None:
            lm = re.search(r"\bpnfs=(\w+)", line)
            if lm:
                cur["layout_type"] = lm.group(1)
        # The lease sits on the same `nfsv4:` line as pnfs=. Read rather
        # than assumed: every case that partitions a client has to outlast
        # the lease to reach its own premise, and on a lab whose lease is
        # not the kernel's 90s a hard-coded wait would be either pointless
        # (too short to expire anything) or wasted minutes.
        if cur["lease_time"] is None and "lease_time=" in line:
            tm = re.search(r"\blease_time=(\d+)", line)
            if tm:
                cur["lease_time"] = int(tm.group(1))
        # Per-operation table: "WRITE: 1234 1234 0 ..." -- the first number
        # is the operation count, which is the only field we use.
        om = re.match(r"([A-Z_]+):\s+(\d+)", line)
        if om:
            cur["ops"][om.group(1)] = int(om.group(2))

    if mountpoint is not None:
        return entries.get(mountpoint)
    return entries


#: How long one ``cat /proc/self/mountstats`` may take. It is a proc read,
#: so a healthy pod answers in milliseconds; the only way it takes longer is
#: a wedged NFS mount, and in that case waiting does not produce an answer.
#: ``exec_in_pod``'s own 300s default is sized for a raw-device FIO verify
#: and is far too long here: in run 42803434177 thirty-four pods each spent
#: 300s on this read, which is what turned a dead cluster into a probe that
#: looked like it had simply failed.
MOUNTSTATS_TIMEOUT = 30


def mountstats(k8s, pod_name, mountpoint="/spdkvol", namespace=None,
               timeout=MOUNTSTATS_TIMEOUT):
    """Read and parse ``/proc/self/mountstats`` from inside *pod_name*.

    Raises :class:`PnfsProbeError` when the read did not happen, which is a
    different thing from reading it and finding no NFS mount. Run
    41428476500 reported "``/spdkvol`` is not an NFS mount at all" for all
    six pods, as a product finding, when the truth was

        error: unable to upgrade connection: container not found
        ("fio-benchmark")

    -- the containers had not started yet. A test that cannot look must say
    it could not look; saying the mount is wrong is worse than saying
    nothing.
    """
    out, err = k8s.exec_in_pod(
        pod_name, "cat /proc/self/mountstats", namespace=namespace,
        timeout=timeout)
    text = out or ""
    if not text.strip():
        raise PnfsProbeError(
            f"could not read /proc/self/mountstats in {pod_name}: "
            f"{(err or 'no output and no error').strip()[:300]}")
    if "device " not in text:
        # Something answered, but not mountstats -- an error on stdout, a
        # truncated exec. Still a probe failure, not a verdict.
        raise PnfsProbeError(
            f"the mountstats read in {pod_name} returned something that is "
            f"not mountstats: {text.strip()[:200]!r}")
    return parse_mountstats(text, mountpoint=mountpoint)


def assert_layouts_taken(k8s, pod_names, mountpoint="/spdkvol",
                         namespace=None, logger=None, context="",
                         retries=10, delay=15, allow_server_io=False):
    """B-03. Every pod holds a pNFS layout and drives no server-side IO.

    Three things have to be true at once, and each one alone is satisfiable
    by a mount that is not doing pNFS:

    1. the mount reports a ``pnfs=`` layout type
    2. ``LAYOUTGET`` is greater than zero -- a type with no LAYOUTGET means
       the client advertised the capability and never used it
    3. server-side ``READ`` and ``WRITE`` are zero -- with a layout held the
       data path goes to the storage nodes directly, so any server-side data
       op means the client fell back without saying so

    Raises :class:`PnfsEvidenceError` naming every pod that failed and why,
    because one pod falling back while five succeed is the interesting case
    and a first-failure abort would hide it.

    ``allow_server_io`` relaxes (3) only, and exists for the soaks. The
    mountstats counters are cumulative since mount and never reset, so a
    single fallback during one outage window marks that mount permanently
    -- the check can never pass again for that pod even after the layout is
    fully restored and every subsequent byte goes direct. On a healthy
    cluster, which is what B-03 tests, zero is the right bar. In a soak that
    deliberately kills nodes it is not: NFS 4.1 falling back is what keeps
    IO alive while the block path is gone, and the counter records that
    forever. Callers that pass True get the counts back under
    ``server_ops`` and decide for themselves, which for the soaks means
    "did this keep growing after the cluster recovered".
    """
    failures, proven, unreadable = [], {}, []
    for pod in pod_names:
        # A container that has not started yet is the normal case for the
        # first few seconds of a Job, so a probe failure is retried rather
        # than reported. Only a probe that never succeeds is reported, and
        # it is reported as a probe failure.
        st, last = None, None
        for attempt in range(max(1, retries)):
            try:
                st = mountstats(k8s, pod, mountpoint=mountpoint,
                                namespace=namespace)
                break
            except PnfsProbeError as exc:
                last = exc
                if logger and attempt == 0:
                    logger.info("[pnfs] %s not readable yet, retrying: %s",
                                pod, str(exc)[:120])
                if attempt + 1 < max(1, retries):
                    time.sleep(delay)
        if st is None and last is not None:
            unreadable.append(f"{pod}: {last}")
            continue
        if st is None:
            failures.append(f"{pod}: {mountpoint} is not an NFS mount at all")
            continue
        lt = st["layout_type"]
        lg = st["ops"].get("LAYOUTGET", 0)
        data = {op: st["ops"].get(op, 0) for op in DATA_OPS}
        why = []
        if not lt:
            why.append("no pnfs= layout type")
        elif lt not in KNOWN_LAYOUT_TYPES:
            why.append(f"unexpected layout type {lt!r}")
        if lg <= 0:
            why.append(f"LAYOUTGET={lg}")
        busy = {op: n for op, n in data.items() if n}
        if busy and not allow_server_io:
            why.append("server-side data ops: "
                       + ", ".join(f"{op}={n}" for op, n in busy.items()))
        if why:
            failures.append(f"{pod}: " + "; ".join(why))
        else:
            # LAYOUTRETURN and GETDEVICEINFO are carried for the soaks, not
            # for B-03. When a mount falls back they are what separates "the
            # server stopped granting layouts" from "the client cannot use
            # the ones it is granted": SFAM-2850 was the second, and the only
            # way to tell was to go back to the raw mountstats in a 743 MB
            # log. Cheap to collect, and a caller that ignores them pays
            # nothing.
            proven[pod] = {"layout_type": lt, "LAYOUTGET": lg,
                           "LAYOUTRETURN": st["ops"].get("LAYOUTRETURN", 0),
                           "GETDEVICEINFO": st["ops"].get("GETDEVICEINFO", 0),
                           "server_ops": sum(data.values())}
            if logger:
                logger.info(
                    "[pnfs] %s: layout=%s LAYOUTGET=%d, no server-side "
                    "READ/WRITE %s", pod, lt, lg, context)
    # An unreadable pod is only fatal when *every* pod is unreadable. The
    # question this function answers is "is any mount serving pNFS without
    # a layout", and one pod that answered settles it for that pod; a pod
    # that could not be reached has not contributed evidence either way.
    # Requiring all 80 to answer is how run 42803434177 failed: the soak
    # relaunches fio pods continuously, so at any instant some fraction is
    # between generations, and `if unreadable: raise` turns that normal
    # state into a verdict.
    if unreadable and not proven and not failures:
        kinds = [probe_failure_kind(u) for u in unreadable]
        breakdown = ", ".join(
            f"{kinds.count(k)} {k}"
            for k in (PROBE_CHURN, PROBE_WEDGED, PROBE_OTHER) if kinds.count(k))
        raise PnfsProbeError(
            f"no pod could be probed {context} ({len(unreadable)} tried: "
            f"{breakdown}), so this says nothing about the product either "
            f"way. Do not read this as a layout failure:\n  "
            + "\n  ".join(unreadable))
    if unreadable and logger:
        kinds = [probe_failure_kind(u) for u in unreadable]
        logger.warning(
            "[pnfs] %d of %d pods could not be probed %s (%s); judging on "
            "the %d that answered", len(unreadable), len(pod_names), context,
            ", ".join(f"{kinds.count(k)} {k}"
                      for k in (PROBE_CHURN, PROBE_WEDGED, PROBE_OTHER)
                      if kinds.count(k)),
            len(proven) + len(failures))
    if failures:
        raise PnfsEvidenceError(
            f"pNFS layouts were not proven {context}. The mount serves data "
            f"either way, so this is the difference between testing pNFS and "
            f"testing NFS:\n  " + "\n  ".join(failures))
    return proven


# ── the device end of the path ───────────────────────────────────────────

def nvme_ns_counters(k8s, node_ip):
    """Per-namespace host read/write counters from a node's NVMe devices.

    Returns ``{namespace_path: {"read": int, "write": int}}`` in 512-byte
    units as the controller reports them. Read twice around a workload and
    diff: B-04 is satisfied when the numbers move on every node, and a node
    whose counters are flat was not in the data path however healthy it
    looked.
    """
    out, _err = k8s.run_on_node(
        node_ip,
        "for d in /dev/nvme*n*; do [ -e \"$d\" ] || continue; "
        "echo \"== $d\"; nvme smart-log \"$d\" -o json 2>/dev/null "
        "|| echo '{}'; done",
        check=False)
    # nvme-cli pretty-prints -o json, so a device's object spans many lines
    # and the first of them is a bare "{". This used to call json.loads on
    # each line that started with one, which is always that bare brace and
    # always a ValueError, so the dict came back empty for every node and
    # B-04 failed with "not one storage node's NVMe counters moved" on a
    # cluster that was moving plenty. Accumulate to the closing brace
    # instead. Run 41572279257.
    counters, cur, buf, depth = {}, None, [], 0
    for line in (out or "").splitlines():
        if depth == 0 and line.startswith("== "):
            cur = line[3:].strip()
            buf = []
            continue
        if cur is None:
            continue
        stripped = line.strip()
        if depth == 0 and not stripped.startswith("{"):
            continue
        buf.append(line)
        depth += line.count("{") - line.count("}")
        if depth > 0:
            continue
        try:
            d = json.loads("\n".join(buf))
        except ValueError:
            d = None
        if isinstance(d, dict) and (
                "data_units_read" in d or "data_units_written" in d):
            counters[cur] = {
                "read": int(d.get("data_units_read", 0)),
                "write": int(d.get("data_units_written", 0)),
            }
        cur, buf = None, []
    return counters


def assert_devices_moved(before, after, logger=None, context=""):
    """B-04. At least one namespace on this node did IO between the samples.

    A node we could not read counters from is a probe failure, not a flat
    device, and the two must not arrive as the same exception: one says fix
    the harness, the other says the data never reached the cluster.
    """
    if not before or not after:
        raise PnfsProbeError(
            f"no NVMe namespace counters could be read {context} "
            f"(before={len(before or {})}, after={len(after or {})}), so "
            f"this says nothing about where the data went. Fix the probe.")
    moved = {}
    for dev, aft in (after or {}).items():
        bef = (before or {}).get(dev)
        if not bef:
            continue
        dr = aft["read"] - bef["read"]
        dw = aft["write"] - bef["write"]
        if dr or dw:
            moved[dev] = (dr, dw)
    if not moved:
        raise PnfsEvidenceError(
            f"no NVMe namespace on this node moved a single data unit "
            f"{context}. Either the layout sent the data elsewhere or the "
            f"workload never reached the devices.")
    if logger:
        logger.info("[pnfs] device counters moved %s: %s", context,
                    ", ".join(f"{d} +{r}r/+{w}w" for d, (r, w) in moved.items()))
    return moved


# ── control-plane objects ────────────────────────────────────────────────

def pvc_volume_ref(k8s, pvc_name, namespace=None):
    """The ``<cluster>:<pool>:<lvol>`` handle the PVC's PV carries.

    This is the same string an NFSExport holds in ``spec.volumeRef``, so it
    is how a claim is joined to its export.
    """
    ns = namespace or k8s.namespace
    pv, _err = k8s._exec_kubectl(
        f"kubectl get pvc {pvc_name} -n {ns} "
        f"-o jsonpath='{{.spec.volumeName}}' 2>/dev/null || true",
        supress_logs=True)
    pv = (pv or "").strip().strip("'")
    if not pv:
        return ""
    handle, _err = k8s._exec_kubectl(
        f"kubectl get pv {pv} "
        f"-o jsonpath='{{.spec.csi.volumeHandle}}' 2>/dev/null || true",
        supress_logs=True)
    return (handle or "").strip().strip("'")


def nfsexport_status(k8s, pvc_name=None, namespace=None):
    """Return the NFSExport CRs, optionally the one backing *pvc_name*.

    The join is ``spec.volumeRef``, which the CRD defines as
    ``<cluster-uuid>:<pool>:<lvol-uuid>`` -- byte for byte the PV's
    ``spec.csi.volumeHandle``. There is no claimRef on an NFSExport and its
    own name is ``nfsexp-<lvol-uuid>``, so neither carries the claim's name.

    This used to look for the PVC name inside a ``spec.claimRef.name`` that
    does not exist and inside the CR's name, which is a uuid. It therefore
    returned None for every claim that had a perfectly good export, and
    B-11 failed run 41614043254 with "no NFSExport exists for the bound
    claim pnfs-b11" against a claim that fio had just written to through
    that very export.
    """
    ns = namespace or k8s.namespace
    out, _err = k8s._exec_kubectl(
        f"kubectl get nfsexports -n {ns} -o json", supress_logs=True)
    try:
        items = json.loads(out or "{}").get("items", [])
    except ValueError:
        return [] if pvc_name is None else None
    if pvc_name is None:
        return items

    ref = pvc_volume_ref(k8s, pvc_name, namespace=ns)
    for it in items:
        vr = ((it.get("spec") or {}).get("volumeRef") or "")
        if ref and vr == ref:
            return it
    # A deleted claim has no PV to resolve, which is exactly B-11's second
    # look: nothing to join on means nothing claims it, so say so rather
    # than falling back to a substring match that would never hit anyway.
    return None


def export_fsid(export):
    """The export's fsid, which the CRD does not store as a field.

    nfsexport_types.go: "the NFS fsid [is] the volume's own id, so [it is]
    read from here rather than stored beside it, where they could disagree
    with it." So the fsid is the lvol uuid, the last component of
    ``spec.volumeRef``. Reading a ``status.fsid`` returns nothing, which is
    not the same as two exports sharing one.
    """
    vr = ((export or {}).get("spec") or {}).get("volumeRef") or ""
    return vr.rsplit(":", 1)[-1] if ":" in vr else ""


def export_event_reasons(k8s, namespace=None, kind="NFSExport"):
    """The Event reasons the operator has published against *kind*.

    Why Events and not status: the operator reports a refusal as a Kubernetes
    Event on the NFSExport, e.g.

        reason: NoMetadataServer
        note:   no SimplyblockDriver configures a pNFS metadata server;
                set spec.pnfs.mds

    and does not copy it into ``status``. B-16 read ``status`` and so saw an
    empty string on run 41853868960 while the operator had in fact said
    exactly the right thing in exactly the right place.

    Joining through the claim does not work here either: a refused claim stays
    Pending, Pending means no PV, and ``pvc_volume_ref`` needs a PV to resolve
    ``spec.volumeRef``. So the lookup is by Event, scoped to the namespace,
    not by claim.

    Returns a list of ``(reason, message)``, oldest first.
    """
    ns = namespace or k8s.namespace
    out, _err = k8s._exec_kubectl(
        f"kubectl get events -n {ns} --sort-by=.lastTimestamp "
        f"-o json 2>/dev/null || true", supress_logs=True)
    try:
        items = json.loads(out or "{}").get("items", [])
    except ValueError:
        return []
    found = []
    for it in items:
        if ((it.get("involvedObject") or {}).get("kind") or "") != kind:
            continue
        found.append((it.get("reason") or "",
                      it.get("message") or it.get("note") or ""))
    return found


def nvme_resv_report(k8s, node_ip, device):
    """Persistent-reservation report for *device*, as the node sees it.

    Reservations are one of the three parts of this feature that are ours
    rather than the NFS client's, so they get asserted directly rather than
    inferred from the filesystem behaving.
    """
    out, _err = k8s.run_on_node(
        node_ip, f"nvme resv-report {device} -o json 2>/dev/null || echo '{{}}'",
        check=False)
    for line in (out or "").splitlines():
        if line.strip().startswith("{"):
            try:
                return json.loads(line)
            except ValueError:
                continue
    return {}


# ── the warm-standby metadata server ────────────────────────────────────
#
# Mirrored from the operator rather than imported, as e2e/AGENTS.md
# requires: the harness and the cluster run independently selected
# versions, and an import would bind an assertion to the runner's copy.
#
#   annotations    atlas-lib/kube/names.go, AnnoMDS* (287-296)
#   event reasons  operator/internal/controllers/pnfs, Reason* and the
#                  Events.Driver/Events.Exports calls
#
# These must move together with their definitions there. A reviewer
# renaming one can grep for the counterpart.
ANNO_MDS_ACTIVE = "storage.simplyblock.io/mds-active"
ANNO_MDS_ACTIVE_INSTANCE = "storage.simplyblock.io/mds-active-instance"
ANNO_MDS_EPOCH = "storage.simplyblock.io/mds-epoch"
ANNO_MDS_STATE_VOLUME = "storage.simplyblock.io/mds-state-volume"
ANNO_MDS_HOST_ORDINALS = "storage.simplyblock.io/mds-host-ordinals"

EVENT_MDS_PROMOTED = "MDSPromoted"
EVENT_MDS_FENCE_FAILED = "MDSFenceFailed"
EVENT_MDS_STANDBY_MISSING = "MDSStandbyMissing"
EVENT_MDS_HOST_NOT_ALLOWED = "MDSHostNotAllowed"
EVENT_MDS_SCALE_HELD = "MDSScaleHeld"
EVENT_MDS_PROMOTION_CONFLICT = "MDSPromotionConflict"

#: design-pnfs-mds-standby.md §5: the pods' terminationGracePeriodSeconds is
#: 45, covering the runner's drain and the guest's power down. A takeover
#: that is waited on for less than this is being timed against the grace
#: period rather than against the product.
MDS_TERMINATION_GRACE = 45


def mds_statefulset(k8s, namespace=None):
    """The MDS StatefulSet, or ``{}``.

    There is one per storage cluster and no CR of its own: the design puts
    the active record on the StatefulSet precisely because no such kind
    exists (§5.2). It is found by name rather than by label because the
    label set is the generic workload one the mds-proxy DaemonSet also
    carries, and matching that returns both.
    """
    ns = namespace or k8s.namespace
    out, _err = k8s._exec_kubectl(
        f"kubectl get statefulset -n {ns} "
        f"-o custom-columns=NAME:.metadata.name --no-headers "
        f"2>/dev/null || true", supress_logs=True)
    names = [ln.strip() for ln in (out or "").splitlines()
             if ln.strip() and "pnfs-mds" in ln]
    if not names:
        return {}
    return k8s.get_resource_json("statefulset", min(names), ns)


def mds_record(sts):
    """``(active_instance, epoch)`` from the StatefulSet's annotations.

    ``epoch`` is an int, or 0 when never promoted: the design says the
    first promotion writes 1, so 0 is "no guest has ever been active",
    which is a different thing from a missing annotation and is what a
    freshly created StatefulSet reads as.
    """
    anno = ((sts.get("metadata") or {}).get("annotations") or {})
    active = (anno.get(ANNO_MDS_ACTIVE_INSTANCE)
              or anno.get(ANNO_MDS_ACTIVE) or "")
    try:
        epoch = int(anno.get(ANNO_MDS_EPOCH) or 0)
    except ValueError:
        epoch = 0
    return active, epoch


def mds_pods(k8s, sts_name=None, namespace=None):
    """``[(name, node, phase, ready)]`` for the MDS guests, ordinal order.

    Ready is the agent's health probe, which design-pnfs-mds-standby.md
    §5.5 defines as SERVING for a booted standby and, once active, the
    state volume mounted and nfsd threads running as well. So a Ready pod
    is not necessarily the active one -- which is the whole reason the
    active is read from the record instead.
    """
    ns = namespace or k8s.namespace
    out, _err = k8s._exec_kubectl(
        f"kubectl get pods -n {ns} "
        f"-o custom-columns=NAME:.metadata.name,NODE:.spec.nodeName,"
        f"PHASE:.status.phase,READY:.status.containerStatuses[0].ready "
        f"--no-headers 2>/dev/null || true", supress_logs=True)
    rows = []
    for line in (out or "").splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        if "pnfs-mds" not in parts[0]:
            continue
        if sts_name and not parts[0].startswith(sts_name):
            continue
        rows.append((parts[0], parts[1], parts[2], parts[3] == "true"))
    return sorted(rows)


def wait_mds_promotion(k8s, sts_name, from_epoch, timeout=300,
                       namespace=None, poll=5):
    """Wait for the epoch to advance past *from_epoch*.

    The epoch, not the active instance: a takeover back onto the pod that
    was active before -- which a delete-and-reschedule can produce -- keeps
    the instance the same and still is a promotion. Returns
    ``(active, epoch, seconds)``, and raises on timeout rather than
    returning a sentinel, because every caller treats "no promotion" as
    the finding.
    """
    import time as _time
    started = _time.time()
    deadline = started + timeout
    last = (None, from_epoch)
    while _time.time() < deadline:
        sts = mds_statefulset(k8s, namespace)
        active, epoch = mds_record(sts)
        last = (active, epoch)
        if epoch > from_epoch:
            return active, epoch, round(_time.time() - started, 1)
        _time.sleep(poll)
    raise PnfsEvidenceError(
        f"the metadata server did not promote within {timeout}s: the "
        f"record still reads active={last[0]!r} epoch={last[1]}, which is "
        f"the epoch it carried before. Exports stay unserved until a "
        f"standby takes over, so every client is stalled for this whole "
        f"window.")


#: design-pnfs-mds-standby.md §6.2:
#:
#:     key(epoch) = 0x01 << 56 | (sha256(<StatefulSet UID>)[0:3] << 32) | epoch
#:
#: so an MDS key is identified by its top byte, the next 24 bits separate
#: StatefulSets, and the low 32 bits are the epoch. Mirrored, not imported.
#: If the derivation changes there, this must change with it.
MDS_KEY_PREFIX = 0x01
MDS_KEY_EPOCH_MASK = 0xFFFFFFFF
MDS_KEY_TAG_SHIFT = 32
MDS_KEY_TAG_MASK = 0xFFFFFF


def mds_key_parts(key):
    """``(is_mds, tag, epoch)`` for a reservation key.

    A client's key is not in the MDS range and must never be touched by a
    promotion, so ``is_mds`` is what separates "a guest we fenced" from "a
    client we must not have".
    """
    try:
        k = int(key, 16) if isinstance(key, str) else int(key)
    except (TypeError, ValueError):
        return False, 0, 0
    if (k >> 56) != MDS_KEY_PREFIX:
        return False, 0, 0
    return (True,
            (k >> MDS_KEY_TAG_SHIFT) & MDS_KEY_TAG_MASK,
            k & MDS_KEY_EPOCH_MASK)


def resv_registrants(report):
    """``[{"key": int, "hostid": str}]`` from an ``nvme resv-report`` body.

    nvme-cli names the registrant list ``regctlext`` when the extended
    data structure was requested and ``regctl`` otherwise, and the key
    field is ``rkey`` in both. Both shapes are accepted because the
    extended report is a storage-build requirement (design P0-3) that a
    lab may predate -- and a parser that silently returns nothing on the
    older shape would report every namespace as unfenced.
    """
    rows = []
    if not isinstance(report, dict):
        return rows
    for field in ("regctlext", "regctl"):
        for entry in (report.get(field) or []):
            if not isinstance(entry, dict):
                continue
            key = entry.get("rkey", entry.get("rkey_", 0))
            try:
                key = int(key, 16) if isinstance(key, str) else int(key)
            except (TypeError, ValueError):
                continue
            rows.append({
                "key": key,
                "hostid": str(entry.get("hostid", entry.get("hostid_", ""))),
            })
        if rows:
            break
    return rows


# ── the client's own NVMe paths ──────────────────────────────────────────
#
# The pNFS data path is the client host's kernel NVMe/TCP connection to the
# storage nodes, which csi-node establishes at stage time. These two
# functions are what F-18 needs: take that path away, give it back, and see
# whether the client's I/O went back to being direct on its own.

#: The CRD that would carry the warm standby, and the field that says the
#: deployed operator implements one.
MDS_CRD = "simplyblockdrivers.storage.simplyblock.io"
MDS_STANDBYS_FIELD = "standbys"


def mds_standby_supported(k8s):
    """Does the *deployed* operator implement a warm standby at all?

    Asked of the CRD schema, never of an operator source tree. The
    harness and the cluster run independently selected versions
    (``e2e/AGENTS.md``), so reading the runner's checkout would answer
    for the wrong operator -- and the answer decides whether a case is a
    failure or is simply not applicable here, which is the worst possible
    thing to get wrong.

    ``spec.pnfs.mds.standbys`` absent means no standby concept exists:
    on operator ``main`` the MDS StatefulSet is ``Replicas: 1`` with the
    comment "One, and never more", there is no such field on
    ``DriverPNFSMDS``, and no ``mds-epoch`` annotation is ever published.
    A takeover case cannot pass against that and is not meant to -- it is
    testing something the deployment does not claim to do.

    Present, and the cases apply again with no change here: a cluster
    that has the field but is configured with none is a configuration
    problem the caller should report as one.

    Returns None when the CRD cannot be read at all, so the caller can
    tell "no standby" from "could not tell".
    """
    out, _err = k8s._exec_kubectl(
        f"kubectl get crd {MDS_CRD} -o json 2>/dev/null || true",
        supress_logs=True)
    if not (out or "").strip():
        return None
    try:
        crd = json.loads(out)
    except ValueError:
        return None
    # Walk every served version: a field added in a newer one still means
    # the operator implements it.
    for ver in ((crd.get("spec") or {}).get("versions") or []):
        node = (((ver.get("schema") or {}).get("openAPIV3Schema") or {})
                .get("properties") or {})
        for step in ("spec", "pnfs", "mds"):
            node = (node.get(step) or {}).get("properties") or {}
            if not node:
                break
        if MDS_STANDBYS_FIELD in node:
            return True
    return False


#: The kernel's default NFSv4 lease, and so the default grace period a
#: restarted nfsd runs. Mirrored, not imported: it is the kernel's
#: `nfsd4_lease` and the operator sets nothing else, and the value is read
#: back per-mount from `lease_time=` in mountstats wherever a case can.
NFS_LEASE_DEFAULT = 90


def nvme_client_controllers(k8s, node_ip, subnqn_match=None):
    """``[{"name", "traddr", "subnqn", "state"}]`` for a node's NVMe/TCP controllers.

    Reads ``/sys/class/nvme`` rather than ``nvme list-subsys`` so the result
    carries the sysfs name, which is what a reset has to be written to.
    *subnqn_match*, when given, keeps only controllers whose subsystem NQN
    contains it -- the discovery controller and any unrelated subsystem are
    not paths to anything this suite is testing.

    Needs a host shell, which Talos does not have. The caller gets the
    RuntimeError ``run_on_node`` raises there and should turn it into a
    probe failure naming the platform, because a path-loss case that
    silently disturbed nothing would pass.
    """
    out, _err = k8s.run_on_node(
        node_ip,
        "for c in /sys/class/nvme/nvme*; do [ -d \"$c\" ] || continue; "
        "echo \"name=$(basename $c) "
        # sysfs `address` is itself "traddr=<ip>,trsvcid=<port>", so it is
        # echoed under `at=` rather than `traddr=` -- labelling it traddr
        # produces "traddr=traddr=..." and a field regex that captures the
        # label as part of the value.
        "at=$(cat $c/address 2>/dev/null | tr -d '\\n') "
        "subnqn=$(cat $c/subsysnqn 2>/dev/null) "
        "state=$(cat $c/state 2>/dev/null)\"; done",
        check=False)
    rows = []
    for line in (out or "").splitlines():
        if "name=" not in line:
            continue
        row = {}
        for field, key in (("name", "name"), ("at", "traddr"),
                           ("subnqn", "subnqn"), ("state", "state")):
            m = re.search(rf"\b{field}=(\S*)", line)
            row[key] = m.group(1) if m else ""
        if not row.get("name"):
            continue
        if subnqn_match and subnqn_match not in row.get("subnqn", ""):
            continue
        rows.append(row)
    return rows


def reset_nvme_controllers(k8s, node_ip, names):
    """Reset each named controller, which drops its paths and reconnects.

    This is the path loss, and it is deliberately not a firewall rule. A
    rule dropping the storage nodes' data addresses would be applied in the
    host's network namespace, and on this topology SPDK runs there too --
    the same rule would cut the storage node's own replication traffic and
    produce a cluster outage wearing a client-side costume. That is how run
    20261002-202420 ended, and it is the mistake ``cut_storage_network``'s
    docstring records.

    A controller reset is scoped to the client's kernel controller alone,
    heals by itself through the initiator's own reconnect logic, and so
    needs no armed undo: there is no state left behind to undo. Which is
    also the point of F-18 -- the heal is the product's, not the test's.

    Returns the number of controllers the write was accepted for.
    """
    done = 0
    for name in names:
        path = f"/sys/class/nvme/{name}/reset_controller"
        out, _err = k8s.run_on_node(
            node_ip,
            f"[ -w {path} ] && echo 1 > {path} && echo RESET-OK || echo RESET-NO",
            check=False)
        if "RESET-OK" in (out or ""):
            done += 1
        else:
            # Not fatal here. The caller asserts on the count, so a
            # controller that refused is visible as a smaller number rather
            # than as an exception from the middle of an injection.
            pass
    return done


def wait_nvme_live(k8s, node_ip, names, timeout=180, poll=2):
    """Block until every named controller is ``live`` again.

    Returns ``(ok, seconds, states)``. The seconds are the headline: this is
    how long the client had no path, which is what F-18 measures and what
    NFR-2's 30s bound is about.

    **The loop runs on the host, not here.** One host command, not one per
    poll. Polling from the runner would be wrong twice over on a cluster
    without ``oc``: every ``run_on_node`` is a whole ``kubectl debug node/``
    pod -- an image pull, a schedule, and on k3s a Completed pod left
    behind -- so a 2s poll over a 240s budget would be a hundred pods and
    would also measure the harness's pod-startup latency rather than the
    initiator's reconnect. Inside the host the resolution is a real two
    seconds.
    """
    start = time.time()
    checks = " ".join(f"/sys/class/nvme/{n}/state" for n in names)
    script = (
        f"end=$(( $(date +%s) + {timeout} )); "
        f"while :; do all=1; "
        f"for f in {checks}; do "
        f"  s=$(cat \"$f\" 2>/dev/null || echo gone); "
        f"  [ \"$s\" = live ] || all=0; "
        f"done; "
        f"if [ $all -eq 1 ]; then echo ALL-LIVE; break; fi; "
        f"if [ $(date +%s) -ge $end ]; then echo TIMED-OUT; break; fi; "
        f"sleep {poll}; done; "
        # The final states, whichever way it went, so the caller can say
        # which controller did not come back rather than only that one
        # did not.
        f"for f in {checks}; do "
        f"  echo \"state $f=$(cat \"$f\" 2>/dev/null || echo gone)\"; "
        f"done")
    out, _err = k8s.run_on_node(
        node_ip, script, timeout=timeout + 120, check=False)
    text = out or ""
    states = {}
    for line in text.splitlines():
        m = re.search(r"state /sys/class/nvme/(\S+?)/state=(\S+)", line)
        if m:
            states[m.group(1)] = m.group(2)
    if not states:
        # The host command itself did not run -- a debug pod that could not
        # start, a node that is gone. Not the same as the controllers not
        # coming back, and the caller must not report it as one.
        raise PnfsProbeError(
            f"could not read the NVMe controller states on {node_ip} after "
            f"the reset: the host command returned "
            f"{text.strip()[:200]!r}. Whether the paths healed is unknown, "
            f"which is not a product verdict.")
    ok = "ALL-LIVE" in text
    return ok, round(time.time() - start, 1), states
