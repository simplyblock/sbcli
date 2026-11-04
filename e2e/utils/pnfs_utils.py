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
    """


# ── /proc/self/mountstats ────────────────────────────────────────────────

def parse_mountstats(text, mountpoint=None):
    """Parse ``/proc/self/mountstats`` into ``{mountpoint: {...}}``.

    Each entry carries:

    ``device``      the server export, e.g. ``10.0.0.9:/export/pvc-abc``
    ``fstype``      ``nfs4`` and the option blob the kernel echoes back
    ``layout_type`` the value of ``pnfs=``, or ``None`` on a plain mount
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
                   "layout_type": None, "ops": {}}
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
        # Per-operation table: "WRITE: 1234 1234 0 ..." -- the first number
        # is the operation count, which is the only field we use.
        om = re.match(r"([A-Z_]+):\s+(\d+)", line)
        if om:
            cur["ops"][om.group(1)] = int(om.group(2))

    if mountpoint is not None:
        return entries.get(mountpoint)
    return entries


def mountstats(k8s, pod_name, mountpoint="/spdkvol", namespace=None):
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
        pod_name, "cat /proc/self/mountstats", namespace=namespace)
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
                         retries=10, delay=15):
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
        if busy:
            why.append("server-side data ops: "
                       + ", ".join(f"{op}={n}" for op, n in busy.items()))
        if why:
            failures.append(f"{pod}: " + "; ".join(why))
        else:
            proven[pod] = {"layout_type": lt, "LAYOUTGET": lg}
            if logger:
                logger.info(
                    "[pnfs] %s: layout=%s LAYOUTGET=%d, no server-side "
                    "READ/WRITE %s", pod, lt, lg, context)
    if unreadable:
        raise PnfsProbeError(
            f"the pNFS evidence could not be collected {context}, so this "
            f"says nothing about the product either way. Fix the probe, do "
            f"not read this as a layout failure:\n  "
            + "\n  ".join(unreadable))
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
