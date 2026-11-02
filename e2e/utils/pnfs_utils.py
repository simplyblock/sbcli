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

#: The layout types the kernel spells in the ``pnfs=`` field. We only ever
#: expect ``flexfiles``, but matching the set makes a surprise readable
#: instead of a KeyError.
KNOWN_LAYOUT_TYPES = ("flexfiles", "filelayout", "blocklayout", "scsilayout")


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
    counters, cur = {}, None
    for line in (out or "").splitlines():
        if line.startswith("== "):
            cur = line[3:].strip()
            continue
        if cur and line.strip().startswith("{"):
            try:
                d = json.loads(line)
            except ValueError:
                continue
            counters[cur] = {
                "read": int(d.get("data_units_read", 0)),
                "write": int(d.get("data_units_written", 0)),
            }
            cur = None
    return counters


def assert_devices_moved(before, after, logger=None, context=""):
    """B-04. At least one namespace on this node did IO between the samples."""
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

def nfsexport_status(k8s, pvc_name=None, namespace=None):
    """Return the NFSExport CRs, optionally the one backing *pvc_name*.

    Used by B-11 (delete means the export is gone) and B-16 (with no
    metadata server the export reports ``NoMetadataServer`` rather than
    silently provisioning).
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
    for it in items:
        spec = it.get("spec") or {}
        if pvc_name in (spec.get("claimRef", {}) or {}).get("name", ""):
            return it
        if pvc_name in (it.get("metadata", {}).get("name") or ""):
            return it
    return None


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
