"""Multi-attach array LUN mode (``Cluster.multi_attach``), phase 2 helpers.

On an array whose LUNs are already protected and presented to several storage
nodes, the cluster runs 1+0. Each node owns one data LUN; exactly one alceml
per LUN is active. Everything a peer (and, through a loopback attach, the owner
itself) uses to reach the LUN is an NVMe-oF controller ``remote_alceml_<uuid>``
whose subsystem NQN does not depend on the exporting host, so a later takeover
can add the LUN's export on another node as one more path of the same
controller:

* the device subsystem NQN is ``<cluster nqn base>:lun:<device uuid>``
  (SPDK refuses a path to a different subnqn);
* each node exports with its own controller-ID range (SPDK multipath refuses
  duplicate cntlids across the paths of one controller);
* peers attach with a long per-controller hold, so I/O waits for a new path
  instead of failing.

Every helper here is a no-op / returns the regular value for a device or
cluster without the flag, so regular clusters are unchanged.
"""

from __future__ import annotations

import uuid as uuid_mod

from simplyblock_core import constants

#: The NQN infix that marks a host-independent multi-attach device subsystem.
LUN_NQN_INFIX = ":lun:"


def is_multi_attach(obj) -> bool:
    """The multi-attach flag of a cluster or device, read defensively: a
    record that predates the field or a test stand-in reads as False."""
    return getattr(obj, "multi_attach", False) is True


def device_nqn(cluster_nqn: str, device_id: str) -> str:
    """The host-independent subsystem NQN of a multi-attach device: the base
    of the cluster NQN (the part every node's ``snode.subsystem`` starts with)
    plus ``:lun:<device uuid>``."""
    base = (cluster_nqn or constants.CLUSTER_NQN).split(":")[0]
    return f"{base}{LUN_NQN_INFIX}{device_id}"


def device_subsystem_nqn(snode, device, cluster) -> str:
    """The subsystem NQN a device is exported under: host-independent for a
    multi-attach device, ``<node subsystem>:dev:<uuid>`` otherwise."""
    if is_multi_attach(device):
        return device_nqn(getattr(cluster, "nqn", ""), device.get_id())
    return snode.subsystem + ":dev:" + device.get_id()


#: Namespace of the derived namespace UUIDs (``namespace_uuid``); fixed forever.
_LUN_NS_UUID_NAMESPACE = uuid_mod.UUID("6f1d3a52-8c4b-5e0a-9d27-3b8e4c1f7a90")


def namespace_uuid(device) -> str:
    """The namespace UUID a device's PT bdev is exported with.

    Regular device: the device UUID, as before. Multi-attach device: a UUID
    derived from the device UUID, the same on every node that exports the LUN
    (SPDK multipath only joins paths whose namespaces are identical). It must
    NOT be the device UUID itself: the alceml bdev is registered with the
    device UUID, SPDK registers every bdev UUID as an alias, and the owner's
    loopback controller (and a takeover node's controller to the LUN) would
    then fail to register its namespace bdev with a duplicate alias.
    """
    if not is_multi_attach(device):
        return device.get_id()
    return str(uuid_mod.uuid5(_LUN_NS_UUID_NAMESPACE, device.get_id()))


def cntlid_range(block: int) -> tuple[int, int]:
    """``(min_cntlid, max_cntlid)`` of controller-ID block ``block``.

    Raises:
        ValueError: ``block`` is negative or beyond the supported node count.
    """
    if not isinstance(block, int) or block < 0 or block >= constants.MULTI_ATTACH_MAX_NODES:
        raise ValueError(
            f"controller-ID block {block!r} out of range [0, {constants.MULTI_ATTACH_MAX_NODES})")
    size = constants.MULTI_ATTACH_CNTLID_BLOCK_SIZE
    lo, hi = block * size + 1, block * size + size
    if hi > constants.MULTI_ATTACH_MAX_CNTLID:
        raise ValueError(f"controller-ID block {block} exceeds cntlid 0x{constants.MULTI_ATTACH_MAX_CNTLID:X}")
    return lo, hi


def ensure_cntlid_block(snode, db_controller=None) -> int:
    """The node's controller-ID block, claimed (smallest free index of the
    cluster, persisted on the node record) when it has none yet. Sets
    ``snode.cntlid_block`` in memory too, so a later full write of this
    object keeps it."""
    block = getattr(snode, "cntlid_block", -1)
    if isinstance(block, int) and block >= 0:
        return block
    if db_controller is None:
        from simplyblock_core.db_controller import DBController
        db_controller = DBController()
    block = db_controller.claim_cntlid_block(snode, constants.MULTI_ATTACH_MAX_NODES)
    snode.cntlid_block = block
    return block


def subsystem_create_kwargs(snode, device, db_controller=None) -> dict:
    """Extra ``rpc_client.subsystem_create`` kwargs for a device subsystem:
    the node's controller-ID range for a multi-attach device, nothing
    otherwise (the regular min_cntlid=1 default stays)."""
    if not is_multi_attach(device):
        return {}
    lo, hi = cntlid_range(ensure_cntlid_block(snode, db_controller))
    return {"min_cntlid": lo, "max_cntlid": hi}


def attach_controller_kwargs(device) -> dict:
    """Per-controller hold timeouts for a peer's controller to a multi-attach
    device (``bdev_nvme_attach_controller``); nothing otherwise, so the
    cluster-wide bdev_nvme options apply as before."""
    if not is_multi_attach(device):
        return {}
    return {
        "ctrlr_loss_timeout_sec": constants.MULTI_ATTACH_HOLD_SEC,
        "reconnect_delay_sec": constants.MULTI_ATTACH_RECONNECT_DELAY_SEC,
        "fast_io_fail_timeout_sec": constants.MULTI_ATTACH_FAST_IO_FAIL_SEC,
    }


def alceml_create_kwargs(device) -> dict:
    """Extra ``bdev_alceml_create`` kwargs for a multi-attach device's alceml
    (the stuck-I/O timeout); nothing otherwise."""
    if not is_multi_attach(device):
        return {}
    return {"stuck_io_timeout_ms": constants.MULTI_ATTACH_ALCEML_STUCK_IO_MS}


def mark_lun_devices(cluster, snode, devices, blk_entries) -> None:
    """Flag the lblk devices of a multi-attach cluster as array LUNs: set
    ``multi_attach``, the LUN WWN (the inventory's WWN, else its serial, which
    lsblk falls back to the WWN for) and the owner's ``node_paths`` entry.
    No-op on any other cluster. An entry configured as the journal is left
    unflagged; a device the add-node path later picks as the journal keeps
    the flag but never uses it (the journal stack does not read it)."""
    if not is_multi_attach(cluster):
        return
    by_serial = {e.get("serial"): e for e in (blk_entries or [])}
    for dev in devices:
        entry = by_serial.get(dev.serial_number, {})
        if entry.get("journal"):
            continue
        dev.multi_attach = True
        dev.lun_wwn = entry.get("wwn") or dev.serial_number or ""
        dev.node_paths = {
            snode.get_id(): {
                "path": dev.by_id_path or dev.device_path,
                "bdev": dev.nvme_bdev,
            }
        }


def own_multi_attach_devices(node, statuses) -> list:
    """The node's own multi-attach data devices in one of ``statuses``: the
    devices its distribs reach through a loopback ``remote_alceml_<uuid>n1``
    controller instead of the local alceml."""
    return [d for d in (getattr(node, "nvme_devices", None) or [])
            if is_multi_attach(d) and d.status in statuses and d.alceml_bdev]


# ------------------------------------------------------------------ phase 3
# LUN takeover (controllers/lun_takeover) and the paths it touches: a device
# may now be served by a node other than its owner (serving_node_id).

_NODE_ONLINE = "online"


def served_elsewhere(device, node_id) -> bool:
    """A multi-attach device served by a node other than ``node_id``: that
    node must not build the device's alceml, subsystem or anything above the
    base bdev, nor report the device lost when it goes away."""
    return is_multi_attach(device) and device.serving_node() != node_id


def fenced_from(device, node_id) -> bool:
    """A takeover fenced ``node_id`` from the LUN of ``device`` at the array
    and nothing unfenced it since."""
    return is_multi_attach(device) and node_id in (getattr(device, "fenced_node_ids", None) or [])


def not_servable_here(device, node_id) -> bool:
    """``node_id`` must not build a stack for ``device``: another node serves
    its LUN, or ``node_id`` is fenced from it (the takeover after its failure
    did not complete; an operator unfences it -- lun_takeover.unfence_node --
    once its host is known to hold no queued writes)."""
    return served_elsewhere(device, node_id) or fenced_from(device, node_id)


def array_host(node) -> str:
    """A storage node's identity on the storage array: its hostname, without
    the ``_<rpc_port>`` suffix a containerised node's hostname carries."""
    host = getattr(node, "hostname", "") or ""
    suffix = f"_{getattr(node, 'rpc_port', '')}"
    if suffix != "_" and host.endswith(suffix):
        host = host[: -len(suffix)]
    return host


def standby_candidates(owner, failed_node_id) -> list[str]:
    """Node ids that may take over ``owner``'s LUNs, in preference order: the
    owner's LVS secondary, its tertiary, then the owner itself (when it is
    alive while the node serving its LUN fails). The failed node is never a
    candidate."""
    out: list[str] = []
    for nid in (getattr(owner, "secondary_node_id", ""), getattr(owner, "tertiary_node_id", ""),
                owner.get_id()):
        if nid and nid != failed_node_id and nid not in out:
            out.append(nid)
    return out


def devices_served_by(nodes, node_id) -> list:
    """``(owner, device)`` for every multi-attach device of ``nodes`` whose
    serving node is ``node_id`` (its own LUNs it still serves, and LUNs it took
    over)."""
    return [(owner, dev) for owner in nodes for dev in (owner.nvme_devices or [])
            if is_multi_attach(dev) and dev.serving_node() == node_id]


def served_by_online_node(owner, nodes_by_id) -> list:
    """``owner``'s multi-attach devices served by another node that is
    ONLINE: reachable although the owner itself is down."""
    out = []
    for dev in getattr(owner, "nvme_devices", None) or []:
        if not served_elsewhere(dev, owner.get_id()):
            continue
        server = nodes_by_id.get(dev.serving_node())
        if server is not None and server.status == _NODE_ONLINE:
            out.append(dev)
    return out


def normalize_wwn(value) -> str:
    v = (value or "").strip().lower()
    return v.removeprefix("0x")


def lun_keys(device) -> set:
    """Identities a host may report for the LUN of ``device``: its WWN and
    its serial (lsblk falls back to the WWN for the serial)."""
    return {k for k in (normalize_wwn(getattr(device, "lun_wwn", "")),
                        normalize_wwn(getattr(device, "serial_number", ""))) if k}


def foreign_lun_keys(nodes, node_id) -> set:
    """LUN identities of the multi-attach devices of every node but
    ``node_id``. A device with one of them, found on ``node_id``'s host, is
    another node's LUN (all LUNs are presented to every node): it is never a
    new device of ``node_id`` and is never formatted."""
    keys: set = set()
    for node in nodes:
        if node.get_id() == node_id:
            continue
        for dev in node.nvme_devices or []:
            if is_multi_attach(dev):
                keys |= lun_keys(dev)
    return keys


def is_foreign_lun(device, foreign_keys) -> bool:
    return bool(foreign_keys) and bool(lun_keys(device) & foreign_keys)


def pick_lun_path(entries, device):
    """The path to open the LUN of ``device`` with on a takeover node, from
    the node agent's ``lun_info`` answer. Returns ``(path, "")`` or
    ``(None, reason)``.

    Only disks whose block 0 carries the alceml signature (read just now) and
    that are at least as large as the device qualify. The by-id path the owner
    opened it with is preferred when the node has it too (a WWN-derived by-id
    name is the same on every host); otherwise exactly one qualifying disk is
    required -- several host paths to one LUN are not chosen between."""
    if not entries:
        return None, "the LUN is not visible on this node"
    readable = [e for e in entries if e.get("has_alceml_signature") is True]
    if not readable:
        states = sorted({str(e.get("has_alceml_signature")) for e in entries})
        return None, f"no path to the LUN carries the alceml signature (signature: {', '.join(states)})"
    size = int(getattr(device, "size", 0) or 0)
    sized = [e for e in readable if int(e.get("size") or 0) >= size]
    if not sized:
        return None, f"the LUN is smaller here than the device ({size} bytes)"
    owner_path = ((getattr(device, "node_paths", None) or {}).get(device.node_id, {}).get("path")
                  or getattr(device, "by_id_path", "") or "")
    if owner_path.startswith("/dev/disk/by-id/"):
        for e in sized:
            if owner_path in (e.get("by_id_paths") or []) or owner_path == e.get("by_id_path"):
                return owner_path, ""
    if len(sized) == 1:
        e = sized[0]
        return e.get("by_id_path") or e.get("device_path"), ""
    return None, f"{len(sized)} disks carry the LUN and none has the owner's path {owner_path!r}"
