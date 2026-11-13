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
