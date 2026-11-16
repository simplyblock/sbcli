"""Device-level capacity view of a cluster.

The cluster's capacity record is an average over its devices. A write fails
when the DEVICE it needs is full, and devices do not fill evenly: data
written while a node is down goes to the others and stays there. Bed bf24
(2026-11-14, 5 nodes x 1 device, 1+2): at 20:51 the cluster was at 70% with
three devices at 74-75% and two at 56%/62% (their nodes had been down at
20:41 and 20:50). The three filled up at 20:54:27, while the cluster was at
88%, below its 90% critical threshold. Every write that needed a new
location then failed, and the clients got EIO.

The capacity guards below therefore use the fullest device, not the cluster
average.
"""

from simplyblock_core import utils
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

# Devices whose capacity counts: they hold data and come back (unavailable)
# or take writes. A failed or removed device's data is migrated elsewhere.
_COUNTED_DEVICE_STATUSES = (
    NVMeDevice.STATUS_ONLINE,
    NVMeDevice.STATUS_CANNOT_ALLOCATE,
    NVMeDevice.STATUS_READONLY,
    NVMeDevice.STATUS_UNAVAILABLE,
)
_COUNTED_NODE_STATUSES = (
    StorageNode.STATUS_ONLINE,
    StorageNode.STATUS_DOWN,
    StorageNode.STATUS_OFFLINE,
    StorageNode.STATUS_SUSPENDED,
    StorageNode.STATUS_UNREACHABLE,
    StorageNode.STATUS_RESTARTING,
)


def device_utilizations(db, cluster_id):
    """[(node, device, size_util %)] for the counted devices of *cluster_id*
    that have a capacity record. A device without one is left out: no reading
    is not a full device."""
    out = []
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if node.status not in _COUNTED_NODE_STATUSES:
            continue
        for dev in node.nvme_devices:
            if dev.status not in _COUNTED_DEVICE_STATUSES:
                continue
            try:
                records = db.get_device_stats(dev, 1)
            except Exception as e:                        # noqa: BLE001
                logger.debug("No capacity record for device %s: %s", dev.get_id(), e)
                continue
            if not records:
                continue
            util = getattr(records[0], "size_util", None)
            if util is None:
                continue
            out.append((node, dev, int(util)))
    return out


def fullest_device(devices):
    """(node, device, util) of the fullest device in *devices*
    (device_utilizations), or None."""
    return max(devices, key=lambda x: x[2]) if devices else None


def nodes_with_room(devices, threshold):
    """Ids of the nodes with at least one device below *threshold* %. A new
    stripe needs that many distinct nodes, ndcs + npcs, to be placed."""
    return {node.get_id() for node, _dev, util in devices if util < threshold}


def effective_utilization(db, cluster):
    """(util %, why) for *cluster*: the larger of the cluster's average
    utilization and its fullest device's. (None, "") without any reading:
    no guard holds on a cluster it cannot measure."""
    util, why = None, ""
    try:
        records = db.get_cluster_capacity(cluster, 1)
    except Exception:                                     # noqa: BLE001
        records = []
    if records and getattr(records[0], "size_util", None) is not None:
        util = int(records[0].size_util)
        why = f"cluster {cluster.get_id()} is at {util}% capacity"
    try:
        top = fullest_device(device_utilizations(db, cluster.get_id()))
    except Exception as e:                                # noqa: BLE001
        logger.debug("Device capacity of cluster %s unreadable: %s", cluster.get_id(), e)
        top = None
    if top is not None and (util is None or top[2] > util):
        node, dev, dutil = top
        util = dutil
        why = (f"device {dev.get_id()} (storage ID {dev.cluster_device_order}) on node "
               f"{node.get_id()} of cluster {cluster.get_id()} is at {dutil}% capacity")
    return util, why
