# coding=utf-8
import time

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import tasks_controller, device_controller
from simplyblock_core.controllers.device_controller import CAUSE_LOCAL_FAILURE
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


logger = utils.get_logger(__name__)


# get DB controller
db = db_controller.DBController()


# --- lblk (AIO) watchdog state -------------------------------------------
# AIO bdevs have no bdev_nvme-style IO timeout (timeout_us +
# action_on_timeout=reset is what converts hung IO into failed IO — and
# thereby distrib error_* events — on the nvme path). For AIO devices the
# control plane compensates here: queue-depth sampling is enabled on every
# aio bdev at creation, so bdev_get_iostat reports queue_depth; a device
# with inflight IO and zero completion progress across
# AIO_HUNG_IO_STALL_POLLS consecutive sweeps is declared stalled and fed
# into the exact same machinery an erroring nvme device hits
# (io_error + UNAVAILABLE with a countable LOCAL_FAILURE cause →
# auto-restart budget → flap limit → FAILED → migration).
#
# _aio_progress: device_id -> (last_total_completed_ops, consecutive_stalls)
# _aio_absent:   device_id -> consecutive polls missing from the host lsblk
_aio_progress: dict = {}
_aio_absent: dict = {}


def _aio_total_ops(stat: dict) -> int:
    return (int(stat.get("num_read_ops") or 0)
            + int(stat.get("num_write_ops") or 0)
            + int(stat.get("num_unmap_ops") or 0))


def _check_aio_hung_io(node, rpc_client) -> list:
    """Return the node's ONLINE aio devices whose IO is stalled past the
    threshold. An RPC failure or missing queue_depth counts as UNKNOWN —
    the stall counter is frozen, not advanced: a wedged SPDK reactor slows
    the RPC path itself, and mgmt-plane slowness must not be converted
    into device failures (cf. constants NVME_TIMEOUT_US rationale)."""
    stalled = []
    for dev in node.nvme_devices:
        if dev.bdev_type != "aio" or dev.status != NVMeDevice.STATUS_ONLINE:
            _aio_progress.pop(dev.get_id(), None)
            continue
        try:
            ret = rpc_client.get_lvol_stats(dev.nvme_bdev)
        except Exception as e:
            logger.debug(f"iostat failed for {dev.nvme_bdev}: {e}")
            continue  # unknown — freeze
        bdevs = (ret or {}).get("bdevs") or []
        if not bdevs:
            continue  # unknown — freeze
        stat = bdevs[0]
        queue_depth = stat.get("queue_depth")
        if queue_depth is None:
            # qd-sampling not active (fork without the fields, or sampling
            # lost across an SPDK restart) — re-arm it and skip this poll.
            try:
                rpc_client.bdev_set_qd_sampling_period(
                    dev.nvme_bdev, constants.AIO_QD_SAMPLING_PERIOD_US)
            except Exception:
                pass
            continue
        total = _aio_total_ops(stat)
        last_total, stalls = _aio_progress.get(dev.get_id(), (None, 0))
        # A stall tick requires inflight IO on THIS poll and zero completion
        # progress since the previous one; any progress resets the window.
        if last_total is not None and total == last_total and queue_depth > 0:
            stalls += 1
        else:
            stalls = 0
        _aio_progress[dev.get_id()] = (total, stalls)
        if stalls >= constants.AIO_HUNG_IO_STALL_POLLS:
            stalled.append(dev)
    return stalled


def _check_aio_device_presence(node) -> list:
    """Return the node's aio devices whose backing block device has been
    absent from the host inventory for AIO_DEVICE_ABSENT_POLLS consecutive
    sweeps (hot-removal). Inventory failure = unknown — counters freeze."""
    aio_devs = [dev for dev in node.nvme_devices
                if dev.bdev_type == "aio"
                and dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE,
                                   NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]]
    if not aio_devs:
        return []
    try:
        inventory, _ = node.client(timeout=10, retry=1).get_blockdevices()
    except Exception as e:
        logger.debug(f"blockdevices inventory failed for node {node.get_id()}: {e}")
        return []
    if not inventory:
        return []
    serials = {d.get("serial") for d in inventory}
    names = {d.get("name") for d in inventory}
    gone = []
    for dev in aio_devs:
        if dev.serial_number in serials or dev.device_name in names:
            _aio_absent.pop(dev.get_id(), None)
            continue
        absent = _aio_absent.get(dev.get_id(), 0) + 1
        _aio_absent[dev.get_id()] = absent
        if absent >= constants.AIO_DEVICE_ABSENT_POLLS:
            gone.append(dev)
    return gone


def _sweep_aio_devices(node) -> None:
    """lblk failure parity: hot-removal → device_remove (the treatment
    SPDK_BDEV_EVENT_REMOVE gets), hung IO → io_error + UNAVAILABLE with a
    countable cause. Node-level pattern (>=2 devices stalled at once —
    reactor stall, controller, expander) escalates to a node auto-restart
    instead of failing devices one by one, mirroring the >=2 rule of the
    io_error auto-restart path below."""
    gone = _check_aio_device_presence(node)
    for dev in gone:
        logger.warning(f"AIO device {dev.get_id()} ({dev.device_name}, serial "
                       f"{dev.serial_number}) disappeared from host inventory; removing")
        _aio_absent.pop(dev.get_id(), None)
        _aio_progress.pop(dev.get_id(), None)
        try:
            device_controller.device_remove(dev.get_id(), cause=CAUSE_LOCAL_FAILURE)
        except Exception as e:
            logger.error(f"device_remove failed for {dev.get_id()}: {e}")

    try:
        rpc_client = node.rpc_client()
        stalled = _check_aio_hung_io(node, rpc_client)
    except Exception as e:
        logger.debug(f"hung-IO sweep failed for node {node.get_id()}: {e}")
        return
    if not stalled:
        return
    for dev in stalled:
        _aio_progress.pop(dev.get_id(), None)
    if len(stalled) >= 2:
        logger.warning(f"{len(stalled)} AIO devices stalled simultaneously on "
                       f"node {node.get_id()}; treating as node-level and "
                       f"queueing node auto-restart")
        tasks_controller.add_node_to_auto_restart(node)
        return
    dev = stalled[0]
    logger.warning(f"AIO device {dev.get_id()} ({dev.nvme_bdev}) has inflight IO "
                   f"with no completion progress for "
                   f"{constants.AIO_HUNG_IO_STALL_POLLS * constants.DEV_MONITOR_INTERVAL_SEC}s; "
                   f"marking unavailable")
    try:
        device_controller.device_set_io_error(dev.get_id(), True)
        device_controller.device_set_unavailable(dev.get_id(), cause=CAUSE_LOCAL_FAILURE)
    except Exception as e:
        logger.error(f"failed to mark stalled device {dev.get_id()}: {e}")


def main():
    logger.info("Starting Device monitor...")
    while True:
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        for cluster in db.get_clusters():
            for node in db.get_storage_nodes_by_cluster_id(cluster.get_id()):
                # Per-node isolation: a failure (e.g. an RPC inside device_set_online)
                # on one node must not abort the sweep over the remaining nodes and
                # clusters for this tick.
                try:
                    auto_restart_devices = []

                    if node.status != StorageNode.STATUS_ONLINE:
                        logger.warning(f"Node status is not online, id: {node.get_id()}, status: {node.status}")
                        continue

                    if cluster.device_mode == constants.DEVICE_MODE_LBLK:
                        _sweep_aio_devices(node)
                        # Re-read: the sweep may have changed device statuses.
                        node = db.get_storage_node_by_id(node.get_id())

                    for dev in node.nvme_devices:
                        if dev.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE,
                                              NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                            logger.warning(f"Device status is not recognised, id: {dev.get_id()}, status: {dev.status}")
                            continue
                        if cluster.status == Cluster.STATUS_ACTIVE:
                            if dev.status in [NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                                dev_stat = db.get_device_stats(dev, 1)
                                if dev_stat and dev_stat[0].size_util < cluster.cap_crit:
                                    device_controller.device_set_online(dev.get_id())

                        elif dev.io_error and dev.status == NVMeDevice.STATUS_UNAVAILABLE and not dev.retries_exhausted:
                            logger.info("Adding device to auto restart")
                            auto_restart_devices.append(dev)

                    if len(auto_restart_devices) >= 2:
                        tasks_controller.add_node_to_auto_restart(node)
                    elif len(auto_restart_devices) == 1:
                        tasks_controller.add_device_to_auto_restart(auto_restart_devices[0])
                except Exception as e:
                    logger.error(f"Device monitor failed for node {node.get_id()}: {e}")
                    logger.exception(e)

        time.sleep(constants.DEV_MONITOR_INTERVAL_SEC)


if __name__ == "__main__":
    main()
