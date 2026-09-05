# coding=utf-8
import time

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import tasks_controller, device_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


logger = utils.get_logger(__name__)


# get DB controller
db = db_controller.DBController()


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
                    for dev in node.nvme_devices:
                        # Evaluated BEFORE the status filter below, which drops
                        # everything that is not online/unavailable/readonly/
                        # cannot-allocate. STATUS_REMOVED lands in that gap: on
                        # 2026-09-05 a device SPDK had unregistered logged
                        # "Device status is not recognised ... status: removed"
                        # 505 times over several hours while six migration
                        # tasks sat on "only 7 devices online", because nothing
                        # downstream of that `continue` could ever see it.
                        # device_repair_due() is the single authority on which
                        # states are repairable and whether the removal was the
                        # operator's doing.
                        if device_controller.device_repair_due(dev):
                            try:
                                device_controller.device_repair(dev.get_id())
                            except Exception as e:
                                logger.error(f"Device repair failed for {dev.get_id()}: {e}")

                        if dev.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE,
                                              NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                            logger.debug(f"Device not actionable here, id: {dev.get_id()}, status: {dev.status}")
                            continue
                        # Bounded self-repair of an `unavailable` device.
                        # Runs regardless of cluster status, and without
                        # requiring dev.io_error: a device marked unavailable by
                        # CONSENSUS (more than half the nodes failing to reach it
                        # over NVMe-oF) carries no io_error -- device_set_unavailable
                        # only sets state -- and the branch below sits in an `elif`
                        # that never runs while the cluster is ACTIVE. Between them
                        # those two conditions meant a consensus-unavailable device
                        # was never repaired at all, and the verdict persisted even
                        # when its cause was a transient network problem.
                        #
                        # device_repair() probes first and only rebuilds what is
                        # actually missing, so an intact stack costs nothing and
                        # does not consume an attempt.
                        if device_controller.device_repair_due(dev):
                            try:
                                device_controller.device_repair(dev.get_id())
                            except Exception as e:
                                logger.error(f"Device repair failed for {dev.get_id()}: {e}")

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
