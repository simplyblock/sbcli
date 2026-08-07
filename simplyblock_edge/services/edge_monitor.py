# coding=utf-8
"""Edge cluster monitor (docs/edge_clusters_spec.md §6-7).

One PollingService sweep over every edge cluster: probe each node through the
edge site's kubernetes API + SPDK RPC, CAS node statuses, enqueue reassembly
tasks for returned nodes, and derive/CAS the cluster status. Runs on the CP.
"""
from simplyblock_core import utils as core_utils
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_lib.monitors import PollingService
from simplyblock_edge import constants as edge_constants, db, k8s
from simplyblock_edge import edge_cluster_ops
from simplyblock_edge.rpc import node_rpc_client
from simplyblock_edge.status import NodeProbe, derive_cluster_status, derive_node_status

logger = core_utils.get_logger(__name__)


def probe_node(cluster, node) -> NodeProbe:
    """Bounded probe (spec §7): k8s ≤5s per call, RPC ≤3s. An unreachable
    kube-apiserver yields k8s_reachable=False — mapped to UNREACHABLE, never
    to anything destructive."""
    try:
        ready = k8s.node_ready(cluster, node)
    except k8s.EdgeK8sError:
        return NodeProbe(k8s_reachable=False)
    try:
        running = k8s.pod_running(cluster, node)
    except k8s.EdgeK8sError:
        return NodeProbe(k8s_reachable=False, node_ready=ready)

    rpc_alive = False
    if running:
        try:
            rpc = node_rpc_client(node, timeout=edge_constants.EDGE_RPC_PROBE_TIMEOUT_SEC,
                                  retry=0)
            rpc_alive = bool(rpc.get_version())
        except Exception:
            rpc_alive = False
    return NodeProbe(k8s_reachable=True, node_ready=ready,
                     pod_running=running, rpc_alive=rpc_alive)


class EdgeMonitor(PollingService):

    def tick(self):
        any_not_active = False
        for cluster in db.get_edge_clusters():
            # Per-cluster isolation: one unreachable site must not stall the
            # sweep over the others.
            try:
                if self.check_cluster(cluster) != Cluster.STATUS_ACTIVE:
                    any_not_active = True
            except Exception as e:
                logger.error(f"Edge monitor failed for cluster {cluster.get_id()}: {e}")
                logger.exception(e)
                any_not_active = True
        return any_not_active

    def check_cluster(self, cluster) -> str:
        nodes = db.get_edge_nodes(cluster.get_id())
        statuses = []
        for node in nodes:
            statuses.append(self.check_node(cluster, node))

        self._maybe_failover(cluster, nodes)

        new_status = derive_cluster_status(statuses)
        if cluster.status != new_status:
            edge_cluster_ops.set_cluster_status(cluster, new_status)
        return new_status

    def _maybe_failover(self, cluster, nodes):
        """2-node clusters: when the lvstore host stops serving while the
        peer is ONLINE, enqueue the fail-over (deduped task). Fail-back is
        driven by the returning node's restart task."""
        from simplyblock_edge.models import EdgeNode
        active = [n for n in nodes if n.status != EdgeNode.STATUS_REMOVED]
        if len(active) < 2:
            return
        host = next((n for n in active if n.lvstore_base), None)
        if host is None:
            return  # no lvstore yet
        not_serving = (EdgeNode.STATUS_OFFLINE, EdgeNode.STATUS_UNREACHABLE,
                       EdgeNode.STATUS_DOWN)
        survivor = next((n for n in active if n.uuid != host.uuid
                         and n.status == EdgeNode.STATUS_ONLINE), None)
        if host.status in not_serving and survivor is not None:
            edge_cluster_ops.add_edge_task(
                JobSchedule.FN_EDGE_FAILOVER, cluster.get_id(), survivor.uuid,
                max_retry=edge_constants.EDGE_NODE_RESTART_MAX_RETRY)

    def check_devices(self, node):
        """Detect backing-device loss (e.g. EBS force-detach): a partition
        whose record says ONLINE but whose aio bdev is gone — or was ejected
        from its raid after IO errors — goes UNAVAILABLE. IO continues on the
        remaining raid redundancy; recovery is explicit (device restart after
        the operator reattaches the disk). Runs only for ONLINE nodes."""
        from simplyblock_edge import stack
        from simplyblock_edge.models import EdgePartition

        rpc = node_rpc_client(node, timeout=edge_constants.EDGE_RPC_PROBE_TIMEOUT_SEC,
                              retry=0)
        try:
            raids = rpc.bdev_raid_get_bdevs() or []
        except Exception:
            return  # transient RPC issue; the node probe owns that verdict
        raid_members = set()
        for raid in raids:
            for member in (raid.get('base_bdevs_list') or []):
                raid_members.add(member.get('name') if isinstance(member, dict) else member)

        plan = stack.plan_local_stack(node)
        lost = []
        for index, part in enumerate(node.partitions):
            if part.status != EdgePartition.STATUS_ONLINE:
                continue
            bdev = stack.aio_bdev_name(node.uuid, index)
            try:
                present = bool(rpc.get_bdevs(name=bdev))
            except Exception:
                return
            in_raid = plan.raid is None or bdev in raid_members
            if not present or not in_raid:
                lost.append(part.device_path)

        if not lost:
            return

        def _mutate(fresh):
            for p in fresh.partitions:
                if p.device_path in lost and p.status == EdgePartition.STATUS_ONLINE:
                    p.status = EdgePartition.STATUS_UNAVAILABLE
            return True
        db.atomic_update(node, _mutate)
        logger.warning(f"Edge node {node.get_id()} ({node.hostname}): "
                       f"devices unavailable: {lost}")

    def check_node(self, cluster, node) -> str:
        probe = probe_node(cluster, node)
        new_status, needs_restart = derive_node_status(node.status, probe)

        if new_status is not None and new_status != node.status:
            logger.info(f"Edge node {node.get_id()} ({node.hostname}): "
                        f"{node.status} -> {new_status}")

            def _mutate(fresh):
                current, _ = derive_node_status(fresh.status, probe)
                if current != new_status:
                    return False  # somebody moved it meanwhile — re-derive next sweep
                fresh.status = new_status
                return True
            db.atomic_update(node, _mutate)
            node.status = new_status

        if needs_restart:
            edge_cluster_ops.add_edge_task(
                JobSchedule.FN_EDGE_NODE_RESTART, cluster.get_id(), node.uuid,
                max_retry=edge_constants.EDGE_NODE_RESTART_MAX_RETRY)

        from simplyblock_edge.models import EdgeNode
        if node.status == EdgeNode.STATUS_ONLINE and probe.rpc_alive:
            try:
                self.check_devices(node)
            except Exception as e:
                logger.error(f"Device check failed for {node.get_id()}: {e}")

        return node.status


def main():
    EdgeMonitor(
        "Edge monitor",
        interval_sec=edge_constants.EDGE_MONITOR_INTERVAL_SEC,
        fast_interval_sec=edge_constants.EDGE_MONITOR_FAST_INTERVAL_SEC,
        failure_threshold=edge_constants.EDGE_MONITOR_FAILURE_THRESHOLD,
        logger=logger,
    ).run_forever()


if __name__ == "__main__":
    main()
