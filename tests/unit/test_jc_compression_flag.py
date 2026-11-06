"""Cluster-level JC compression switch (``Cluster.jc_compression_enabled``).

SPDK keeps JC compression suspended until the control plane resumes it with
``jc_suspend_compression(suspend=False)``. Disabling the flag must therefore
stop every resume path; ``cluster set-jc-compression`` additionally suspends
or resumes compression on the online LVS group members at runtime.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import cluster_ops
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.release_upgrades import jc_compression_upgrade
from simplyblock_core.rpc_client import RPCConnectionError
from simplyblock_core.services import lvol_monitor


def _cluster(enabled=True, held=False):
    c = Cluster()
    c.uuid = "cluster-1"
    c.jc_compression_enabled = enabled
    if held:
        c.release_upgrade_state = {jc_compression_upgrade.STATE_KEY: {"to_release": "RC26.3"}}
    return c


def _node(uid, status=StorageNode.STATUS_ONLINE, jm_vuid=0, secondary="", tertiary=""):
    n = StorageNode()
    n.uuid = uid
    n.cluster_id = "cluster-1"
    n.status = status
    n.jm_vuid = jm_vuid
    n.lvstore = f"LVS_{jm_vuid}" if jm_vuid else ""
    n.secondary_node_id = secondary
    n.tertiary_node_id = tertiary
    return n


class _SetCtx:
    """Patch cluster_ops collaborators around set_jc_compression."""

    def __init__(self, cluster, nodes, busy=(), rpc_result=(True, None), rpc_raises=False):
        self.cluster = cluster
        self.nodes = nodes
        self.rpc = MagicMock()
        if rpc_raises:
            self.rpc.jc_suspend_compression.side_effect = RPCConnectionError("down")
        else:
            self.rpc.jc_suspend_compression.return_value = rpc_result
        for n in nodes:
            n.rpc_client = MagicMock(return_value=self.rpc)
        self.busy = set(busy)

    def __enter__(self):
        db = MagicMock()
        db.get_cluster_by_id.return_value = self.cluster
        db.get_storage_nodes_by_cluster_id.return_value = self.nodes
        db.atomic_update.side_effect = lambda obj, fn: fn(self.cluster)
        tasks = MagicMock()
        tasks.get_active_node_tasks.side_effect = lambda cl, nid: [object()] if nid in self.busy else []
        tasks.get_jc_comp_task.return_value = None
        self.tasks = tasks
        self._patches = [patch.object(cluster_ops, "db_controller", db),
                         patch.object(cluster_ops, "tasks_controller", tasks)]
        for p in self._patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self._patches:
            p.stop()

    def calls(self):
        return [c.kwargs for c in self.rpc.jc_suspend_compression.call_args_list]


class TestSetJcCompression(unittest.TestCase):

    def _group(self):
        primary = _node("n1", jm_vuid=100, secondary="n2", tertiary="n3")
        return [primary, _node("n2"), _node("n3", status=StorageNode.STATUS_OFFLINE)]

    def test_disable_persists_and_suspends_online_members(self):
        cluster = _cluster(enabled=True)
        with _SetCtx(cluster, self._group()) as ctx:
            cluster_ops.set_jc_compression("cluster-1", enable=False)
        self.assertFalse(cluster.jc_compression_enabled)
        # n3 is offline: it starts suspended and honours the flag on restart.
        self.assertEqual(ctx.calls(), [{"jm_vuid": 100, "suspend": True}] * 2)
        ctx.tasks.add_jc_comp_resume_task.assert_not_called()

    def test_enable_resumes_online_members(self):
        cluster = _cluster(enabled=False)
        with _SetCtx(cluster, self._group()) as ctx:
            cluster_ops.set_jc_compression("cluster-1", enable=True)
        self.assertTrue(cluster.jc_compression_enabled)
        self.assertEqual(ctx.calls(), [{"jm_vuid": 100, "suspend": False}] * 2)

    def test_enable_on_busy_node_queues_resume_task(self):
        with _SetCtx(_cluster(enabled=False), self._group(), busy={"n2"}) as ctx:
            cluster_ops.set_jc_compression("cluster-1", enable=True)
        self.assertEqual(len(ctx.calls()), 1)
        ctx.tasks.add_jc_comp_resume_task.assert_called_once_with("cluster-1", "n2", jm_vuid=100)

    def test_enable_rpc_failure_queues_resume_task(self):
        with _SetCtx(_cluster(enabled=False), self._group(), rpc_raises=True) as ctx:
            cluster_ops.set_jc_compression("cluster-1", enable=True)
        self.assertEqual(ctx.tasks.add_jc_comp_resume_task.call_count, 2)

    def test_disable_rpc_failure_is_reported_not_queued(self):
        with _SetCtx(_cluster(enabled=True), self._group(), rpc_raises=True) as ctx:
            messages = cluster_ops.set_jc_compression("cluster-1", enable=False)
        ctx.tasks.add_jc_comp_resume_task.assert_not_called()
        self.assertTrue(all("suspend failed" in m for m in messages))

    def test_enable_while_upgrade_hold_does_not_resume(self):
        cluster = _cluster(enabled=False, held=True)
        with _SetCtx(cluster, self._group()) as ctx:
            cluster_ops.set_jc_compression("cluster-1", enable=True)
        self.assertTrue(cluster.jc_compression_enabled)
        ctx.rpc.jc_suspend_compression.assert_not_called()


class TestResumePathsHonourFlag(unittest.TestCase):

    def test_lvol_monitor_resume_skipped_when_disabled(self):
        node = _node("n1", jm_vuid=100)
        node.rpc_client = MagicMock()
        db = MagicMock()
        db.get_storage_node_by_id.return_value = node
        db.get_cluster_by_id.return_value = _cluster(enabled=False)
        lvol = MagicMock(node_id="n1")
        with patch.object(lvol_monitor, "db", db), \
                patch.object(lvol_monitor, "tasks_controller") as tasks:
            lvol_monitor.resume_comp(lvol)
        node.rpc_client.assert_not_called()
        tasks.add_jc_comp_resume_task.assert_not_called()

    def test_upgrade_complete_does_not_resume_when_disabled(self):
        cluster = _cluster(enabled=False, held=True)
        node = _node("n1", jm_vuid=100)
        node.rpc_client = MagicMock()
        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        db.get_storage_nodes_by_cluster_id.return_value = [node]
        with patch.object(jc_compression_upgrade, "DBController", return_value=db):
            jc_compression_upgrade.JCCompressionUpgrade().upgrade_complete(cluster)
        self.assertFalse(jc_compression_upgrade.resume_is_held(cluster))
        node.rpc_client.assert_not_called()

    def test_pre_update_rollback_does_not_resume_when_disabled(self):
        node = MagicMock()
        jc_compression_upgrade.JCCompressionUpgrade._rollback(_cluster(enabled=False), [(node, 100)])
        node.rpc_client.assert_not_called()


class TestCli(unittest.TestCase):

    def _parse(self, argv, dev=False):
        from simplyblock_cli.cli import CLIWrapper
        with patch("sys.argv", ["sbctl", *(["--dev"] if dev else []), *argv]):
            return CLIWrapper().parser.parse_args(argv)

    def test_create_and_add_flag_is_developer_only(self):
        for cmd in (["cluster", "add"], ["cluster", "create"]):
            self.assertFalse(self._parse(cmd, dev=True).disable_jc_compression)
            self.assertTrue(self._parse([*cmd, "--disable-jc-compression"], dev=True).disable_jc_compression)
            with self.assertRaises(SystemExit), patch("sys.stderr"):
                self._parse([*cmd, "--disable-jc-compression"])

    def test_set_jc_compression_dispatch(self):
        from simplyblock_cli.clibase import CLIWrapperBase
        args = self._parse(["cluster", "set-jc-compression", "cluster-1", "--disable"])
        with patch("simplyblock_cli.clibase.cluster_ops.set_jc_compression", return_value=["ok"]) as fn:
            self.assertEqual(CLIWrapperBase.cluster__set_jc_compression(MagicMock(), None, args), "ok")
        fn.assert_called_once_with("cluster-1", enable=False)
