# coding=utf-8
"""v2 distrib write protection: which generation a cluster creates with, and
how an upgraded cluster is moved onto the new one.

The data plane exposes write protection under two mutually exclusive create
keys -- ``write_protection`` (v1) and ``write_protection_v2`` -- plus a runtime
``distr_write_protection_v2`` RPC. The RPC exists because a create parameter
cannot retrofit a bdev that already exists, which is exactly the situation
after upgrading a cluster whose distribs were created by an older release.

That makes the generation a CLUSTER property rather than a per-bdev one, and
these tests pin the three places it matters:

  1. a fresh cluster is v2 from the start, so nothing needs migrating;
  2. an upgrade stamps the cluster back to v1, because the running bdevs are
     still v1 until the switch command has actually said so;
  3. `cluster switch-write-protection` records v2 only after every online node
     confirmed the RPC -- never before, never on partial success.

The stored distrib params are replayed on every restart, so they are
re-normalised against the cluster flag rather than trusted verbatim.
"""

import inspect
import unittest
from unittest.mock import MagicMock, patch

from pydantic import SecretStr

from simplyblock_core import cluster_ops, storage_node_ops
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient


# ---------------------------------------------------------------------------
# The create parameter
# ---------------------------------------------------------------------------


class TestCreateParam(unittest.TestCase):
    """bdev_distrib_create must send exactly one generation."""

    def _params_for(self, **kw):
        c = RPCClient("1.2.3.4", 8080, "u", SecretStr("p"))
        captured = {}
        with patch.object(c, "_request",
                          side_effect=lambda m, p: captured.update(p) or True), \
             patch.object(c, "get_bdevs", return_value=None):
            c.bdev_distrib_create(
                "distrib_1", 7001, 2, 1, 1000, 4096, ["jm1"], 4096, **kw)
        return captured

    def test_v2_sends_only_the_v2_key(self):
        p = self._params_for(write_protection_v2=True)
        self.assertTrue(p["write_protection_v2"])
        self.assertNotIn("write_protection", p)

    def test_v1_sends_only_the_v1_key(self):
        p = self._params_for(write_protection=True)
        self.assertTrue(p["write_protection"])
        self.assertNotIn("write_protection_v2", p)

    def test_v2_wins_when_both_are_passed(self):
        """v2 supersedes v1 in the data plane (b_write_protection is derived
        from it), so sending both would be redundant at best."""
        p = self._params_for(write_protection=True, write_protection_v2=True)
        self.assertTrue(p["write_protection_v2"])
        self.assertNotIn("write_protection", p)

    def test_off_sends_neither(self):
        p = self._params_for()
        self.assertNotIn("write_protection", p)
        self.assertNotIn("write_protection_v2", p)


class TestRuntimeRPC(unittest.TestCase):

    def _capture(self, **kw):
        c = RPCClient("1.2.3.4", 8080, "u", SecretStr("p"))
        seen = {}
        with patch.object(
                c, "_request",
                side_effect=lambda m, p: seen.update({"method": m, "params": p}) or True):
            c.distr_write_protection_v2(**kw)
        return seen

    def test_method_name_and_enable_flag(self):
        seen = self._capture(enable=True)
        self.assertEqual(seen["method"], "distr_write_protection_v2")
        self.assertIs(seen["params"]["enable"], True)

    def test_no_name_means_every_distrib_on_the_node(self):
        seen = self._capture(enable=True)
        self.assertNotIn("name", seen["params"])

    def test_a_name_targets_one_bdev(self):
        seen = self._capture(name="distrib_9", enable=True)
        self.assertEqual(seen["params"]["name"], "distrib_9")


# ---------------------------------------------------------------------------
# Replaying a stored stack
# ---------------------------------------------------------------------------


class TestApplyWriteProtectionMode(unittest.TestCase):
    """A stack entry says whether write protection is ON; the cluster says
    under which key. Replaying the stored key verbatim would re-create a bdev
    on the generation the cluster has since left."""

    def test_v1_stack_replayed_as_v2(self):
        p = storage_node_ops.apply_write_protection_mode(
            {"name": "d", "write_protection": True}, True)
        self.assertEqual(p, {"name": "d", "write_protection_v2": True})

    def test_v2_stack_replayed_as_v1(self):
        p = storage_node_ops.apply_write_protection_mode(
            {"name": "d", "write_protection_v2": True}, False)
        self.assertEqual(p, {"name": "d", "write_protection": True})

    def test_off_stays_off_under_v2(self):
        """ndcs == 1 clusters have no write protection at all; switching
        generation must not turn it on."""
        p = storage_node_ops.apply_write_protection_mode(
            {"name": "d", "write_protection": False}, True)
        self.assertEqual(p, {"name": "d"})

    def test_absent_stays_absent(self):
        p = storage_node_ops.apply_write_protection_mode({"name": "d"}, True)
        self.assertEqual(p, {"name": "d"})

    def test_other_params_are_untouched(self):
        p = storage_node_ops.apply_write_protection_mode(
            {"name": "d", "ndcs": 2, "shared_placement": True,
             "write_protection": True}, True)
        self.assertEqual(p["ndcs"], 2)
        self.assertTrue(p["shared_placement"])

    def test_it_is_applied_on_the_restart_replay_path(self):
        src = inspect.getsource(storage_node_ops._create_bdev_stack)
        self.assertIn(
            "apply_write_protection_mode(params, cluster.write_protection_v2)",
            src)

    def test_creation_picks_the_key_from_the_cluster(self):
        src = inspect.getsource(storage_node_ops.create_lvstore)
        self.assertIn('"write_protection_v2" if cluster.write_protection_v2', src)
        self.assertIn("wp_key: write_protection,", src)


# ---------------------------------------------------------------------------
# Cluster-level generation
# ---------------------------------------------------------------------------


class TestClusterFlag(unittest.TestCase):

    def test_default_is_v1(self):
        """A cluster row written by a release without this field must read
        back as 'not switched yet', never as v2."""
        self.assertFalse(Cluster().write_protection_v2)

    def test_fresh_clusters_are_v2(self):
        src = inspect.getsource(cluster_ops)
        self.assertEqual(src.count("cluster.write_protection_v2 = True"), 2,
                         "both create paths must stamp v2")

    def test_upgrade_stamps_back_to_v1(self):
        src = inspect.getsource(cluster_ops.update_cluster)
        i = src.index('setattr(c, "write_protection_v2", False)')
        # ...and before the rolling restart, whose replays must use the key the
        # running bdevs actually carry.
        self.assertLess(i, src.index("Restarting cluster"))


# ---------------------------------------------------------------------------
# switch_write_protection
# ---------------------------------------------------------------------------


def _make_node(node_id, status=StorageNode.STATUS_ONLINE, rpc_ok=True,
               rpc_raises=False, wp=True):
    n = MagicMock(spec=StorageNode)
    n.uuid = node_id
    n.status = status
    n.get_id.return_value = node_id
    n.lvstore_stack = [
        {"type": "bdev_distr", "name": "distrib_1",
         "params": {"name": "distrib_1", "ndcs": 2, "write_protection": wp}},
        {"type": "bdev_raid", "name": "raid_1", "params": {}},
    ]
    rpc = MagicMock()
    if rpc_raises:
        rpc.distr_write_protection_v2.side_effect = Exception("boom")
    else:
        rpc.distr_write_protection_v2.return_value = rpc_ok
    n.rpc_client_mock = rpc
    n.rpc_client.return_value = rpc
    n.write_to_db = MagicMock()
    return n


class _Patched(unittest.TestCase):

    def _patch(self, cluster, nodes):
        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        db.get_storage_nodes_by_cluster_id.return_value = nodes
        db.kv_store = MagicMock()
        by_id = {n.get_id(): n for n in nodes}
        db.get_storage_node_by_id.side_effect = lambda nid: by_id.get(nid)
        db.atomic_update.side_effect = lambda obj, fn: (fn(obj), obj)[1]
        cluster.write_to_db = MagicMock()
        self._p = patch.object(cluster_ops, "db_controller", db)
        self._p.start()
        return db

    def tearDown(self):
        p = getattr(self, "_p", None)
        if p:
            p.stop()


class TestSwitch(_Patched):

    def _cluster(self, v2=False):
        c = Cluster()
        c.uuid = "cl-1"
        c.write_protection_v2 = v2
        return c

    def test_all_online_nodes_get_the_rpc_and_the_flag_is_set(self):
        c = self._cluster()
        nodes = [_make_node("n1"), _make_node("n2"), _make_node("n3")]
        self._patch(c, nodes)

        self.assertTrue(cluster_ops.switch_write_protection(c.uuid))
        self.assertTrue(c.write_protection_v2)
        for n in nodes:
            n.rpc_client_mock.distr_write_protection_v2.assert_called_once_with(
                enable=True)

    def test_stored_stacks_are_moved_to_the_v2_key(self):
        c = self._cluster()
        nodes = [_make_node("n1")]
        self._patch(c, nodes)

        cluster_ops.switch_write_protection(c.uuid)

        params = nodes[0].lvstore_stack[0]["params"]
        self.assertTrue(params["write_protection_v2"])
        self.assertNotIn("write_protection", params)

    def test_non_distrib_entries_are_left_alone(self):
        c = self._cluster()
        nodes = [_make_node("n1")]
        self._patch(c, nodes)

        cluster_ops.switch_write_protection(c.uuid)

        self.assertEqual(nodes[0].lvstore_stack[1]["params"], {})

    def test_one_failing_node_aborts_and_leaves_the_cluster_on_v1(self):
        """Partial success is the one outcome the flag must never record: it
        would make later restarts create v2 bdevs beside un-switched ones."""
        c = self._cluster()
        nodes = [_make_node("n1"), _make_node("n2", rpc_ok=False),
                 _make_node("n3")]
        self._patch(c, nodes)

        self.assertFalse(cluster_ops.switch_write_protection(c.uuid))
        self.assertFalse(c.write_protection_v2)
        self.assertIn("write_protection", nodes[0].lvstore_stack[0]["params"])

    def test_a_raising_node_aborts_too(self):
        c = self._cluster()
        nodes = [_make_node("n1"), _make_node("n2", rpc_raises=True)]
        self._patch(c, nodes)

        self.assertFalse(cluster_ops.switch_write_protection(c.uuid))
        self.assertFalse(c.write_protection_v2)

    def test_offline_nodes_are_not_dialled_but_are_still_restamped(self):
        """An offline node has no running bdev to migrate; its distribs come
        back on v2 because its stored stack was rewritten."""
        c = self._cluster()
        online = _make_node("n1")
        offline = _make_node("n2", status=StorageNode.STATUS_OFFLINE)
        self._patch(c, [online, offline])

        self.assertTrue(cluster_ops.switch_write_protection(c.uuid))
        offline.rpc_client_mock.distr_write_protection_v2.assert_not_called()
        self.assertTrue(offline.lvstore_stack[0]["params"]["write_protection_v2"])
        self.assertTrue(c.write_protection_v2)

    def test_no_online_node_is_a_failure(self):
        c = self._cluster()
        nodes = [_make_node("n1", status=StorageNode.STATUS_OFFLINE)]
        self._patch(c, nodes)

        self.assertFalse(cluster_ops.switch_write_protection(c.uuid))
        self.assertFalse(c.write_protection_v2)

    def test_already_v2_is_a_no_op(self):
        c = self._cluster(v2=True)
        nodes = [_make_node("n1")]
        self._patch(c, nodes)

        self.assertTrue(cluster_ops.switch_write_protection(c.uuid))
        nodes[0].rpc_client_mock.distr_write_protection_v2.assert_not_called()

    def test_the_flag_is_written_after_the_rpcs_not_before(self):
        src = inspect.getsource(cluster_ops.switch_write_protection)
        self.assertLess(src.index("distr_write_protection_v2(enable=True)"),
                        src.index('setattr(c, "write_protection_v2", True)'))


class TestCliWiring(unittest.TestCase):

    def test_command_is_registered_parsed_and_dispatched(self):
        from simplyblock_cli import cli, clibase
        src = inspect.getsource(cli)
        self.assertIn("init_cluster__switch_write_protection", src)
        self.assertIn("'switch-write-protection'", src)
        self.assertIn("cluster__switch_write_protection",
                      inspect.getsource(clibase))

    def test_handler_calls_cluster_ops(self):
        from simplyblock_cli import clibase
        self.assertIn(
            "cluster_ops.switch_write_protection(args.cluster_id)",
            inspect.getsource(clibase.CLIWrapperBase.cluster__switch_write_protection))


if __name__ == "__main__":
    unittest.main()
