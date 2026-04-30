# coding=utf-8
"""End-to-end-shape unit tests for the single-node expansion entry point.

Covers ``cluster_expand_executor.integrate_new_node_into_cluster`` plus
``storage_node_ops.reattach_sibling_failover``. Mocks the DB and the
executor so the planning + dispatch wiring is exercised without touching
SPDK or the existing SPDK glue.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.cluster_expand_executor import (
    integrate_new_node_into_cluster,
)
from simplyblock_core.cluster_expand_orchestrator import NoopMoveExecutor
from simplyblock_core.cluster_expand_planner import (
    EXPAND_PHASE_COMPLETED,
    ROLE_PRIMARY,
    compute_role_diff,
    make_expand_state,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.hublvol import HubLVol
from simplyblock_core.models.storage_node import StorageNode


def _node(uuid, lvstore="", status=StorageNode.STATUS_ONLINE,
          active_rdma=False, active_tcp=True):
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = "cluster-1"
    n.status = status
    n.lvstore = lvstore
    n.mgmt_ip = f"10.0.0.{abs(hash(uuid)) % 254 + 1}"
    n.rpc_port = 8080
    n.rpc_username = "u"
    n.rpc_password = "p"
    n.active_tcp = active_tcp
    n.active_rdma = active_rdma
    n.hublvol = HubLVol({"nvmf_port": 5000, "uuid": f"hub-{uuid}",
                          "nqn": f"nqn.hub.{uuid}",
                          "bdev_name": f"hub_{uuid}",
                          "model_number": "m", "nguid": "0" * 32})
    nic = IFace()
    nic.ip4_address = n.mgmt_ip
    nic.trtype = "TCP"
    n.data_nics = [nic]
    n.write_to_db = MagicMock()
    return n


def _cluster(ftt=2, expand_state=None):
    c = Cluster()
    c.uuid = "cluster-1"
    c.ha_type = "ha"
    c.max_fault_tolerance = ftt
    c.expand_state = expand_state or {}
    c.write_to_db = MagicMock()
    return c


# ---------------------------------------------------------------------------
# 1. integrate_new_node_into_cluster — fresh plan
# ---------------------------------------------------------------------------

class TestIntegrateFreshPlan(unittest.TestCase):

    def test_plans_correct_diff_for_4_to_5_ftt2(self):
        cluster = _cluster(ftt=2)
        existing_ids = ["n1", "n2", "n3", "n4"]
        snodes = [_node(uuid, lvstore=f"LVS_{uuid}") for uuid in existing_ids]
        new_node = _node("n5")
        snodes.append(new_node)

        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = snodes

        executor = NoopMoveExecutor()
        integrate_new_node_into_cluster(
            cluster, new_node, executor=executor, db_controller=db)

        expected = compute_role_diff(existing_ids, "n5", ftt=2)
        self.assertEqual(executor.executed, expected)
        self.assertEqual(cluster.expand_state["phase"], EXPAND_PHASE_COMPLETED)
        self.assertEqual(cluster.expand_state["new_node_id"], "n5")

    def test_excludes_newcomer_from_existing_rotation(self):
        """The newcomer is in get_storage_nodes_by_cluster_id results once
        add_node has registered it, but it doesn't yet have an lvstore.
        The rotation must be built from primaries only — and excluding
        the newcomer is doubly defensive."""
        cluster = _cluster(ftt=2)
        snodes = [_node(uuid, lvstore=f"LVS_{uuid}")
                  for uuid in ["n1", "n2", "n3"]]
        new_node = _node("n4")  # lvstore="" already
        snodes.append(new_node)

        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = snodes

        executor = NoopMoveExecutor()
        integrate_new_node_into_cluster(
            cluster, new_node, executor=executor, db_controller=db)

        expected = compute_role_diff(["n1", "n2", "n3"], "n4", ftt=2)
        self.assertEqual(executor.executed, expected)

    def test_skips_offline_nodes_from_existing_rotation(self):
        cluster = _cluster(ftt=2)
        snodes = [
            _node("n1", lvstore="LVS_1"),
            _node("n2", lvstore="LVS_2"),
            _node("n3", lvstore="LVS_3"),
            _node("n4", lvstore="LVS_4", status=StorageNode.STATUS_OFFLINE),
        ]
        new_node = _node("n5")
        snodes.append(new_node)

        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = snodes

        executor = NoopMoveExecutor()
        # n4 is offline → the planner sees only 3 existing primaries; FTT2
        # minimum is 3, so this should still succeed (3 → 4 plan).
        integrate_new_node_into_cluster(
            cluster, new_node, executor=executor, db_controller=db)
        expected = compute_role_diff(["n1", "n2", "n3"], "n5", ftt=2)
        self.assertEqual(executor.executed, expected)


# ---------------------------------------------------------------------------
# 2. integrate_new_node_into_cluster — resume case
# ---------------------------------------------------------------------------

class TestIntegrateResume(unittest.TestCase):

    def test_resume_takes_priority_over_new_node(self):
        # An interrupted plan exists; the operator just ran sn add --expansion
        # for n6. Resume must complete the n5 plan first, ignoring n6.
        existing_ids = ["n1", "n2", "n3", "n4"]
        moves = compute_role_diff(existing_ids, "n5", ftt=2)
        in_progress = make_expand_state("n5", moves)
        in_progress["cursor"] = 3

        cluster = _cluster(ftt=2, expand_state=in_progress)
        snodes = [_node(uuid, lvstore=f"LVS_{uuid}") for uuid in existing_ids]
        snodes.append(_node("n5", lvstore=""))  # mid-add from interrupted plan
        snodes.append(_node("n6"))  # new attempt

        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = snodes

        executor = NoopMoveExecutor()
        integrate_new_node_into_cluster(
            cluster, _node("n6"), executor=executor, db_controller=db)

        # Only the trailing n5 moves were executed; n6 wasn't planned.
        self.assertEqual(executor.executed, moves[3:])
        self.assertEqual(cluster.expand_state["new_node_id"], "n5")
        self.assertEqual(cluster.expand_state["phase"], EXPAND_PHASE_COMPLETED)


# ---------------------------------------------------------------------------
# 3. reattach_sibling_failover
# ---------------------------------------------------------------------------

class TestReattachSiblingFailover(unittest.TestCase):

    def _set_rpc(self, sibling, attach_ok=True, remove_ok=True):
        rpc = MagicMock()
        rpc.bdev_nvme_attach_controller.return_value = attach_ok
        rpc.bdev_nvme_remove_trid.return_value = remove_ok
        sibling.rpc_client = MagicMock(return_value=rpc)
        return rpc

    def test_attaches_new_then_removes_old(self):
        from simplyblock_core.storage_node_ops import reattach_sibling_failover
        sibling = _node("sib-1")
        primary = _node("primary-1", lvstore="LVS_100")
        old = _node("donor-1")
        new = _node("recipient-1")
        rpc = self._set_rpc(sibling)

        reattach_sibling_failover(sibling, primary,
                                  old_failover_node=old,
                                  new_failover_node=new)

        # Attach call uses recipient's IP
        rpc.bdev_nvme_attach_controller.assert_called_once()
        attach_args = rpc.bdev_nvme_attach_controller.call_args.args
        self.assertEqual(attach_args[0], primary.hublvol.bdev_name)
        self.assertEqual(attach_args[1], primary.hublvol.nqn)
        self.assertEqual(attach_args[2], new.mgmt_ip)
        self.assertEqual(attach_args[3], primary.hublvol.nvmf_port)

        # Remove call uses donor's IP
        rpc.bdev_nvme_remove_trid.assert_called_once()
        remove_args = rpc.bdev_nvme_remove_trid.call_args.args
        self.assertEqual(remove_args[0], primary.hublvol.bdev_name)
        self.assertEqual(remove_args[1], old.mgmt_ip)
        self.assertEqual(remove_args[2], primary.hublvol.nvmf_port)

    def test_remove_failure_does_not_raise(self):
        """If the attach succeeded but the remove failed, the dead path
        will go inert when the donor is torn down. Logging the failure is
        the right response — raising would mask the successful attach."""
        from simplyblock_core.storage_node_ops import reattach_sibling_failover
        sibling = _node("sib-1")
        primary = _node("primary-1", lvstore="LVS_100")
        old = _node("donor-1")
        new = _node("recipient-1")
        rpc = self._set_rpc(sibling, attach_ok=True, remove_ok=False)
        rpc.bdev_nvme_remove_trid.side_effect = RuntimeError("boom")

        # Should not raise.
        reattach_sibling_failover(sibling, primary,
                                  old_failover_node=old,
                                  new_failover_node=new)

    def test_attach_failure_raises(self):
        """Attach failure on every NIC means the sibling is left with only
        the dead failover path. That's a real problem and must be
        surfaced — orchestrator records it as the abort reason."""
        from simplyblock_core.storage_node_ops import reattach_sibling_failover
        sibling = _node("sib-1")
        primary = _node("primary-1", lvstore="LVS_100")
        old = _node("donor-1")
        new = _node("recipient-1")
        rpc = self._set_rpc(sibling, attach_ok=False)

        with self.assertRaises(RuntimeError):
            reattach_sibling_failover(sibling, primary,
                                      old_failover_node=old,
                                      new_failover_node=new)
        rpc.bdev_nvme_remove_trid.assert_not_called()


if __name__ == "__main__":
    unittest.main()
