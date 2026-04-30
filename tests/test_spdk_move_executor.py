# coding=utf-8
"""Unit tests for ``cluster_expand_executor.SpdkMoveExecutor``.

Mocks the DB, RPC client, and the existing primitives
(``recreate_lvstore_on_sec``, ``create_lvstore``,
``teardown_non_leader_lvstore``, ``reattach_sibling_failover``) so the
executor's per-move dispatch and DB-state setup are exercised without
touching SPDK.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.cluster_expand_executor import SpdkMoveExecutor
from simplyblock_core.cluster_expand_planner import (
    ROLE_PRIMARY,
    ROLE_SECONDARY,
    ROLE_TERTIARY,
    RoleMove,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.hublvol import HubLVol
from simplyblock_core.models.storage_node import StorageNode


def _node(uuid, status=StorageNode.STATUS_ONLINE,
          lvstore="", secondary_node_id="", secondary_node_id_2="",
          lvstore_stack_secondary_1="", lvstore_stack_secondary_2=""):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    n.cluster_id = "cluster-1"
    n.lvstore = lvstore
    n.secondary_node_id = secondary_node_id
    n.secondary_node_id_2 = secondary_node_id_2
    n.lvstore_stack_secondary_1 = lvstore_stack_secondary_1
    n.lvstore_stack_secondary_2 = lvstore_stack_secondary_2
    n.mgmt_ip = f"10.0.0.{abs(hash(uuid)) % 254 + 1}"
    n.rpc_port = 8080
    n.rpc_username = "u"
    n.rpc_password = "p"
    n.hublvol = HubLVol({"nvmf_port": 5000, "uuid": f"hub-{uuid}",
                          "nqn": f"nqn.hub.{uuid}",
                          "bdev_name": f"hub_{uuid}",
                          "model_number": "m", "nguid": "0" * 32})
    nic = IFace()
    nic.ip4_address = n.mgmt_ip
    nic.trtype = "TCP"
    n.data_nics = [nic]
    n.active_tcp = True
    n.active_rdma = False
    n.write_to_db = MagicMock()
    return n


def _cluster():
    c = Cluster()
    c.uuid = "cluster-1"
    c.distr_ndcs = 2
    c.distr_npcs = 2
    c.distr_bs = 4096
    c.distr_chunk_bs = 4096
    c.page_size_in_blocks = 2097152
    c.max_fault_tolerance = 2
    return c


def _db_with_nodes(nodes_by_id):
    db = MagicMock()
    db.get_storage_node_by_id.side_effect = lambda nid: nodes_by_id[nid]
    db.get_cluster_capacity.return_value = [{"size_total": 1024 ** 4}]
    return db


# ---------------------------------------------------------------------------
# 1. create-primary
# ---------------------------------------------------------------------------

class TestCreatePrimary(unittest.TestCase):

    @patch("simplyblock_core.storage_node_ops.create_lvstore")
    def test_calls_create_lvstore_with_cluster_config(self, mock_create):
        new_node = _node("n5")
        nodes = {"n5": new_node}
        db = _db_with_nodes(nodes)
        cluster = _cluster()
        mock_create.return_value = True

        ex = SpdkMoveExecutor(cluster=cluster, db_controller=db)
        ex.execute(RoleMove("n5", ROLE_PRIMARY, "", "n5"))

        mock_create.assert_called_once()
        args = mock_create.call_args.args
        self.assertIs(args[0], new_node)  # snode
        self.assertEqual(args[1:], (cluster.distr_ndcs, cluster.distr_npcs,
                                     cluster.distr_bs, cluster.distr_chunk_bs,
                                     cluster.page_size_in_blocks, 1024 ** 4))
        # Status persisted as "ready"
        self.assertEqual(new_node.lvstore_status, "ready")
        new_node.write_to_db.assert_called()

    @patch("simplyblock_core.storage_node_ops.create_lvstore")
    def test_create_lvstore_failure_raises_and_marks_failed(self, mock_create):
        new_node = _node("n5")
        nodes = {"n5": new_node}
        db = _db_with_nodes(nodes)
        mock_create.return_value = False

        ex = SpdkMoveExecutor(cluster=_cluster(), db_controller=db)
        with self.assertRaises(RuntimeError):
            ex.execute(RoleMove("n5", ROLE_PRIMARY, "", "n5"))

        self.assertEqual(new_node.lvstore_status, "failed")


# ---------------------------------------------------------------------------
# 2. create-secondary / create-tertiary
# ---------------------------------------------------------------------------

class TestCreateSec(unittest.TestCase):

    def _setup(self, mock_recreate, role):
        primary = _node("primary-1", lvstore="LVS_100")
        holder = _node("holder-1")
        nodes = {"primary-1": primary, "holder-1": holder}
        db = _db_with_nodes(nodes)
        mock_recreate.return_value = True

        ex = SpdkMoveExecutor(cluster=_cluster(), db_controller=db)
        ex.execute(RoleMove("primary-1", role, "", "holder-1"))
        return primary, holder

    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_sec")
    def test_create_secondary_sets_back_ref_and_pointer(self, mock_recreate):
        primary, holder = self._setup(mock_recreate, ROLE_SECONDARY)
        self.assertEqual(holder.lvstore_stack_secondary_1, "primary-1")
        self.assertEqual(holder.lvstore_stack_secondary_2, "")
        self.assertEqual(primary.secondary_node_id, "holder-1")
        self.assertEqual(primary.secondary_node_id_2, "")
        mock_recreate.assert_called_once_with(holder)

    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_sec")
    def test_create_tertiary_sets_secondary_2_slot(self, mock_recreate):
        primary, holder = self._setup(mock_recreate, ROLE_TERTIARY)
        self.assertEqual(holder.lvstore_stack_secondary_2, "primary-1")
        self.assertEqual(holder.lvstore_stack_secondary_1, "")
        self.assertEqual(primary.secondary_node_id_2, "holder-1")
        self.assertEqual(primary.secondary_node_id, "")

    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_sec")
    def test_recreate_failure_raises(self, mock_recreate):
        primary = _node("primary-1", lvstore="LVS_100")
        holder = _node("holder-1")
        db = _db_with_nodes({"primary-1": primary, "holder-1": holder})
        mock_recreate.return_value = False
        ex = SpdkMoveExecutor(cluster=_cluster(), db_controller=db)
        with self.assertRaises(RuntimeError):
            ex.execute(RoleMove("primary-1", ROLE_SECONDARY, "", "holder-1"))


# ---------------------------------------------------------------------------
# 3. re-home secondary / re-home tertiary
# ---------------------------------------------------------------------------

class TestRehomeSec(unittest.TestCase):

    def _build(self, slot_suffix="_1"):
        """Set up a primary with donor in the given sec slot and a healthy
        sibling in the other slot."""
        primary = _node(
            "primary-1", lvstore="LVS_100",
            secondary_node_id="donor-1" if slot_suffix == "_1" else "sib-1",
            secondary_node_id_2="sib-1" if slot_suffix == "_1" else "donor-1")
        donor = _node(
            "donor-1",
            lvstore_stack_secondary_1="primary-1" if slot_suffix == "_1" else "",
            lvstore_stack_secondary_2="primary-1" if slot_suffix == "_2" else "")
        recipient = _node("recipient-1")
        sibling = _node(
            "sib-1",
            lvstore_stack_secondary_1="primary-1" if slot_suffix == "_2" else "",
            lvstore_stack_secondary_2="primary-1" if slot_suffix == "_1" else "")
        return primary, donor, recipient, sibling

    @patch("simplyblock_core.storage_node_ops.reattach_sibling_failover")
    @patch("simplyblock_core.storage_node_ops.teardown_non_leader_lvstore")
    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_sec")
    def test_rehome_secondary_full_sequence(
            self, mock_recreate, mock_teardown, mock_reattach):
        primary, donor, recipient, sibling = self._build(slot_suffix="_1")
        nodes = {n.get_id(): n for n in (primary, donor, recipient, sibling)}
        db = _db_with_nodes(nodes)
        mock_recreate.return_value = True
        mock_teardown.return_value = True

        ex = SpdkMoveExecutor(cluster=_cluster(), db_controller=db)
        ex.execute(RoleMove("primary-1", ROLE_SECONDARY, "donor-1", "recipient-1"))

        # DB state set up correctly
        self.assertEqual(recipient.lvstore_stack_secondary_1, "primary-1")
        self.assertEqual(primary.secondary_node_id, "recipient-1")
        # Recreate called on recipient
        mock_recreate.assert_called_once_with(recipient)
        # Teardown called on donor with explicit slot
        mock_teardown.assert_called_once()
        td_kwargs = mock_teardown.call_args.kwargs
        td_args = mock_teardown.call_args.args
        self.assertEqual(td_args[0].get_id(), "donor-1")
        self.assertEqual(td_kwargs.get("slot"), "_1")
        # Sibling reattach for sec_1 move
        mock_reattach.assert_called_once()
        ra_kwargs = mock_reattach.call_args.kwargs
        ra_args = mock_reattach.call_args.args
        self.assertEqual(ra_args[0].get_id(), "sib-1")
        self.assertEqual(ra_kwargs["old_failover_node"].get_id(), "donor-1")
        self.assertEqual(ra_kwargs["new_failover_node"].get_id(), "recipient-1")

    @patch("simplyblock_core.storage_node_ops.reattach_sibling_failover")
    @patch("simplyblock_core.storage_node_ops.teardown_non_leader_lvstore")
    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_sec")
    def test_rehome_tertiary_skips_sibling_reattach(
            self, mock_recreate, mock_teardown, mock_reattach):
        primary, donor, recipient, sibling = self._build(slot_suffix="_2")
        nodes = {n.get_id(): n for n in (primary, donor, recipient, sibling)}
        db = _db_with_nodes(nodes)
        mock_recreate.return_value = True
        mock_teardown.return_value = True

        ex = SpdkMoveExecutor(cluster=_cluster(), db_controller=db)
        ex.execute(RoleMove("primary-1", ROLE_TERTIARY, "donor-1", "recipient-1"))

        self.assertEqual(recipient.lvstore_stack_secondary_2, "primary-1")
        self.assertEqual(primary.secondary_node_id_2, "recipient-1")
        td_kwargs = mock_teardown.call_args.kwargs
        self.assertEqual(td_kwargs.get("slot"), "_2")
        # Sec_2 moves do NOT trigger sibling reattach.
        mock_reattach.assert_not_called()

    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_sec")
    def test_rehome_aborts_if_donor_offline(self, mock_recreate):
        primary, donor, recipient, sibling = self._build()
        donor.status = StorageNode.STATUS_OFFLINE
        nodes = {n.get_id(): n for n in (primary, donor, recipient, sibling)}
        db = _db_with_nodes(nodes)
        ex = SpdkMoveExecutor(cluster=_cluster(), db_controller=db)
        with self.assertRaises(RuntimeError) as ctx:
            ex.execute(RoleMove(
                "primary-1", ROLE_SECONDARY, "donor-1", "recipient-1"))
        self.assertIn("donor", str(ctx.exception))
        # No DB writes should have happened — pre-check is the first step.
        recipient.write_to_db.assert_not_called()
        primary.write_to_db.assert_not_called()
        mock_recreate.assert_not_called()

    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_sec")
    def test_rehome_aborts_if_recipient_offline(self, mock_recreate):
        primary, donor, recipient, sibling = self._build()
        recipient.status = StorageNode.STATUS_OFFLINE
        nodes = {n.get_id(): n for n in (primary, donor, recipient, sibling)}
        db = _db_with_nodes(nodes)
        ex = SpdkMoveExecutor(cluster=_cluster(), db_controller=db)
        with self.assertRaises(RuntimeError) as ctx:
            ex.execute(RoleMove(
                "primary-1", ROLE_SECONDARY, "donor-1", "recipient-1"))
        self.assertIn("recipient", str(ctx.exception))

    @patch("simplyblock_core.storage_node_ops.reattach_sibling_failover")
    @patch("simplyblock_core.storage_node_ops.teardown_non_leader_lvstore")
    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_sec")
    def test_rehome_skips_sibling_reattach_if_no_sec_2(
            self, mock_recreate, mock_teardown, mock_reattach):
        primary = _node("primary-1", lvstore="LVS_100",
                        secondary_node_id="donor-1",
                        secondary_node_id_2="")  # FTT1: no sec_2
        donor = _node("donor-1", lvstore_stack_secondary_1="primary-1")
        recipient = _node("recipient-1")
        nodes = {n.get_id(): n for n in (primary, donor, recipient)}
        db = _db_with_nodes(nodes)
        mock_recreate.return_value = True
        mock_teardown.return_value = True

        ex = SpdkMoveExecutor(cluster=_cluster(), db_controller=db)
        ex.execute(RoleMove(
            "primary-1", ROLE_SECONDARY, "donor-1", "recipient-1"))

        mock_reattach.assert_not_called()


# ---------------------------------------------------------------------------
# 4. Dispatch validation
# ---------------------------------------------------------------------------

class TestDispatchValidation(unittest.TestCase):

    def test_primary_with_from_node_rejected(self):
        ex = SpdkMoveExecutor(cluster=_cluster(), db_controller=MagicMock())
        # Primary moves are create-only by construction; defensive guard.
        with self.assertRaises(ValueError):
            ex.execute(RoleMove("primary-1", ROLE_PRIMARY, "n1", "primary-1"))

    def test_unknown_role_rejected(self):
        ex = SpdkMoveExecutor(cluster=_cluster(), db_controller=MagicMock())
        with self.assertRaises(ValueError):
            ex.execute(RoleMove("primary-1", "quaternary", "", "x"))


if __name__ == "__main__":
    unittest.main()
