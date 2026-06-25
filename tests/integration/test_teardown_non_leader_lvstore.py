# coding=utf-8
"""Unit tests for ``storage_node_ops.teardown_non_leader_lvstore``.

Mocks the SPDK RPC client and the DB layer; exercises the helper's contract:
which subsystems get deleted, which bdev stack gets removed, that the donor's
back-reference field is cleared, and the refusal path when the donor is not
actually a sec/tert for the given primary.
"""

import unittest
from unittest.mock import MagicMock, patch

from pydantic import SecretStr

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.hublvol import HubLVol
from simplyblock_core.models.lvol_model import LVol


def _node(uuid, lvstore="", secondary_node_id="", tertiary_node_id="",
          lvstore_stack=None,
          lvstore_stack_secondary="", lvstore_stack_tertiary="",
          mgmt_ip=""):
    n = StorageNode()
    n.uuid = uuid
    n.status = StorageNode.STATUS_ONLINE
    n.cluster_id = "cluster-1"
    n.hostname = f"host-{uuid[:8]}"
    n.lvstore = lvstore
    n.secondary_node_id = secondary_node_id
    n.tertiary_node_id = tertiary_node_id
    n.lvstore_stack = list(lvstore_stack) if lvstore_stack else []
    n.lvstore_stack_secondary = lvstore_stack_secondary
    n.lvstore_stack_tertiary = lvstore_stack_tertiary
    n.mgmt_ip = mgmt_ip or f"10.0.0.{abs(hash(uuid)) % 254 + 1}"
    n.rpc_port = 8080
    n.rpc_username = "user"
    n.rpc_password = SecretStr("pass")
    n.hublvol = HubLVol({"nvmf_port": 5000, "uuid": f"hub-{uuid}",
                          "nqn": f"nqn.hub.{uuid}",
                          "bdev_name": f"lvs/{uuid}/hublvol",
                          "model_number": "m", "nguid": "0" * 32})
    nic = IFace()
    nic.ip4_address = n.mgmt_ip
    nic.trtype = "TCP"
    n.data_nics = [nic]
    n.write_to_db = MagicMock()
    return n


def _lvol(lvol_id, node_id, nqn, status=LVol.STATUS_ONLINE):
    lv = LVol()
    lv.uuid = lvol_id
    lv.lvol_name = f"lvol-{lvol_id[:6]}"
    lv.node_id = node_id
    lv.nqn = nqn
    lv.status = status
    return lv


def _stack(lvstore_name="LVS_100"):
    """A small but realistic bdev stack: distrib + raid + lvstore."""
    return [
        {"type": "bdev_distr", "name": f"distr_{lvstore_name}_0", "params": {}},
        {"type": "bdev_distr", "name": f"distr_{lvstore_name}_1", "params": {}},
        {"type": "bdev_raid",  "name": f"raid_{lvstore_name}",
         "params": {"strip_size_kb": 32},
         "distribs_list": [f"distr_{lvstore_name}_0", f"distr_{lvstore_name}_1"]},
        {"type": "bdev_lvstore", "name": lvstore_name, "params": {}},
    ]


class TestTeardownNonLeaderLvstore(unittest.TestCase):

    def _setup_mocks(self, primary, donor, lvols, mock_db_cls):
        db = mock_db_cls.return_value
        db.get_lvols_by_node_id.return_value = lvols
        # The function re-fetches the donor at the end; return the same object
        # so the mocked write_to_db is the one we assert on.
        db.get_storage_node_by_id.return_value = donor

        rpc = MagicMock()
        rpc.subsystem_delete.return_value = True
        rpc.bdev_distrib_delete.return_value = True
        rpc.bdev_raid_delete.return_value = True
        rpc.bdev_lvol_delete_lvstore.return_value = True
        rpc.bdev_nvme_detach_controller.return_value = True
        # storage_node_ops.teardown_non_leader_lvstore obtains the RPC client
        # via ``donor_node.rpc_client()`` (the method on the StorageNode
        # instance), not by constructing ``RPCClient`` from the module-level
        # symbol — so patching that symbol is a no-op. Intercept the instance
        # method instead so the helper sees our mock.
        donor.rpc_client = MagicMock(return_value=rpc)
        return rpc

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_donor_is_secondary_full_teardown(self, mock_db_cls):
        from simplyblock_core.storage_node_ops import teardown_non_leader_lvstore

        donor = _node("donor-id", lvstore_stack_secondary="primary-id")
        primary = _node("primary-id", lvstore="LVS_100",
                        secondary_node_id="donor-id",
                        lvstore_stack=_stack("LVS_100"))
        lvols = [
            _lvol("lv-1", "primary-id", "nqn.test:lv-1"),
            _lvol("lv-2", "primary-id", "nqn.test:lv-2"),
        ]
        rpc = self._setup_mocks(primary, donor, lvols, mock_db_cls)

        ok = teardown_non_leader_lvstore(donor, primary)

        self.assertTrue(ok)
        # Subsystems for both lvols deleted
        self.assertEqual(rpc.subsystem_delete.call_count, 2)
        rpc.subsystem_delete.assert_any_call("nqn.test:lv-1")
        rpc.subsystem_delete.assert_any_call("nqn.test:lv-2")
        # Bdev stack walked in reverse: lvstore -> raid -> distrs
        rpc.bdev_lvol_delete_lvstore.assert_called_once_with("LVS_100")
        rpc.bdev_raid_delete.assert_called_once_with("raid_LVS_100")
        self.assertEqual(rpc.bdev_distrib_delete.call_count, 2)
        # Hublvol controller detached
        rpc.bdev_nvme_detach_controller.assert_called_once_with(
            primary.hublvol.bdev_name)
        # Back-reference cleared on donor and persisted
        self.assertEqual(donor.lvstore_stack_secondary, "")
        donor.write_to_db.assert_called_once()

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_donor_is_tertiary_clears_tertiary(self, mock_db_cls):
        from simplyblock_core.storage_node_ops import teardown_non_leader_lvstore

        donor = _node("donor-id", lvstore_stack_tertiary="primary-id")
        primary = _node("primary-id", lvstore="LVS_200",
                        secondary_node_id="other-sec-id",
                        tertiary_node_id="donor-id",
                        lvstore_stack=_stack("LVS_200"))
        rpc = self._setup_mocks(primary, donor, [], mock_db_cls)

        ok = teardown_non_leader_lvstore(donor, primary)

        self.assertTrue(ok)
        # No lvols → no subsystem deletes
        rpc.subsystem_delete.assert_not_called()
        # Stack still removed
        rpc.bdev_lvol_delete_lvstore.assert_called_once_with("LVS_200")
        # Correct field cleared
        self.assertEqual(donor.lvstore_stack_tertiary, "")
        # secondary_1 left untouched
        self.assertEqual(donor.lvstore_stack_secondary, "")  # was "" already

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_donor_not_a_sec_refuses(self, mock_db_cls):
        from simplyblock_core.storage_node_ops import teardown_non_leader_lvstore

        donor = _node("donor-id")
        # primary points at a different node for both sec slots
        primary = _node("primary-id", lvstore="LVS_300",
                        secondary_node_id="sec-x",
                        tertiary_node_id="sec-y",
                        lvstore_stack=_stack("LVS_300"))
        rpc = self._setup_mocks(primary, donor, [], mock_db_cls)

        ok = teardown_non_leader_lvstore(donor, primary)

        self.assertFalse(ok)
        # Refusal must be silent on the wire — no RPCs issued.
        rpc.subsystem_delete.assert_not_called()
        rpc.bdev_lvol_delete_lvstore.assert_not_called()
        rpc.bdev_nvme_detach_controller.assert_not_called()
        donor.write_to_db.assert_not_called()

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_skips_lvols_in_deletion(self, mock_db_cls):
        from simplyblock_core.storage_node_ops import teardown_non_leader_lvstore

        donor = _node("donor-id", lvstore_stack_secondary="primary-id")
        primary = _node("primary-id", lvstore="LVS_100",
                        secondary_node_id="donor-id",
                        lvstore_stack=_stack("LVS_100"))
        lvols = [
            _lvol("lv-1", "primary-id", "nqn.test:lv-1"),
            _lvol("lv-2", "primary-id", "nqn.test:lv-2",
                  status=LVol.STATUS_IN_DELETION),
        ]
        rpc = self._setup_mocks(primary, donor, lvols, mock_db_cls)

        ok = teardown_non_leader_lvstore(donor, primary)

        self.assertTrue(ok)
        # Only the non-deleting lvol's subsystem is touched.
        rpc.subsystem_delete.assert_called_once_with("nqn.test:lv-1")

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_subsystem_delete_failure_does_not_abort(self, mock_db_cls):
        """A failing subsystem_delete must be logged but must not prevent the
        bdev stack teardown — leaving stale subsystems with no backing bdevs
        is much worse than logging the failure and continuing."""
        from simplyblock_core.storage_node_ops import teardown_non_leader_lvstore

        donor = _node("donor-id", lvstore_stack_secondary="primary-id")
        primary = _node("primary-id", lvstore="LVS_100",
                        secondary_node_id="donor-id",
                        lvstore_stack=_stack("LVS_100"))
        lvols = [_lvol("lv-1", "primary-id", "nqn.test:lv-1")]
        rpc = self._setup_mocks(primary, donor, lvols, mock_db_cls)
        rpc.subsystem_delete.side_effect = RuntimeError("boom")

        ok = teardown_non_leader_lvstore(donor, primary)

        self.assertTrue(ok)
        rpc.bdev_lvol_delete_lvstore.assert_called_once_with("LVS_100")
        self.assertEqual(donor.lvstore_stack_secondary, "")

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_does_not_mutate_primary_lvstore_stack(self, mock_db_cls):
        """_remove_bdev_stack writes a 'deleted' status field into the dicts
        it processes. The teardown must not let that leak into the primary's
        canonical lvstore_stack record, which would corrupt later
        recreates."""
        from simplyblock_core.storage_node_ops import teardown_non_leader_lvstore

        donor = _node("donor-id", lvstore_stack_secondary="primary-id")
        original_stack = _stack("LVS_100")
        primary = _node("primary-id", lvstore="LVS_100",
                        secondary_node_id="donor-id",
                        lvstore_stack=original_stack)
        self._setup_mocks(primary, donor, [], mock_db_cls)

        teardown_non_leader_lvstore(donor, primary)

        for original_bdev in primary.lvstore_stack:
            self.assertNotIn("status", original_bdev,
                             "primary.lvstore_stack must not be mutated")


if __name__ == "__main__":
    unittest.main()
