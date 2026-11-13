"""add_clone / convert reach every lvstore member that answers, whatever its
DB status (replica_chain).

bf23, 2026-11-12, LVS_16: the tertiary never got the add_clone that linked a
clone under its snapshot, so its in-memory tree had the snapshot clone-less.
After it took leadership through IO it deleted that snapshot's data. The call
sites had picked members by DB status (ONLINE only, or the secondary only).

Pure logic tests: DB and RPC clients are mocked.
"""
import unittest
from unittest.mock import MagicMock

from requests.exceptions import ConnectionError as ReqConnectionError
from requests.exceptions import ReadTimeout

from simplyblock_core.controllers import replica_chain as rc
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCConnectionError, RPCRemoteError

WRITABLE = {"driver_specific": {"lvol": {"is_snapshot": False}}}
SNAPSHOT = {"driver_specific": {"lvol": {"is_snapshot": True}}}


def _conn_error(cause):
    try:
        raise RPCConnectionError("x") from cause
    except RPCConnectionError as e:
        return e


def _node(node_id, status=StorageNode.STATUS_ONLINE, bdev=WRITABLE):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.status = status
    rpc = n.rpc_client.return_value
    rpc.bdev_get.return_value = bdev
    rpc.bdev_lvol_add_clone.return_value = True
    rpc.bdev_lvol_convert.return_value = True
    return n


def _db(nodes):
    db = MagicMock()
    db.get_storage_node_by_id.side_effect = lambda nid: nodes[nid]
    return db


def _primary(sec="sec", ter="ter"):
    p = MagicMock()
    p.secondary_node_id = sec
    p.tertiary_node_id = ter
    return p


class TestMembers(unittest.TestCase):

    def test_every_status_but_removed_is_a_member(self):
        nodes = {"sec": _node("sec", StorageNode.STATUS_DOWN),
                 "ter": _node("ter", StorageNode.STATUS_REMOVED)}
        got = rc.lvstore_replicas(_primary(), db=_db(nodes))
        self.assertEqual([(r, n.get_id()) for r, n in got], [("secondary", "sec")])

    def test_unknown_ids_and_exclusions_are_left_out(self):
        nodes = {"sec": _node("sec")}
        self.assertEqual(rc.lvstore_replicas(_primary(), exclude_ids={"sec"}, db=_db(nodes)), [])


class TestAddCloneAndConvert(unittest.TestCase):

    def test_tertiary_not_online_still_gets_both_calls(self):
        """The bf23 shape: a tertiary in restart answers RPCs and holds the
        tree, so it is told about the link and the convert."""
        nodes = {"sec": _node("sec"), "ter": _node("ter", StorageNode.STATUS_RESTARTING)}
        db = _db(nodes)
        self.assertIsNone(rc.add_clone_on_replicas(_primary(), "LVS/C", "LVS/S", db=db))
        self.assertIsNone(rc.convert_on_replicas(_primary(), "LVS/C", db=db))
        for n in nodes.values():
            n.rpc_client.return_value.bdev_lvol_add_clone.assert_called_once_with("LVS/C", "LVS/S")
            n.rpc_client.return_value.bdev_lvol_convert.assert_called_once_with("LVS/C")

    def test_unreachable_member_is_skipped_not_failed(self):
        ter = _node("ter", StorageNode.STATUS_OFFLINE)
        ter.rpc_client.return_value.bdev_get.side_effect = _conn_error(ReqConnectionError())
        self.assertEqual(rc.add_clone_on(ter, "tertiary", "LVS/C", "LVS/S"), (rc.SKIPPED, None))
        self.assertEqual(rc.convert_on(ter, "tertiary", "LVS/C"), (rc.SKIPPED, None))
        ter.rpc_client.return_value.bdev_lvol_add_clone.assert_not_called()

    def test_a_read_timeout_reached_the_member_and_fails(self):
        ter = _node("ter")
        ter.rpc_client.return_value.bdev_lvol_convert.side_effect = _conn_error(ReadTimeout())
        outcome, err = rc.convert_on(ter, "tertiary", "LVS/C")
        self.assertEqual(outcome, rc.FAILED)
        self.assertIn("ter", err)

    def test_an_error_answer_fails_and_the_other_member_still_runs(self):
        nodes = {"sec": _node("sec"), "ter": _node("ter")}
        nodes["sec"].rpc_client.return_value.bdev_lvol_add_clone.side_effect = \
            RPCRemoteError("File exists", -17)
        err = rc.add_clone_on_replicas(_primary(), "LVS/C", "LVS/S", db=_db(nodes))
        self.assertIn("secondary sec", err)
        nodes["ter"].rpc_client.return_value.bdev_lvol_add_clone.assert_called_once()

    def test_a_false_answer_fails(self):
        sec = _node("sec")
        sec.rpc_client.return_value.bdev_lvol_convert.return_value = False
        self.assertEqual(rc.convert_on(sec, "secondary", "LVS/C")[0], rc.FAILED)

    def test_member_without_the_bdev_is_skipped(self):
        sec = _node("sec", bdev=None)
        self.assertEqual(rc.add_clone_on(sec, "secondary", "LVS/C", "LVS/S"), (rc.SKIPPED, None))
        self.assertEqual(rc.convert_on(sec, "secondary", "LVS/C"), (rc.SKIPPED, None))

    def test_retries_are_idempotent(self):
        linked = _node("sec", bdev={"driver_specific": {"lvol": {"base_snapshot": "S"}}})
        self.assertEqual(rc.add_clone_on(linked, "secondary", "LVS/C", "LVS/S"), (rc.DONE, None))
        linked.rpc_client.return_value.bdev_lvol_add_clone.assert_not_called()
        converted = _node("sec", bdev=SNAPSHOT)
        self.assertEqual(rc.add_clone_on(converted, "secondary", "LVS/C", "LVS/S"), (rc.DONE, None))
        self.assertEqual(rc.convert_on(converted, "secondary", "LVS/C"), (rc.DONE, None))
        converted.rpc_client.return_value.bdev_lvol_convert.assert_not_called()


if __name__ == "__main__":
    unittest.main()
