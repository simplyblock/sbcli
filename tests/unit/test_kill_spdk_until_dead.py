# coding=utf-8
"""_kill_spdk_until_dead must not report death it did not observe.

Its return value gates whether the caller drops the StorageNode record
(storage_node_ops.add_node, "did not come up after creation" path). If it
says True while the pod is alive, the record goes and the pod stays --
referenced by nothing, reaped by nothing, holding the host's hugepages, so
every later add-node attempt on that host stays Pending.

Seen 2026-09-11: worker tqmtr's API stopped answering, which is *why* the
add failed; the liveness probe therefore raised, the old code read that as
"confirmed down", the record was dropped and pod 4426 was left behind. The
deploy wedged at 11/12 until it was deleted by hand.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.snode_client import SNodeClientException


def _node():
    n = MagicMock()
    n.get_id = MagicMock(return_value="node-1")
    n.rpc_port = 4426
    n.cluster_id = "cluster-1"
    n.mgmt_ip = "10.0.0.1"
    return n


class TestKillSpdkUntilDead(unittest.TestCase):

    def setUp(self):
        patcher = patch.object(storage_node_ops.time, "sleep", lambda *_a, **_k: None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _run(self, api):
        node = _node()
        node.client = MagicMock(return_value=api)
        return storage_node_ops._kill_spdk_until_dead(node, max_attempts=2,
                                                      poll_per_attempt_sec=1,
                                                      poll_interval=0.25)

    def test_true_when_probe_answers_not_up(self):
        api = MagicMock()
        api.spdk_process_kill = MagicMock(return_value=(True, ""))
        api.spdk_process_is_up = MagicMock(return_value=(False, ""))
        self.assertTrue(self._run(api))

    def test_false_when_probe_keeps_saying_up(self):
        api = MagicMock()
        api.spdk_process_kill = MagicMock(return_value=(True, ""))
        api.spdk_process_is_up = MagicMock(return_value=(True, ""))
        self.assertFalse(self._run(api))

    def test_false_when_the_probe_cannot_be_made(self):
        """The regression: node API unreachable must read as UNKNOWN, not dead."""
        api = MagicMock()
        api.spdk_process_kill = MagicMock(side_effect=SNodeClientException("connection refused"))
        api.spdk_process_is_up = MagicMock(side_effect=SNodeClientException("connection refused"))
        self.assertFalse(
            self._run(api),
            "an unreachable node API must not be reported as SPDK confirmed down -- "
            "the caller would drop the StorageNode record and orphan a live pod")

    def test_false_when_only_the_probe_fails(self):
        """Kill 'succeeds' but liveness cannot be verified -- still not death."""
        api = MagicMock()
        api.spdk_process_kill = MagicMock(return_value=(True, ""))
        api.spdk_process_is_up = MagicMock(side_effect=SNodeClientException("name resolution"))
        self.assertFalse(self._run(api))

    def test_recovers_when_the_probe_answers_on_a_later_round(self):
        """A transient probe failure must not abort the wait."""
        api = MagicMock()
        api.spdk_process_kill = MagicMock(return_value=(True, ""))
        api.spdk_process_is_up = MagicMock(
            side_effect=[SNodeClientException("blip"), (False, "")])
        self.assertTrue(self._run(api))


if __name__ == "__main__":
    unittest.main()
