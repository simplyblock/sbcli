"""Failback fixes from the lblk_rapid_outage run (2026-09-26, LVS_1).

a) The primary's restart must quiesce the acting leader even when no peer
   reports "lvs leadership" (_presumed_acting_leader).
b) Listeners: a new one is created in its intended ANA state; an existing
   one is never added again, its ANA state is changed with
   nvmf_subsystem_listener_set_ana_state (ensure_listener,
   demote_old_leader_listeners, publish_lvol_listeners).
"""
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from simplyblock_core import storage_node_ops
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.utils import nvmf_listener

NQN = "nqn.2023-02.io.simplyblock:c:lvol:abc"


def _listener(ip, port, states=None, trtype="TCP"):
    return {
        "address": {"trtype": trtype, "adrfam": "IPv4", "traddr": ip, "trsvcid": str(port)},
        "ana_states": [{"ana_group": g, "ana_state": s} for g, s in (states or {}).items()],
    }


class EnsureListenerTest(unittest.TestCase):

    def test_absent_listener_is_created_in_its_ana_state(self):
        rpc = MagicMock()
        rpc.listeners_list.return_value = []
        rpc.listeners_create.return_value = True
        out = nvmf_listener.ensure_listener(
            rpc, NQN, "TCP", "10.0.0.1", 4428, ana_state="non_optimized")
        self.assertEqual(out, nvmf_listener.CREATED)
        rpc.listeners_create.assert_called_once_with(
            NQN, "TCP", "10.0.0.1", 4428, ana_state="non_optimized")
        rpc.nvmf_subsystem_listener_set_ana_state.assert_not_called()

    def test_existing_listener_is_never_added_again(self):
        rpc = MagicMock()
        rpc.listeners_list.return_value = [_listener("10.0.0.1", 4428, {1: "optimized"})]
        out = nvmf_listener.ensure_listener(
            rpc, NQN, "TCP", "10.0.0.1", 4428, ana_state="optimized")
        self.assertEqual(out, nvmf_listener.PRESENT)
        rpc.listeners_create.assert_not_called()
        rpc.nvmf_subsystem_listener_set_ana_state.assert_not_called()

    def test_existing_listener_changes_state_with_set_ana_state(self):
        rpc = MagicMock()
        rpc.listeners_list.return_value = [
            _listener("10.0.0.1", 4428, {1: "optimized", 2: "optimized"})]
        rpc.nvmf_subsystem_listener_set_ana_state.return_value = True
        out = nvmf_listener.ensure_listener(
            rpc, NQN, "TCP", "10.0.0.1", 4428, ana_state="non_optimized",
            anagrpid=2, set_existing_ana=True)
        self.assertEqual(out, nvmf_listener.ANA_SET)
        rpc.listeners_create.assert_not_called()
        rpc.nvmf_subsystem_listener_set_ana_state.assert_called_once_with(
            NQN, "10.0.0.1", 4428, trtype="TCP", ana="non_optimized", anagrpid=2)

    def test_existing_listener_already_in_state_is_left_alone(self):
        rpc = MagicMock()
        rpc.listeners_list.return_value = [
            _listener("10.0.0.1", 4428, {1: "optimized", 2: "non_optimized"})]
        out = nvmf_listener.ensure_listener(
            rpc, NQN, "TCP", "10.0.0.1", 4428, ana_state="non_optimized",
            anagrpid=2, set_existing_ana=True)
        self.assertEqual(out, nvmf_listener.PRESENT)
        rpc.nvmf_subsystem_listener_set_ana_state.assert_not_called()

    def test_other_address_does_not_count_as_existing(self):
        rpc = MagicMock()
        rpc.listeners_list.return_value = [_listener("10.0.0.2", 4428)]
        rpc.listeners_create.return_value = True
        out = nvmf_listener.ensure_listener(rpc, NQN, "TCP", "10.0.0.1", 4428)
        self.assertEqual(out, nvmf_listener.CREATED)

    def test_lost_race_is_treated_as_present(self):
        rpc = MagicMock()
        rpc.listeners_list.side_effect = [[], [_listener("10.0.0.1", 4428)]]
        rpc.listeners_create.return_value = None   # SPDK: Listener already exists
        out = nvmf_listener.ensure_listener(
            rpc, NQN, "TCP", "10.0.0.1", 4428, ana_state="optimized")
        self.assertEqual(out, nvmf_listener.PRESENT)

    def test_failed_create_raises(self):
        rpc = MagicMock()
        rpc.listeners_list.return_value = []
        rpc.listeners_create.return_value = None
        with self.assertRaises(RuntimeError):
            nvmf_listener.ensure_listener(rpc, NQN, "TCP", "10.0.0.1", 4428)

    def test_unreadable_listeners_raise_instead_of_guessing(self):
        rpc = MagicMock()
        rpc.listeners_list.side_effect = TimeoutError("rpc timeout")
        with self.assertRaises(TimeoutError):
            nvmf_listener.ensure_listener(rpc, NQN, "TCP", "10.0.0.1", 4428)
        rpc.listeners_create.assert_not_called()


def _node(node_id, ips=("10.0.0.2",), port=4428, rpc=None):
    nics = [SimpleNamespace(ip4_address=ip, trtype="TCP") for ip in ips]
    node = MagicMock()
    node.get_id.return_value = node_id
    node.data_nics = nics
    node.active_rdma = False
    node.active_tcp = True
    node.get_lvol_subsys_port.return_value = port
    node.rpc_client.return_value = rpc or MagicMock()
    return node


class DemoteOldLeaderTest(unittest.TestCase):

    def test_existing_listeners_get_set_ana_state_per_volume_group(self):
        rpc = MagicMock()
        rpc.listeners_list.return_value = [
            _listener("10.0.0.2", 4428, {1: "optimized", 2: "optimized"})]
        rpc.nvmf_subsystem_listener_set_ana_state.return_value = True
        sec = _node("sec", rpc=rpc)
        lvols = [SimpleNamespace(nqn=NQN, lvs_name="LVS_1", ns_id=1),
                 SimpleNamespace(nqn=NQN, lvs_name="LVS_1", ns_id=2)]
        storage_node_ops.demote_old_leader_listeners(sec, lvols)
        rpc.listeners_create.assert_not_called()
        groups = sorted(c.kwargs["anagrpid"] for c in
                        rpc.nvmf_subsystem_listener_set_ana_state.call_args_list)
        self.assertEqual(groups, [1, 2])
        for c in rpc.nvmf_subsystem_listener_set_ana_state.call_args_list:
            self.assertEqual(c.kwargs["ana"], "non_optimized")

    def test_missing_listener_is_created_non_optimized(self):
        rpc = MagicMock()
        rpc.listeners_list.return_value = []
        rpc.listeners_create.return_value = True
        sec = _node("sec", rpc=rpc)
        storage_node_ops.demote_old_leader_listeners(
            sec, [SimpleNamespace(nqn=NQN, lvs_name="LVS_1", ns_id=1)])
        rpc.listeners_create.assert_called_once_with(
            NQN, "TCP", "10.0.0.2", 4428, ana_state="non_optimized")
        rpc.nvmf_subsystem_listener_set_ana_state.assert_not_called()


class PublishLvolListenersTest(unittest.TestCase):

    def _lvol(self):
        return SimpleNamespace(nqn=NQN, lvs_name="LVS_1", fabric="tcp",
                               node_id="prim", get_id=lambda: "lvol-1")

    def test_existing_listener_is_not_added_again(self):
        rpc = MagicMock()
        rpc.listeners_list.return_value = [_listener("10.0.0.2", 4428)]
        node = _node("prim", rpc=rpc)
        ok, err = lvol_controller.publish_lvol_listeners(self._lvol(), node, rpc_client=rpc)
        self.assertTrue(ok)
        rpc.listeners_create.assert_not_called()
        rpc.nvmf_subsystem_add_listener.assert_not_called()

    def test_new_listener_is_created_in_the_role_state(self):
        rpc = MagicMock()
        rpc.listeners_list.return_value = []
        rpc.listeners_create.return_value = True
        node = _node("sec", rpc=rpc)
        ok, err = lvol_controller.publish_lvol_listeners(
            self._lvol(), node, rpc_client=rpc, is_primary=False)
        self.assertTrue(ok)
        rpc.listeners_create.assert_called_once_with(
            NQN, "TCP", "10.0.0.2", 4428, ana_state="non_optimized")


class PresumedActingLeaderTest(unittest.TestCase):

    def _lvs(self, owner="prim", sec="sec", ter="ter"):
        lvs = MagicMock()
        lvs.get_id.return_value = owner
        lvs.secondary_node_id = sec
        lvs.tertiary_node_id = ter
        return lvs

    def test_secondary_is_presumed_when_connected(self):
        snode = _node("prim")
        sec, ter = _node("sec"), _node("ter")
        got = storage_node_ops._presumed_acting_leader(self._lvs(), [sec, ter], set(), snode)
        self.assertIs(got, sec)

    def test_tertiary_when_secondary_disconnected(self):
        snode = _node("prim")
        sec, ter = _node("sec"), _node("ter")
        got = storage_node_ops._presumed_acting_leader(self._lvs(), [sec, ter], {"sec"}, snode)
        self.assertIs(got, ter)

    def test_none_when_no_peer_connected(self):
        snode = _node("prim")
        sec, ter = _node("sec"), _node("ter")
        got = storage_node_ops._presumed_acting_leader(
            self._lvs(), [sec, ter], {"sec", "ter"}, snode)
        self.assertIsNone(got)

    def test_none_when_snode_is_not_the_lvs_owner(self):
        snode = _node("sec")
        ter = _node("ter")
        got = storage_node_ops._presumed_acting_leader(self._lvs(), [ter], set(), snode)
        self.assertIsNone(got)


if __name__ == "__main__":
    unittest.main()


class RecreateUsesPresumedLeaderTest(unittest.TestCase):
    """Structural guard: the fallback runs right after the leader probe and
    before anything that depends on current_leader (compression check,
    replication wait, port fence, quiesce)."""

    def _src(self):
        import inspect
        import re
        src = inspect.getsource(storage_node_ops._recreate_lvstore_impl)
        return re.sub('""".*?"""', "", src, flags=re.DOTALL)

    def test_fallback_follows_the_probe_and_precedes_the_quiesce(self):
        src = self._src()
        i_probe = src.index('ret[0].get("lvs leadership")')
        i_fallback = src.index("_presumed_acting_leader(")
        i_compression = src.index("jc_compression_get_status(lvs_jm_vuid)")
        i_fence = src.index(
            "port_block.set_port(sec_node, snode_lvs_port, block=True")
        i_disable = src.index(".jc_disable_replication(lvs_jm_vuid)")
        i_examine = src.index('_fenced("bdev_distrib_force_to_non_leader"')
        self.assertLess(i_probe, i_fallback)
        self.assertLess(i_fallback, i_compression)
        self.assertLess(i_fallback, i_fence)
        self.assertLess(i_disable, i_examine)

    def test_fallback_only_when_the_probe_found_no_leader(self):
        src = self._src()
        i_fallback = src.index("_presumed_acting_leader(")
        head = src[max(0, i_fallback - 200):i_fallback]
        self.assertIn("if current_leader is None:", head)

    def test_step_11_no_longer_adds_existing_listeners(self):
        src = self._src()
        i = src.index("Demoted subsystems to non_optimized on old leader")
        window = src[max(0, i - 600):i]
        self.assertIn("demote_old_leader_listeners(", window)
        self.assertNotIn("listeners_create(", window)
