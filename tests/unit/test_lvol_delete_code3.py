"""Delete status 3 (async done, leadership moved) must self-heal.

`bdev_lvol_get_lvol_delete_status` returns 3 when the async (data-plane) delete
finished but LVS leadership then moved, so the follow-up per-node sync deletes
are blocked. The data clusters are already freed; only the per-node record /
subsystem / bdev cleanup remains, and that does NOT depend on who holds
leadership now.

The `ret == 3` branch in lvol_monitor used to only log and fall through -- the
sole delete status that took no action (`ret == 4` and `ret == -35`, both also
leadership situations, call process_lvol_delete_try_again). Production run
n_plus_k_failover_multi_client_ha_all_nodes-20260908-174343: a supported 2-host
outage moved LVS_1's primary+secondary together, the clone's async delete
completed on the tertiary, leadership moved back to the primary 44s later, and
LVolMonitor then logged "error code: 3" 2250x for one lvol with zero state
change -- still going 14 min after the test gave up -- while retried client
DELETEs short-circuited on in_deletion and returned success in ms.
"""
import inspect

from simplyblock_core.services import lvol_monitor


def _finish_src():
    return inspect.getsource(lvol_monitor.process_lvol_delete_finish)


class TestRet3NowRecovers:
    def test_ret3_no_longer_only_logs(self):
        mon = inspect.getsource(lvol_monitor)
        i = mon.index("elif ret == 3:")
        j = mon.index("elif ret == 4:")
        branch = mon[i:j]
        assert "process_lvol_delete_finish(cluster, lvol" in branch
        assert "leader_independent=True" in branch

    def test_ret3_drives_the_leader_independent_completion(self):
        mon = inspect.getsource(lvol_monitor)
        i = mon.index("elif ret == 3:")
        j = mon.index("elif ret == 4:")
        assert "leader_independent=True" in mon[i:j]

    def test_ret3_is_no_longer_the_only_inert_status(self):
        """Every delete status must take an action."""
        mon = inspect.getsource(lvol_monitor)
        # each elif ret == N branch should reference a recovery/finish call
        i = mon.index("if ret == 0 or ret == 2:")
        j = mon.index("except Exception", i) if "except Exception" in mon[i:] else len(mon)
        block = mon[i:j]
        # the whole dispatch must not contain a branch that only logs the code 3
        assert 'logger.error("Async deletion is done, but leadership has changed (sync deletion is now blocked)")' not in block


class TestLeaderIndependentFinish:
    def test_signature_has_the_flag_defaulting_false(self):
        sig = inspect.signature(lvol_monitor.process_lvol_delete_finish)
        assert "leader_independent" in sig.parameters
        assert sig.parameters["leader_independent"].default is False

    def test_it_skips_the_leader_probe_when_independent(self):
        src = _finish_src()
        i = src.index("if leader_independent:")
        end = src.index(chr(10)+"    else:"+chr(10)+"        leader_node = None", i)
        window = src[i:end]
        # no get_lvstores leadership probe inside the independent branch
        assert "bdev_lvol_get_lvstores" not in window

    def test_it_never_raises_no_leader_in_the_independent_path(self):
        """The old finish raised 'Failed to get leader node'; that must be
        guarded out when the async delete is already done."""
        src = _finish_src()
        assert 'if not leader_independent:' in src
        # the raise is now inside the not-leader_independent block
        i = src.index('raise Exception("Failed to get leader node")')
        guard = src.rfind("if not leader_independent:", 0, i)
        stickiness = src.rfind("if leader_independent:", 0, i)
        assert guard > stickiness, "the raise must be under the non-independent guard"

    def test_it_prefers_the_async_delete_node_first(self):
        src = _finish_src()
        i = src.index("if leader_independent:")
        end = src.index(chr(10)+"    else:"+chr(10)+"        leader_node = None", i)
        window = src[i:end]
        assert "lvol.deletion_status" in window
        assert "lvol.nodes" in window  # then P->S->T fallback

    def test_it_forces_the_primary_sync_delete_past_the_gate(self):
        """The async node is no longer leader, so its sync delete would be
        skipped by check_non_leader_for_operation without force."""
        src = _finish_src()
        assert "force=leader_independent" in src

    def test_no_online_node_retries_next_pass_rather_than_crashing(self):
        src = _finish_src()
        i = src.index("if leader_independent:")
        end = src.index(chr(10)+"    else:"+chr(10)+"        leader_node = None", i)
        window = src[i:end]
        assert "will retry next pass" in window
        assert "return" in window

    def test_the_normal_path_still_probes_leadership_and_can_raise(self):
        """leader_independent=False must behave exactly as before."""
        src = _finish_src()
        assert "bdev_lvol_get_lvstores" in src
        assert 'raise Exception("Failed to get leader node")' in src

    def test_the_normal_primary_delete_is_not_forced(self):
        """force must be True only in the independent path."""
        src = _finish_src()
        # the single delete_lvol_from_node(...sync=True...) uses force=leader_independent
        assert "force=True" not in src.replace("force=leader_independent", "")
