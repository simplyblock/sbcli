"""Every port-blocked peer must be drained before the journal is synchronised
and leadership moves -- not just the acting leader.

Blocking a peer's client port stops NEW IO reaching it. It does not empty what
is already in its distrib pipeline. The restart then does two things that
change journal ownership while that IO is still landing --
``jc_explicit_synchronization`` and the leadership take in ### 7 -- and the
peer's own hublvol redirect is not established until ### 8b, later in the same
fence. A peer holding IO with no redirect promotes itself on the next write
(``spdk_lvs_trigger_leadership_switch``: "Leadership changed due to receive new
IO") and a dual-leader writer conflict follows.

2026-09-04 17:11:58, cluster fc32e143, LVS_10. The tertiary's port 4440 was
blocked at .721 while the control plane itself logged "LVS_10/hublvoln1 not
surfaced yet". jc_explicit_synchronization went to the primary at .729. Three
milliseconds later all four JM replicas reported jfi_r_wr_lock conflicts with
disagreeing lock holders. The tertiary connected its hublvol only at .913 --
198 ms after leadership had already moved. Four nodes that were never part of
the outage self-aborted by 17:12:11 and the cluster suspended.

The existing ### 4 drain already encodes this reasoning; it was simply only
ever applied to the acting leader.
"""
import inspect

from simplyblock_core import storage_node_ops


def _fence_body():
    return inspect.getsource(storage_node_ops._recreate_lvstore_impl)


class TestPeerDrainExists:
    def test_there_is_a_peer_drain_step(self):
        assert "### 5b- drain in-flight IO on the OTHER blocked peers too" in _fence_body()

    def test_it_iterates_the_blocked_peers(self):
        body = _fence_body()
        i = body.index("### 5b-")
        window = body[i:i + 4500]
        assert "for _peer in blocked_peers:" in window

    def test_it_skips_the_leader_already_drained_by_step_4(self):
        body = _fence_body()
        i = body.index("### 5b-")
        window = body[i:i + 4500]
        assert "continue" in window and "current_leader" in window

    def test_it_polls_the_same_inflight_rpc_as_the_leader_drain(self):
        body = _fence_body()
        i = body.index("### 5b-")
        window = body[i:i + 4500]
        assert "bdev_distrib_check_inflight_io" in window


class TestOrdering:
    """The drain is worthless if it runs after the thing it protects."""

    def test_peer_drain_precedes_the_journal_sync(self):
        body = _fence_body()
        assert body.index("### 5b-") < body.index('_fenced("jc_explicit_synchronization"')

    def test_peer_drain_precedes_the_leadership_take(self):
        body = _fence_body()
        assert body.index("### 5b-") < body.index("### 7- take leadership")

    def test_peer_drain_precedes_the_peer_hublvol_connect(self):
        """It must also sit ahead of ### 8b, since the whole point is that the
        peer has no redirect yet at this moment."""
        body = _fence_body()
        assert body.index("### 5b-") < body.index("### 8b-")


class TestPolicy:
    def test_an_undrained_peer_warns_and_proceeds(self):
        """Deliberately NOT an abort, unlike the leader drain.

        A secondary/tertiary serves REDIRECTED IO from the leader through its
        hublvol, and blocking its client port does not stop that -- its
        pipeline for this jm_vuid can be legitimately busy for the whole
        window. Aborting there fails restarts routinely: 19 of the
        ftt2/test_restart_scenarios cases did exactly that on the first cut."""
        body = _fence_body()
        i = body.index("### 5b-")
        window = body[i:i + 5200]
        assert "Inflight IO did not drain on blocked peer" in window
        # the abort belongs to the leader drain, not this one
        after = window[window.index("if not _peer_drained:"):]
        assert "_abort_restart_and_unblock(" not in after

    def test_the_fence_deadline_is_checked_inside_the_poll(self):
        """Several peers x a 2s bound must not be able to overrun the fence."""
        body = _fence_body()
        i = body.index("### 5b-")
        window = body[i:i + 4500]
        assert '_check_fence_deadline("peer inflight drain")' in window

    def test_the_bound_is_short_and_shared_with_the_leader_drain(self):
        assert storage_node_ops._DRAIN_BOUND_SEC_DEFAULT <= 2.0
        assert storage_node_ops._DRAIN_POLL_SEC_DEFAULT <= 0.1
        # the leader drain must use the same constant, not a private literal
        body = _fence_body()
        assert "_DRAIN_BOUND_SEC = _DRAIN_BOUND_SEC_DEFAULT" in body

    def test_peer_rpcs_are_bounded_by_the_fence_budget(self):
        body = _fence_body()
        i = body.index("### 5b-")
        window = body[i:i + 4500]
        assert "constants.FENCE_RPC_TIMEOUT_SEC" in window
        assert "constants.FENCE_RPC_RETRY" in window
