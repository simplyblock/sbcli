"""The port fence must be released by us, never converted to reject by SPDK.

A blocked nvmf port auto-converts to REJECT at ack_timeout * 4 -- 8s with
nvmf_create_transport ack_timeout=2000 -- and the conversion marks every qpair
on the port rejected and drives it to QUIESCING, so the client loses the path
rather than waiting for it.

2026-09-01 iteration 29: a fence on an untouched third node ran the full 8s. A
request sat 7,999,742us in TCP_REQUEST_STATE_READY_TO_COMPLETE and was freed by
the reject timer at 8,000,000us -- 258us apart, i.e. the fence was still up.
That surfaced as an IO timeout, the JC demoted the leader, and IO arriving
afterwards was failed with a generic INTERNAL DEVICE ERROR, which
nvme-multipath does not retry on another path: client EIO, fio rc=4.

These cover the arithmetic of the guard rather than the restart flow itself.
"""
import pytest

from simplyblock_core import constants


class TestBudgets:
    def test_deadline_is_under_the_reject_threshold(self):
        """ack_timeout(2000ms) * 4 = 8s. The deadline must land below it."""
        reject_threshold = 2000 * 4 / 1000.0
        assert constants.FENCE_DEADLINE_SEC < reject_threshold
        assert reject_threshold - constants.FENCE_DEADLINE_SEC >= 0.15

    def test_one_retry_and_both_attempts_fit_the_deadline(self):
        """One retry absorbs a transient refusal without aborting a restart.

        What has to hold is that a full call -- both attempts -- still fits
        inside the deadline; the clamp in _fenced() enforces the rest.
        """
        assert constants.FENCE_RPC_RETRY == 1
        attempts = 1 + constants.FENCE_RPC_RETRY
        assert attempts * constants.FENCE_RPC_TIMEOUT_SEC < constants.FENCE_DEADLINE_SEC

    def test_single_call_budgets_fit_the_deadline(self):
        assert constants.FENCE_RPC_TIMEOUT_SEC <= constants.FENCE_DEADLINE_SEC
        assert constants.FENCE_WAIT_EXAMINE_TIMEOUT_SEC <= constants.FENCE_DEADLINE_SEC

    def test_client_keepalive_outlasts_the_worst_fence(self):
        """The client must not give up while we still hold the port."""
        assert constants.LVOL_NVME_KEEP_ALIVE_TO_TCP >= constants.FENCE_DEADLINE_SEC
        assert constants.LVOL_NVME_KEEP_ALIVE_TO >= constants.FENCE_DEADLINE_SEC


class TestClamp:
    """_fenced() clamps each call's timeout to the time left on the fence, so a
    long call started late cannot overrun the deadline."""

    @staticmethod
    def _clamp(budget, elapsed):
        remaining = constants.FENCE_DEADLINE_SEC - elapsed
        return min(budget, remaining)

    def test_full_budget_early_in_the_window(self):
        got = self._clamp(constants.FENCE_WAIT_EXAMINE_TIMEOUT_SEC, elapsed=0.2)
        assert got == constants.FENCE_WAIT_EXAMINE_TIMEOUT_SEC

    def test_long_call_started_late_is_clamped(self):
        """6s wait_for_examine begun at t=5.0 must not run to t=11.0."""
        got = self._clamp(constants.FENCE_WAIT_EXAMINE_TIMEOUT_SEC, elapsed=5.0)
        assert got == pytest.approx(2.8)
        assert 5.0 + got <= constants.FENCE_DEADLINE_SEC

    def test_clamped_call_can_never_pass_the_deadline(self):
        for elapsed in (0.0, 1.0, 3.3, 5.0, 7.0, 7.79):
            for budget in (constants.FENCE_RPC_TIMEOUT_SEC,
                           constants.FENCE_WAIT_EXAMINE_TIMEOUT_SEC):
                assert elapsed + self._clamp(budget, elapsed) <= constants.FENCE_DEADLINE_SEC + 1e-9

    def test_exhausted_budget_yields_no_time(self):
        assert self._clamp(constants.FENCE_RPC_TIMEOUT_SEC,
                           elapsed=constants.FENCE_DEADLINE_SEC) <= 0


class TestIncidentArithmetic:
    def test_the_observed_hold_would_now_be_cut_short(self):
        """The 7.9997s hold is past the deadline, so it would have aborted."""
        observed_hold_sec = 7999742 / 1_000_000
        assert observed_hold_sec > constants.FENCE_DEADLINE_SEC

    def test_deadline_beats_the_reject_timer(self):
        reject_at = 8.0
        assert constants.FENCE_DEADLINE_SEC < reject_at
