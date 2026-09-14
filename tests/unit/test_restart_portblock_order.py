"""On a primary restart, block non-leader ports BEFORE the leader, and start
the demote only once every port is blocked.

The leader path (_recreate_lvstore_impl) used to block the leader first,
suspend its replication, and only then block the non-leaders. That left a
window between the leader's replication-disable and the non-leader block in
which a non-leader (e.g. the tertiary) kept serving client IO and redirected it
through the hublvol to a leader whose leadership was mid-transition ->
writer_conflict on the journal.

Correct order:
    1. block every non-leader peer's LVS port,
    2. block the leader's port + suspend its replication,
    3. drain in-flight IO + drop leadership (the demote).
"""
import inspect
import re

from simplyblock_core import storage_node_ops


def _src():
    return inspect.getsource(storage_node_ops._recreate_lvstore_impl)


def _code(src):
    return re.sub('""".*?"""', "", src, flags=re.DOTALL)


class TestPortBlockOrder:
    def test_nonleader_block_precedes_leader_block(self):
        src = _code(_src())
        # the non-leader loop blocks sec_node ports; the leader block opens
        # with its own retry bound, _REPL_SUSPEND_MAX_ATTEMPTS. Do NOT anchor
        # on `if current_leader and ... not in disconnected_peers:` -- the
        # pre-fence JM-replication wait guards on that same condition and is
        # deliberately earlier, so the string is not unique.
        i_nonleader = src.index(
            "port_block.set_port(sec_node, snode_lvs_port, block=True")
        i_leader_open = src.index("_REPL_SUSPEND_MAX_ATTEMPTS = 10")
        assert i_nonleader < i_leader_open, \
            "non-leader ports must be blocked before the leader block opens"

    def test_demote_starts_after_all_ports_blocked(self):
        src = _code(_src())
        i_nonleader = src.index(
            "port_block.set_port(sec_node, snode_lvs_port, block=True")
        # the demote == suspend replication then drop leadership
        i_disable_repl = src.index(".jc_disable_replication(lvs_jm_vuid)")
        i_drop = src.index("bdev_lvol_set_leader(lvs_name, leader=False")
        assert i_nonleader < i_disable_repl, \
            "replication-suspend (demote start) must follow the non-leader block"
        assert i_nonleader < i_drop, \
            "leadership drop (demote) must follow the non-leader block"

    def test_leader_block_precedes_the_demote(self):
        src = _code(_src())
        i_leader_open = src.index("_REPL_SUSPEND_MAX_ATTEMPTS = 10")
        i_disable_repl = src.index(".jc_disable_replication(lvs_jm_vuid)")
        assert i_leader_open < i_disable_repl

    def test_the_order_is_documented(self):
        src = _src()
        assert "ORDER MATTERS" in src
        assert "non-leader peers FIRST" in src

    def test_nonleader_block_still_aborts_on_failure(self):
        """The reorder must keep the abort-on-failure behaviour."""
        src = _code(_src())
        i = src.index("port_block.set_port(sec_node, snode_lvs_port, block=True")
        window = src[i:i + 1100]
        assert "_abort_restart_and_unblock(" in window

    def test_nonleader_block_skips_leader_and_disconnected_and_already_blocked(self):
        src = _code(_src())
        # anchor on the non-leader block's own port_block, then look at the
        # loop head just above it (there are several sec_node loops).
        j = src.index("port_block.set_port(sec_node, snode_lvs_port, block=True")
        i = src.rfind("for sec_node in sec_nodes:", 0, j)
        window = src[i:j]
        assert "sec_node is current_leader" in window
        assert "disconnected_peers" in window
        assert "in blocked_peers" in window

    def test_no_duplicate_nonleader_block_remains(self):
        """The block must have MOVED, not been copied."""
        src = _src()
        assert src.count(
            "port_block.set_port(sec_node, snode_lvs_port, block=True") == 1


class TestJmReplicationWaitStaysOutsideTheFence:
    """Nothing that can sleep for seconds may run while a peer port is fenced.

    Regression: 2026-09-10 14:32:20, LVS_13. The reorder above put the
    non-leader block ahead of the leader's suspend loop, and step (a) of that
    loop -- wait_for_jm_rep_tasks_to_finish -- defaults to retry=10/delay=20:
    a single sleep of 20s, against FENCE_DEADLINE_SEC of 7.5s. The leader
    reported active replication, the wait slept 20s, and the next fence check
    (the inflight drain, ~350 lines later) aborted the restart at 20.087s.

    SPDK converts a port block to reject at ack_timeout * 4 = 8s, quiescing
    every qpair on the port, so the fenced peer's clients lost the path
    instead of waiting for it. With replication active on the leader that
    abort was certain, not a flake.

    The fix is structural: pay the patient wait BEFORE anything is fenced,
    and leave only an unpaced confirmation poll inside.
    """

    def test_patient_wait_runs_before_any_port_is_fenced(self):
        """Located by argument shape, not by source text: the call spans
        lines and its exact formatting is not the invariant."""
        src = _code(_src())
        i_fence = src.index(
            "port_block.set_port(sec_node, snode_lvs_port, block=True")
        patient = [
            m.start() for m in re.finditer(
                r"wait_for_jm_rep_tasks_to_finish\((.*?)\)", src, re.DOTALL)
            if "retry=" not in m.group(1)
        ]
        assert patient, (
            "the patient (default-budget) JM-replication wait has gone "
            "missing -- it must still be paid, just not under the fence")
        assert all(i < i_fence for i in patient), (
            "the patient JM-replication wait must be paid before any peer "
            "port is fenced -- inside the fence its 20s sleep blows "
            "FENCE_DEADLINE_SEC and the 8s reject threshold")

    def test_every_wait_inside_the_fence_is_explicitly_bounded(self):
        src = _code(_src())
        i_fence = src.index(
            "port_block.set_port(sec_node, snode_lvs_port, block=True")
        calls = re.findall(
            r"wait_for_jm_rep_tasks_to_finish\((.*?)\)",
            src[i_fence:], re.DOTALL)
        assert calls, "expected an in-fence confirmation poll to still exist"
        for args in calls:
            assert "retry=" in args and "delay=" in args, (
                "a wait_for_jm_rep_tasks_to_finish call inside the fence must "
                f"override the 10x20s default budget, got: {args!r}")

    def test_fence_clock_is_checked_before_the_demote_begins(self):
        """The clock went unchecked from the fence to the inflight drain."""
        src = _code(_src())
        i_fence = src.index(
            "port_block.set_port(sec_node, snode_lvs_port, block=True")
        i_check = src.index('_check_fence_deadline("jm replication confirm")')
        i_disable = src.index(".jc_disable_replication(lvs_jm_vuid)")
        assert i_fence < i_check < i_disable, (
            "the fence deadline must be checked between the non-leader fence "
            "and the replication-suspend, so an overrun aborts while still "
            "under the 8s reject threshold")

    def test_the_fence_check_is_not_swallowed_by_the_replication_except(self):
        """_check_fence_deadline raises to abort. Inside the try, the
        `except Exception` below re-wraps it as "replication-wait failed",
        which mislabels the abort in the logs."""
        src = _code(_src())
        i_check = src.index('_check_fence_deadline("jm replication confirm")')
        i_except = src.index(
            "Abort restart: replication-wait on leader")
        assert i_except < i_check, (
            "the fence check must sit after the replication-wait except "
            "block, not inside its try")
