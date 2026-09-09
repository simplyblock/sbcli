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
        # the non-leader loop blocks sec_node ports; the leader block opens with
        # `if current_leader and current_leader.get_id() not in disconnected_peers:`
        i_nonleader = src.index(
            "port_block.set_port(sec_node, snode_lvs_port, block=True")
        i_leader_open = src.index(
            "if current_leader and current_leader.get_id() not in disconnected_peers:")
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
        i_leader_open = src.index(
            "if current_leader and current_leader.get_id() not in disconnected_peers:")
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
