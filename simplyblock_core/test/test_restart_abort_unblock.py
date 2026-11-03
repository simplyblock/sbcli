"""The restart abort path must release every fenced peer.

unblock_peer_port removes the peer from blocked_peers, so a loop over the
live list skips every second entry: the acting leader (appended after the
non-leaders) stayed fenced, its client port went to reject mode, and the
cluster lost two more nodes to a JM writer race (EKS bed 2026-10-30). The
loops sit in _LvsFence.abort and in the hand-off's cleanup, so this checks
the source.
"""
import ast
import inspect
import textwrap

from simplyblock_core import storage_node_ops

_UNBLOCK = "unblock_peer_port"


def _name(expr):
    """The trailing name of ``x`` or ``a.b.x``; None for anything else."""
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Attribute):
        return expr.attr
    return None


def _unblock_loops(tree):
    for node in ast.walk(tree):
        if not isinstance(node, ast.For):
            continue
        if any(isinstance(c, ast.Call) and (_name(c.func) or "").lstrip("_") == _UNBLOCK
               for c in ast.walk(node)):
            yield node


def test_unblock_loops_iterate_over_a_copy():
    tree = ast.parse(inspect.getsource(storage_node_ops))
    loops = list(_unblock_loops(tree))
    abort = ast.parse(textwrap.dedent(inspect.getsource(storage_node_ops._LvsFence.abort)))
    assert list(_unblock_loops(abort)), "expected the unblock loop of _LvsFence.abort"
    for loop in loops:
        assert _name(loop.iter) != "blocked_peers", (
            f"line {loop.lineno}: iterating blocked_peers while unblock_peer_port "
            f"removes from it skips peers; iterate over list(blocked_peers)")


def test_removing_while_iterating_skips_the_second_peer():
    # The failure mode itself: the leader is the second entry.
    blocked = ["tertiary", "leader"]
    seen = []
    for peer in blocked:  # noqa: PERF101
        seen.append(peer)
        blocked.remove(peer)
    assert seen == ["tertiary"]

    blocked = ["tertiary", "leader"]
    seen = []
    for peer in list(blocked):  # noqa: PERF101
        seen.append(peer)
        blocked.remove(peer)
    assert seen == ["tertiary", "leader"]
