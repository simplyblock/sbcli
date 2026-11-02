"""The restart abort path must release every fenced peer.

_unblock_peer_port removes the peer from blocked_peers, so a loop over the
live list skips every second entry: the acting leader (appended after the
non-leaders) stayed fenced, its client port went to reject mode, and the
cluster lost two more nodes to a JM writer race (EKS bed 2026-10-30). The loop
sits inside a closure of _recreate_lvstore_impl, so this checks the source.
"""
import ast
import inspect

from simplyblock_core import storage_node_ops


def _loops_calling(tree, fn_name):
    for node in ast.walk(tree):
        if not isinstance(node, ast.For):
            continue
        calls = [c for c in ast.walk(node) if isinstance(c, ast.Call)
                 and isinstance(c.func, ast.Name) and c.func.id == fn_name]
        if calls:
            yield node


def test_unblock_loops_iterate_over_a_copy():
    tree = ast.parse(inspect.getsource(storage_node_ops))
    loops = list(_loops_calling(tree, "_unblock_peer_port"))
    assert loops, "expected the abort path's unblock loop"
    for loop in loops:
        it = loop.iter
        assert not (isinstance(it, ast.Name) and it.id == "blocked_peers"), (
            f"line {loop.lineno}: iterating blocked_peers while _unblock_peer_port "
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
