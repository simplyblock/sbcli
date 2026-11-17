"""The del-sync lock must not outlive the pass that set it.

bf8 (2026-11-02): replication of two volumes stalled for over an hour because
every internal snapshot on their node failed with "Timed out waiting for
in-flight LVol sync deletion to drain", while no sync-delete task was pending
anywhere: process_lvol_delete_finish had set the lock (a peer looked down) and
left through an early return that skipped the reset.
"""
import ast
import inspect
import textwrap

from simplyblock_core.controllers import snapshot_controller as sc
from simplyblock_core.services import lvol_monitor


class _Node:
    def __init__(self, pending_task):
        self.locked, self.pending_task, self.resets = True, pending_task, 0

    def lvol_sync_del(self):
        return self.locked

    def lvol_del_sync_lock_reset(self):
        self.resets += 1
        self.locked = self.pending_task


def test_a_stale_lock_is_cleared_instead_of_failing_the_create():
    n = _Node(pending_task=False)
    assert sc._wait_for_node_sync_delete(n, timeout=0, poll=0) is True
    assert n.resets == 1 and not n.locked


def test_a_lock_backed_by_a_pending_task_still_blocks():
    n = _Node(pending_task=True)
    assert sc._wait_for_node_sync_delete(n, timeout=0, poll=0) is False
    assert n.locked


def test_every_return_after_taking_the_lock_releases_it():
    src = textwrap.dedent(inspect.getsource(lvol_monitor.process_lvol_delete_finish))
    tree = ast.parse(src)
    func = tree.body[0]
    lines = src.splitlines()
    taken = next(i for i, l in enumerate(lines, 1) if "primary_node.lvol_del_sync_lock()" in l)
    released = next(i for i, l in enumerate(lines, 1)
                    if i > taken and l.strip() == "primary_node.lvol_del_sync_lock_reset()"
                    and "Release the primary's del-sync gate" in "\n".join(lines[i - 6:i]))
    returns = [n.lineno for n in ast.walk(func)
               if isinstance(n, ast.Return) and taken < n.lineno < released]
    assert returns, "expected early returns after the lock is taken"
    for ln in returns:
        before = "\n".join(lines[max(0, ln - 4):ln - 1])
        assert "lvol_del_sync_lock_reset()" in before, f"return at line {ln} leaves the lock set"
