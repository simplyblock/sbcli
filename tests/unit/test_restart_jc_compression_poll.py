"""Restart: an active journal compression on the LVS leader is polled every few
seconds, for at most 10 minutes as before.

Polling once a minute added up to a minute to a restart whose node was killed
during a compression (the survivors finish it seconds after the kill once their
JMs drop the dead reader), and the 10-minute budget is the same.
"""

import inspect

from simplyblock_core import storage_node_ops


def test_poll_interval_is_short():
    assert 1 <= storage_node_ops.RESTART_JC_COMPRESSION_POLL_S <= 10


def test_total_wait_budget_is_unchanged():
    src = inspect.getsource(storage_node_ops)
    assert "retries = int(600 // poll_s)" in src
    assert "time.sleep(poll_s)" in src
    assert "time.sleep(60)" not in src.split("JC compression active on leader")[0][-2000:]
