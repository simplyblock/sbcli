"""Two-node event collector: idle back-off without arbitrated clusters
(docs/design/two-node-arbitration.md, section 4). Pure logic, no DB.
"""

import threading

from simplyblock_core import constants
from simplyblock_core.services import two_node_event_collector as collector


def test_next_wait_is_idle_with_no_workers():
    collector._workers.clear()
    assert collector._next_wait_s() == constants.TWO_NODE_IDLE_RESCAN_S


def test_next_wait_is_the_reconcile_interval_with_a_worker():
    collector._workers.clear()
    collector._workers["n1"] = (threading.Thread(), threading.Event())
    try:
        assert collector._next_wait_s() == constants.TWO_NODE_COLLECTOR_RECONCILE_S
    finally:
        collector._workers.clear()
