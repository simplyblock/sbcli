"""Two-node arbiter service (docs/design/two-node-arbitration.md, section 7).

Two loops in one process:

- the lease loop renews every arbitrated node's lease every
  ``TWO_NODE_LEASE_RENEW_MS`` (its own thread, so a slow decision never delays
  a renewal);
- the decision loop folds the events the Go collector queued in FDB, decides
  and applies verdicts every ``TWO_NODE_DECISION_TICK_MS``.

Only clusters with ``two_node_arbitration`` on and exactly two members are
touched; everything else is ignored, so the service is safe to run everywhere.
"""

import threading
import time

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.arbitration.arbiter import Arbiter

logger = utils.get_logger(__name__)


def _lease_loop(arbiter: Arbiter, stop: threading.Event) -> None:
    interval = constants.TWO_NODE_LEASE_RENEW_MS / 1000.0
    while not stop.is_set():
        started = time.monotonic()
        try:
            arbiter.renew_all()
        except Exception:                           # noqa: BLE001 - keep renewing
            logger.exception("Lease loop failed")
        stop.wait(max(0.0, interval - (time.monotonic() - started)))


def main() -> None:
    db = db_controller.DBController()
    arbiter = Arbiter(db)
    stop = threading.Event()
    threading.Thread(target=_lease_loop, args=(arbiter, stop), daemon=True,
                     name="lease-loop").start()
    logger.info("Two-node arbiter started")
    interval = constants.TWO_NODE_DECISION_TICK_MS / 1000.0
    while True:
        try:
            arbiter.tick()
        except Exception:                           # noqa: BLE001 - next tick retries
            logger.exception("Decision loop failed")
        time.sleep(interval)


if __name__ == "__main__":
    main()
