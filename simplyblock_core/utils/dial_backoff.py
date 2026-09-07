"""Per-address backoff for cluster-internal dial-outs.

A dial-out is any attempt to (re)connect an NVMe controller path to a peer:
device attaches, JM attaches, the health service's multipath repairs. When the
peer address cannot answer, every one of those fails after a connect timeout —
and repeating them accomplishes nothing except burning the *caller's* SPDK
app-thread time on connect polling.

Status-gating (health_controller.repairs_allowed) cannot cover the case that
motivated this module: in run mass_create_delete_docker-20260821 a node's SPDK
was dead for hours while its DB record said ONLINE (the monitor thread that
would have flipped it was captured elsewhere). Every repair path in the
cluster kept dialling its addresses — connection refused, attach, refused,
attach — until a healthy peer's app thread was so busy failing connects that
its own RPC port stopped answering, the monitor declared *it* dead, and the
cascade suspended the cluster. The record was wrong, so no status check could
have helped; only the dial failures themselves carried the truth.

So: consecutive failures against the same address earn that address a hold,
doubling from BASE_HOLD_SEC up to MAX_HOLD_SEC. One success clears it. The
first FAILURES_BEFORE_HOLD failures are free, so ordinary transient hiccups
(a restart racing a reconnect) never delay a repair.

Process-local by design — each service backs off based on what it itself
observed, and a restart of the service starts fresh. That is the behaviour
wanted from a circuit breaker guarding thread time, not a cluster-wide truth
store.
"""

import threading
import time


#: Failures against one address before dials to it start being held.
FAILURES_BEFORE_HOLD = 3
#: First hold, seconds. Doubles per further failure.
BASE_HOLD_SEC = 10.0
#: Ceiling for the hold. A dead peer is probed at least this often.
#:
#: Was 300s, which is what a dead peer deserves but not what a 30-second NIC
#: outage deserves. In soak run 20260825_155730 every all-nodes NIC flap left
#: path healing stalled on a plateau of exactly 15 missing paths -- all of them
#: one held address -- for 250-306s, i.e. this ceiling, because the hold can
#: only be cleared by a success that allowed() refuses to let anybody attempt.
#: The address was reachable again within 30s; we simply would not look. All
#: three data corruptions in that run happened inside those windows.
#:
#: The breaker's purpose is to stop a caller burning app-thread time on connect
#: polling, and a probe once a minute does not do that, so a minute is enough
#: ceiling to serve the purpose while bounding how long a returning path stays
#: unrepaired.
MAX_HOLD_SEC = 60.0

_lock = threading.Lock()
#: key -> [consecutive_failures, next_allowed_monotonic]
_state: dict = {}


def allowed(key) -> bool:
    """Whether a dial to ``key`` (usually a peer traddr) may be attempted now."""
    with _lock:
        entry = _state.get(key)
        if entry is None:
            return True
        return time.monotonic() >= entry[1]


def record_failure(key) -> None:
    """Note a failed dial to ``key``; enough of them earn the address a hold."""
    with _lock:
        entry = _state.setdefault(key, [0, 0.0])
        entry[0] += 1
        excess = entry[0] - FAILURES_BEFORE_HOLD
        if excess >= 0:
            hold = min(BASE_HOLD_SEC * (2 ** excess), MAX_HOLD_SEC)
            entry[1] = time.monotonic() + hold


def record_success(key) -> None:
    """A successful dial clears the address entirely."""
    with _lock:
        _state.pop(key, None)


def clear(key) -> bool:
    """Drop any hold on ``key`` because it is known to be reachable again.

    For evidence of reachability that is not itself a dial: a peer's NIC came
    back, or a dial to the same address succeeded for a different bdev. State
    is keyed by address, so one caller clearing it unblocks every bdev that
    shares that address -- which is the point, since a single held address
    accounted for all fifteen unrepaired paths per iteration on 2026-08-25.

    Deliberately NOT driven by the peer's DB status: a node whose record says
    ONLINE while its SPDK is dead is the exact case this module exists for
    (mass_create_delete_docker-20260821), so status is not evidence. Only
    something that actually observed traffic to the address may call this.

    Returns True if a hold was dropped, so callers can log the transition.
    """
    with _lock:
        entry = _state.pop(key, None)
    return entry is not None and entry[1] > 0.0


def held_keys() -> list:
    """Addresses currently under a hold (for logging/inspection)."""
    now = time.monotonic()
    with _lock:
        return sorted(k for k, v in _state.items() if v[1] > now)
