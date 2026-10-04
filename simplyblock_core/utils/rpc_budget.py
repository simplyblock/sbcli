"""Ambient RPC budget for code running inside a port-fence window.

While a peer's client client port is blocked, every RPC issued underneath must be
short. Threading ``timeout=``/``retry=`` through each call site does not work
here: the fenced region calls model methods (``recreate_hublvol``,
``connect_to_hublvol``, ``create_transfer_hublvol``, ...) which build their own
``rpc_client()`` internally, and ``expose_bdev`` sits a further level down. A
2026-09-01 audit of the window found 14 bounded calls against 45 unbounded ones
reached that way -- and two earlier passes over the same window each missed
sites.

So the budget is ambient instead: set while the fence is held, consulted by
``StorageNode.rpc_client()``. Anything created beneath the fence is bounded,
including code added later, and callers that pass an explicit timeout keep it
(``bdev_wait_for_examine`` legitimately needs longer than the default).

Thread-local rather than a contextvar: a restart runs on one task-runner
thread, and peers are fenced and released on that same thread. It must be
cleared when the last port is released -- an unbounded window is bad, but a
budget leaking onto normal work afterwards would be worse, so every exit path
(unblock, abort, outer finally) clears it.
"""
import threading

_state = threading.local()


def set_budget(timeout, retry):
    """Apply an ambient budget to rpc_client() calls on this thread."""
    _state.budget = (timeout, retry)


def clear_budget():
    """Drop the ambient budget. Safe to call when none is set."""
    _state.budget = None


def current_budget():
    """(timeout, retry) if a fence budget is active on this thread, else None."""
    return getattr(_state, "budget", None)


class fence_budget:
    """Context manager form, for regions with a clean lexical scope."""

    def __init__(self, timeout, retry):
        self._timeout = timeout
        self._retry = retry
        self._previous = None

    def __enter__(self):
        self._previous = current_budget()
        set_budget(self._timeout, self._retry)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._previous is None:
            clear_budget()
        else:
            set_budget(*self._previous)
        return False
