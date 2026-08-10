# coding=utf-8
"""Host-lease primitives for DB-backed background tasks.

A *lease* is soft mutual exclusion between runner replicas on different hosts:
the task record carries an ``owner`` (hostname) and every write refreshes
``updated_at``. A second runner replica on a different host is locked out
until the lease goes stale (``ttl_sec``), which prevents two replicas from
both executing the same side-effecting task during a rolling deploy or a
transient dual-manager window. A runner on the *same* host always wins
immediately, so the common single-replica deployment is unaffected.

The task object is duck-typed; it must provide:
- ``status`` (``done_status`` means terminal — never claimable),
- ``owner`` (str, empty = unclaimed),
- ``updated_at`` (ISO-format str; the lease timestamp),
- ``uuid`` (for log messages).

The db object must provide ``atomic_update(obj, mutate_fn)`` with
compare-and-swap semantics: ``mutate_fn`` is applied to a fresh read of the
object and must be side-effect-free (it can replay on conflict); the call
returns the object, or ``None`` if it no longer exists.
"""

import contextlib
import datetime
import logging
import socket
import threading

DEFAULT_DONE_STATUS = 'done'


class TaskLease:
    """Claim/refresh/heartbeat helper bound to one db and one owner identity.

    Owner identity defaults to the hostname (not pid) so a runner that crashes
    and restarts on the same host re-claims its own in-flight tasks immediately.
    """

    def __init__(self, db, ttl_sec, heartbeat_sec, owner=None, done_status=DEFAULT_DONE_STATUS,
                 logger=None):
        self._db = db
        self.ttl_sec = ttl_sec
        self.heartbeat_sec = heartbeat_sec
        self.owner = owner or socket.gethostname()
        self.done_status = done_status
        self._logger = logger or logging.getLogger(__name__)

    def is_stale(self, task):
        """True if the task's lease (its last write) is older than the TTL, i.e.
        the owning runner host is presumed dead and another host may take over."""
        if not task.updated_at:
            return True
        try:
            last = datetime.datetime.fromisoformat(task.updated_at)
        except (ValueError, TypeError):
            return True
        if last.tzinfo is None:
            last = last.replace(tzinfo=datetime.timezone.utc)
        age = (datetime.datetime.now(datetime.timezone.utc) - last).total_seconds()
        return age > self.ttl_sec

    def claim(self, task, owner=None):
        """Atomically claim a task for this runner host before executing it.

        Returns True if this host now holds the lease and may run the task, or
        False if another still-alive host owns it (caller must skip it this
        cycle). Done tasks are never claimed.
        """
        owner = owner or self.owner
        decision = {"won": False}
        now = str(datetime.datetime.now(datetime.timezone.utc))

        def _mutate(t):
            if t.status == self.done_status:
                return False  # not claimable; decision stays False
            if t.owner and t.owner != owner and not self.is_stale(t):
                return False  # owned by another live host
            t.owner = owner
            t.updated_at = now  # refresh the lease (atomic_update bypasses write_to_db)
            decision["won"] = True
            return True

        if self._db.atomic_update(task, _mutate) is None:
            return False
        if decision["won"]:
            # atomic_update mutates a *fresh* read of the record, not the object
            # the caller holds. Mirror the committed lease fields onto the
            # caller's copy so a later full-object write (e.g. marking the task
            # RUNNING) doesn't clobber the owner back to its stale value.
            task.owner = owner
            task.updated_at = now
        return decision["won"]

    def refresh(self, task, owner=None):
        """Heartbeat: refresh this host's lease on a task it already owns, so a
        live owner is never preempted while blocking on long RPCs. Returns False
        (without touching the task) if the task is done or owned by another host —
        the caller lost the lease and should treat the takeover as authoritative."""
        owner = owner or self.owner
        now = str(datetime.datetime.now(datetime.timezone.utc))
        refreshed = {"ok": False}

        def _mutate(t):
            if t.status == self.done_status:
                return False
            if t.owner != owner:
                return False
            t.updated_at = now
            refreshed["ok"] = True
            return True

        if self._db.atomic_update(task, _mutate) is None:
            return False
        if refreshed["ok"]:
            task.updated_at = now  # keep the caller's copy in sync (see claim)
        return refreshed["ok"]

    @contextlib.contextmanager
    def heartbeat(self, task, owner=None):
        """Refresh this host's lease on ``task`` every ``heartbeat_sec`` for the
        duration of the with-block.

        Every runner that executes long-blocking work under a claimed lease MUST
        wrap that work in this: when ``ttl_sec`` is far shorter than the work
        (node add / restart / migration), a lease that is only refreshed on task
        writes goes stale mid-execution, and a second runner host (e.g. the new
        pod during a rolling update) would claim the task and double-drive it.

        The heartbeat stops on its own if the lease is lost to another host
        (refresh returns False) — the takeover is authoritative.
        """
        stop = threading.Event()

        def _beat():
            while not stop.wait(self.heartbeat_sec):
                try:
                    if not self.refresh(task, owner):
                        return
                except Exception as e:
                    self._logger.debug(f"Lease heartbeat failed for task {task.uuid}: {e}")

        thread = threading.Thread(target=_beat, daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop.set()
