"""Durable per-run state, so a stress run can be resumed instead of restarted.

Why this exists
---------------
A stress run is 5 to 27 hours. When it fails at iteration 15 the cluster still
holds every object it built -- stress.py passes delete_lvols=False -- but the
next run's setup() deletes every lvol, snapshot, clone and pool before doing
anything else. So investigating a late failure costs a full re-run, every time.

What makes resume possible is not the iteration counter. It is the name
prefixes. lvol_base / clone_base / snap_base are randomised per process
(continuous_failover_ha_multi_client_quick_outage.py:85-87), so a resumed run
that does not know the previous process's prefixes cannot tell its own objects
from anything else on the cluster, and "adopt what is already there" degrades
into "delete it all and start over". Persisting them is the whole feature.

Where it lives
--------------
<NFS_LOG_BASE>/_resume/<test_name>__<cluster_id>.json

Stable and non-timestamped, because the next process has to find it without
knowing when the previous one started -- the run directory is timestamped, so
state cannot live there alone. It also must not live under e2e/logs/: the CI
wipes that between runs (stress-run-only.yml runs logs/cleanup.py), while NFS
survives. A copy is dropped into the run directory at each checkpoint for the
record, and that copy is never read back.

Deliberately small
------------------
Names only. Everything else -- sizes, UUIDs, mount points, device paths -- is
re-derived from the API at adoption time. Persisting a UUID that the cluster has
since recycled would be worse than persisting nothing, because it reads as
authoritative. This state is enough to *adopt*, not to *replay*: in-flight FIO
and exact device paths are intentionally not captured, and FIO is re-kicked
fresh on resume.
"""

import json
import os
import shutil
import tempfile
from datetime import datetime, timezone


class RunState:
    """Load/save/clear the resume checkpoint for one (test, cluster) pair."""

    #: Fields carried across a resume. Anything not listed is re-derived.
    FIELDS = (
        "iter", "iteration", "case_id", "cluster_id", "test_name", "updated_at",
        "lvol_base", "clone_base", "snap_base", "pool_name",
        "lvols", "clones", "snapshots", "last_outage",
    )

    def __init__(self, nfs_log_base, test_name, cluster_id, logger=None):
        self.nfs_log_base = nfs_log_base
        self.test_name = test_name
        self.cluster_id = cluster_id or ""
        self.logger = logger
        self.dir = os.path.join(nfs_log_base, "_resume")
        safe_cluster = (self.cluster_id or "nocluster").replace("/", "_")
        self.path = os.path.join(
            self.dir, "%s__%s.json" % (test_name, safe_cluster))

    # ── logging helpers ───────────────────────────────────────────────────
    def _log(self, msg):
        if self.logger:
            self.logger.info("[run-state] %s", msg)

    def _warn(self, msg):
        if self.logger:
            self.logger.warning("[run-state] %s", msg)

    # ── write ─────────────────────────────────────────────────────────────
    def save(self, run_dir=None, **fields):
        """Checkpoint the run. Never raises.

        A checkpoint that kills a 20-hour run because NFS hiccupped would cost
        far more than the resume it enables, so every failure here is a warning
        and the run continues.
        """
        doc = {k: fields.get(k) for k in self.FIELDS if k in fields}
        doc["test_name"] = self.test_name
        doc["cluster_id"] = self.cluster_id
        doc["updated_at"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        try:
            os.makedirs(self.dir, exist_ok=True)
            # Write-then-rename: a run killed mid-write must not leave a
            # truncated checkpoint that the next run then refuses to parse.
            fd, tmp = tempfile.mkstemp(dir=self.dir, suffix=".tmp")
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(doc, fh, indent=2, sort_keys=True)
            os.replace(tmp, self.path)
            self._log("saved iter=%s case=%s -> %s"
                      % (doc.get("iter"), doc.get("case_id"), self.path))
            if run_dir and os.path.isdir(run_dir):
                shutil.copy2(self.path,
                             os.path.join(run_dir, "resume_state.json"))
        except Exception as exc:                      # noqa: BLE001
            self._warn("could not save state: %s: %s"
                       % (type(exc).__name__, exc))
        return doc

    # ── read ──────────────────────────────────────────────────────────────
    def load(self, require_cluster_match=True):
        """Return the saved state, or None when there is nothing usable.

        Refuses state from a different cluster. Adopting objects by name across
        clusters would silently bind the run to whatever happened to share a
        prefix, which is exactly the class of confusion this file exists to
        avoid.
        """
        if not os.path.exists(self.path):
            self._log("no state at %s -- starting fresh" % self.path)
            return None
        try:
            with open(self.path, encoding="utf-8") as fh:
                doc = json.load(fh)
        except Exception as exc:                      # noqa: BLE001
            self._warn("state at %s is unreadable (%s) -- starting fresh"
                       % (self.path, exc))
            return None

        saved_cluster = doc.get("cluster_id") or ""
        if require_cluster_match and saved_cluster != self.cluster_id:
            self._warn(
                "state is for cluster %r but this run is on %r -- refusing to "
                "adopt. Clear it with RunState.clear() if that is intended."
                % (saved_cluster, self.cluster_id))
            return None

        if not doc.get("lvol_base"):
            self._warn("state has no lvol_base, so adopted objects could not "
                       "be identified -- starting fresh")
            return None

        self._log("loaded iter=%s case=%s prefixes=%s/%s/%s from %s"
                  % (doc.get("iter"), doc.get("case_id"), doc.get("lvol_base"),
                     doc.get("clone_base"), doc.get("snap_base"),
                     doc.get("updated_at")))
        return doc

    # ── delete ────────────────────────────────────────────────────────────
    def clear(self):
        """Drop the checkpoint. Called on a clean finish, so the next run on
        this cluster starts fresh rather than adopting a completed run."""
        try:
            if os.path.exists(self.path):
                os.remove(self.path)
                self._log("cleared %s" % self.path)
        except Exception as exc:                      # noqa: BLE001
            self._warn("could not clear state: %s" % exc)


def adopt_by_prefix(names, prefix):
    """The names in `names` that belong to a previous run of this test.

    Prefix matching is the only handle available: the API returns everything on
    the cluster, and a stress cluster routinely carries objects from other
    suites. Anything that does not match is left strictly alone.
    """
    if not prefix:
        return []
    return sorted(n for n in names if n and n.startswith(prefix))


def reconcile(expected, found, logger=None, kind="lvol"):
    """Compare the saved inventory against what the cluster still has.

    Missing objects are reported, not fatal. A node that died mid-delete can
    legitimately leave the cluster short, and refusing to resume would throw
    away the whole point; the run needs to know, and then continue.
    """
    exp, fnd = set(expected or ()), set(found or ())
    missing, extra = sorted(exp - fnd), sorted(fnd - exp)
    if logger:
        logger.info("[run-state] %s: %d expected, %d present, %d missing, "
                    "%d unexpected", kind, len(exp), len(fnd), len(missing),
                    len(extra))
        if missing:
            logger.warning("[run-state] %s missing since checkpoint: %s",
                           kind, ", ".join(missing[:20]))
        if extra:
            logger.info("[run-state] %s present but not in state (adopting "
                        "anyway, prefix matched): %s", kind,
                        ", ".join(extra[:20]))
    return missing, extra
