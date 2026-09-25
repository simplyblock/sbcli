"""Keep FIO alive across a rapid outage loop, and notice the moment it fails.

The rapid tests used to run FIO as a synchronised WAVE: launch every job, do
five outages, then block until the slowest job finished, validate, relaunch.
Two things were wrong with that.

*Detection was late.* Nothing looked at a FIO log until the wave ended, so a
failure could sit unreported for the whole runtime -- up to 2h13m at
runtime=8000 -- and could not be attributed to the outage that caused it. On
lblk_rapid_outage_docker-20260924-171741 the five outages took fifteen minutes
and the run then spent eighty-eight more waiting.

*And the barrier could fail on its own.* That same run died with "FIO process
list not empty" twenty-eight seconds before the job it was waiting for would
have exited cleanly.

So FIO becomes a service rather than a wave. After every outage:

  * scan the logs for errors -- measured at 16ms for 42 logs, because they are
    a few KB each and sit on the runner's own NFS mount;
  * ask which jobs are still running -- sub-second for both clients on the
    warm connections the suite already holds;
  * relaunch any job that ENDED CLEANLY, one at a time, with no barrier.

The distinction in that last step is the whole design. A job stops for two
reasons and they must not be confused: it hit an error (stop the run) or it
reached the end of its runtime (relaunch it). Treating a finish as a failure
fails green runs; treating a failure as a finish relaunches over a real defect
and loses it.

**Object churn is deliberately not here.** Both rapid lineages already delete
and create lvols, snapshots and clones at their checkpoints -- _run_checkpoint_churn
on the docker side and on the k8s side -- and both already hold a permanent set
back from it so some volumes age across the whole run. A second churn mechanism
on an outage counter would delete against those same caps and permanent sets
from outside their bookkeeping, so churn stays where it is; this module only
keeps IO running and reads what the logs say.

Platform differences live behind four hooks, listed in _RapidFioHooks. Both
the docker rapid lineage and the k8s one implement them; nothing else in the
mixin knows which platform it is on.
"""

import random
import re
import time

from utils.common_utils import sleep_n_sec


class RapidFioFailure(RuntimeError):
    """A FIO job reported a real IO or verification error."""


class _RapidFioHooks:
    """What a platform must provide. Documented here so a new one is a list.

    Every method raises rather than returning a harmless default: a lifecycle
    that silently does nothing is worse than one that refuses to start, since
    the run would look like it had IO coverage it never had.
    """

    def fio_jobs(self):
        """[(name, record)] for every FIO job that should be running."""
        raise NotImplementedError

    def fio_log_path(self, name, record):
        """Absolute path of this job's FIO output log, readable locally.

        Return None where the log is not a file the runner can open -- a k8s
        Job writes to a pod, not the NFS mount -- and override fio_log_text
        instead.
        """
        raise NotImplementedError

    def fio_log_text(self, name, record):
        """This job's FIO output as text. Defaults to reading fio_log_path.

        The one seam that lets a k8s Job and a client-side tmux session share
        every line of the lifecycle: one returns a file, the other a pod log,
        and nothing above here cares which.
        """
        path = self.fio_log_path(name, record)
        if not path:
            return ""
        try:
            with open(path, "r", errors="replace") as fh:
                return fh.read()
        except FileNotFoundError:
            return ""

    def fio_job_alive(self, name, record):
        """True while this job's FIO is still executing."""
        raise NotImplementedError

    def fio_relaunch(self, name, record, runtime):
        """Start this job's FIO again for *runtime* seconds."""
        raise NotImplementedError


class RapidFioLifecycle(_RapidFioHooks):
    """Scan, classify and revive FIO around a rapid outage loop."""

    #: Lines that mean the workload actually failed. `err= 0` and progress
    #: chatter are not errors, and `err=110` is a latency ceiling, judged
    #: elsewhere -- lumping it in here is what once reported "47 IO errors"
    #: on a run that had none.
    FIO_ERROR_MARKERS = (
        "io_u error",
        "verify failed",
        "bad magic header",
        "hdr_fail",
        "data mismatch",
        "checksum error",
    )

    #: A non-zero err= that is not the latency ceiling.
    _ERR_RE = re.compile(r"\berr=\s*([1-9]\d*)")
    _LAT_RE = re.compile(r"\berr=\s*110\b")

    #: A job has finished cleanly when fio has written its run summary.
    _DONE_RE = re.compile(r"\brun=\d+")

    #: Per-job runtime jitter at relaunch. Without it every job revived in
    #: the same check ends at the same moment too -- the barrier would be gone
    #: but the bunching would remain, leaving a window with almost no IO in
    #: flight, which is the state an outage must never land in.
    RUNTIME_JITTER = (0.7, 1.3)

    # ── state ────────────────────────────────────────────────────────────
    _rapid_base_runtime = None
    _rapid_lifecycle_ready = False

    #: name -> (launched_at, runtime). Written by rapid_runtime_for.
    _rapid_launched = None

    def rapid_fio_init(self, runtime):
        """Record the base runtime and arm the lifecycle. Idempotent."""
        if self._rapid_lifecycle_ready:
            return
        self._rapid_base_runtime = runtime
        self._rapid_lifecycle_ready = True
        self.logger.info(
            "[rapid-fio] armed over %d job(s); base runtime %ss, jittered %s "
            "per relaunch so revived jobs do not all end together. Object "
            "churn stays on the checkpoint clock where it already lives.",
            len(self.fio_jobs()), runtime, str(self.RUNTIME_JITTER))

    def rapid_runtime_for(self, name):
        """This job's runtime, jittered so endings spread out.

        Called once per launch, which is also what makes it the place to
        record when the job started and how long it should take -- see
        rapid_overdue.
        """
        base = getattr(self, "_rapid_base_runtime", None) or 1800
        lo, hi = self.RUNTIME_JITTER
        runtime = int(base * random.uniform(lo, hi))
        if self._rapid_launched is None:
            self._rapid_launched = {}
        self._rapid_launched[name] = (time.time(), runtime)
        return runtime

    def rapid_grace_seconds(self):
        """How long past its runtime a job may still be running.

        fio's --time_based --runtime clock starts only once file layout is
        done, so a job legitimately outlives its runtime by however long it
        spent laying out. The suite already budgets that as
        FIO_LAYOUT_ALLOWANCE_SEC, so use it where it exists rather than
        inventing a second number that can drift from it.
        """
        return int(getattr(self, "FIO_LAYOUT_ALLOWANCE_SEC", 3600)) + 600

    def rapid_overdue(self):
        """Jobs still running well past when they should have finished.

        A job that never ends is never relaunched, so its volume quietly stops
        getting fresh IO for the rest of the run and the outages that follow
        are landing on an idle volume. Left to the logs that reads as a healthy
        run with fewer jobs; it is a stuck workload and the run should say so.
        """
        launched = self._rapid_launched or {}
        grace = self.rapid_grace_seconds()
        now = time.time()
        late = []
        for name, rec in self.fio_jobs():
            when = launched.get(name)
            if not when:
                continue            # never launched through the lifecycle
            started, runtime = when
            overdue_by = now - started - runtime - grace
            if overdue_by > 0 and self.fio_job_alive(name, rec):
                late.append((name, int(overdue_by), runtime))
        return late

    # ── A: read what the logs already say ────────────────────────────────
    def rapid_scan_fio_logs(self):
        """Every error line the FIO logs hold right now.

        fio writes `io_u error` and friends the moment they happen, not only
        in its end-of-run summary, so this sees a failure without waiting for
        the job to finish. That is why no --status-interval is needed: adding
        one would repeat each marker in every periodic block and inflate the
        counts.
        """
        hits = []
        for name, rec in self.fio_jobs():
            try:
                for line in (self.fio_log_text(name, rec) or "").splitlines():
                    low = line.strip().lower()
                    if not low:
                        continue
                    if self._LAT_RE.search(low):
                        continue              # latency ceiling, not an IO error
                    if (any(m in low for m in self.FIO_ERROR_MARKERS)
                            or self._ERR_RE.search(low)):
                        hits.append(f"{name}: {line.strip()[:160]}")
            except Exception as exc:           # noqa: BLE001
                self.logger.warning(
                    "[rapid-fio] could not read %s's log: %s",
                    name, str(exc)[:120])
        return hits

    # ── B: finished, or failed? ──────────────────────────────────────────
    def rapid_job_finished_cleanly(self, name, rec):
        """True if this job's log carries a completed run summary."""
        try:
            return bool(self._DONE_RE.search(
                self.fio_log_text(name, rec) or ""))
        except Exception:                      # noqa: BLE001
            return False

    def rapid_allow_relaunch(self):
        """May a finished job be relaunched right now? Default yes.

        A platform whose loop still ends in a barrier that waits for every job
        to finish overrides this to say no just before that barrier. Otherwise
        a job relaunched one outage earlier holds the barrier for a whole
        fresh runtime, and the checkpoint -- which is where validation and
        churn live -- inherits a stall the lifecycle was meant to remove.

        Error scanning is never suppressed by this; only relaunching is.
        """
        return True

    def rapid_check_and_revive(self, context):
        """The whole check, after one outage. Raises on a real failure.

        Order matters: errors first. A job can both fail AND leave a summary,
        and relaunching it would erase the evidence and carry on as if the run
        were healthy.
        """
        if not self._rapid_lifecycle_ready:
            return 0

        errors = self.rapid_scan_fio_logs()
        if errors:
            # Take fio's own dumps of the bytes it actually read, before the
            # run ends and the clients are reused. They are the only artefact
            # that separates real corruption from a stale read, a second
            # writer, or misdirected IO -- and fio writes them to /root on the
            # client, where nothing else in the run preserves them.
            #
            # Wrapped because this must not replace a corruption report with a
            # collection error: the finding is what matters, the dumps only
            # explain it.
            if hasattr(self, "collect_fio_hdr_dumps"):
                try:
                    self.collect_fio_hdr_dumps("rapid_fio_failure")
                except Exception as exc:          # noqa: BLE001
                    self.logger.warning(
                        "[rapid-fio] could not collect hdr_fail dumps: %s",
                        str(exc)[:160])
            else:
                self.logger.warning(
                    "[rapid-fio] this platform has no collect_fio_hdr_dumps; "
                    "the bytes fio actually read are NOT being preserved, and "
                    "without them a corruption report cannot be told apart "
                    "from a stale read or a second writer.")
            raise RapidFioFailure(
                f"[rapid-fio] FIO reported {len(errors)} IO error(s) "
                f"{context}. Detected within one outage of happening, so this "
                f"outage is the one to look at:\n    "
                + "\n    ".join(errors[:8]))

        late = self.rapid_overdue()
        if late:
            raise RapidFioFailure(
                f"[rapid-fio] {len(late)} job(s) are still running long after "
                f"their runtime ended {context}. fio's clock starts after file "
                f"layout, and {self.rapid_grace_seconds()}s of slack is already "
                f"allowed on top of that, so this is a workload that is not "
                f"progressing -- its volume has stopped taking fresh IO and "
                f"every outage since has landed on an idle one:\n    "
                + "\n    ".join(
                    f"{n}: {late_by}s past a {rt}s runtime"
                    for n, late_by, rt in late[:8]))

        if not self.rapid_allow_relaunch():
            self.logger.info(
                "[rapid-fio] %s: logs are clean; holding off relaunches "
                "because a barrier is next and a job started now would make "
                "it wait a full runtime.", context)
            return 0

        revived = 0
        for name, rec in self.fio_jobs():
            if self.fio_job_alive(name, rec):
                continue
            if not self.rapid_job_finished_cleanly(name, rec):
                # Gone, no summary, no error line. Say so rather than guess:
                # relaunching would paper over a job that died silently.
                self.logger.warning(
                    "[rapid-fio] %s is not running %s and wrote neither an "
                    "error nor a run summary. Not relaunching it -- that "
                    "would hide whatever stopped it.", name, context)
                continue
            rt = self.rapid_runtime_for(name)
            try:
                self.fio_relaunch(name, rec, rt)
                revived += 1
                self.logger.info(
                    "[rapid-fio] %s finished its run; relaunched for %ss "
                    "(no barrier -- the other jobs kept going)", name, rt)
            except Exception as exc:           # noqa: BLE001
                self.logger.warning("[rapid-fio] could not relaunch %s: %s",
                                    name, str(exc)[:160])
        return revived

    def rapid_after_outage(self, context):
        """Call once after each outage: read the logs, revive what finished."""
        started = time.time()
        revived = self.rapid_check_and_revive(context)
        self.logger.info(
            "[rapid-fio] post-outage check %s took %.1fs (%d relaunched)",
            context, time.time() - started, revived)


def wait_for_settle(seconds=2):
    """Small pause so a just-started job is visible to the next check."""
    sleep_n_sec(seconds)
