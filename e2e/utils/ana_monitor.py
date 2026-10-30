"""Watch a migrating subsystem's ANA state from the host that consumes it.

WHY THIS EXISTS, because it is not obvious and it is the only check of its
kind in the suite.

A migration's cutover deliberately parks every path of the subsystem for a
short window while the volume moves, then flips ANA so the target's paths
serve. The window is by design. What is NOT by design is a window that runs
long, or a *second* window -- which means the cutover was attempted again.

That second window is the thing worth catching. From the feature team's own
migration script, which is where this check comes from
(``operator/test/fio_migration_test.py``):

    "on run fiomig-1787171993 it was exact: the four migrations that froze
    more than once are precisely the four that silently lost writes (mig-20
    twice, mig-29 four times, mig-38 and mig-42 five times each), and none
    of the other 42 lost anything. [...] Loss is roughly one block per
    affected member per event and reads succeed, so nothing else in the
    test notices; the freeze count is the only pre-corruption signal seen
    so far."

So: an fio verify catches the loss only once the read pattern comes back
around to that block, and attributes it to nothing. This catches it as it
happens, and names the migration.

WHAT IT READS. The ANA state lives per PATH (``nvmeXcYnZ``), below the
controller -- not on the multipath head device, which has no ``ana_state``
file at all. Hence walking ``/sys/class/nvme-subsystem/nvme-subsys*``,
matching ``subsysnqn``, then every controller under it that has a ``state``
file, then every path under that.

WHERE IT RUNS. On the host consuming the volume, which differs by backend:
over ssh on the client machine (docker), and inside the CSI node-plugin pod
on the consuming worker (k8s) -- that DaemonSet already mounts the host's
/sys, so no privileged pod of our own is needed. Both are passed in as a
callable, so this module knows about neither.
"""
import os
import re
import threading
import time

#: ANA states the kernel will route I/O to. Everything else -- inaccessible,
#: persistent-loss, change -- parks the path.
ANA_ACCESSIBLE = ("optimized", "non-optimized", "nonoptimized")

#: One line per controller: "<traddr>:<trsvcid>|<state>|<nsid>:<ana>,...".
#: Taken from the feature team's script so the two measure the same thing;
#: $NQN is supplied by the caller's environment.
ANA_PROBE_SCRIPT = r'''
for s in /sys/class/nvme-subsystem/nvme-subsys*; do
  [ -r "$s/subsysnqn" ] || continue
  [ "$(cat "$s/subsysnqn")" = "$NQN" ] || continue
  for c in "$s"/nvme*; do
    [ -r "$c/state" ] || continue
    ad=$(cat "$c/address" 2>/dev/null)
    tr=$(echo "$ad" | sed -n 's/.*traddr=\([^,]*\).*/\1/p')
    sv=$(echo "$ad" | sed -n 's/.*trsvcid=\([^,]*\).*/\1/p')
    ana=""
    for p in "$c"/nvme*c*n*; do
      [ -r "$p/ana_state" ] || continue
      ana="$ana,$(cat "$p/nsid" 2>/dev/null):$(cat "$p/ana_state" 2>/dev/null)"
    done
    echo "$tr:$sv|$(cat "$c/state")|${ana#,}"
  done
done
'''

#: How long the control plane deliberately leaves every path inaccessible
#: while it moves the volume. By design, so it is bounded rather than
#: forbidden -- but it is also the window the application has to survive.
#:
#: 2.0 because that is where the operator's own detector got it
#: (test/framework/sbtest/detectors/ana.py): "Measured, not guessed: it is
#: a time.sleep(2) in the batch migration barrier."
#:
#: TWO CAVEATS, both of which say this number does not govern most of our
#: lane. It is the BATCH path's window --
#: tasks_runner_batch_migration.py:821 takes every SRC and TGT path
#: inaccessible and then sleeps 2s before batch_final_step. The
#: SINGLE-volume path has no such sleep: it takes only the source's
#: secondary/tertiary out of the read path and then blocks in
#: bdev_lvol_transfer_final_step, so its window is however long SPDK's
#: drain and delta copy take, which nobody has measured. And that sleep is
#: labelled "(diagnostic)" twice, next to "Re-enable the narrower
#: pre-freeze once this test is done" -- so it is a deliberate temporary
#: widening, not a designed budget.
#:
#: Hence DURATION_FATAL below.
CUTOVER_PAUSE_DESIGN_S = float(
    os.environ.get("MIG_CUTOVER_PAUSE_DESIGN_S", 2.0))

#: Extra allowance inside the freeze, beyond the design window.
#:
#: ZERO, after dev settled where the namespace await actually sits.
#:
#: Asked whether 2.0s was still right, dev first said "I think this needs
#: to be increased. The namespace await is new to mitigate some other
#: issue with slow namespace bringup in spdk" -- so this carried 15s for a
#: while, the figure the operator gives that wait. Dev then clarified:
#: "the namespace await happens during connection preparation
#: (validation)".
#:
#: Which puts it OUTSIDE what this module measures. The freeze is the
#: cutover window: the span where every path of the subsystem is
#: ANA-inaccessible and no client can reach the volume. The namespace
#: await runs earlier, in Validating, while the target's paths are being
#: connected and verified -- the source is still serving throughout it,
#: so it costs a migration time but costs the application nothing. Adding
#: it here made the budget 20s and would have hidden a real four-fold
#: regression in the freeze.
#:
#: Left as a knob rather than deleted, because if a wait ever does move
#: into the freeze this is where it belongs.
NAMESPACE_AWAIT_S = float(os.environ.get("MIG_FREEZE_EXTRA_S", 0.0))

#: Whether exceeding the duration budget FAILS a case or only warns.
#:
#: Warns, by default, because of the caveats above: a bound derived from
#: the batch path's temporary diagnostic sleep is not a bound we can
#: defend on a single-volume migration, and failing runs against a number
#: we cannot justify is worse than reporting it.
#:
#: The FREEZE COUNT is fatal regardless, and it is the better check
#: anyway: it needs no threshold at all. A migration moves the subsystem
#: once, so it takes the pause once -- "more than one" is a defect under
#: either path's timing, and on the operator's own evidence the
#: migrations that froze more than once were exactly the ones that
#: silently lost writes.
DURATION_FATAL = False

#: Default sampling interval. The bound below is design + interval + slack,
#: because that is the widest a well-behaved pause can be *measured* as: a
#: pause starting just after one sample and ending just before the sample
#: after next is observed as design + interval.
DEFAULT_INTERVAL_S = 2.0

#: How many times one migration may freeze the volume. Once -- a migration
#: moves the subsystem once, so it takes the pause once, and a second
#: window means a step was retried. This is the sharper of the two checks.
FREEZES_ALLOWED = 1


def pause_budget(interval=DEFAULT_INTERVAL_S):
    """The longest a well-behaved cutover pause may be measured as.

    design window + the namespace await + one sampling interval + 1s.
    The interval is in there because a pause starting just after one
    sample and ending just before the sample after next is OBSERVED as
    design + interval, so without it a correctly-sized pause measured
    coarsely would read as a violation.
    """
    return (CUTOVER_PAUSE_DESIGN_S + NAMESPACE_AWAIT_S + interval + 1.0)


class AnaSample:
    """One reading of a subsystem's paths on one host."""

    __slots__ = ("addresses", "at", "by_ns", "paths", "phase", "raw",
                 "serving")

    def __init__(self, at, by_ns, paths, raw, serving=None, addresses=None,
                 phase=""):
        self.at = at
        #: {nsid: True when some path for it was accessible}
        self.by_ns = by_ns
        #: total number of (controller, namespace) paths seen
        self.paths = paths
        self.raw = raw
        #: {nsid: {traddr, ...}} -- which ADDRESSES were serving it. Kept
        #: because "is anything serving" cannot see the opposite failure
        #: from a freeze: the source and the target serving the same
        #: namespace at the same instant, which is two writers.
        self.serving = serving or {}
        #: every traddr seen, serving or not
        self.addresses = addresses or set()
        #: The migration step this sample was taken during, when the
        #: caller knew it. Borrowed from the operator's own sampler, whose
        #: reasoning is exactly right: "this is what turns a wall-clock
        #: ANA transition into a readable one: 'all paths went
        #: inaccessible during Cutover' is a finding, 'all paths went
        #: inaccessible at 09:31:02' is a timestamp."
        #:
        #: It matters most for split-brain. Both ends serving during the
        #: cutover is the dangerous case; the same reading during
        #: Validating, while the operator is still connecting target
        #: paths and the source legitimately still serves, may be
        #: ordinary. Without the phase the two are one finding.
        self.phase = phase or ""

    @property
    def readable(self):
        return bool(self.by_ns)


def parse_probe(out):
    """-> ({nsid: accessible}, path_count, {nsid: {addr}}, {addr}).

    Tolerant of partial output: a probe that caught sysfs mid-rescan
    returns fewer lines rather than wrong ones.
    """
    by_ns, paths, serving, addresses = {}, 0, {}, set()
    for line in (out or "").splitlines():
        line = line.strip()
        if not line or "|" not in line:
            continue
        parts = line.split("|")
        if len(parts) < 3:
            continue
        # "<traddr>:<trsvcid>" -- the address identifies the NODE serving
        # this path, which is what makes source-vs-target answerable.
        addr = parts[0].strip().rsplit(":", 1)[0].strip()
        if addr:
            addresses.add(addr)
        for item in parts[2].split(","):
            item = item.strip()
            if not item or ":" not in item:
                continue
            nsid, ana = item.rsplit(":", 1)
            nsid, ana = nsid.strip(), ana.strip().lower()
            if not nsid:
                continue
            paths += 1
            ok = ana in ANA_ACCESSIBLE
            # A namespace counts as served when ANY of its paths is
            # accessible. Multipath is the whole point: one inaccessible
            # path beside an optimized one is normal, not a freeze.
            by_ns[nsid] = by_ns.get(nsid, False) or ok
            if ok and addr:
                serving.setdefault(nsid, set()).add(addr)
    return by_ns, paths, serving, addresses


class AnaReport:
    """What a watch saw. ``ok`` is None when nothing could be sampled."""

    def __init__(self, samples, interval, freezes_allowed=FREEZES_ALLOWED,
                 duration_fatal=DURATION_FATAL, source_addrs=(),
                 target_addrs=()):
        self.samples = samples
        self.interval = interval
        self.freezes_allowed = freezes_allowed
        self.duration_fatal = duration_fatal
        self.source_addrs = set(source_addrs or ())
        self.target_addrs = set(target_addrs or ())
        self.budget = pause_budget(interval)
        self.readable = [s for s in samples if s.readable]
        self.freezes_by_ns = {}
        self.stall_by_ns = {}
        #: [(t, nsid, {src addrs}, {tgt addrs})] -- instants where the
        #: source and the target were BOTH serving the same namespace.
        self.split_brain = []
        self._measure()
        self._measure_split_brain()

    def _measure(self):
        """Per namespace, find the maximal runs with no accessible path.

        Per namespace rather than "was anything accessible", because a
        subsystem whose namespaces flip at slightly different moments still
        leaves one volume unserved, and that volume's writes are the ones
        at risk. A count can look healthy while one member froze twice.
        """
        seen_ns = set()
        for s in self.readable:
            seen_ns.update(s.by_ns)
        for nsid in sorted(seen_ns):
            runs, start = [], None
            for s in self.readable:
                # A namespace missing from a sample is not a freeze: it can
                # mean the path has gone away entirely (a disconnect, which
                # other assertions cover) or that sysfs was mid-rescan.
                # Treating absence as inaccessible would make every
                # reconnect look like a cutover freeze.
                if nsid not in s.by_ns:
                    if start is not None:
                        runs.append((start, s.at))
                        start = None
                    continue
                if s.by_ns[nsid]:
                    if start is not None:
                        runs.append((start, s.at))
                        start = None
                elif start is None:
                    start = s.at
            if start is not None and self.readable:
                runs.append((start, self.readable[-1].at))
            self.freezes_by_ns[nsid] = len(runs)
            self.stall_by_ns[nsid] = max(
                (b - a for a, b in runs), default=0.0)

    def _measure_split_brain(self):
        """Instants where source and target both served one namespace.

        The opposite failure from a freeze, and the more dangerous one. A
        cutover is meant to be a handover: the source stops serving at the
        moment the target starts. If both answer at once, two nodes are
        writable for the same namespace and a client's multipath layer is
        free to send writes to either -- so writes land on one side and
        reads come back from the other, and the loser's writes are simply
        gone. Nothing in a migration's own status can see this; it is only
        visible from the host's fabric.

        Needs to know which addresses belong to which node, so it is
        skipped rather than guessed when the caller did not say. Multiple
        accessible paths to ONE node are normal (one per data NIC) and are
        not this.
        """
        if not (self.source_addrs and self.target_addrs):
            return
        for smp in self.readable:
            for nsid, addrs in (smp.serving or {}).items():
                src = addrs & self.source_addrs
                tgt = addrs & self.target_addrs
                if src and tgt:
                    self.split_brain.append(
                        (smp.at, nsid, src, tgt, smp.phase))

    @property
    def unserved_by_target(self):
        """Namespaces the TARGET never served, after a completed move.

        A migration that reports Succeeded while the host cannot reach a
        namespace through the target is a volume nobody can use, and the
        control plane's own placement record says it is fine. Reported
        from the last readable sample, so it is only meaningful once the
        cutover has happened.
        """
        if not (self.target_addrs and self.readable):
            return set()
        last = self.readable[-1]
        out = set()
        for nsid in last.by_ns:
            if not ((last.serving or {}).get(nsid, set()) & self.target_addrs):
                out.add(nsid)
        return out

    @property
    def freezes(self):
        return max(self.freezes_by_ns.values(), default=0)

    @property
    def stall_s(self):
        return max(self.stall_by_ns.values(), default=0.0)

    @property
    def ok(self):
        """False only on a FATAL violation. See DURATION_FATAL."""
        if not self.readable:
            return None
        if self.freezes > self.freezes_allowed:
            return False
        if self.split_brain:
            return False
        return not (self.duration_fatal and self.stall_s > self.budget)

    @property
    def over_budget(self):
        """True when the pause ran long, fatal or not."""
        return bool(self.readable) and self.stall_s > self.budget

    def violations(self):
        """Human-readable reasons this report is a failure."""
        out = []
        if not self.readable:
            return out
        for at, nsid, src, tgt, phase in self.split_brain[:3]:
            out.append(
                f"namespace {nsid} was served by the SOURCE "
                f"({', '.join(sorted(src))}) and the TARGET "
                f"({', '.join(sorted(tgt))}) at the same instant "
                f"({f'during {phase}, ' if phase else ''}"
                f"t+{at - self.readable[0].at:.1f}s"
                f"{f', {len(self.split_brain)} such samples' if len(self.split_brain) > 1 else ''}). "
                f"A cutover is a handover: both ends answering at once "
                f"means two nodes are writable for one namespace, and a "
                f"client's multipath layer may send a write to either -- "
                f"so writes land on one side while reads come from the "
                f"other and the loser's writes are gone. No migration "
                f"status field can show this")
        for nsid, n in sorted(self.freezes_by_ns.items()):
            if n > self.freezes_allowed:
                out.append(
                    f"namespace {nsid} lost every accessible path {n} times "
                    f"during one migration (at most {self.freezes_allowed} "
                    f"is expected: a migration moves the subsystem once, so "
                    f"it takes the pause once -- more than one window means "
                    f"the cutover was retried, and that is the signal that "
                    f"has accompanied silent write loss)")
        for nsid, d in sorted(self.stall_by_ns.items()):
            if d > self.budget and self.duration_fatal:
                out.append(
                    f"namespace {nsid} had no accessible path for {d:.1f}s, "
                    f"past the {self.budget:.1f}s budget "
                    f"({CUTOVER_PAUSE_DESIGN_S:.1f}s by design + "
                    f"{self.interval:.1f}s sampling + 1s slack). An "
                    f"application sees this as stalled I/O")
        return out

    def summary(self):
        if not self.readable:
            return ("no ANA sample was readable, so the cutover window was "
                    "not measured")
        sb = (f", SPLIT-BRAIN x{len(self.split_brain)}"
              if self.split_brain else
              ("" if (self.source_addrs and self.target_addrs)
               else ", split-brain not checked (no src/tgt addresses)"))
        return (f"{len(self.readable)}/{len(self.samples)} samples readable, "
                f"{len(self.freezes_by_ns)} namespace(s), worst freeze count "
                f"{self.freezes}, longest stall {self.stall_s:.1f}s "
                f"(budget {self.budget:.1f}s){sb}")


class AnaWatcher:
    """Sample a subsystem's ANA state in the background.

    ``probe`` is a callable taking the probe shell and returning its
    stdout. It is the only backend-specific part, so this class works
    unchanged over ssh and over ``kubectl exec``.
    """

    def __init__(self, nqn, probe, interval=DEFAULT_INTERVAL_S, logger=None,
                 freezes_allowed=FREEZES_ALLOWED, source_addrs=(),
                 target_addrs=()):
        self.nqn = nqn
        self.probe = probe
        self.interval = max(0.2, float(interval))
        self.logger = logger
        self.freezes_allowed = freezes_allowed
        #: Data addresses of the source and target nodes. Optional: with
        #: them the report can answer split-brain and
        #: unserved-after-cutover, and without them it says those were
        #: not checked rather than guessing.
        self.source_addrs = set(source_addrs or ())
        self.target_addrs = set(target_addrs or ())
        self.samples = []
        self._phase = ""
        self._stop = threading.Event()
        self._thread = None
        self._errors = 0

    def _command(self):
        # Single-quoted NQN, with any embedded quote escaped the POSIX way.
        # An NQN has no quotes in practice; building it correctly anyway
        # costs nothing and a malformed command here would silently sample
        # nothing, which reads as "the cutover was clean".
        safe = str(self.nqn).replace("'", "'\\''")
        return f"NQN='{safe}' sh -c {_shq(ANA_PROBE_SCRIPT)}"

    def _loop(self):
        while not self._stop.is_set():
            t = time.time()
            try:
                out = self.probe(self._command())
            except Exception as exc:                  # noqa: BLE001
                self._errors += 1
                if self._errors <= 3 and self.logger:
                    self.logger.warning("[ANA] probe failed: %s",
                                        str(exc)[:160])
                out = ""
            by_ns, paths, serving, addrs = parse_probe(out)
            self.samples.append(
                AnaSample(t, by_ns, paths, out, serving, addrs,
                          phase=self._phase))
            # Wait the remainder of the interval, so a slow probe does not
            # stretch the sampling period and widen the measured window.
            self._stop.wait(max(0.0, self.interval - (time.time() - t)))

    def set_phase(self, phase):
        """Stamp subsequent samples with the migration's current step."""
        self._phase = str(phase or "")
        return self

    def start(self):
        if self._thread:
            return self
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name="ana-watch")
        self._thread.start()
        if self.logger:
            self.logger.info("[ANA] watching %s every %.1fs", self.nqn,
                             self.interval)
        return self

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.interval * 3 + 5)
            self._thread = None
        report = AnaReport(self.samples, self.interval, self.freezes_allowed,
                           source_addrs=self.source_addrs,
                           target_addrs=self.target_addrs)
        if self.logger:
            self.logger.info("[ANA] %s", report.summary())
        return report

    def __enter__(self):
        return self.start()

    def __exit__(self, *_exc):
        self.report = self.stop()
        return False


def _shq(s):
    """Quote *s* for POSIX sh. The probe is multi-line with $ and quotes."""
    if not s:
        return "''"
    if re.fullmatch(r"[A-Za-z0-9_@%+=:,./-]+", s):
        return s
    return "'" + s.replace("'", "'\\''") + "'"
