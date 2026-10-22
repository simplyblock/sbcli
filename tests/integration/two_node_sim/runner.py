"""Run one scenario and judge it against the campaign's criteria."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Outcome:
    name: str
    failures: list = field(default_factory=list)
    max_stall_s: float = 0.0
    max_gap_s: float = 0.0
    io_errors: int = 0
    grant_after_s: float | None = None
    healed_after_s: float | None = None
    aborts: list = field(default_factory=list)
    trace: list = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.failures

    def report(self) -> str:
        head = "%s: %s (stall %d s [gap %.2f s], IO errors %d, grant %s, healed %s)" % (
            self.name, "PASS" if self.passed else "FAIL", self.max_stall_s, self.max_gap_s,
            self.io_errors,
            "-" if self.grant_after_s is None else "%.2f s" % self.grant_after_s,
            "-" if self.healed_after_s is None else "%.0f s" % self.healed_after_s)
        return "\n".join([head] + ["  - " + f for f in self.failures] +
                         ["  trace:"] + ["    " + t for t in self.trace[-80:]])


def run(world, sc, stop_on_failure: bool = False, heal_s: float | None = None) -> Outcome:
    """Inject ``sc`` into a booted ``world``, observe, wait for the heal.

    ``stop_on_failure``: return at the first hard failure (regression proofs:
    the buggy variant only needs to be caught, not run to the end)."""
    out = Outcome(sc.name)
    vt = world.vt
    # Inject half a second into a second of fio's 1 s IOPS log: the bed's
    # stall metric counts whole empty seconds, so the alignment of a fault
    # changes the count by one; the bed's was random, ours is fixed.
    phase = (vt.time() - world.client.fio_start) % 1.0
    world.run((0.5 - phase) % 1.0)
    t0 = vt.time()
    world.log("=== scenario %s ===" % sc.name)
    sc.inject(world)

    def hard_failures():
        fails = list(world.violations)
        for n in world.nodes:
            for t, why in n.aborts:
                # allowed: the scenario's own kill, and a frozen node's
                # abort-on-wake; never e.g. an aborted restart (F1) or a JCERR (F17)
                expected = n.name in sc.aborts_ok and ("killed" in why or "stalled" in why)
                if not expected:
                    fails.append("unexpected abort of %s at %.2f s: %s" % (n.name, t - t0, why))
        errs = sum(world.client.io_errors.values())
        if errs and not sc.io_errors_ok:
            fails.append("%d client IO errors" % errs)
        return fails

    # fault phase
    end = vt.now + sc.observe_s
    while vt.now < end:
        world.run(0.5)
        if stop_on_failure and hard_failures():
            break

    # recovery without manual steps
    bound = sc.heal_s if heal_s is None else min(sc.heal_s, heal_s)
    if bound and not (stop_on_failure and hard_failures()):
        ok = world.run_until(lambda: not world.unhealthy_reasons(all_groups=sc.ana_all_groups),
                             bound, step=1.0)
        if ok:
            out.healed_after_s = vt.time() - t0
        else:
            out.failures.append("not healthy %.0f s after the fault without a manual step: %s"
                                % (vt.time() - t0, world.unhealthy_reasons(all_groups=sc.ana_all_groups)))

    out.failures[:0] = hard_failures()
    # The bed's metric: whole seconds of fio's 1 s IOPS log without a
    # completion, counted from the injection (c5 02b: a ~4 s gap = 3).
    out.max_stall_s = max(world.client.stall_s(world.fault_at or t0).values())
    out.max_gap_s = max(world.client.max_stall.values())
    out.io_errors = sum(world.client.io_errors.values())
    if sc.stall_limit_s is not None and out.max_stall_s > sc.stall_limit_s:
        out.failures.append("client stall %d s > %.0f s (gap %.2f s)"
                            % (out.max_stall_s, sc.stall_limit_s, out.max_gap_s))
    base = world.fault_at if world.fault_at is not None else t0
    grants = [t - base for t, _ in world.grants if t >= base]
    out.grant_after_s = grants[0] if grants else None
    if sc.grant_within_s is not None:
        if out.grant_after_s is None:
            out.failures.append("no grant")
        elif out.grant_after_s > sc.grant_within_s:
            out.failures.append("grant after %.2f s > %.1f s" % (out.grant_after_s, sc.grant_within_s))
    if sc.host_fenced is not None:
        seen = sorted({fenced for _, fenced, _w in world.host_fences_seen})
        if seen != sorted(sc.host_fenced):
            out.failures.append("host fenced %s, expected %s" % (seen, sorted(sc.host_fenced)))
    if world.unknown_rpcs:
        out.failures.append("RPCs the simulated node does not implement: %s" % sorted(world.unknown_rpcs))
    out.aborts = [(n.name, t - t0, why) for n in world.nodes for t, why in n.aborts]
    out.trace = list(world.trace)
    return out
