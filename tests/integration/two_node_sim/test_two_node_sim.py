"""Two-node HA simulation: the AWS bed's scenarios and one regression scenario
per campaign finding, against the real arbiter and control-plane code.

* ``test_bed_scenario``: the bed list (01a ... 10b) must pass on main.
* ``test_finding_is_fixed``: each finding's scenario passes on main ...
* ``test_finding_is_detected``: ... and fails with the finding's bug put back
  (``scenarios.BUGS``), which proves the scenario would have caught it.
* ``KNOWN_FAILING``: open findings; xfail until their fix lands.

See ``AGENTS.md`` in this directory for what is real and what is modelled.
"""

import pytest

from .runner import run
from .scenarios import (
    BED,
    EXERCISED,
    EXTRA,
    FAST,
    KNOWN_FAILING,
    NOT_MODELLED,
    PENDING_FIX,
    REGRESSIONS,
)

pytestmark = pytest.mark.two_node_sim

# Every scenario drives minutes of simulated time against the real FDB; the
# arbiter alone does ~13 FDB reads per 100 ms tick. Measured: 20-60 s wall.
SCENARIO_TIMEOUT_S = 240


def _fast(name):
    return [pytest.mark.two_node_sim_fast] if name in FAST else []


@pytest.mark.timeout(SCENARIO_TIMEOUT_S)
@pytest.mark.parametrize("sc", [pytest.param(s, id=s.name, marks=_fast(s.name)) for s in BED + EXTRA])
def test_bed_scenario(make_world, sc):
    world = make_world(conditions=sc.conditions)
    out = run(world, sc)
    known = KNOWN_FAILING.get(sc.name)
    if known is None:
        assert out.passed, out.report()
        return
    reason, signature = known
    assert not out.passed, "%s passes now: remove it from KNOWN_FAILING (%s)" % (sc.name, reason)
    assert any(signature in f for f in out.failures), (
        "%s fails, but not for its known reason (%s):\n%s" % (sc.name, reason, out.report()))
    pytest.xfail(reason)


@pytest.mark.timeout(SCENARIO_TIMEOUT_S)
@pytest.mark.parametrize("finding", sorted(REGRESSIONS, key=lambda f: int(f[1:])))
def test_finding_is_fixed(make_world, finding):
    from .simnode import DEPLOYED_CAPS
    sc, _bugs, _sig = REGRESSIONS[finding]
    world = make_world(conditions=sc.conditions, caps=DEPLOYED_CAPS | sc.caps)
    out = run(world, sc)
    assert out.passed, out.report()
    exercised = EXERCISED.get(finding)
    if exercised is not None:
        assert exercised(out), "%s passed without reaching its critical step:\n%s" % (
            finding, out.report())


@pytest.mark.timeout(SCENARIO_TIMEOUT_S)
@pytest.mark.parametrize("finding", [pytest.param(f, marks=_fast("detect:" + f))
                                     for f in sorted(REGRESSIONS, key=lambda f: int(f[1:]))])
def test_finding_is_detected(make_world, finding):
    from .simnode import DEPLOYED_CAPS
    sc, bugs, signature = REGRESSIONS[finding]
    world = make_world(bugs=bugs, conditions=sc.conditions, caps=DEPLOYED_CAPS | sc.caps)
    # a buggy run only has to be caught: on main every scenario heals well
    # within 60 s after its observation window (measured)
    out = run(world, sc, stop_on_failure=True, heal_s=60.0)
    assert not out.passed, ("the %s scenario did not detect the bug %s:\n%s"
                            % (finding, sorted(bugs), out.report()))
    signatures = signature if isinstance(signature, tuple) else (signature,)
    assert any(sig in f for f in out.failures for sig in signatures), (
        "the %s scenario failed, but not for %r:\n%s" % (finding, signature, out.report()))


@pytest.mark.timeout(SCENARIO_TIMEOUT_S)
@pytest.mark.parametrize("sc", [s for s in EXTRA if s.name in PENDING_FIX], ids=lambda s: s.name)
def test_pending_fix_passes(make_world, sc):
    """An open finding passes with the capabilities of its pending fix (the PR
    stack under test on the bed), while ``test_bed_scenario`` keeps it failing
    for its known reason on what main deploys."""
    from .simnode import DEPLOYED_CAPS
    caps = DEPLOYED_CAPS | PENDING_FIX[sc.name]
    world = make_world(conditions=sc.conditions, caps=caps)
    out = run(world, sc)
    assert out.passed, "%s with %s:\n%s" % (sc.name, sorted(caps), out.report())


@pytest.mark.timeout(SCENARIO_TIMEOUT_S)
def test_f21_needs_its_own_fix(make_world):
    """F21 is a separate defect: with only F19's capability (the redirect stops at
    the grant) the takeover reader still waits on the frozen peer's JM (c8 02c).
    F23's capability isolates it from the survivor's own writes."""
    from .simnode import (
        CAP_GRANT_STOPS_REDIRECT,
        CAP_STOPLIST_PEER_DEVICES,
        DEPLOYED_CAPS,
    )
    sc = next(s for s in EXTRA if s.name == "F21-takeover-reader-frozen-peer-jm")
    caps = DEPLOYED_CAPS | {CAP_GRANT_STOPS_REDIRECT, CAP_STOPLIST_PEER_DEVICES}
    world = make_world(conditions=sc.conditions, caps=caps)
    out = run(world, sc)
    assert not out.passed, out.report()
    assert any("F21" in f for f in out.failures), out.report()


@pytest.mark.timeout(SCENARIO_TIMEOUT_S)
def test_f23_needs_its_own_fix(make_world):
    """F23 is a separate defect: with the F19 and F21 capabilities (redirect stop,
    reader skips the dead peer JM) the survivor's writes still wait on a frozen
    secondary's data devices after the grant (bed c9/02a)."""
    from .simnode import (
        CAP_GRANT_STOPS_REDIRECT,
        CAP_READER_SKIPS_DEAD_PEER_JM,
        DEPLOYED_CAPS,
    )
    sc = next(s for s in EXTRA if s.name == "F23-frozen-secondary-data-io")
    caps = DEPLOYED_CAPS | {CAP_GRANT_STOPS_REDIRECT, CAP_READER_SKIPS_DEAD_PEER_JM}
    world = make_world(conditions=sc.conditions, caps=caps)
    out = run(world, sc)
    assert not out.passed, out.report()
    assert any("F23" in f for f in out.failures), out.report()


@pytest.mark.timeout(SCENARIO_TIMEOUT_S)
def test_f24_needs_its_own_fix(make_world):
    """F24 is a separate defect: with #219 as run in c11 (redirect stop, reader skips
    the dead peer JM, the peer's devices isolated by closing them) the follower's
    takeover read of a frozen primary's LVS fails on a closed device and the
    survivor aborts (bed c11/02c: 160 IO errors)."""
    from .simnode import (
        CAP_GRANT_STOPS_REDIRECT,
        CAP_READER_SKIPS_DEAD_PEER_JM,
        CAP_STOPLIST_PEER_DEVICES,
        DEPLOYED_CAPS,
    )
    sc = next(s for s in EXTRA if s.name == "F24-frozen-primary-takeover-read")
    caps = DEPLOYED_CAPS | {CAP_GRANT_STOPS_REDIRECT, CAP_READER_SKIPS_DEAD_PEER_JM, CAP_STOPLIST_PEER_DEVICES}
    world = make_world(conditions=sc.conditions, caps=caps)
    out = run(world, sc)
    assert not out.passed, out.report()
    assert any("F24" in f for f in out.failures), out.report()


@pytest.mark.timeout(SCENARIO_TIMEOUT_S)
def test_f25_needs_its_own_fix(make_world):
    """F25 is a separate defect: with #219 as run in c12 (F19, F21, F23 and F24
    fixed) the follower's takeover of a frozen primary's LVS no longer aborts, but
    its first write waits for the append lock on the frozen peer's JM until the
    peer aborts on wake (bed c12/02c: 6-8 s client stall, 0 IO errors)."""
    from .simnode import (
        CAP_GRANT_STOPS_REDIRECT,
        CAP_ISOLATE_MARKS_UNAVAILABLE,
        CAP_READER_SKIPS_DEAD_PEER_JM,
        CAP_STOPLIST_PEER_DEVICES,
        DEPLOYED_CAPS,
    )
    sc = next(s for s in EXTRA if s.name == "F25-frozen-primary-takeover-append-lock")
    caps = DEPLOYED_CAPS | {CAP_GRANT_STOPS_REDIRECT, CAP_READER_SKIPS_DEAD_PEER_JM,
                            CAP_STOPLIST_PEER_DEVICES, CAP_ISOLATE_MARKS_UNAVAILABLE}
    world = make_world(conditions=sc.conditions, caps=caps)
    out = run(world, sc)
    assert not out.passed, out.report()
    assert any("F25" in f for f in out.failures), out.report()
    assert not any("F24" in f for f in out.failures), out.report()


@pytest.mark.timeout(SCENARIO_TIMEOUT_S)
def test_f26_needs_its_own_fix(make_world):
    """F26 is a separate defect: with #219 as run in c12b (F19, F21, F23, F24 and
    F25 fixed) a history read that the survivor's reader has in flight on the
    frozen peer's JM still holds the takeover until the 8 s NVMe IO timeout or the
    peer's abort on wake (bed c12b/02c-r1: 7 s client stall, 0 IO errors)."""
    from .simnode import (
        CAP_APPENDER_LOCK_SKIPS_PEER_JM,
        CAP_GRANT_STOPS_REDIRECT,
        CAP_ISOLATE_MARKS_UNAVAILABLE,
        CAP_READER_SKIPS_DEAD_PEER_JM,
        CAP_STOPLIST_PEER_DEVICES,
        DEPLOYED_CAPS,
    )
    sc = next(s for s in EXTRA if s.name == "F26-takeover-read-inflight-on-peer-jm")
    caps = DEPLOYED_CAPS | {CAP_GRANT_STOPS_REDIRECT, CAP_READER_SKIPS_DEAD_PEER_JM,
                            CAP_STOPLIST_PEER_DEVICES, CAP_ISOLATE_MARKS_UNAVAILABLE,
                            CAP_APPENDER_LOCK_SKIPS_PEER_JM}
    world = make_world(conditions=sc.conditions, caps=caps)
    out = run(world, sc)
    assert not out.passed, out.report()
    assert any("F26" in f for f in out.failures), out.report()
    assert not any("F25" in f or "F24" in f or "F21" in f for f in out.failures), out.report()


@pytest.mark.timeout(SCENARIO_TIMEOUT_S)
def test_f20_client_kato4_detected(make_world):
    """F20: with F19's fix in place, the old client keep-alive (4 s) still
    stalls a frozen primary's volume past the limit; KATO 2 (main) does not
    (``test_pending_fix_passes[F20-frozen-leader-client-kato]``)."""
    from .simnode import DEPLOYED_CAPS
    sc = next(s for s in EXTRA if s.name == "F20-frozen-leader-client-kato")
    caps = DEPLOYED_CAPS | PENDING_FIX[sc.name]
    world = make_world(bugs={"client_kato4"}, conditions=sc.conditions, caps=caps)
    out = run(world, sc, stop_on_failure=True, heal_s=60.0)
    assert not out.passed, "KATO 4 was not detected:\n%s" % out.report()
    assert any("client stall" in f for f in out.failures), out.report()


@pytest.mark.timeout(SCENARIO_TIMEOUT_S)
@pytest.mark.parametrize("sc", BED, ids=lambda s: s.name)
def test_bed_scenario_with_pending_caps(make_world, sc):
    """The pending capabilities must not break any bed scenario."""
    from .simnode import ALL_CAPS
    world = make_world(conditions=sc.conditions, caps=ALL_CAPS)
    out = run(world, sc)
    assert out.passed, "%s with %s:\n%s" % (sc.name, sorted(ALL_CAPS), out.report())


@pytest.mark.parametrize("finding", sorted(NOT_MODELLED))
def test_not_modelled(finding):
    pytest.skip("%s is not modelled: %s" % (finding, NOT_MODELLED[finding]))
