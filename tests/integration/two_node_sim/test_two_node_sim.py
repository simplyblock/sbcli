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
    sc, _bugs, _sig = REGRESSIONS[finding]
    world = make_world(conditions=sc.conditions)
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
    sc, bugs, signature = REGRESSIONS[finding]
    world = make_world(bugs=bugs, conditions=sc.conditions)
    # a buggy run only has to be caught: on main every scenario heals well
    # within 60 s after its observation window (measured)
    out = run(world, sc, stop_on_failure=True, heal_s=60.0)
    assert not out.passed, ("the %s scenario did not detect the bug %s:\n%s"
                            % (finding, sorted(bugs), out.report()))
    assert any(signature in f for f in out.failures), (
        "the %s scenario failed, but not for %r:\n%s" % (finding, signature, out.report()))


@pytest.mark.parametrize("finding", sorted(NOT_MODELLED))
def test_not_modelled(finding):
    pytest.skip("%s is not modelled: %s" % (finding, NOT_MODELLED[finding]))
