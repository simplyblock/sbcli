"""The simulated pair boots healthy and stays so."""

import pytest

pytestmark = [pytest.mark.two_node_sim, pytest.mark.two_node_sim_fast]


@pytest.mark.timeout(120)  # simulated minutes run against real FDB; most time is FDB I/O
def test_pair_boots_and_stays_healthy(make_world):
    w = make_world()
    w.run(20.0)
    assert not w.unhealthy_reasons()
    assert not w.violations
    assert not w.unknown_rpcs
    assert all(n.aborts == [] for n in w.nodes)
    assert all(v == 0 for v in w.client.io_errors.values())
    assert max(w.client.max_stall.values()) < 0.5
