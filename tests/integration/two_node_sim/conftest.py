"""Fixtures for the two-node simulator (real FoundationDB, simulated nodes)."""

import logging

import pytest


@pytest.fixture
def make_world(monkeypatch):
    """Build a booted ``World``: seeded in FDB, the arbiter configured both
    nodes, leases running, the pair healthy. ``bugs`` re-introduces fixed
    behaviour for a regression proof (``scenarios.BUGS``)."""
    from simplyblock_core.db_controller import DBController

    from .scenarios import apply_bugs
    from .world import World

    logging.getLogger("two_node_sim").setLevel(logging.INFO)
    created = []

    def make(bugs=(), conditions=(), seed=1, caps=None, host_fencing=False):
        db = DBController()
        if db.kv_store is None:
            pytest.skip("FoundationDB is not available")
        world = World(db, seed=seed, bugs=set(bugs), caps=caps, host_fencing=host_fencing)
        world.conditions = set(conditions)
        world.install(monkeypatch)
        # The heal's stable window only delays the heal; no finding depends on
        # its length. 5 s instead of 30 s keeps every scenario under a minute.
        from simplyblock_core import constants
        monkeypatch.setattr(constants, "TWO_NODE_STABLE_FOR_S", 5)
        apply_bugs(world, monkeypatch, set(bugs))
        world.seed_db()
        world.start()
        ok = world.run_until(lambda: not world.unhealthy_reasons() and all(
            n.arbitration for n in world.nodes), 30.0)
        assert ok, "the simulated pair did not become healthy: %s" % world.unhealthy_reasons()
        world.run(2.0)
        world.client.reset_metrics()
        created.append(world)
        return world

    yield make

    for w in created:
        if w.violations or w.unknown_rpcs:
            logging.getLogger("two_node_sim").info("trace:\n%s", "\n".join(w.trace[-200:]))
