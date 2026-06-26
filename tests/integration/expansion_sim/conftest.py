# coding=utf-8
"""Pytest fixtures for the expansion simulator framework.

Connects to the integration tier's FoundationDB (provisioned in
``tests/integration/conftest.py``) and patches every known import site of
``simplyblock_core.rpc_client.RPCClient`` with the ``RpcRouter`` so all RPC
traffic is routed to the simulators.

Requires:
* the integration FDB cluster file at ``$FDB_CLUSTER_FILE`` (or
  ``/etc/foundationdb/fdb.cluster``)
* the ``foundationdb`` Python client + the matching libfdb shared library

Tests in this directory mutate FDB. The ``fdb_clean`` fixture wipes the
cluster's keyspace at the start of each test so test ordering doesn't
matter.
"""

import os
import pytest

import fdb

_FDB_CLUSTER_FILE = os.environ.get(
    "FDB_CLUSTER_FILE", "/etc/foundationdb/fdb.cluster")


@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch):
    """Neutralize real ``time.sleep`` for the duration of every sim test.

    The expansion path runs through ``storage_node_ops`` helpers
    (recreate_lvstore_on_sec, teardown_non_leader_lvstore, the JC-compression
    wait, etc.) that contain many multi-second-to-multi-minute sleeps and
    poll-until-deadline loops. Against the in-memory RPC simulator those waits
    serve no purpose but run in real wall-clock — they are the reason the
    end-to-end sim suite took hours. Patching ``time.sleep`` to a no-op on the
    modules the expansion drives turns the simulation into a logic check that
    completes in milliseconds. Real deployments keep the real sleeps.
    """
    import time as _time
    _real_sleep = _time.sleep

    def _no_sleep(_seconds):
        # Yield nothing; honor a zero/near-zero sleep so any genuine
        # cooperative-yield semantics still hold without the wall-clock cost.
        return None

    # ``storage_node_ops`` does ``import time``; patch the bound module attr so
    # every ``time.sleep(...)`` in the expansion path becomes a no-op without
    # touching the 200+ call sites or affecting unrelated modules.
    import simplyblock_core.storage_node_ops as sno
    monkeypatch.setattr(sno.time, "sleep", _no_sleep)
    # Keep a reference so a test that genuinely needs to wait can opt back in.
    yield _real_sleep


@pytest.fixture(scope="session")
def fdb_db():
    """Session-level real-FDB connection. Reused across tests."""
    if not os.path.isfile(_FDB_CLUSTER_FILE):
        pytest.skip(f"FDB cluster file not found at {_FDB_CLUSTER_FILE}")
    fdb.api_version(730)
    db = fdb.open(_FDB_CLUSTER_FILE)
    db.options.set_transaction_timeout(10000)
    return db


@pytest.fixture
def fdb_clean(fdb_db):
    """Wipe the FDB keyspace before the test. Yields the db handle."""
    # Clear everything. This is a destructive op — the test's instance is
    # dedicated to this work so it is acceptable; do not run against a
    # production FDB.
    fdb_db.clear_range(b"\x00", b"\xff")
    yield fdb_db


@pytest.fixture
def patched_rpc_router():
    """Replace simplyblock_core's RPCClient with RpcRouter in every known
    import site for the duration of the test. Yields the
    :class:`ClusterSim` the test should populate."""
    from tests.integration.expansion_sim._rpc_sim import (
        ClusterSim, RpcRouter, FirewallClientSim,
        install_rpc_router, restore_rpc_router,
        install_firewall_stub, restore_firewall_stub,
    )

    cluster_sim = ClusterSim()
    RpcRouter.set_active_cluster(cluster_sim)
    FirewallClientSim.set_active_cluster(cluster_sim)

    # Modules that do `from simplyblock_core.rpc_client import RPCClient`.
    # Enumerated by `grep -l RPCClient simplyblock_core/**/*.py`. Excludes
    # ``services.*`` which start daemon loops on import (see
    # health_check_service:logger.info("Starting health check service")) —
    # the expansion path doesn't import them anyway.
    target_modules = [
        "simplyblock_core.rpc_client",  # patches the canonical name too
        "simplyblock_core.storage_node_ops",
        "simplyblock_core.distr_controller",
        "simplyblock_core.models.storage_node",  # snode.rpc_client() factory
        "simplyblock_core.controllers.lvol_controller",
        "simplyblock_core.controllers.health_controller",
        "simplyblock_core.controllers.device_controller",
        "simplyblock_core.controllers.snapshot_controller",
        "simplyblock_core.controllers.pool_controller",
        "simplyblock_core.controllers.backup_controller",
    ]
    saved_rpc = install_rpc_router(target_modules)
    saved_fw = install_firewall_stub([
        "simplyblock_core.storage_node_ops",
        "simplyblock_core.controllers.lvol_controller",
        "simplyblock_core.controllers.health_controller",
    ])
    try:
        yield cluster_sim
    finally:
        restore_firewall_stub(saved_fw)
        restore_rpc_router(saved_rpc)
        FirewallClientSim.set_active_cluster(None)
        RpcRouter.set_active_cluster(None)


@pytest.fixture
def db_controller_singleton_reset():
    """Clear the DBController singleton instance so each test gets a fresh
    one bound to the test-scoped fdb_db."""
    from simplyblock_core.db_controller import DBController, Singleton
    Singleton._instances.pop(DBController, None)
    yield
    Singleton._instances.pop(DBController, None)


@pytest.fixture
def db_controller(fdb_clean, db_controller_singleton_reset):
    """A fresh DBController bound to the cleaned FDB."""
    from simplyblock_core.db_controller import DBController
    return DBController()
