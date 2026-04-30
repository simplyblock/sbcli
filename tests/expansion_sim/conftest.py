# coding=utf-8
"""Pytest fixtures for the expansion simulator framework.

Connects to a real FoundationDB (no in-memory shortcut) and patches every
known import site of ``simplyblock_core.rpc_client.RPCClient`` with the
``RpcRouter`` so all RPC traffic is routed to the simulators.

Requires:
* a reachable FDB cluster file at the path indicated by
  ``$FDB_CLUSTER_FILE`` (or ``/etc/foundationdb/fdb.cluster``)
* the ``foundationdb`` Python client + the matching libfdb shared library

Tests in this directory mutate FDB. The ``fdb_clean`` fixture wipes the
cluster's keyspace at the start of each test so test ordering doesn't
matter.
"""

import os
import sys
import pytest

_FDB_CLUSTER_FILE = os.environ.get(
    "FDB_CLUSTER_FILE", "/etc/foundationdb/fdb.cluster")


def _require_fdb():
    if not os.path.isfile(_FDB_CLUSTER_FILE):
        pytest.skip(f"FDB cluster file not found at {_FDB_CLUSTER_FILE}; "
                    f"set FDB_CLUSTER_FILE or run on the EC2 sim host.")
    try:
        import fdb
    except ImportError:
        pytest.skip("foundationdb python client not installed")
    return fdb


@pytest.fixture(scope="session")
def fdb_db():
    """Session-level real-FDB connection. Reused across tests."""
    fdb = _require_fdb()
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
    from tests.expansion_sim._rpc_sim import (
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
