# coding=utf-8
"""
conftest.py – pytest fixtures for migration integration tests.

Each test receives:
  - Mock RPC servers (source / target / optional secondary) started on loopback.
  - FDB objects created from a topology JSON spec via topology_loader.
  - A ``run_migration_task`` helper that drives the task runner to completion.

Background monitoring / repair / distrib-event / restart services are never
started; the test process only imports the task runner directly.
"""

import os
import time
import logging
import unittest.mock
import uuid as _uuid_mod
import pytest

from simplyblock_core.models.lvol_migration import LVolMigration

from tests.integration.migration.mock_rpc_server import MockRpcServer
from tests.integration.migration.topology_loader import (
    TestContext, load_topology, set_cluster_status, set_node_status, set_lvol_status, set_snap_status,
)

logger = logging.getLogger(__name__)

_MIGRATION_DIR = os.path.dirname(__file__)


def pytest_collection_modifyitems(items):
    """Tag every migration test as ``slow`` so the default integration run can
    deselect them (see ``-m "not slow"`` in tox.ini). Applied here rather than
    on each of the ~138 test functions so new migration tests inherit it
    automatically."""
    for item in items:
        if str(item.fspath).startswith(_MIGRATION_DIR):
            item.add_marker(pytest.mark.slow)


# ---------------------------------------------------------------------------
# Cluster bootstrap
# ---------------------------------------------------------------------------

@pytest.fixture()
def ensure_cluster():
    """
    Guarantee that at least one Cluster record exists in FDB before a test
    runs.  This covers two scenarios:

    1. **Real control-plane deployment** – a cluster was already created by
       ``sbcli cluster create``.  This fixture detects it and does nothing.

    2. **Standalone dev / CI** – only FDB is running, no control-plane setup.
       The fixture creates a minimal cluster record for the test and removes
       it afterward.

    Function-scoped (not session-scoped) because ``tests/integration/conftest.py``
    wipes the FDB keyspace once per test module; a session-scoped cluster
    created before the first module's wipe would vanish for every module
    after the first.

    Either way, topology fixtures always find an existing cluster to attach to.
    """
    from simplyblock_core.db_controller import DBController
    from simplyblock_core.models.cluster import Cluster

    db = DBController()
    if db.kv_store is None or isinstance(db.kv_store, unittest.mock.MagicMock):
        pytest.skip("FoundationDB is not available – skipping migration e2e tests")

    existing = db.get_clusters()
    if existing:
        # Real cluster present – leave it alone.
        yield
        return

    # No cluster found – create a minimal one for this test.
    cluster = Cluster()
    cluster.uuid = f"test-session-{_uuid_mod.uuid4().hex[:12]}"
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.ha_type = "single"
    cluster.blk_size = 4096
    cluster.distr_ndcs = 1
    cluster.distr_npcs = 1
    cluster.distr_bs = 4096
    cluster.distr_chunk_bs = 4096
    cluster.nqn = f"nqn.2023-02.io.simplyblock:{cluster.uuid[:8]}"
    cluster.write_to_db(db.kv_store)
    logger.info(f"Created test cluster {cluster.uuid}")

    yield

    try:
        cluster.remove(db.kv_store)
        logger.info(f"Removed test cluster {cluster.uuid}")
    except Exception as e:
        logger.warning(f"Could not remove test cluster: {e}")


# ---------------------------------------------------------------------------
# Port allocation
# ---------------------------------------------------------------------------

_BASE_PORT_SRC = 9901
_BASE_PORT_TGT = 9902
_BASE_PORT_SEC = 9903
_BASE_PORT_SRC_SEC = 9904


def _worker_port_offset() -> int:
    """Return 0 in single-worker mode, or 10 * worker_id in xdist mode."""
    worker = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
    try:
        return int(worker.replace("gw", "")) * 10
    except ValueError:
        return 0


# ---------------------------------------------------------------------------
# Mock server fixtures  (session-scoped: started once, reset per test)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def mock_src_server():
    """Mock RPC server for the source node."""
    offset = _worker_port_offset()
    srv = MockRpcServer(
        host="127.0.0.1", port=_BASE_PORT_SRC + offset,
        lvstore="lvs_src", node_id="src",
    )
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture(scope="session")
def mock_tgt_server():
    """Mock RPC server for the target node."""
    offset = _worker_port_offset()
    srv = MockRpcServer(
        host="127.0.0.1", port=_BASE_PORT_TGT + offset,
        lvstore="lvs_tgt", node_id="tgt",
    )
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture(scope="session")
def mock_sec_server():
    """Mock RPC server for the target secondary node (HA tests).

    Shares the primary target's lvstore name ("lvs_tgt"): the migration
    runner always builds secondary-bound composites from ``tgt_node.lvstore``
    (the primary's), never the secondary's own lvstore field, matching the
    real-world convention that HA peers mirror the same lvstore name.
    """
    offset = _worker_port_offset()
    srv = MockRpcServer(
        host="127.0.0.1", port=_BASE_PORT_SEC + offset,
        lvstore="lvs_tgt", node_id="tgt-sec",
    )
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture(scope="session")
def mock_src_sec_server():
    """Mock RPC server for the source node's own HA secondary.

    Some data-plane operations against an ha_type="ha" lvol (e.g. taking an
    intermediate snapshot mid-migration) unconditionally resolve the current
    host node's secondary/tertiary peers. The source node needs a real,
    resolvable secondary for those code paths — sharing the source's own
    lvstore name ("lvs_src"), per the same HA-peer-naming convention as
    ``mock_sec_server``.
    """
    offset = _worker_port_offset()
    srv = MockRpcServer(
        host="127.0.0.1", port=_BASE_PORT_SRC_SEC + offset,
        lvstore="lvs_src", node_id="src-sec",
    )
    srv.start()
    yield srv
    srv.stop()


# ---------------------------------------------------------------------------
# Per-test helpers
# ---------------------------------------------------------------------------

def _reset_servers(*servers: MockRpcServer):
    for srv in servers:
        srv.reset_state()
        srv.set_failure_rate(0.0)


# ---------------------------------------------------------------------------
# Topology fixtures  (per-test: create FDB objects, teardown after)
# ---------------------------------------------------------------------------

TOPOLOGIES_DIR = os.path.join(os.path.dirname(__file__), "topologies")


@pytest.fixture()
def topology_two_node(ensure_cluster, mock_src_server, mock_tgt_server):
    """Two-node topology loaded from topologies/two_node.json."""
    _reset_servers(mock_src_server, mock_tgt_server)

    spec = _load_spec("two_node.json")

    # Patch rpc_ports to match the session-scoped mock servers
    offset = _worker_port_offset()
    for node in spec["nodes"]:
        if node["id"] == "src":
            node["rpc_port"] = _BASE_PORT_SRC + offset
        elif node["id"] == "tgt":
            node["rpc_port"] = _BASE_PORT_TGT + offset

    ctx = load_topology(spec)
    yield ctx
    ctx.teardown()


@pytest.fixture()
def topology_two_node_ha(ensure_cluster, mock_src_server, mock_tgt_server, mock_sec_server,
                         mock_src_sec_server):
    """Two-node HA topology (primary + secondary on both source and target)."""
    _reset_servers(mock_src_server, mock_tgt_server, mock_sec_server, mock_src_sec_server)

    spec = _load_spec("two_node_ha.json")

    offset = _worker_port_offset()
    for node in spec["nodes"]:
        if node["id"] == "src":
            node["rpc_port"] = _BASE_PORT_SRC + offset
        elif node["id"] == "tgt":
            node["rpc_port"] = _BASE_PORT_TGT + offset
        elif node["id"] == "tgt-sec":
            node["rpc_port"] = _BASE_PORT_SEC + offset
        elif node["id"] == "src-sec":
            node["rpc_port"] = _BASE_PORT_SRC_SEC + offset

    ctx = load_topology(spec)
    yield ctx
    ctx.teardown()


@pytest.fixture()
def topology_clone_chain(ensure_cluster, mock_src_server, mock_tgt_server):
    """Clone-chain topology (l1 → s3 → s2 → s1, clone c1 from s2)."""
    _reset_servers(mock_src_server, mock_tgt_server)

    spec = _load_spec("clone_chain.json")

    offset = _worker_port_offset()
    for node in spec["nodes"]:
        if node["id"] == "src":
            node["rpc_port"] = _BASE_PORT_SRC + offset
        elif node["id"] == "tgt":
            node["rpc_port"] = _BASE_PORT_TGT + offset

    ctx = load_topology(spec)
    yield ctx
    ctx.teardown()


@pytest.fixture()
def topology_four_node(ensure_cluster, mock_src_server, mock_tgt_server):
    """Four-node cluster; l1 lives on n1 with a 4-snapshot chain; n2 is the target."""
    _reset_servers(mock_src_server, mock_tgt_server)

    spec = _load_spec("four_node.json")

    offset = _worker_port_offset()
    for node in spec["nodes"]:
        if node["id"] == "n1":
            node["rpc_port"] = _BASE_PORT_SRC + offset
            node["lvstore"] = mock_src_server.lvstore
        elif node["id"] == "n2":
            node["rpc_port"] = _BASE_PORT_TGT + offset
            node["lvstore"] = mock_tgt_server.lvstore

    ctx = load_topology(spec)
    yield ctx
    ctx.teardown()


@pytest.fixture()
def topology_complex_tree(ensure_cluster, mock_src_server, mock_tgt_server):
    """Complex snapshot/lvol tree: 7 lvols, 9 shared snapshots, 1 independent lvol."""
    _reset_servers(mock_src_server, mock_tgt_server)

    spec = _load_spec("complex_tree.json")

    offset = _worker_port_offset()
    for node in spec["nodes"]:
        if node["id"] == "src":
            node["rpc_port"] = _BASE_PORT_SRC + offset
        elif node["id"] == "tgt":
            node["rpc_port"] = _BASE_PORT_TGT + offset

    ctx = load_topology(spec)
    yield ctx
    ctx.teardown()


@pytest.fixture()
def custom_topology(ensure_cluster, mock_src_server, mock_tgt_server, mock_sec_server):
    """
    Fixture for tests that want to supply their own topology dict.

    Usage::

        def test_something(custom_topology):
            spec = {
                "cluster": {},          # omit id → auto-discover from FDB
                "nodes": [...],
                "volumes": [...],
                "snapshots": [...],
            }
            ctx = custom_topology(spec)
            # ... test body ...
            # teardown is automatic
    """
    _reset_servers(mock_src_server, mock_tgt_server, mock_sec_server)

    created: list = []

    def _factory(spec: dict) -> TestContext:
        ctx = load_topology(spec)
        created.append(ctx)
        return ctx

    yield _factory

    for ctx in created:
        ctx.teardown()


# ---------------------------------------------------------------------------
# start_migration adapter
# ---------------------------------------------------------------------------

def start_migration(lvol_id, target_node_id, max_retries=None, deadline_seconds=None,
                    ctrl_loss_tmo=None, host_nqn=None):
    """
    Test-only adapter over the real two-step migration API.

    ``migration_controller`` split the old single-call ``start_migration(lvol_id,
    target_node_id)`` into ``create_migration(lvol_id, target_node_id)`` (sets up
    target infra, returns a PHASE_PRE_CREATED migration_id) followed by
    ``start_migration(migration_id)`` (validates preconditions and launches the
    task runner). This suite's tests were written against the old single-call,
    ``(migration_id, error)``-tuple shape, so this adapter composes the two real
    calls and reports the first failure either one raises.
    """
    from simplyblock_core.controllers import migration_controller
    from simplyblock_core.exceptions import MigrationConflictError, PreconditionError
    from simplyblock_core.rpc_client import RPCException

    create_kwargs = {}
    if ctrl_loss_tmo is not None:
        create_kwargs["ctrl_loss_tmo"] = ctrl_loss_tmo
    if host_nqn is not None:
        create_kwargs["host_nqn"] = host_nqn

    try:
        migration_id, _connect_strings = migration_controller.create_migration(
            lvol_id, target_node_id, **create_kwargs)
    except (ValueError, MigrationConflictError, PreconditionError, RPCException) as e:
        return False, str(e)

    start_kwargs = {}
    if max_retries is not None:
        start_kwargs["max_retries"] = max_retries
    if deadline_seconds is not None:
        start_kwargs["deadline_seconds"] = deadline_seconds

    try:
        migration_controller.start_migration(migration_id, **start_kwargs)
    except (ValueError, RPCException) as e:
        return False, str(e)

    return migration_id, None


# ---------------------------------------------------------------------------
# Task-runner helper
# ---------------------------------------------------------------------------

def run_migration_task(migration_id: str, max_steps: int = 200,
                       step_sleep: float = 0.05) -> LVolMigration:
    """
    Drive the migration task-runner synchronously until the migration reaches a
    terminal state or ``max_steps`` iterations are exhausted.

    Imports the task runner lazily to avoid pulling heavy service modules into
    the collection phase.
    """
    from simplyblock_core.db_controller import DBController
    from simplyblock_core.services.tasks_runner_lvol_migration import task_runner

    db = DBController()
    task = _find_migration_task(db, migration_id)
    if task is None:
        raise RuntimeError(f"No task found for migration {migration_id}")

    task_id = task.uuid
    cluster_id = task.cluster_id

    for step in range(max_steps):
        # Re-fetch fresh task state directly (targeted scan for this cluster only)
        task = next(
            (t for t in db.get_active_migration_tasks(cluster_id)
             if t.uuid == task_id),
            None,
        )
        if task is None:
            break  # task marked done and dropped from active list

        terminal = task_runner(task)

        migration = db.get_migration_by_id(migration_id)
        if migration.status in (LVolMigration.STATUS_DONE,
                                 LVolMigration.STATUS_FAILED,
                                 LVolMigration.STATUS_CANCELLED):
            logger.info("Migration %s → %s after %d steps",
                        migration_id, migration.status, step + 1)
            return migration

        if terminal:
            break
        time.sleep(step_sleep)

    return db.get_migration_by_id(migration_id)


def run_migration_with_crashes(migration_id: str, crash_points: list,
                               max_steps: int = 2000,
                               step_sleep: float = 0.02) -> LVolMigration:
    """
    Drive a migration to completion, simulating process crashes at specified
    step counts.  At each crash point, the runner loop is aborted (as if the
    process was killed), then restarted from scratch — re-discovering the task
    and migration from FDB.

    ``crash_points`` is a list of step numbers at which to "crash".
    Example: [3, 8, 15] means crash after steps 3, 8, and 15.

    This verifies that the migration's FDB-persisted state is sufficient to
    resume correctly after an unclean process restart.
    """
    from simplyblock_core.db_controller import DBController
    from simplyblock_core.services.tasks_runner_lvol_migration import task_runner

    db = DBController()
    crash_set = set(crash_points)
    global_step = 0

    for restart in range(len(crash_points) + 1):
        # Re-discover task from FDB (simulates fresh process startup)
        task = _find_migration_task(db, migration_id)
        if task is None:
            break  # task completed

        task_id = task.uuid
        cluster_id = task.cluster_id

        for local_step in range(max_steps):
            task = next(
                (t for t in db.get_active_migration_tasks(cluster_id)
                 if t.uuid == task_id),
                None,
            )
            if task is None:
                break

            terminal = task_runner(task)

            migration = db.get_migration_by_id(migration_id)
            if migration.status in (LVolMigration.STATUS_DONE,
                                     LVolMigration.STATUS_FAILED,
                                     LVolMigration.STATUS_CANCELLED):
                logger.info("Migration %s → %s after %d total steps (%d restarts)",
                            migration_id, migration.status, global_step + 1, restart)
                return migration

            global_step += 1

            if global_step in crash_set:
                logger.info("Simulating crash at step %d (restart #%d)",
                            global_step, restart + 1)
                break  # "crash" — abandon current run, restart outer loop

            if terminal:
                break
            time.sleep(step_sleep)
        else:
            # max_steps exhausted without crash or completion
            break

    return db.get_migration_by_id(migration_id)


def _find_migration_task(db, migration_id: str):
    migration = db.get_migration_by_id(migration_id)
    for task in db.get_active_migration_tasks(migration.cluster_id):
        if (task.function_params or {}).get("migration_id") == migration_id:
            return task
    return None


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _load_spec(filename: str) -> dict:
    """Load a topology JSON file from the topologies/ directory as a dict."""
    import json
    path = os.path.join(TOPOLOGIES_DIR, filename)
    with open(path, 'r', encoding='utf-8') as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Re-export for convenience in test modules
# ---------------------------------------------------------------------------

__all__ = [
    # fixtures
    "mock_src_server",
    "mock_tgt_server",
    "mock_sec_server",
    "topology_two_node",
    "topology_two_node_ha",
    "topology_clone_chain",
    "topology_four_node",
    "topology_complex_tree",
    "custom_topology",
    # helpers
    "run_migration_task",
    "run_migration_with_crashes",
    "set_node_status",
    "set_cluster_status",
    "set_lvol_status",
    "set_snap_status",
    "TestContext",
]
