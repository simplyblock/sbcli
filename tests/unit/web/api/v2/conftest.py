# coding=utf-8
"""Fixtures for API v2 endpoint unit tests.

The tests drive the real v2 FastAPI routers through ``TestClient`` while all
core logic is mocked out:

- ``db`` replaces the shared ``DBController`` instance in every v2 module, so
  resource-lookup dependencies resolve to the factory-built models.
- One fixture per core controller module (``cluster_ops``, ``lvol_controller``,
  …) swaps the module reference inside the router under test for a
  ``MagicMock``, letting tests assert the controller was hit with the right
  parameters.
- Authentication is bypassed via ``app.dependency_overrides``; it has its own
  unit tests in ``test_auth.py``.
- ``Thread`` is replaced with an inline runner so fire-and-forget endpoints
  (cluster start/shutdown, node restart, …) can be asserted synchronously.
"""

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from simplyblock_core.db_controller import DBController

import simplyblock_web.api.v2 as v2
import simplyblock_web.api.v2._auth as auth_module
import simplyblock_web.api.v2._dependencies as dependencies_module
import simplyblock_web.api.v2._dtos as dtos_module
import simplyblock_web.api.v2.cluster as cluster_module
import simplyblock_web.api.v2.cluster.backup as backup_module
import simplyblock_web.api.v2.cluster.storage_node as storage_node_module
import simplyblock_web.api.v2.cluster.storage_node.device as device_module
import simplyblock_web.api.v2.cluster.storage_pool as storage_pool_module
import simplyblock_web.api.v2.cluster.storage_pool.snapshot as snapshot_module
import simplyblock_web.api.v2.cluster.storage_pool.volume as volume_module
import simplyblock_web.api.v2.cluster.storage_pool.volume.migration as migration_module
import simplyblock_web.api.v2.cluster.task as task_module
import simplyblock_web.api.v2.management_node as management_node_module

from tests.unit.web.api.v2 import _factories as factories


class _InlineThread:
    """``threading.Thread`` stand-in that runs the target on ``start()``."""

    def __init__(self, target=None, args=(), kwargs=None):
        self._target = target
        self._args = args
        self._kwargs = kwargs or {}

    def start(self):
        self._target(*self._args, **self._kwargs)


@pytest.fixture(autouse=True)
def _inline_threads(monkeypatch):
    monkeypatch.setattr(cluster_module, 'Thread', _InlineThread)
    monkeypatch.setattr(storage_node_module, 'Thread', _InlineThread)


@pytest.fixture(autouse=True)
def _dto_migration_controller(monkeypatch):
    """Neutralize the migration lookup VolumeDTO/SnapshotDTO perform."""
    mock = MagicMock()
    mock.get_active_migration_for_lvol.return_value = None
    monkeypatch.setattr(dtos_module, 'migration_controller', mock)
    return mock


@pytest.fixture()
def db(monkeypatch):
    mock = MagicMock(spec=DBController)
    monkeypatch.setattr(dependencies_module, '_db', mock)
    for module in (
        cluster_module,
        backup_module,
        storage_node_module,
        device_module,
        storage_pool_module,
        snapshot_module,
        volume_module,
        task_module,
        management_node_module,
    ):
        monkeypatch.setattr(module, 'db', mock)
    # These modules instantiate DBController() at call time
    monkeypatch.setattr(dtos_module, 'DBController', lambda: mock)
    monkeypatch.setattr(migration_module, 'DBController', lambda: mock)

    mock.get_cluster_capacity.return_value = []
    mock.get_node_capacity.return_value = []
    mock.get_device_stats.return_value = []
    mock.get_policy_for_lvol.return_value = None
    return mock


@pytest.fixture(scope='session')
def app():
    app = FastAPI()
    app.include_router(v2.api, prefix='/api/v2')
    app.dependency_overrides[auth_module.verify_api_token] = lambda: None
    return app


@pytest.fixture(scope='session')
def client(app):
    return TestClient(app)


# --- Controller mocks -------------------------------------------------------

@pytest.fixture()
def cluster_ops(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(cluster_module, 'cluster_ops', mock)
    return mock


@pytest.fixture()
def pool_controller(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(storage_pool_module, 'pool_controller', mock)
    return mock


@pytest.fixture()
def lvol_controller(monkeypatch):
    mock = MagicMock()
    mock.get_replication_info.return_value = None
    monkeypatch.setattr(volume_module, 'lvol_controller', mock)
    return mock


@pytest.fixture()
def snapshot_controller(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(volume_module, 'snapshot_controller', mock)
    monkeypatch.setattr(snapshot_module, 'snapshot_controller', mock)
    return mock


@pytest.fixture()
def backup_controller(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(volume_module, 'backup_controller', mock)
    monkeypatch.setattr(backup_module, 'backup_controller', mock)
    return mock


@pytest.fixture()
def storage_node_ops(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(storage_node_module, 'storage_node_ops', mock)
    return mock


@pytest.fixture()
def tasks_controller(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(storage_node_module, 'tasks_controller', mock)
    return mock


@pytest.fixture()
def device_controller(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(device_module, 'device_controller', mock)
    return mock


@pytest.fixture()
def migration_controller(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(migration_module, 'migration_controller', mock)
    return mock


# --- Entities resolved by the lookup dependencies ---------------------------

@pytest.fixture()
def cluster(db):
    cluster = factories.make_cluster()
    db.get_clusters.return_value = [cluster]
    db.get_cluster_by_id.return_value = cluster
    return cluster


@pytest.fixture()
def pool(db, cluster):
    pool = factories.make_pool()
    db.get_pools.return_value = [pool]
    db.get_pool_by_id.return_value = pool
    return pool


@pytest.fixture()
def volume(db, pool):
    volume = factories.make_volume()
    db.get_lvols_by_pool_id.return_value = [volume]
    db.get_lvol_by_id.return_value = volume
    return volume


@pytest.fixture()
def snapshot(db, pool):
    snapshot = factories.make_snapshot()
    db.get_snapshots_by_pool_id.return_value = [snapshot]
    db.get_snapshot_by_id.return_value = snapshot
    return snapshot


@pytest.fixture()
def storage_node(db, cluster):
    node = factories.make_storage_node()
    db.get_storage_nodes_by_cluster_id.return_value = [node]
    db.get_storage_node_by_id.return_value = node
    return node


@pytest.fixture()
def device(db, storage_node):
    device = factories.make_device()
    storage_node.nvme_devices = [device]
    return device


@pytest.fixture()
def task(db, cluster):
    task = factories.make_task()
    db.get_job_tasks.return_value = [task]
    db.get_task_by_id.return_value = task
    return task


@pytest.fixture()
def management_node(db, cluster):
    node = factories.make_management_node()
    db.get_mgmt_nodes.return_value = [node]
    db.get_mgmt_node_by_id.return_value = node
    return node


@pytest.fixture()
def backup(db, cluster):
    backup = factories.make_backup()
    db.get_backups.return_value = [backup]
    db.get_backup_by_id.return_value = backup
    return backup


@pytest.fixture()
def backup_policy(db, cluster):
    policy = factories.make_backup_policy()
    db.get_backup_policies.return_value = [policy]
    db.get_backup_policy_by_id.return_value = policy
    return policy


@pytest.fixture()
def migration(db, volume):
    migration = factories.make_migration()
    db.get_migrations.return_value = [migration]
    db.get_migration_by_id.return_value = migration
    return migration
