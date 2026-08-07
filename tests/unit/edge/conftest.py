# coding=utf-8
"""Fixtures for edge-cluster unit tests.

- ``kv``: dict-backed store wired into the DBController singleton cache, plus
  a faithful fresh-read CAS stand-in for atomic_update (the real one runs the
  mutator on a fresh read, not on the caller's object).
- ``spdk``: per-node stateful FakeSpdk registry replacing node_rpc_client.
- ``fake_k8s``: replaces the simplyblock_edge.k8s entry points ops/monitor use.

The fakes themselves live in tests/_mocks.py (shared with the integration
tier, which runs the same flows against real FDB).
"""
import json

import pytest

from simplyblock_core.db_controller import DBController, Singleton
from simplyblock_edge import db as edge_db
from simplyblock_edge import k8s as edge_k8s
from tests._mocks import FakeEdgeK8s, SpdkRegistry


class FakeKV:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value

    def clear(self, key):
        self.data.pop(key, None)

    def get_range_startswith(self, prefix, limit=0, reverse=False):
        items = sorted((k, v) for k, v in self.data.items() if k.startswith(prefix))
        if reverse:
            items = items[::-1]
        if limit:
            items = items[:limit]
        return items


@pytest.fixture()
def kv(monkeypatch):
    fake = FakeKV()
    dbc = DBController()
    dbc.kv_store = fake
    Singleton._instances[DBController] = dbc

    def atomic_update(obj, mutate_fn):
        key = obj.get_db_id().encode()
        raw = fake.get(key)
        if raw is None:
            return None
        fresh = type(obj)().from_dict(json.loads(raw))
        if mutate_fn(fresh) is not False:
            fake.set(key, json.dumps(fresh.to_dict(unwrap_secrets=True)).encode())
        return fresh

    monkeypatch.setattr(dbc, "atomic_update", atomic_update)
    monkeypatch.setattr(edge_db, "_db", dbc)
    yield fake
    Singleton._instances.pop(DBController, None)


@pytest.fixture()
def spdk(monkeypatch):
    registry = SpdkRegistry()
    from simplyblock_edge import edge_cluster_ops
    from simplyblock_edge.services import edge_monitor
    monkeypatch.setattr(edge_cluster_ops, "node_rpc_client", registry)
    monkeypatch.setattr(edge_monitor, "node_rpc_client", registry)
    return registry


@pytest.fixture()
def fake_k8s(monkeypatch):
    fake = FakeEdgeK8s()
    for attr in ("deploy_spdk_pod", "delete_spdk_pod", "node_ready", "pod_running",
                 "deploy_cpu_topology_job"):
        monkeypatch.setattr(edge_k8s, attr, getattr(fake, attr))
    return fake
