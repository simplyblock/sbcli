# coding=utf-8
"""Fixtures for edge-cluster integration tests: real FoundationDB (provisioned
by tests/integration/conftest.py), fake SPDK proxies and fake edge k8s."""
import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_edge import k8s as edge_k8s
from tests._mocks import FakeEdgeK8s, SpdkRegistry


@pytest.fixture()
def db():
    controller = DBController()
    if controller.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return controller


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
