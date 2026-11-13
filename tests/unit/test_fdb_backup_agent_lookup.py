"""The fdb-backup-agent container is searched on every mgmt node.

The agent is a one-replica swarm service constrained only to managers, so
swarm places it on any mgmt node and moves it on a redeploy or node restart.
The lookup used to query the oldest mgmt node alone: once the agent had
moved, every ``fdb_backup`` task ended at max retry with no backup written
and no log line saying why (az2, Nov 2026).
"""
import logging
from unittest import mock

import pytest
import requests

import docker
from simplyblock_core.controllers import fdb_backup_controller
from simplyblock_core.models.mgmt_node import MgmtNode

# Module-level dunder names are not mangled, but reading the attribute by
# name keeps the test independent of where it is referenced from.
lookup = getattr(fdb_backup_controller, "__get_fdb_cont")


def _mnode(hostname: str, ip: str) -> MgmtNode:
    node = MgmtNode()
    node.hostname = hostname
    node.docker_ip_port = f"{ip}:2375"
    node.status = MgmtNode.STATUS_ONLINE
    return node


def _container(name: str) -> mock.MagicMock:
    container = mock.MagicMock()
    container.name = name
    return container


AGENT = _container("app_fdb-backup-agent.1.9ahxoqlk4piga7294gstwcvdz")
OTHER = _container("app_WebAppAPI.1.abc")


@pytest.fixture
def mgmt_nodes(monkeypatch) -> list[MgmtNode]:
    nodes = [_mnode("mgmt-1", "10.0.0.1"), _mnode("mgmt-2", "10.0.0.2"), _mnode("mgmt-3", "10.0.0.3")]
    monkeypatch.setattr(fdb_backup_controller.db_controller, "get_mgmt_nodes", lambda *a, **k: nodes)
    return nodes


def _docker_apis(monkeypatch, per_url: dict) -> list[str]:
    """Fake ``docker.DockerClient``: ``per_url`` maps a base_url to the
    containers that node lists, or to an exception raised when connecting.
    Returns the list of base_urls that were contacted, in order."""
    contacted: list[str] = []

    def factory(base_url: str, **_kwargs):
        contacted.append(base_url)
        outcome = per_url[base_url]
        if isinstance(outcome, Exception):
            raise outcome
        client = mock.MagicMock()
        client.containers.list.return_value = outcome
        return client

    monkeypatch.setattr(fdb_backup_controller.docker, "DockerClient", factory)
    return contacted


def test_agent_found_on_a_later_node_past_an_unreachable_one(monkeypatch, mgmt_nodes):
    contacted = _docker_apis(monkeypatch, {
        "tcp://10.0.0.1:2375": [OTHER],
        "tcp://10.0.0.2:2375": docker.errors.DockerException("connection refused"),
        "tcp://10.0.0.3:2375": [OTHER, AGENT],
    })

    assert lookup() is AGENT
    assert contacted == ["tcp://10.0.0.1:2375", "tcp://10.0.0.2:2375", "tcp://10.0.0.3:2375"]


def test_lookup_stops_at_the_first_node_that_has_the_agent(monkeypatch, mgmt_nodes):
    contacted = _docker_apis(monkeypatch, {
        "tcp://10.0.0.1:2375": [AGENT],
        "tcp://10.0.0.2:2375": [OTHER],
        "tcp://10.0.0.3:2375": [OTHER],
    })

    assert lookup() is AGENT
    assert contacted == ["tcp://10.0.0.1:2375"]


def test_agent_on_no_reachable_node_is_none_and_names_the_nodes_searched(monkeypatch, mgmt_nodes, caplog):
    _docker_apis(monkeypatch, {
        "tcp://10.0.0.1:2375": [OTHER],
        "tcp://10.0.0.2:2375": requests.exceptions.ConnectionError("timed out"),
        "tcp://10.0.0.3:2375": [],
    })

    with caplog.at_level(logging.WARNING, logger=""):
        assert lookup() is None
        assert fdb_backup_controller.create_backup("cluster-1") is False

    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert any("mgmt-1" in m and "mgmt-3" in m and "mgmt-2" not in m for m in errors), errors
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("mgmt-2" in m for m in warnings), warnings
