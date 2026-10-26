"""The node-restart task's reachability gate on a node whose SPDK pod is on
attached networks.

Such a node holds its data addresses inside the SPDK pod, and
``health_controller._check_ping_from_node`` asks that pod's proxy. A node that
is down to be restarted has no pod, so the probe is inconclusive (``None``) on
every pass, and the gate used to read that as "unreachable" and retry forever:
on the Talos lab (2026-10-25) all four nodes stayed offline after a reboot
until restarted by hand. The restart recreates the pod and verifies the
attachment itself, so the gate on a pod-network node is the management path
alone. A host-network node keeps the data-NIC requirement.
"""
from unittest.mock import MagicMock

import pytest

import simplyblock_core.services.tasks_runner_restart as restart_runner
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import task_runner_base
from simplyblock_core.services.task_runner_base import TaskAbort, TaskRetry


def _offline_node(on_pod_network):
    node = StorageNode()
    node.uuid = "node-1"
    node.cluster_id = "cl-1"
    node.status = StorageNode.STATUS_OFFLINE
    node.mgmt_ip = "192.168.10.145"
    node.data_nics = [MagicMock(ip4_address="10.10.10.223", if_name="data0")]
    if on_pod_network:
        node.networks = [{"name": "storage-vf", "interface": "data0"}]
    return node


def _task():
    task = JobSchedule()
    task.uuid = "task-1"
    task.function_name = JobSchedule.FN_NODE_RESTART
    task.node_id = "node-1"
    task.cluster_id = "cl-1"
    task.status = JobSchedule.STATUS_RUNNING
    task.retry = 0
    task.max_retry = 5
    task.canceled = False
    task.function_params = {}
    return task


def _gate_environment(monkeypatch, node, data_nic_probe):
    """The handler up to and including its reachability gate. The task is
    read back as canceled right after the gate, so a node that passes it
    stops with TaskAbort("canceled") instead of going on to the restart."""
    task = _task()
    canceled = _task()
    canceled.canceled = True

    fake_db = MagicMock()
    fake_db.get_storage_node_by_id.return_value = node
    fake_db.get_task_by_id.return_value = canceled
    fake_db.get_cluster_by_id.return_value = MagicMock(status="active", suspend_drain_complete=False)
    fake_db.get_storage_nodes_by_cluster_id.return_value = []
    fake_db.atomic_update.side_effect = lambda obj, fn: (fn(obj), obj)[1]
    monkeypatch.setattr(restart_runner, "db", fake_db)
    monkeypatch.setattr(task_runner_base, "db", fake_db)
    monkeypatch.setattr(JobSchedule, "write_to_db", MagicMock())

    sops = MagicMock()
    sops.fd_dead_recovery_allowed.return_value = False
    monkeypatch.setattr(restart_runner, "storage_node_ops", sops)

    hc = MagicMock()
    hc._check_node_ping.return_value = True
    hc._check_node_api.return_value = True
    hc._check_ping_from_node.return_value = data_nic_probe
    monkeypatch.setattr(restart_runner, "health_controller", hc)
    return task, hc


def test_a_pod_network_node_whose_pod_is_gone_passes_the_gate_on_the_management_path(monkeypatch):
    task, hc = _gate_environment(monkeypatch, _offline_node(on_pod_network=True), data_nic_probe=None)

    with pytest.raises(TaskAbort, match="canceled"):
        restart_runner.task_runner_node(task)

    hc._check_node_ping.assert_called_once()
    hc._check_node_api.assert_called_once()
    hc._check_ping_from_node.assert_not_called()


def test_a_pod_network_node_still_needs_its_management_path(monkeypatch):
    task, hc = _gate_environment(monkeypatch, _offline_node(on_pod_network=True), data_nic_probe=None)
    hc._check_node_api.return_value = False

    with pytest.raises(TaskRetry, match="unreachable"):
        restart_runner.task_runner_node(task)


def test_a_host_network_node_with_an_inconclusive_data_nic_probe_is_still_unreachable(monkeypatch):
    node = _offline_node(on_pod_network=False)
    task, hc = _gate_environment(monkeypatch, node, data_nic_probe=None)

    with pytest.raises(TaskRetry, match="unreachable"):
        restart_runner.task_runner_node(task)

    hc._check_ping_from_node.assert_called_once_with("10.10.10.223", ifname="data0", node=node)


def test_a_host_network_node_whose_data_nic_answers_passes_the_gate(monkeypatch):
    task, _ = _gate_environment(monkeypatch, _offline_node(on_pod_network=False), data_nic_probe=True)

    with pytest.raises(TaskAbort, match="canceled"):
        restart_runner.task_runner_node(task)
