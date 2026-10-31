"""A restarted leader must resume compression of its own journal, and a
restart must bind its devices back to SPDK.

bf7 (2026-10-30), EKS, 5 nodes, FTT2, page copy on, heavy fio: a JC context
starts with auto-compression suspended (ultra 38fb5595), and with page copy
every first write after a snapshot appends a journal record. After the 16:38
restarts nothing resumed the nodes' own jm_vuids -- only a finished data
migration on the node does that, and under load none finished -- so the
13 GiB JM partitions filled. 19:20: JM full -> JCERR -> abort on every
survivor -> cluster suspended, the cycle repeating after each re-activation.

The nodes the suspend drain shut down then never came back: the shutdown
hands the NVMe to the kernel nvme driver and SPDK's setup.sh refuses to take
a partitioned device back, so every restart failed with "No NVMe devices was
found!".
"""
import ast
import inspect
import textwrap
from types import SimpleNamespace

import pytest

from simplyblock_core import cluster_ops, storage_node_ops


class _RPC:
    def __init__(self, result=True, exc=None):
        self.calls, self._result, self._exc = [], result, exc

    def jc_suspend_compression(self, jm_vuid, suspend):
        self.calls.append((jm_vuid, suspend))
        if self._exc:
            raise self._exc
        return self._result


class _Node:
    def __init__(self, rpc, nid="N1"):
        self._rpc, self._id, self.cluster_id = rpc, nid, "CL"

    def get_id(self):
        return self._id

    def rpc_client(self, **_):
        return self._rpc


@pytest.fixture
def tasks(monkeypatch):
    added = []
    monkeypatch.setattr(storage_node_ops.tasks_controller, "add_jc_comp_resume_task",
                        lambda cl, nid, jm_vuid: added.append((cl, nid, jm_vuid)))
    monkeypatch.setattr(storage_node_ops, "DBController",
                        lambda: SimpleNamespace(get_cluster_by_id=lambda _cid: object()))
    monkeypatch.setattr(storage_node_ops.jc_compression_upgrade, "resume_is_held", lambda _cl: False)
    return added


def test_resume_sends_unsuspend_for_the_given_vuid(tasks):
    rpc = _RPC()
    assert storage_node_ops.resume_jc_compression(_Node(rpc), 22) is True
    assert rpc.calls == [(22, False)]
    assert tasks == []


@pytest.mark.parametrize("rpc", [_RPC(result=False), _RPC(exc=RuntimeError("timeout"))])
def test_a_failed_resume_leaves_a_retry_task(tasks, rpc):
    assert storage_node_ops.resume_jc_compression(_Node(rpc), 22) is False
    assert tasks == [("CL", "N1", 22)]


def test_the_upgrade_hold_is_respected(tasks, monkeypatch):
    monkeypatch.setattr(storage_node_ops.jc_compression_upgrade, "resume_is_held", lambda _cl: True)
    rpc = _RPC()
    assert storage_node_ops.resume_jc_compression(_Node(rpc), 22) is False
    assert rpc.calls == [] and tasks == []


def _calls(func, name):
    tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
    return [n for n in ast.walk(tree) if isinstance(n, ast.Call)
            and getattr(n.func, "attr", getattr(n.func, "id", None)) == name]


def test_recreate_lvstore_resumes_the_leaders_own_lvs_journal():
    calls = _calls(storage_node_ops._recreate_lvstore_impl, "resume_jc_compression")
    assert [(ast.unparse(c.args[0]), ast.unparse(c.args[1])) for c in calls] == [("snode", "lvs_jm_vuid")]


def test_every_activation_resumes_every_online_node():
    src = inspect.getsource(cluster_ops._cluster_activate)
    assert "storage_node_ops.resume_jc_compression(node, node.jm_vuid)" in src
    # The resume used to sit under `if ols_status == Cluster.STATUS_UNREADY`,
    # so re-activating a suspended cluster left every journal suspended.
    tree = ast.parse(textwrap.dedent(src))
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and "STATUS_UNREADY" in ast.unparse(node.test):
            assert not any(isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "resume_jc_compression"
                           for n in ast.walk(node))


class _API:
    def __init__(self, fail=()):
        self.bound, self._fail = [], set(fail)

    def bind_device_to_spdk(self, pci):
        if pci in self._fail:
            raise RuntimeError("bind failed")
        self.bound.append(pci)


def _snode(ssd_pcie, devices):
    return SimpleNamespace(ssd_pcie=ssd_pcie, get_id=lambda: "N1",
                           nvme_devices=[SimpleNamespace(bdev_type=t, pcie_address=a) for t, a in devices])


def test_restart_binds_own_kernel_bound_devices_back_to_spdk():
    api = _API()
    node_info = {"nvme_pcie_list": ["0000:00:04.0", "0000:00:1f.0"], "spdk_pcie_list": []}
    storage_node_ops._rebind_own_devices_to_spdk(
        _snode(["0000:00:1f.0"], [("nvme", "0000:00:1f.0")]), api, node_info)
    # 00:04.0 is the root EBS volume: on the kernel driver, but not ours.
    assert api.bound == ["0000:00:1f.0"]


def test_devices_already_on_spdk_and_lblk_devices_are_left_alone():
    api = _API()
    node_info = {"nvme_pcie_list": ["0000:00:1e.0"], "spdk_pcie_list": ["0000:00:1f.0"]}
    storage_node_ops._rebind_own_devices_to_spdk(
        _snode(["0000:00:1f.0"], [("nvme", "0000:00:1f.0"), ("aio", "0000:00:1e.0")]), api, node_info)
    assert api.bound == []


def test_devices_known_only_from_the_device_records_are_bound_too():
    # ssd_pcie is empty on auto-discovered nodes; the device records still
    # name the PCI functions.
    api = _API(fail={"0000:00:1d.0"})
    node_info = {"nvme_pcie_list": ["0000:00:1d.0", "0000:00:1f.0"]}
    storage_node_ops._rebind_own_devices_to_spdk(
        _snode([], [("nvme", "0000:00:1d.0"), ("nvme", "0000:00:1f.0")]), api, node_info)
    # One failing bind does not stop the others.
    assert api.bound == ["0000:00:1f.0"]


def test_restart_rebinds_before_starting_spdk():
    src = inspect.getsource(storage_node_ops._restart_storage_node_impl)
    assert src.index("_rebind_own_devices_to_spdk(") < src.index("snode_api.spdk_process_start(")


def _task(fn, node_id="N1", status="running"):
    from simplyblock_core.models.job_schedule import JobSchedule
    t = JobSchedule()
    t.function_name, t.node_id, t.status, t.canceled = fn, node_id, status, False
    return t


def test_replication_tasks_do_not_hold_back_the_compression_resume(monkeypatch):
    from simplyblock_core.controllers import tasks_controller
    from simplyblock_core.models.job_schedule import JobSchedule as J
    tasks = [_task(J.FN_SNAPSHOT_REPLICATION), _task(J.FN_SNAPSHOT_REPLICATION, status="new"),
             _task(J.FN_REPLICATION_FINAL), _task(J.FN_BACKUP), _task(J.FN_LVOL_SYNC_DEL),
             _task(J.FN_DEV_MIG, node_id="N2")]
    monkeypatch.setattr(tasks_controller.db, "get_job_tasks", lambda _cid: tasks)
    # Every node that replicates always has one of these open; the resume
    # waited on them and never fired.
    assert tasks_controller.get_active_node_tasks("CL", "N1")
    assert tasks_controller.get_jc_comp_blocking_node_tasks("CL", "N1") == []


@pytest.mark.parametrize("fn", ["device_migration", "failed_device_migration", "new_device_migration",
                                "node_restart", "lvol_migration"])
def test_data_movement_on_the_node_still_holds_it_back(monkeypatch, fn):
    from simplyblock_core.controllers import tasks_controller
    tasks = [_task(fn), _task("snapshot_replication")]
    monkeypatch.setattr(tasks_controller.db, "get_job_tasks", lambda _cid: tasks)
    assert [t.function_name for t in tasks_controller.get_jc_comp_blocking_node_tasks("CL", "N1")] == [fn]
    tasks[0].status = "done"
    assert tasks_controller.get_jc_comp_blocking_node_tasks("CL", "N1") == []


@pytest.mark.parametrize("module", ["tasks_runner_migration", "tasks_runner_jc_comp"])
def test_both_resume_paths_use_the_narrow_check(module):
    import importlib
    src = inspect.getsource(importlib.import_module(f"simplyblock_core.services.{module}"))
    assert "get_jc_comp_blocking_node_tasks(" in src
    assert "get_active_node_tasks(" not in src
