"""The JM raid of a node is rebuilt when the node's devices change.

A device that fails and is migrated out takes its journal partition with it
for good. The raid over the node's journal partitions cannot give a member up
in place, so a ``jm_raid_reshape`` task builds it anew, empty, over the
members the node's devices call for, has every journal it is a copy of
synchronised to it and waits until they are in sync.

These tests run the flow against the real FoundationDB: the device change
queues the task, the restart runner's spec drives it, and the node record is
read back from the database. Only what sits above the database is replaced:
the SPDK RPC client of each node, the leadership probe and the device-status
fan-out to the cluster's distribs.
"""
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import jm_raid, storage_node_ops, utils
from simplyblock_core.controllers import (
    device_controller,
    lvol_controller,
    tasks_controller,
)
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import JMDevice, NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import tasks_runner_restart
from tests.integration.task_driver import drive

CLUSTER = "7d2c1a4e-0000-4000-8000-000000000001"
NODES = [f"7d2c1a4e-0000-4000-8000-00000000010{i}" for i in (1, 2, 3)]
# Every node leads its own LVS, whose journal group is its index + 1.
ALL_SYNCED = sorted((node_id, index + 1) for index, node_id in enumerate(NODES))
ONLINE = NVMeDevice.STATUS_ONLINE
MIGRATED = NVMeDevice.STATUS_FAILED_AND_MIGRATED


def _device_id(node_index, device_index):
    return f"7d2c1a4e-0000-4000-8000-00000000{node_index}0{device_index:02d}"


def _members(count):
    return [f"nvme_{i}n1p1" for i in range(count)]


class Rpc:
    """The SPDK side of one node: what the control plane asked of it, and the
    answers a healthy node gives. bdev_jm_get_status, the JM's debugging
    status, is not part of the flow: asking for it fails the test."""

    def __init__(self, env, node_id):
        self.env, self.node_id = env, node_id
        self.mock = MagicMock()
        self.mock.bdev_get.side_effect = lambda name: (
            None if name in env.absent else {"name": name, "block_size": 4096, "num_blocks": 1000})
        self.mock.bdev_list.side_effect = lambda: list(env.bdevs)
        self.mock.bdev_raid_create.side_effect = self._raid_create
        self.mock.bdev_raid_get_bdevs.side_effect = lambda category="online": [
            {"name": name} for name in env.raids[node_id]]
        self.mock.bdev_raid_delete.side_effect = lambda name: env.raids[node_id].remove(name)
        self.mock.bdev_jm_create.return_value = True
        self.mock.subsystem_get.return_value = None
        self.mock.jc_get_jm_status.side_effect = self._jc_status
        self.mock.bdev_jm_get_status.side_effect = AssertionError("bdev_jm_get_status is a debugging call")
        self.mock.jc_explicit_synchronization.side_effect = self._explicit_sync

    def _raid_create(self, name, members, level, *args, **kwargs):
        if name in self.env.failing_raids:
            return False
        self.env.raids[self.node_id].append(name)
        self.env.created.append((self.node_id, name, list(members), level))
        return True

    def _jc_status(self, jm_vuid):
        if jm_vuid in self.env.unreachable:
            raise RPCException("leader does not answer")
        return {"jm_local": jm_vuid not in self.env.replicating, "remote_jm_n1": True}

    def _explicit_sync(self, jm_vuid):
        self.env.synced.append((self.node_id, jm_vuid))
        return {"res": 1}


class Env:
    def __init__(self, db):
        self.db = db
        self.absent: set = set()
        self.failing_raids: set = set()
        self.replicating: set = set()
        self.unreachable: set = set()
        self.created: list = []
        self.bdevs: list = []
        self.synced: list = []
        self.raids = {node_id: [] for node_id in NODES}
        self.leaders: dict = {}
        self.rpcs = {node_id: Rpc(self, node_id) for node_id in NODES}

    def node(self, index=0) -> StorageNode:
        return self.db.get_storage_node_by_id(NODES[index])

    def task(self, task_id) -> JobSchedule:
        return self.db.get_task_by_id(task_id)

    def reshape_tasks(self, node_index=None):
        return [t for t in self.db.get_job_tasks(CLUSTER)
                if t.function_name == JobSchedule.FN_JM_RAID_RESHAPE
                and (node_index is None or t.node_id == NODES[node_index])]

    def set_device(self, node_index, device_index, status):
        def _mut(node):
            node.nvme_devices[device_index].status = status
        self.db.atomic_update(self.node(node_index), _mut)

    def set_node(self, node_index, **fields):
        def _mut(node):
            for name, value in fields.items():
                setattr(node, name, value)
        self.db.atomic_update(self.node(node_index), _mut)

    def set_cluster(self, **fields):
        def _mut(cluster):
            for name, value in fields.items():
                setattr(cluster, name, value)
        self.db.atomic_update(self.db.get_cluster_by_id(CLUSTER), _mut)

    def queue(self, node_index=0) -> str:
        task_id = tasks_controller.add_jm_raid_reshape_task(CLUSTER, NODES[node_index])
        assert task_id
        return task_id

    def drive(self, task_id) -> JobSchedule:
        return drive(tasks_runner_restart.SPEC, self.task(task_id))

    def run_to_done(self, task_id, cycles=12) -> JobSchedule:
        """Drive the task until it is done; fails when it is not after ``cycles``."""
        for _ in range(cycles):
            task = self.drive(task_id)
            if task.status == JobSchedule.STATUS_DONE:
                return task
        raise AssertionError(f"task not done after {cycles} cycles: {task.status} "
                             f"{task.function_params} {task.function_result}")


def _seed_node(db, index, devices=4, legacy=False):
    """A node whose drives are each cut into a journal partition (p1) and a
    data partition (p2), with the JM raid over all the journal partitions. Its
    JM is also a copy of the next node's journal, as in a three-node cluster
    where every node keeps two remote journal copies."""
    node = StorageNode()
    node.uuid = NODES[index]
    node.cluster_id = CLUSTER
    node.status = StorageNode.STATUS_ONLINE
    node.hostname = f"worker-{index + 1}"
    node.lvstore = f"LVS_{index + 1}"
    node.jm_vuid = index + 1
    node.enable_ha_jm = False
    node.num_partitions_per_dev = 1
    node.jm_percent = 3
    node.secondary_node_id = NODES[(index + 1) % len(NODES)]
    node.jm_ids = [n for n in NODES if n != NODES[index]]
    node.nvme_devices = []
    for i in range(devices):
        dev = NVMeDevice()
        dev.uuid = _device_id(index + 1, i)
        dev.cluster_id = CLUSTER
        dev.node_id = node.uuid
        dev.nvme_controller = f"nvme_{i}"
        dev.nvme_bdev = f"nvme_{i}n1p2"
        dev.is_partition = True
        dev.status = ONLINE
        dev.cluster_device_order = index * 16 + i
        node.nvme_devices.append(dev)
    members = _members(devices)
    legs = [] if legacy or devices < 2 else jm_raid.split_groups(members)
    jm = JMDevice()
    jm.uuid = node.uuid
    jm.node_id = node.uuid
    jm.status = JMDevice.STATUS_ONLINE
    jm.jm_nvme_bdev_list = members
    jm.jm_bdev = f"jm_{node.uuid}"
    jm.alceml_bdev = f"alceml_jm_{node.uuid}"
    jm.raid_bdev = f"raid_jm_{node.uuid}" if devices > 1 else members[0]
    jm.jm_leg_members = legs
    jm.jm_leg_bdevs = [leg[0] if len(leg) == 1 else f"raid_jm_{node.uuid}_l{i}"
                       for i, leg in enumerate(legs)]
    node.jm_device = jm
    node.write_to_db(db.kv_store)
    return node


@pytest.fixture()
def env():
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    cluster = Cluster()
    cluster.uuid = CLUSTER
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.write_to_db(db.kv_store)
    for index in range(len(NODES)):
        _seed_node(db, index)
    environment = Env(db)
    for index, node_id in enumerate(NODES):
        environment.raids[node_id] = [f"raid_jm_{node_id}_l0", f"raid_jm_{node_id}_l1",
                                      f"raid_jm_{node_id}"]
        environment.leaders[f"LVS_{index + 1}"] = node_id
    with patch.object(StorageNode, "rpc_client", autospec=True,
                      side_effect=lambda self, **kw: environment.rpcs[self.get_id()].mock), \
            patch.object(lvol_controller, "is_node_leader",
                         side_effect=lambda node, lvs: environment.leaders.get(lvs) == node.get_id()), \
            patch("simplyblock_core.distr_controller.send_dev_status_event"), \
            patch.object(tasks_runner_restart.time, "sleep"):
        yield environment


def _raids_built(env, node_index=0):
    return [(name, members, level) for node_id, name, members, level in env.created
            if node_id == NODES[node_index]]


# -- the device change queues the task ---------------------------------------

def test_a_device_migrated_out_queues_the_rebuild_of_its_node(env):
    env.set_device(0, 1, NVMeDevice.STATUS_FAILED)

    assert device_controller.device_set_failed_and_migrated(_device_id(1, 1))

    assert env.node().nvme_devices[1].status == MIGRATED
    tasks = env.reshape_tasks()
    assert [t.node_id for t in tasks] == [NODES[0]]
    assert tasks[0].status == JobSchedule.STATUS_NEW


def test_a_second_device_of_the_node_does_not_queue_a_second_task(env):
    env.set_device(0, 1, NVMeDevice.STATUS_FAILED)
    env.set_device(0, 2, NVMeDevice.STATUS_FAILED)

    device_controller.device_set_failed_and_migrated(_device_id(1, 1))
    device_controller.device_set_failed_and_migrated(_device_id(1, 2))

    assert len(env.reshape_tasks()) == 1


def test_each_node_gets_its_own_task(env):
    env.set_device(0, 1, NVMeDevice.STATUS_FAILED)
    env.set_device(1, 0, NVMeDevice.STATUS_FAILED)

    device_controller.device_set_failed_and_migrated(_device_id(1, 1))
    device_controller.device_set_failed_and_migrated(_device_id(2, 0))

    assert sorted(t.node_id for t in env.reshape_tasks()) == sorted(NODES[:2])


def test_nothing_is_queued_for_a_journal_on_a_dedicated_device(env):
    def _mut(node):
        node.jm_device.jm_nvme_bdev_list = []
    env.db.atomic_update(env.node(), _mut)
    env.set_device(0, 1, NVMeDevice.STATUS_FAILED)

    device_controller.device_set_failed_and_migrated(_device_id(1, 1))

    assert env.reshape_tasks() == []


def test_a_finished_task_does_not_stand_in_for_the_next_change(env):
    env.set_device(0, 1, MIGRATED)
    first = env.queue()
    env.run_to_done(first)

    env.set_device(0, 2, NVMeDevice.STATUS_FAILED)
    device_controller.device_set_failed_and_migrated(_device_id(1, 2))

    assert len([t for t in env.reshape_tasks() if t.status != JobSchedule.STATUS_DONE]) == 1


# -- the rebuild -------------------------------------------------------------

def test_the_raid_is_rebuilt_over_the_remaining_devices_and_the_task_ends_in_sync(env):
    env.set_device(0, 1, MIGRATED)
    task_id = env.queue()

    task = env.drive(task_id)

    node = env.node()
    remaining = ["nvme_0n1p1", "nvme_2n1p1", "nvme_3n1p1"]
    assert node.jm_device.jm_nvme_bdev_list == remaining
    assert node.jm_device.jm_leg_members == [remaining[:2], remaining[2:]]
    assert node.jm_device.status == JMDevice.STATUS_ONLINE
    assert node.jm_device.stale_jm_legs == []
    # The old raid1 and both of its legs are gone before the new ones are made.
    assert _raids_built(env) == [
        (f"raid_jm_{NODES[0]}_l0", remaining[:2], "0"),
        (f"raid_jm_{NODES[0]}", [f"raid_jm_{NODES[0]}_l0", "nvme_3n1p1"], "1"),
    ]
    assert sorted(env.raids[NODES[0]]) == sorted([f"raid_jm_{NODES[0]}_l0", f"raid_jm_{NODES[0]}"])
    # Formatted, not read back.
    assert env.rpcs[NODES[0]].mock.bdev_alceml_create.call_args.kwargs["pba_init_mode"] == 3
    assert task.status == JobSchedule.STATUS_RUNNING
    assert task.function_params["phase"] == "resync"
    assert task.function_params["members"] == remaining
    # Every journal the new JM is a copy of was synchronised on its leader,
    # although every status said it was there already.
    assert sorted(env.synced) == ALL_SYNCED
    assert task.function_params["synced"] == ["LVS_1", "LVS_2", "LVS_3"]

    for _ in range(tasks_runner_restart.JM_RESHAPE_READY_POLLS - 1):
        assert env.drive(task_id).status == JobSchedule.STATUS_RUNNING
    task = env.drive(task_id)
    assert task.status == JobSchedule.STATUS_DONE
    assert "journals in sync" in task.function_result
    assert sorted(env.synced) == ALL_SYNCED            # each once
    assert len(_raids_built(env)) == 2                 # built once


def test_nothing_to_do_ends_the_task_without_touching_the_node(env):
    task = env.drive(env.queue())

    assert task.status == JobSchedule.STATUS_DONE
    assert "already matches" in task.function_result
    assert env.created == []
    assert env.node().jm_device.jm_nvme_bdev_list == _members(4)


def test_shrinking_to_two_devices_is_a_plain_mirror_and_to_one_no_raid(env):
    env.set_device(0, 0, MIGRATED)
    env.set_device(0, 1, MIGRATED)
    task_id = env.queue()
    env.run_to_done(task_id)
    node = env.node()
    assert node.jm_device.jm_nvme_bdev_list == ["nvme_2n1p1", "nvme_3n1p1"]
    assert node.jm_device.jm_leg_bdevs == ["nvme_2n1p1", "nvme_3n1p1"]
    assert _raids_built(env) == [(f"raid_jm_{NODES[0]}", ["nvme_2n1p1", "nvme_3n1p1"], "1")]

    env.created.clear()
    env.set_device(0, 2, MIGRATED)
    task_id = env.queue()
    task = env.run_to_done(task_id)
    node = env.node()
    assert task.status == JobSchedule.STATUS_DONE
    assert node.jm_device.jm_nvme_bdev_list == ["nvme_3n1p1"]
    assert node.jm_device.raid_bdev == "nvme_3n1p1"
    assert node.jm_device.jm_leg_bdevs == []
    assert env.created == []
    assert env.raids[NODES[0]] == []                   # no raid left behind


def test_a_single_device_jm_grows_into_a_mirror_not_into_the_legacy_layout(env):
    """A device that brings a journal partition joins the raid. Its record of
    one member looks like a legacy mirror (a raid_bdev and no legs)."""
    def _mut(node):
        jm = node.jm_device
        jm.jm_nvme_bdev_list = ["nvme_0n1p1"]
        jm.raid_bdev = "nvme_0n1p1"
        jm.jm_leg_bdevs = []
        jm.jm_leg_members = []
    env.db.atomic_update(env.node(), _mut)
    env.raids[NODES[0]] = []
    for index in (2, 3):
        env.set_device(0, index, NVMeDevice.STATUS_NEW)

    env.drive(env.queue())

    node = env.node()
    assert node.jm_device.jm_nvme_bdev_list == ["nvme_0n1p1", "nvme_1n1p1"]
    assert node.jm_device.jm_leg_bdevs == ["nvme_0n1p1", "nvme_1n1p1"]
    assert _raids_built(env) == [(f"raid_jm_{NODES[0]}", ["nvme_0n1p1", "nvme_1n1p1"], "1")]


def test_a_legacy_mirror_is_rebuilt_as_a_legacy_mirror(env):
    """A cluster pinned to the mirror over all members keeps that layout."""
    env.set_cluster(jm_raid_layout=jm_raid.LAYOUT_LEGACY)

    def _mut(node):
        node.jm_device.jm_leg_bdevs = []
        node.jm_device.jm_leg_members = []
    env.db.atomic_update(env.node(), _mut)
    env.raids[NODES[0]] = [f"raid_jm_{NODES[0]}"]
    env.set_device(0, 1, MIGRATED)

    env.drive(env.queue())

    node = env.node()
    remaining = ["nvme_0n1p1", "nvme_2n1p1", "nvme_3n1p1"]
    assert _raids_built(env) == [(f"raid_jm_{NODES[0]}", remaining, "1")]
    assert node.jm_device.jm_nvme_bdev_list == remaining
    assert node.jm_device.jm_leg_bdevs == []


# -- the resync --------------------------------------------------------------

def test_every_journal_is_synchronised_although_its_leader_reports_the_jm_ready(env):
    """Lab, 2026-11-24: the JM was away for five seconds during the rebuild,
    under the 15 the journal coordinator needs to replicate by itself. The
    leader reported the empty JM ready, nothing was replicated to it in 24
    minutes of client IO, and a restart of the node found the journal not on
    its disks."""
    env.set_device(0, 1, MIGRATED)

    task = env.run_to_done(env.queue())

    assert sorted(env.synced) == ALL_SYNCED
    assert task.function_params["sync_calls"] == 1


def test_the_sync_goes_to_the_node_that_leads_the_lvs(env):
    env.set_device(0, 1, MIGRATED)
    env.leaders["LVS_1"] = NODES[1]                     # failed over to its secondary

    env.drive(env.queue())

    assert (NODES[1], 1) in env.synced
    assert (NODES[0], 1) not in env.synced


def test_no_sync_without_a_confirmed_leader(env):
    """On a follower an explicit synchronisation takes the leadership away."""
    env.set_device(0, 1, MIGRATED)
    task_id = env.queue()
    original = device_controller.reshape_jm_raid

    def _reshape_then_lose_the_leader(node_id):
        members = original(node_id)
        del env.leaders["LVS_1"]
        return members

    with patch.object(device_controller, "reshape_jm_raid", side_effect=_reshape_then_lose_the_leader):
        env.drive(task_id)
    task = env.drive(task_id)

    assert sorted(env.synced) == [(NODES[1], 2), (NODES[2], 3)]
    assert task.status == JobSchedule.STATUS_RUNNING
    assert "no leader found" in task.function_result

    env.leaders["LVS_1"] = NODES[0]
    env.run_to_done(task_id)
    assert sorted(env.synced) == ALL_SYNCED


def test_a_journal_being_replicated_is_synchronised_once_that_is_over(env):
    """A JM reached over the network is brought up to date by the journal
    coordinator when it reconnects; a second writer is not started on top."""
    env.set_device(0, 1, MIGRATED)
    task_id = env.queue()
    original = device_controller.reshape_jm_raid

    def _reshape_then_replicate(node_id):
        members = original(node_id)
        env.replicating.add(2)                          # starts as the new JM connects
        return members

    with patch.object(device_controller, "reshape_jm_raid", side_effect=_reshape_then_replicate):
        task = env.drive(task_id)

    assert sorted(env.synced) == [(NODES[0], 1), (NODES[2], 3)]
    task = env.drive(task_id)
    assert sorted(env.synced) == [(NODES[0], 1), (NODES[2], 3)]
    assert task.status == JobSchedule.STATUS_RUNNING
    assert "LVS_2" in task.function_result

    env.replicating.clear()
    task = env.run_to_done(task_id)
    assert sorted(env.synced) == ALL_SYNCED


# -- when the rebuild must not start -----------------------------------------

def _assert_untouched(env, task, reason):
    assert task.status == JobSchedule.STATUS_SUSPENDED
    assert task.retry == 0
    assert reason in task.function_result
    assert "phase" not in task.function_params
    assert env.created == []
    assert env.node().jm_device.jm_nvme_bdev_list == _members(4)
    assert env.node().jm_device.status == JMDevice.STATUS_ONLINE


def test_it_waits_for_an_active_cluster(env):
    env.set_device(0, 1, MIGRATED)
    env.set_cluster(status=Cluster.STATUS_DEGRADED)
    _assert_untouched(env, env.drive(env.queue()), "cluster is degraded")


def test_it_waits_while_another_node_is_not_online(env):
    env.set_device(0, 1, MIGRATED)
    env.set_node(2, status=StorageNode.STATUS_OFFLINE)
    _assert_untouched(env, env.drive(env.queue()), f"node {NODES[2]} is offline")


def test_a_removed_node_does_not_hold_it_up(env):
    env.set_device(0, 1, MIGRATED)
    env.set_node(2, status=StorageNode.STATUS_REMOVED)

    task = env.drive(env.queue())

    assert task.function_params["phase"] == "resync"


def test_it_waits_for_data_movement_to_end(env):
    env.set_device(0, 1, MIGRATED)
    migration = JobSchedule()
    migration.uuid = "7d2c1a4e-0000-4000-8000-000000000a01"
    migration.cluster_id = CLUSTER
    migration.node_id = NODES[1]
    migration.function_name = JobSchedule.FN_NEW_DEV_MIG
    migration.status = JobSchedule.STATUS_RUNNING
    migration.write_to_db(env.db.kv_store)

    _assert_untouched(env, env.drive(env.queue()), "new_device_migration")


def test_it_waits_while_a_journal_is_being_replicated(env):
    env.set_device(0, 1, MIGRATED)
    env.replicating.add(2)                              # a journal this JM is a remote copy of
    _assert_untouched(env, env.drive(env.queue()), "journal not in sync")


def test_it_waits_while_a_leader_cannot_be_asked(env):
    env.set_device(0, 1, MIGRATED)
    env.unreachable.add(1)
    _assert_untouched(env, env.drive(env.queue()), "leader does not answer")


def test_it_waits_while_a_member_it_needs_is_absent(env):
    """A device that is only missing keeps its member; the raid is not built
    over a list with a hole in it."""
    env.set_device(0, 1, MIGRATED)
    env.set_device(0, 3, NVMeDevice.STATUS_REMOVED)
    env.absent.add("nvme_3n1p1")
    task_id = env.queue()

    task = env.drive(task_id)

    assert task.status == JobSchedule.STATUS_SUSPENDED
    assert task.retry == 0
    assert "phase" not in task.function_params
    assert "nvme_3n1p1" in task.function_result
    assert env.created == []
    node = env.node()
    assert node.jm_device.jm_nvme_bdev_list == _members(4)
    assert node.jm_device.status == JMDevice.STATUS_ONLINE     # the old stack was not taken down
    assert sorted(env.raids[NODES[0]]) == sorted(
        [f"raid_jm_{NODES[0]}_l0", f"raid_jm_{NODES[0]}_l1", f"raid_jm_{NODES[0]}"])

    env.absent.clear()
    env.set_device(0, 3, ONLINE)
    task = env.drive(task_id)
    assert task.function_params["phase"] == "resync"
    assert env.node().jm_device.jm_nvme_bdev_list == ["nvme_0n1p1", "nvme_2n1p1", "nvme_3n1p1"]


def test_one_node_at_a_time(env):
    env.set_device(0, 1, MIGRATED)
    env.set_device(1, 1, MIGRATED)
    first, second = env.queue(0), env.queue(1)

    assert env.drive(first).function_params["phase"] == "resync"
    waiting = env.drive(second)

    assert waiting.status == JobSchedule.STATUS_SUSPENDED
    assert f"node {NODES[0]} is being rebuilt" in waiting.function_result
    assert env.node(1).jm_device.jm_nvme_bdev_list == _members(4)
    assert _raids_built(env, 1) == []

    env.run_to_done(first)

    assert env.drive(second).function_params["phase"] == "resync"
    assert env.node(1).jm_device.jm_nvme_bdev_list == ["nvme_0n1p1", "nvme_2n1p1", "nvme_3n1p1"]


def test_a_queued_task_that_has_not_started_does_not_hold_up_another(env):
    env.set_device(0, 1, MIGRATED)
    env.set_device(1, 1, MIGRATED)
    env.queue(0)
    second = env.queue(1)

    assert env.drive(second).function_params["phase"] == "resync"


# -- failures ----------------------------------------------------------------

def test_a_failed_build_is_retried_until_the_node_has_a_jm_again(env):
    env.set_device(0, 1, MIGRATED)
    env.failing_raids.add(f"raid_jm_{NODES[0]}")
    task_id = env.queue()

    task = env.drive(task_id)

    assert task.status == JobSchedule.STATUS_SUSPENDED
    assert task.retry == 1
    assert task.function_params["phase"] == "rebuild"
    node = env.node()
    # The record still names the old members: the raid still does not match.
    assert node.jm_device.jm_nvme_bdev_list == _members(4)
    assert node.jm_device.status == JMDevice.STATUS_UNAVAILABLE

    # The stack is down: the retry builds whatever the cluster looks like now.
    env.failing_raids.clear()
    env.set_cluster(status=Cluster.STATUS_DEGRADED)
    task = env.drive(task_id)

    node = env.node()
    assert task.function_params["phase"] == "resync"
    assert node.jm_device.jm_nvme_bdev_list == ["nvme_0n1p1", "nvme_2n1p1", "nvme_3n1p1"]
    assert node.jm_device.status == JMDevice.STATUS_ONLINE
    # The leg the failed attempt left behind was deleted, not reused.
    assert [name for name, _, _ in _raids_built(env)].count(f"raid_jm_{NODES[0]}_l0") == 2


def test_a_canceled_task_does_not_start_the_rebuild(env):
    env.set_device(0, 1, MIGRATED)
    task_id = env.queue()

    def _cancel(task):
        task.canceled = True
    env.db.atomic_update(env.task(task_id), _cancel)

    task = env.drive(task_id)

    assert task.status == JobSchedule.STATUS_DONE
    assert env.created == []
    assert env.node().jm_device.jm_nvme_bdev_list == _members(4)


def test_a_node_that_is_not_online_waits_and_a_removed_one_ends_the_task(env):
    env.set_device(0, 1, MIGRATED)
    task_id = env.queue()
    env.set_node(0, status=StorageNode.STATUS_OFFLINE)

    task = env.drive(task_id)
    assert task.status == JobSchedule.STATUS_SUSPENDED
    assert task.retry == 0

    env.set_node(0, status=StorageNode.STATUS_REMOVED)
    task = env.drive(task_id)
    assert task.status == JobSchedule.STATUS_DONE
    assert env.created == []


# -- a JM that holds a journal keeps its legs --------------------------------

def _twelve_member_node(db, legs):
    node = _seed_node(db, 0, devices=12)

    def _mut(fresh):
        fresh.jm_device.jm_leg_members = legs
        fresh.jm_device.jm_leg_bdevs = [f"raid_jm_{NODES[0]}_l{i}" for i in range(len(legs))]
    db.atomic_update(node, _mut)
    return db.get_storage_node_by_id(NODES[0])


def test_a_restart_rebuilds_a_twelve_device_jm_from_the_two_halves_it_was_built_in(env):
    """Built before the legs were capped at four devices. Re-planned into
    three legs, every stripe would be read from the wrong drives."""
    members = _members(12)
    node = _twelve_member_node(env.db, [members[:6], members[6:]])
    env.raids[NODES[0]] = []

    jm = storage_node_ops._create_jm_stack_on_raid(
        env.rpcs[NODES[0]].mock, members, node, after_restart=True)

    assert _raids_built(env) == [
        (f"raid_jm_{NODES[0]}_l0", members[:6], "0"),
        (f"raid_jm_{NODES[0]}_l1", members[6:], "0"),
        (f"raid_jm_{NODES[0]}", [f"raid_jm_{NODES[0]}_l0", f"raid_jm_{NODES[0]}_l1"], "1"),
    ]
    assert jm.jm_leg_members == [members[:6], members[6:]]
    assert env.rpcs[NODES[0]].mock.bdev_alceml_create.call_args.kwargs["pba_init_mode"] == 1


def test_a_new_twelve_device_jm_is_three_legs_of_four(env):
    members = _members(12)
    node = _twelve_member_node(env.db, [members[:6], members[6:]])
    env.raids[NODES[0]] = []

    jm = storage_node_ops._create_jm_stack_on_raid(
        env.rpcs[NODES[0]].mock, members, node, after_restart=False)

    assert [(name, level) for name, _, level in _raids_built(env)] == [
        (f"raid_jm_{NODES[0]}_l0", "0"), (f"raid_jm_{NODES[0]}_l1", "0"),
        (f"raid_jm_{NODES[0]}_l2", "0"), (f"raid_jm_{NODES[0]}", "1")]
    assert jm.jm_leg_members == [members[:4], members[4:8], members[8:]]


def test_a_restart_with_a_member_of_one_of_three_legs_missing_runs_on_the_other_two(env):
    members = _members(12)
    legs = [members[:4], members[4:8], members[8:]]
    node = _twelve_member_node(env.db, legs)
    env.raids[NODES[0]] = []

    jm = storage_node_ops._create_jm_stack_on_raid(
        env.rpcs[NODES[0]].mock, members, node, after_restart=True, missing=[members[5]])

    assert _raids_built(env) == [
        (f"raid_jm_{NODES[0]}_l0", legs[0], "0"),
        (f"raid_jm_{NODES[0]}_l2", legs[2], "0"),
        (f"raid_jm_{NODES[0]}", [f"raid_jm_{NODES[0]}_l0", f"raid_jm_{NODES[0]}_l2"], "1"),
    ]
    assert jm.raid_bdev == f"raid_jm_{NODES[0]}"
    assert jm.jm_leg_members == legs                    # the full geometry stays on record
    assert jm.stale_jm_legs == [f"raid_jm_{NODES[0]}_l1"]


# -- removing the journal device ----------------------------------------------

def test_removing_the_jm_device_takes_the_raid0_legs_down_with_the_raid(env):
    """The legs used to stay: only the top raid was deleted, and the next
    build of the node's JM was handed the old legs with their old members."""
    jm_id = env.node().jm_device.get_id()
    assert len(env.raids[NODES[0]]) == 3

    assert device_controller.remove_jm_device(jm_id)

    assert env.raids[NODES[0]] == []
    assert env.raids[NODES[1]] == [f"raid_jm_{NODES[1]}_l0", f"raid_jm_{NODES[1]}_l1",
                                   f"raid_jm_{NODES[1]}"]
    assert env.node().jm_device.status == JMDevice.STATUS_REMOVED
    mock = env.rpcs[NODES[0]].mock
    mock.bdev_jm_delete.assert_called_once()
    mock.bdev_alceml_delete.assert_called_once()


def test_a_teardown_that_fails_leaves_the_jm_unavailable_unless_forced(env):
    jm_id = env.node().jm_device.get_id()
    env.rpcs[NODES[0]].mock.bdev_raid_delete.side_effect = RPCException("raid is busy")

    assert device_controller.remove_jm_device(jm_id) is False
    assert env.node().jm_device.status == JMDevice.STATUS_UNAVAILABLE

    assert device_controller.remove_jm_device(jm_id, force=True)
    assert env.node().jm_device.status == JMDevice.STATUS_REMOVED


def test_a_node_that_is_not_online_has_its_jm_record_removed_without_a_teardown(env):
    jm_id = env.node().jm_device.get_id()
    env.set_node(0, status=StorageNode.STATUS_OFFLINE)

    assert device_controller.remove_jm_device(jm_id)

    assert env.node().jm_device.status == JMDevice.STATUS_REMOVED
    env.rpcs[NODES[0]].mock.bdev_raid_delete.assert_not_called()


# -- a device added to the node ------------------------------------------------

def _new_whole_drive(env, index=4):
    """A drive the node found on a restart: one record for the whole drive,
    not yet in the cluster."""
    dev = NVMeDevice()
    dev.uuid = _device_id(1, index)
    dev.cluster_id = CLUSTER
    dev.node_id = NODES[0]
    dev.nvme_controller = f"nvme_{index}"
    dev.nvme_bdev = f"nvme_{index}n1"
    dev.device_name = dev.nvme_bdev
    dev.size = 100 * 1024 ** 3
    dev.status = NVMeDevice.STATUS_NEW
    dev.cluster_device_order = -1
    env.db.atomic_update(env.node(), lambda n, d=dev: n.nvme_devices.append(d))
    return dev.uuid


def _adding(env, cut_works=True):
    """add_device with the data plane replaced: the drive is cut into a
    journal partition and one data partition when the node is asked to."""
    def _cut(rpc_client, nvme, snode, num_partitions, jm_percent, partition_size, nbd_index):
        if cut_works:
            env.bdevs.extend({"name": f"{nvme.nvme_bdev}p{i}", "block_size": 4096,
                              "num_blocks": 1000 * i} for i in range(1, num_partitions + 2))
        return cut_works

    stack = ExitStack()
    stack.enter_context(patch.object(storage_node_ops, "_create_device_partitions", side_effect=_cut))
    stack.enter_context(patch.object(device_controller, "_def_create_device_stack", return_value=True))
    stack.enter_context(patch.object(storage_node_ops, "_connect_to_remote_devs", return_value=[]))
    stack.enter_context(patch("simplyblock_core.distr_controller.send_cluster_map_add_device"))
    stack.enter_context(patch.object(tasks_controller, "add_new_device_mig_task"))
    return stack


def _device(env, device_id):
    return next(d for d in env.node().nvme_devices if d.get_id() == device_id)


def test_a_new_whole_drive_is_cut_like_the_others_and_extends_the_jm_raid(env):
    """A drive added later used to be taken whole: it had no journal
    partition, and the node's JM raid could never grow onto it."""
    device_id = _new_whole_drive(env)

    with _adding(env):
        assert device_controller.add_device(device_id) == device_id

    dev = _device(env, device_id)
    assert (dev.nvme_bdev, dev.is_partition, dev.status) == ("nvme_4n1p2", True, ONLINE)
    assert device_controller.jm_raid_target_members(env.node()) == _members(5)
    tasks = env.reshape_tasks()
    assert len(tasks) == 1

    env.run_to_done(tasks[0].uuid)

    assert env.node().jm_device.jm_nvme_bdev_list == _members(5)


def test_a_drive_that_cannot_be_cut_is_not_added(env):
    device_id = _new_whole_drive(env)

    with _adding(env, cut_works=False):
        assert device_controller.add_device(device_id) is False

    dev = _device(env, device_id)
    assert (dev.nvme_bdev, dev.is_partition, dev.status) == ("nvme_4n1", False, NVMeDevice.STATUS_NEW)
    assert env.reshape_tasks() == []


def test_a_new_data_partition_of_a_drive_already_cut_is_added_as_it_is(env):
    device_id = _new_whole_drive(env)

    def _as_partition(node):
        for dev in node.nvme_devices:
            if dev.get_id() == device_id:
                dev.nvme_bdev = "nvme_4n1p2"
                dev.is_partition = True

    env.db.atomic_update(env.node(), _as_partition)

    with _adding(env) as stack:
        cut = stack.enter_context(patch.object(device_controller, "_cut_new_drive"))
        assert device_controller.add_device(device_id) == device_id

    cut.assert_not_called()
    assert device_controller.jm_raid_target_members(env.node()) == _members(5)


def test_an_lblk_device_added_back_has_its_journal_partition_opened_again(env):
    """Its bdev was dropped when the device went away; the rebuild waits for
    every member to be present."""
    device_id = _device_id(1, 9)
    member = utils.aio_bdev_name_for_serial("J9")

    def _mut(node):
        node.lblk_devices = [
            {"name": "sdx1", "serial": "J9", "journal": True, "journal_raid": True, "data_serial": "D9"},
            {"name": "sdx2", "serial": "D9"}]
        dev = NVMeDevice()
        dev.uuid = device_id
        dev.cluster_id = CLUSTER
        dev.node_id = NODES[0]
        dev.bdev_type = "aio"
        dev.serial_number = "D9"
        dev.nvme_bdev = utils.aio_bdev_name_for_serial("D9")
        dev.status = NVMeDevice.STATUS_NEW
        dev.cluster_device_order = -1
        node.nvme_devices.append(dev)

    env.db.atomic_update(env.node(), _mut)

    with _adding(env) as stack:
        ensure = stack.enter_context(
            patch.object(device_controller, "_ensure_aio_jm_member", return_value=True))
        assert device_controller.add_device(device_id) == device_id

    assert ensure.call_args.args[2] == member
    assert device_controller.jm_raid_target_members(env.node()) == _members(4) + [member]
    assert len(env.reshape_tasks()) == 1
