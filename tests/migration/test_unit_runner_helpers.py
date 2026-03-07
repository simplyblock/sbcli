# coding=utf-8
"""
test_unit_runner_helpers.py – unit tests for pure helper functions in
tasks_runner_lvol_migration.py.

No FDB connection or RPC calls are needed; all external dependencies are mocked.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode

import simplyblock_core.services.tasks_runner_lvol_migration as runner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _node(uuid="node-1", status=StorageNode.STATUS_ONLINE, mgmt_ip="10.0.0.1",
          active_rdma=False, secondary_node_id=""):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    n.mgmt_ip = mgmt_ip
    n.active_rdma = active_rdma
    n.active_tcp = not active_rdma
    n.secondary_node_id = secondary_node_id
    n.data_nics = []
    return n


def _nic(ip):
    from simplyblock_core.models.iface import IFace
    nic = IFace()
    nic.ip4_address = ip
    return nic


def _snap(snap_bdev="lvs_src/snap_foo"):
    from simplyblock_core.models.snapshot import SnapShot
    s = SnapShot()
    s.snap_bdev = snap_bdev
    return s


def _migration_obj(status=LVolMigration.STATUS_RUNNING, error=""):
    m = LVolMigration()
    m.uuid = "mig-1"
    m.status = status
    m.error_message = error
    m.write_to_db = MagicMock()
    return m


def _task(status=JobSchedule.STATUS_RUNNING):
    t = JobSchedule()
    t.uuid = "task-1"
    t.status = status
    t.function_result = ""
    t.retry = 0
    t.write_to_db = MagicMock()
    return t


# ---------------------------------------------------------------------------
# _snap_short_name
# ---------------------------------------------------------------------------

class TestSnapShortName(unittest.TestCase):

    def test_strips_lvstore_prefix(self):
        s = _snap("lvs_src/snap_myvol")
        assert runner._snap_short_name(s) == "snap_myvol"

    def test_no_prefix_returned_as_is(self):
        s = _snap("snap_myvol")
        assert runner._snap_short_name(s) == "snap_myvol"

    def test_only_first_slash_split(self):
        s = _snap("lvs/lvs2/snap_name")
        assert runner._snap_short_name(s) == "lvs2/snap_name"

    def test_empty_bdev(self):
        s = _snap("")
        assert runner._snap_short_name(s) == ""


# ---------------------------------------------------------------------------
# _snap_composite
# ---------------------------------------------------------------------------

class TestSnapComposite(unittest.TestCase):

    def test_prepends_lvstore(self):
        s = _snap("lvs_src/snap_foo")
        assert runner._snap_composite("lvs_tgt", s) == "lvs_tgt/snap_foo"

    def test_no_prefix_in_snap_bdev(self):
        s = _snap("snap_foo")
        assert runner._snap_composite("lvs_tgt", s) == "lvs_tgt/snap_foo"

    def test_result_format(self):
        s = _snap("old_store/snap_bar")
        result = runner._snap_composite("new_store", s)
        assert result.startswith("new_store/")
        assert "snap_bar" in result


# ---------------------------------------------------------------------------
# _bytes_to_mib
# ---------------------------------------------------------------------------

class TestBytesToMib(unittest.TestCase):

    def test_zero_returns_one(self):
        assert runner._bytes_to_mib(0) == 1

    def test_negative_returns_one(self):
        assert runner._bytes_to_mib(-100) == 1

    def test_exact_mib(self):
        assert runner._bytes_to_mib(1024 * 1024) == 1
        assert runner._bytes_to_mib(2 * 1024 * 1024) == 2

    def test_rounds_up(self):
        # 1 byte over 1 MiB → 2 MiB
        assert runner._bytes_to_mib(1024 * 1024 + 1) == 2

    def test_1_byte_returns_one(self):
        assert runner._bytes_to_mib(1) == 1

    def test_1_gib(self):
        assert runner._bytes_to_mib(1024 * 1024 * 1024) == 1024


# ---------------------------------------------------------------------------
# _get_migration_nic
# ---------------------------------------------------------------------------

class TestGetMigrationNic(unittest.TestCase):

    def test_returns_first_nic_ip(self):
        node = _node(mgmt_ip="10.0.0.1")
        node.data_nics = [_nic("192.168.1.10"), _nic("192.168.1.11")]
        trtype, ip = runner._get_migration_nic(node)
        assert ip == "192.168.1.10"
        assert trtype == "TCP"

    def test_falls_back_to_mgmt_ip(self):
        node = _node(mgmt_ip="10.0.0.1")
        node.data_nics = []
        trtype, ip = runner._get_migration_nic(node)
        assert ip == "10.0.0.1"
        assert trtype == "TCP"

    def test_rdma_preferred_when_active(self):
        node = _node(mgmt_ip="10.0.0.1", active_rdma=True)
        node.data_nics = [_nic("192.168.1.10")]
        trtype, ip = runner._get_migration_nic(node)
        assert trtype == "RDMA"

    def test_nic_with_empty_ip_skipped(self):
        node = _node(mgmt_ip="10.0.0.1")
        empty_nic = _nic("")
        real_nic = _nic("192.168.1.20")
        node.data_nics = [empty_nic, real_nic]
        trtype, ip = runner._get_migration_nic(node)
        assert ip == "192.168.1.20"

    def test_all_nics_empty_falls_back_to_mgmt(self):
        node = _node(mgmt_ip="10.0.0.1")
        node.data_nics = [_nic(""), _nic("")]
        trtype, ip = runner._get_migration_nic(node)
        assert ip == "10.0.0.1"


# ---------------------------------------------------------------------------
# _get_target_secondary_node
# ---------------------------------------------------------------------------

class TestGetTargetSecondaryNode(unittest.TestCase):

    def test_no_secondary_configured(self):
        tgt = _node(secondary_node_id="")
        sec_node, err = runner._get_target_secondary_node(tgt)
        assert sec_node is None
        assert err is None

    def test_secondary_online_returned(self):
        tgt = _node(secondary_node_id="sec-1")
        sec = _node(uuid="sec-1", status=StorageNode.STATUS_ONLINE)

        mock_db = MagicMock()
        mock_db.get_storage_node_by_id.return_value = sec

        with patch.object(runner, 'db', mock_db):
            sec_node, err = runner._get_target_secondary_node(tgt)

        assert sec_node is sec
        assert err is None

    def test_secondary_offline_skipped(self):
        tgt = _node(secondary_node_id="sec-1")
        sec = _node(uuid="sec-1", status=StorageNode.STATUS_OFFLINE)

        mock_db = MagicMock()
        mock_db.get_storage_node_by_id.return_value = sec

        with patch.object(runner, 'db', mock_db):
            sec_node, err = runner._get_target_secondary_node(tgt)

        assert sec_node is None
        assert err is None

    def test_secondary_bad_state_returns_error(self):
        tgt = _node(secondary_node_id="sec-1")
        sec = _node(uuid="sec-1", status="in_restart")

        mock_db = MagicMock()
        mock_db.get_storage_node_by_id.return_value = sec

        with patch.object(runner, 'db', mock_db):
            sec_node, err = runner._get_target_secondary_node(tgt)

        assert sec_node is None
        assert err is not None
        assert "in_restart" in err

    def test_secondary_not_in_db_skipped(self):
        tgt = _node(secondary_node_id="sec-missing")

        mock_db = MagicMock()
        mock_db.get_storage_node_by_id.side_effect = KeyError("sec-missing")

        with patch.object(runner, 'db', mock_db):
            sec_node, err = runner._get_target_secondary_node(tgt)

        assert sec_node is None
        assert err is None


# ---------------------------------------------------------------------------
# _suspend_task
# ---------------------------------------------------------------------------

class TestSuspendTask(unittest.TestCase):

    def test_sets_task_suspended_and_increments_retry(self):
        task = _task(status=JobSchedule.STATUS_RUNNING)
        task.retry = 2
        mig = _migration_obj()

        mock_db = MagicMock()
        with patch.object(runner, 'db', mock_db):
            result = runner._suspend_task(task, mig, "waiting for node")

        assert result is False
        assert task.status == JobSchedule.STATUS_SUSPENDED
        assert task.function_result == "waiting for node"
        assert task.retry == 3
        assert mig.status == LVolMigration.STATUS_SUSPENDED
        assert mig.error_message == "waiting for node"
        task.write_to_db.assert_called_once()
        mig.write_to_db.assert_called_once()


# ---------------------------------------------------------------------------
# _fail_task
# ---------------------------------------------------------------------------

class TestFailTask(unittest.TestCase):

    def test_single_arg_form_marks_task_done_no_migration(self):
        task = _task()

        mock_db = MagicMock()
        with patch.object(runner, 'db', mock_db):
            result = runner._fail_task(task, "something went wrong")

        assert result is True
        assert task.status == JobSchedule.STATUS_DONE
        assert task.function_result == "something went wrong"
        task.write_to_db.assert_called_once()

    def test_two_arg_form_marks_migration_failed(self):
        task = _task()
        mig = _migration_obj()

        mock_db = MagicMock()
        with patch.object(runner, 'db', mock_db), \
             patch.object(runner, 'migration_events'):
            result = runner._fail_task(task, mig, "rpc error")

        assert result is True
        assert mig.status == LVolMigration.STATUS_FAILED
        assert mig.error_message == "rpc error"
        assert mig.completed_at > 0
        assert task.status == JobSchedule.STATUS_DONE
        mig.write_to_db.assert_called_once()
        task.write_to_db.assert_called_once()


# ---------------------------------------------------------------------------
# _delete_bdev_blocking
# ---------------------------------------------------------------------------

class TestDeleteBdevBlocking(unittest.TestCase):

    def test_happy_path_calls_all_three_steps(self):
        primary_rpc = MagicMock()
        primary_rpc.delete_lvol.return_value = (True, None)
        # status: 1 (in progress), then 0 (done)
        primary_rpc.bdev_lvol_get_lvol_delete_status.side_effect = [1, 0]

        with patch('simplyblock_core.services.tasks_runner_lvol_migration.time') as mock_time:
            mock_time.sleep = MagicMock()
            runner._delete_bdev_blocking("lvs/mybdev", primary_rpc)

        # async start
        primary_rpc.delete_lvol.assert_any_call("lvs/mybdev")
        # sync finalize
        primary_rpc.delete_lvol.assert_any_call("lvs/mybdev", del_async=True)

    def test_already_deleted_status_2_still_finalizes(self):
        primary_rpc = MagicMock()
        primary_rpc.delete_lvol.return_value = (True, None)
        primary_rpc.bdev_lvol_get_lvol_delete_status.return_value = 2  # not found

        with patch('simplyblock_core.services.tasks_runner_lvol_migration.time'):
            runner._delete_bdev_blocking("lvs/mybdev", primary_rpc)

        primary_rpc.delete_lvol.assert_any_call("lvs/mybdev", del_async=True)

    def test_secondary_rpc_called_on_sync_finalize(self):
        primary_rpc = MagicMock()
        primary_rpc.delete_lvol.return_value = (True, None)
        primary_rpc.bdev_lvol_get_lvol_delete_status.return_value = 0

        secondary_rpc = MagicMock()

        with patch('simplyblock_core.services.tasks_runner_lvol_migration.time'):
            runner._delete_bdev_blocking("lvs/mybdev", primary_rpc, secondary_rpc)

        secondary_rpc.delete_lvol.assert_called_once_with("lvs/mybdev", del_async=True)

    def test_no_secondary_rpc_does_not_call_secondary(self):
        primary_rpc = MagicMock()
        primary_rpc.delete_lvol.return_value = (True, None)
        primary_rpc.bdev_lvol_get_lvol_delete_status.return_value = 0

        secondary_rpc = MagicMock()

        with patch('simplyblock_core.services.tasks_runner_lvol_migration.time'):
            runner._delete_bdev_blocking("lvs/mybdev", primary_rpc, secondary_rpc=None)

        secondary_rpc.delete_lvol.assert_not_called()
