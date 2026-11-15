"""Capacity guards on the fullest device, not only the cluster average.

Bed bf24 (2026-11-14, 5 nodes x 1 device, 1+2): at 20:51 the cluster was at
70% with three devices at 74-75% and two at 56%/62%. The three filled up at
20:54:27, while the cluster average was 88%, below cap_crit (90). Writes that
needed a new location then failed with EIO.
"""
from unittest.mock import patch

from simplyblock_core import capacity_guard
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import cap_monitor
from simplyblock_core.services import snapshot_monitor as sm


class _Rec:
    def __init__(self, util):
        self.size_util = util


class _Dev:
    def __init__(self, did, util, status=NVMeDevice.STATUS_ONLINE, order=0):
        self.id = did
        self.util = util
        self.status = status
        self.cluster_device_order = order

    def get_id(self):
        return self.id


class _Node:
    def __init__(self, nid, devs, status=StorageNode.STATUS_ONLINE):
        self.id = nid
        self.nvme_devices = devs
        self.status = status

    def get_id(self):
        return self.id


class _Cluster:
    cap_warn = 80
    cap_crit = 90
    distr_ndcs = 1
    distr_npcs = 2
    sync_replication = False

    def get_id(self):
        return "CL1"


class _DB:
    def __init__(self, cluster_util, dev_utils, node_status=StorageNode.STATUS_ONLINE):
        self.cluster_util = cluster_util
        self.nodes = [_Node(f"N{i}", [_Dev(f"D{i}", u, order=i)], node_status)
                      for i, u in enumerate(dev_utils)]

    def get_cluster_by_id(self, cid):
        return _Cluster()

    def get_cluster_capacity(self, cl, limit=1):
        return [] if self.cluster_util is None else [_Rec(self.cluster_util)]

    def get_storage_nodes_by_cluster_id(self, cluster_id):
        return self.nodes

    def get_device_stats(self, dev, limit=1):
        return [] if dev.util is None else [_Rec(dev.util)]


BF24_2054 = [100, 100, 100, 70, 70]       # 88% average


class TestEffectiveUtilization:

    def test_the_fullest_device_counts_when_above_the_average(self):
        util, why = capacity_guard.effective_utilization(_DB(70, [75, 75, 74, 56, 62]), _Cluster())
        assert util == 75
        assert "device D0" in why and "75%" in why

    def test_the_average_counts_when_no_device_is_above_it(self):
        util, why = capacity_guard.effective_utilization(_DB(70, [60, 60, 60]), _Cluster())
        assert util == 70 and "cluster CL1" in why

    def test_no_reading_at_all(self):
        assert capacity_guard.effective_utilization(_DB(None, [None]), _Cluster()) == (None, "")

    def test_failed_and_removed_devices_do_not_count(self):
        db = _DB(50, [99, 40])
        db.nodes[0].nvme_devices[0].status = NVMeDevice.STATUS_FAILED_AND_MIGRATED
        assert capacity_guard.effective_utilization(db, _Cluster())[0] == 50

    def test_a_down_node_s_device_still_counts(self):
        # its data comes back with it
        db = _DB(50, [99, 40], node_status=StorageNode.STATUS_DOWN)
        assert capacity_guard.effective_utilization(db, _Cluster())[0] == 99


class TestReadOnlyOnFullDevices:

    def _full(self, utils):
        with patch.object(cap_monitor, "db", _DB(88, utils)):
            return cap_monitor._devices_full(_Cluster())

    def test_bf24_too_few_devices_left_for_a_stripe(self):
        why = self._full(BF24_2054)
        assert "only 2 device(s) below 90%" in why and "a stripe needs 3" in why

    def test_enough_devices_left(self):
        assert self._full([100, 95, 80, 70, 70]) == ""

    def test_a_cluster_smaller_than_a_stripe_is_not_judged(self):
        assert self._full([100, 100]) == ""

    def test_devices_without_a_reading_do_not_count_as_full(self):
        assert self._full([None, None, None, 70, 70]) == ""


class TestStopAllocatingOnFullDevices:

    def test_only_online_devices_on_online_nodes_at_or_above_cap_crit(self):
        db = _DB(70, [91, 90, 89, 95])
        db.nodes[3].status = StorageNode.STATUS_DOWN
        with patch.object(cap_monitor, "db", db), \
                patch.object(cap_monitor.device_controller, "device_set_state") as dss:
            cap_monitor._stop_allocating_on_full_devices(_Cluster())
        assert [c.args for c in dss.call_args_list] == [
            ("D0", NVMeDevice.STATUS_CANNOT_ALLOCATE), ("D1", NVMeDevice.STATUS_CANNOT_ALLOCATE)]

    def test_a_device_already_cannot_allocate_is_left_alone(self):
        db = _DB(70, [95, 50, 50, 50])
        db.nodes[0].nvme_devices[0].status = NVMeDevice.STATUS_CANNOT_ALLOCATE
        with patch.object(cap_monitor, "db", db), \
                patch.object(cap_monitor.device_controller, "device_set_state") as dss:
            cap_monitor._stop_allocating_on_full_devices(_Cluster())
        dss.assert_not_called()


class TestCadenceHoldOnTheFullestDevice:

    def test_held_by_a_device_past_cap_warn_below_the_average(self):
        with patch.object(sm, "db", _DB(70, [85, 60, 60])):
            reason = sm._cadence_capacity_hold("CL1")
        assert "device D0" in reason and "85%" in reason and "80%" in reason

    def test_not_held_when_every_device_is_below(self):
        with patch.object(sm, "db", _DB(70, [75, 60, 60])):
            assert sm._cadence_capacity_hold("CL1") == ""
