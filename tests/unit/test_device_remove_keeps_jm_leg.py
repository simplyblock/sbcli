"""device_remove() takes a JM partition out of the JM raid only when it is gone.

A logical removal -- the alceml stuck-IO watchdog on a slow SSD -- also pulled
that SSD's still-present JM partition out of raid_jm. The JM kept writing to
the other leg only, the dropped leg went stale, and on the next node restart
the raid (superblock=False) was rebuilt with both legs and no resync: the JM
alceml read stale page headers and failed with "Bad address" on every restart
attempt (SFAM-2843).
"""
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import device_controller
from simplyblock_core.models.nvme_device import JMDevice


def _run(partition_present, in_raid=True, raid_name="raid_jm_n1"):
    snode = MagicMock()
    snode.jm_device.raid_bdev = "raid_jm_n1"
    snode.jm_device.jm_nvme_bdev_list = ["nvme_61bn1p1", "nvme_611n1p1"]
    snode.jm_device.get_id.return_value = "jm1"
    device = MagicMock()
    device.nvme_controller = "nvme_611"

    members = [{"name": "nvme_61bn1p1"}]
    if in_raid:
        members.append({"name": "nvme_611n1p1"})
    rpc = MagicMock()
    rpc.bdev_raid_get_bdevs.return_value = [
        {"name": raid_name, "base_bdevs_list": members}]
    rpc.bdev_get.side_effect = (
        lambda name: {"name": name} if partition_present else None)

    with patch.object(device_controller, "remove_from_jm_device") as remove, \
            patch.object(device_controller, "set_jm_device_state") as set_state:
        device_controller._remove_jm_raid_leg_of_device(snode, device, rpc)
    return remove, set_state


def test_present_partition_stays_in_the_raid():
    remove, set_state = _run(partition_present=True)
    remove.assert_not_called()
    set_state.assert_not_called()


def test_vanished_partition_is_removed_from_the_raid():
    remove, _ = _run(partition_present=False)
    remove.assert_called_once_with("jm1", "nvme_611n1p1")


def test_partition_no_longer_in_the_raid_is_left_alone():
    remove, _ = _run(partition_present=False, in_raid=False)
    remove.assert_not_called()


def test_missing_raid_marks_the_jm_unavailable():
    remove, set_state = _run(partition_present=True, raid_name="other")
    remove.assert_not_called()
    set_state.assert_called_once_with("jm1", JMDevice.STATUS_UNAVAILABLE)
