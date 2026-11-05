"""Restarting a storage node after its host rebooted.

A reboot returns the node's NVMe devices to the kernel nvme driver, and the
restart only finds devices bound to SPDK. _rebind_nvme_devices_to_spdk binds
them back before the restart looks for them.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

from simplyblock_core.storage_node_ops import _rebind_nvme_devices_to_spdk


def _node(ssd_pcie=None, devices=()):
    return SimpleNamespace(
        ssd_pcie=list(ssd_pcie or []),
        nvme_devices=[SimpleNamespace(pcie_address=a) for a in devices],
    )


def test_rebinds_known_devices_after_a_reboot():
    api = MagicMock()
    node = _node(devices=["0000:00:1e.0", "0000:00:1f.0"])

    assert _rebind_nvme_devices_to_spdk(node, api, []) is True
    assert [c.args[0] for c in api.bind_device_to_spdk.call_args_list] == ["0000:00:1e.0", "0000:00:1f.0"]


def test_binds_only_what_is_not_bound():
    api = MagicMock()
    node = _node(devices=["0000:00:1e.0", "0000:00:1f.0"])

    assert _rebind_nvme_devices_to_spdk(node, api, ["0000:00:1e.0"]) is True
    api.bind_device_to_spdk.assert_called_once_with("0000:00:1f.0")


def test_nothing_to_do_when_every_device_is_bound():
    api = MagicMock()
    node = _node(devices=["0000:00:1e.0"])

    assert _rebind_nvme_devices_to_spdk(node, api, ["0000:00:1e.0"]) is False
    api.bind_device_to_spdk.assert_not_called()


def test_configured_ssd_pcie_wins_over_recorded_devices():
    api = MagicMock()
    node = _node(ssd_pcie=["0000:00:1f.0"], devices=["0000:00:1e.0", "0000:00:1f.0"])

    _rebind_nvme_devices_to_spdk(node, api, [])
    api.bind_device_to_spdk.assert_called_once_with("0000:00:1f.0")


def test_a_failed_bind_does_not_stop_the_others():
    api = MagicMock()
    api.bind_device_to_spdk.side_effect = [RuntimeError("busy"), True]
    node = _node(devices=["0000:00:1e.0", "0000:00:1f.0"])

    assert _rebind_nvme_devices_to_spdk(node, api, []) is True
    assert api.bind_device_to_spdk.call_count == 2
