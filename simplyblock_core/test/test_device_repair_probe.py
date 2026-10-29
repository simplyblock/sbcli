"""The device-repair probe must read subsystem_get's dict.

rpc_client.subsystem_get returns the one subsystem as a dict (None when
absent). probe_device_stack extended a list with it, which appended the dict's
KEYS, so the entry it read was a string: listener and namespace always looked
missing, and device_repair could never bring a device back (bf5 2026-10-29:
device b09c1063 looped "still missing after repair: listener, namespace" until
its budget ran out, with both in place).
"""
from types import SimpleNamespace

import pytest

from simplyblock_core.controllers import device_controller
from simplyblock_core.models.nvme_device import NVMeDevice


class _Rpc:
    def __init__(self, subsystem):
        self.subsystem = subsystem

    def bdev_nvme_controller_list(self, name):
        return [{"name": name}]

    def bdev_get(self, name):
        return {"name": name}

    def subsystem_get(self, nqn):
        return self.subsystem


def _device():
    return SimpleNamespace(nvme_controller="nvme_1", alceml_bdev="alceml_d1",
                           nvmf_nqn="nqn.x:dev:d1", get_id=lambda: "d1")


def test_intact_stack_reads_listener_and_namespace():
    rpc = _Rpc({"nqn": "nqn.x:dev:d1",
                "listen_addresses": [{"traddr": "10.0.0.1"}],
                "namespaces": [{"name": "alceml_d1_PT"}]})
    present, missing = device_controller.probe_device_stack(_device(), None, rpc)
    assert missing == []
    assert present["listener"] and present["namespace"]


def test_subsystem_without_listener_or_namespace():
    rpc = _Rpc({"nqn": "nqn.x:dev:d1", "listen_addresses": [], "namespaces": []})
    _present, missing = device_controller.probe_device_stack(_device(), None, rpc)
    assert set(missing) == {"listener", "namespace"}


def test_absent_subsystem():
    _present, missing = device_controller.probe_device_stack(_device(), None, _Rpc(None))
    assert set(missing) == {"subsystem", "listener", "namespace"}


# --- device_repair with an intact local stack --------------------------------

@pytest.fixture
def repair(monkeypatch):
    calls = []
    dev = SimpleNamespace(status=NVMeDevice.STATUS_REMOVED, admin_removed=False,
                          retries_exhausted=False, repair_attempts=0, node_id="N1",
                          get_id=lambda: "d1")
    node = SimpleNamespace(nvme_devices=[dev], rpc_client=lambda: None,
                           get_id=lambda: "N1")

    class _DB:
        def get_storage_device_by_id(self, _id):
            return dev

        def get_storage_node_by_id(self, _id):
            return node

    monkeypatch.setattr(device_controller, "DBController", _DB)
    monkeypatch.setattr(device_controller, "probe_device_stack",
                        lambda d, n, r: ({"alceml": True}, []))
    monkeypatch.setattr(device_controller, "device_set_io_error",
                        lambda i, e: calls.append(("io_error", e)))
    monkeypatch.setattr(device_controller, "device_set_online",
                        lambda i, cause=None, connect_peers=True: calls.append(("online", connect_peers)))
    monkeypatch.setattr(device_controller, "_def_create_device_stack",
                        lambda *a, **k: pytest.fail("an intact stack must not be rebuilt"))
    return dev, calls


def test_removed_device_with_intact_stack_goes_online(repair):
    """An earlier attempt rebuilt the stack; only the online step was left."""
    _dev, calls = repair
    assert device_controller.device_repair("d1") is True
    assert ("online", True) in calls


def test_unavailable_device_with_intact_stack_stays_put(repair):
    """Unavailable by consensus is remote: a local rebuild cannot fix it."""
    dev, calls = repair
    dev.status = NVMeDevice.STATUS_UNAVAILABLE
    assert device_controller.device_repair("d1") is False
    assert calls == []
