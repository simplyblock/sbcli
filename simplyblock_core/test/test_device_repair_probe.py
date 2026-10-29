"""The device-repair probe must read subsystem_get's dict.

rpc_client.subsystem_get returns the one subsystem as a dict (None when
absent). probe_device_stack extended a list with it, which appended the dict's
KEYS, so the entry it read was a string: listener and namespace always looked
missing, and device_repair could never bring a device back (bf5 2026-10-29:
device b09c1063 looped "still missing after repair: listener, namespace" until
its budget ran out, with both in place).
"""
from types import SimpleNamespace

from simplyblock_core.controllers import device_controller


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
