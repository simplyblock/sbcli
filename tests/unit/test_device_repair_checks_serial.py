"""A device record is held to the drive it describes, by serial.

A record's bdev and controller names are derived from the PCI address, and a
PCI address names a slot, not a disk. A node relocated onto a host built like
its old one (StorageNodeOps Migrate, Talos lab 2026-11-12) found controllers
of the right NAME holding drives it had never seen: the restart marked its
three origin devices removed by serial, the self-repair then rebuilt their
alceml stacks on the target's namespaces by name, and two alcemls on one
namespace corrupted a verifying client's data.
"""
from types import SimpleNamespace

import pytest

from simplyblock_core import utils
from simplyblock_core.controllers import device_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


def _nvme_bdev(serial, pci="0000:00:04.0", nsid=1):
    return {
        "name": "nvme_4n1",
        "driver_specific": {"nvme": [{
            "pci_address": pci,
            "ctrlr_data": {"serial_number": serial},
            "ns_data": {"id": nsid},
        }]},
    }


class FakeRPC:
    def __init__(self, bdevs=None, controllers=()):
        self.bdevs = bdevs or {}
        self.controllers = set(controllers)
        self.calls = []

    def bdev_get(self, name):
        self.calls.append(("bdev_get", name))
        return self.bdevs.get(name)

    def bdev_nvme_controller_list(self, name):
        self.calls.append(("bdev_nvme_controller_list", name))
        return [{"name": name}] if name in self.controllers else []

    def bdev_nvme_controller_attach(self, name, pci):
        self.calls.append(("attach", name, pci))
        self.controllers.add(name)

    def bdev_examine(self, name):
        self.calls.append(("examine", name))

    def bdev_wait_for_examine(self):
        self.calls.append(("wait_for_examine",))

    def bdev_nvme_detach_controller(self, name):
        self.calls.append(("detach", name))
        self.controllers.discard(name)

    def subsystem_get(self, nqn):
        return None


def _device(serial="1433", **kw):
    d = NVMeDevice()
    d.uuid = "6047e2f2-8f0e-4e52-989c-9c4a7a7b54be"
    d.serial_number = serial
    d.pcie_address = "0000:00:04.0"
    d.nvme_controller = "nvme_4"
    d.nvme_bdev = "nvme_4n1"
    d.status = NVMeDevice.STATUS_REMOVED
    d.node_id = "node-1"
    for k, v in kw.items():
        setattr(d, k, v)
    return d


def _node(rpc, device):
    snode = StorageNode()
    snode.uuid = "node-1"
    snode.nvme_devices = [device]
    snode.rpc_client = lambda **kw: rpc
    snode.client = lambda **kw: SimpleNamespace(bind_device_to_spdk=lambda pci: (True, None))
    return snode


class TestTheSerialIsDerivedOnce:
    def test_the_controller_serial_is_the_record_serial(self):
        nv = _nvme_bdev("1433")["driver_specific"]["nvme"][0]
        assert utils.nvme_namespace_serial(nv) == "1433"

    def test_a_node_identifying_by_nqn_keys_on_address_and_namespace(self):
        nv = _nvme_bdev("1433", pci="0000:00:04.0", nsid=1)["driver_specific"]["nvme"][0]
        assert utils.nvme_namespace_serial(nv, id_device_by_nqn=True) == "0000:00:04.01"

    def test_the_inventory_uses_it(self):
        import inspect
        src = inspect.getsource(utils.addNvmeDevices)
        assert "nvme_namespace_serial(" in src


class TestDriveIdentity:
    def test_the_recorded_drive_is_the_same(self):
        rpc = FakeRPC({"nvme_4n1": _nvme_bdev("1433")})
        dev = _device("1433")
        assert device_controller.drive_identity(dev, _node(rpc, dev), rpc) == (device_controller.DRIVE_SAME, "1433")

    def test_a_drive_with_another_serial_in_the_same_slot_is_other(self):
        rpc = FakeRPC({"nvme_4n1": _nvme_bdev("1453")})
        dev = _device("1433")
        assert device_controller.drive_identity(dev, _node(rpc, dev), rpc) == (device_controller.DRIVE_OTHER, "1453")

    def test_no_bdev_is_absent(self):
        rpc = FakeRPC({})
        dev = _device("1433")
        assert device_controller.drive_identity(dev, _node(rpc, dev), rpc) == (device_controller.DRIVE_ABSENT, None)

    def test_an_aio_device_has_nothing_to_compare(self):
        rpc = FakeRPC({"aio_1": {"name": "aio_1", "driver_specific": {"aio": {}}}})
        dev = _device("1433", bdev_type="aio", nvme_bdev="aio_1", nvme_controller="", pcie_address="")
        assert device_controller.drive_identity(dev, _node(rpc, dev), rpc)[0] == device_controller.DRIVE_SAME

    def test_a_record_without_a_serial_has_nothing_to_compare(self):
        rpc = FakeRPC({"nvme_4n1": _nvme_bdev("1453")})
        dev = _device("")
        assert device_controller.drive_identity(dev, _node(rpc, dev), rpc)[0] == device_controller.DRIVE_SAME


class _FakeDB:
    def __init__(self, device, snode):
        self.device, self.snode = device, snode

    def get_storage_device_by_id(self, device_id):
        return self.device

    def get_storage_node_by_id(self, node_id):
        return self.snode

    def get_cluster_by_id(self, cluster_id):
        raise AssertionError("the cluster was read: the refusal came too late")


@pytest.fixture
def repair_env(monkeypatch):
    """device_repair against fakes: records the attempt accounting, and
    fails the test if the stack builder is ever reached."""
    recorded = {}

    def install(device, rpc):
        snode = _node(rpc, device)
        monkeypatch.setattr(device_controller, "DBController", lambda: _FakeDB(device, snode))
        monkeypatch.setattr(device_controller, "_atomic_device_set",
                            lambda db, node_id, device_id, fn: recorded.update(
                                {"node": node_id, "device": device_id, "applied": fn(device)}))

        def _never(*a, **kw):
            raise AssertionError("_def_create_device_stack was called for a foreign drive")

        monkeypatch.setattr(device_controller, "_def_create_device_stack", _never)
        monkeypatch.setattr(device_controller, "device_set_online",
                            lambda *a, **kw: pytest.fail("a foreign drive was set online"))
        return recorded

    return install


class TestTheRepairRefusesAForeignDrive:
    def test_a_controller_of_the_right_name_over_another_drive_is_not_repaired(self, repair_env):
        rpc = FakeRPC({"nvme_4n1": _nvme_bdev("1453")}, controllers={"nvme_4"})
        dev = _device("1433")
        repair_env(dev, rpc)

        assert device_controller.device_repair(dev.get_id()) is False
        assert ("bdev_nvme_controller_list", "nvme_4") not in rpc.calls, "it refused before probing the stack"

    def test_the_refusal_spends_an_attempt_so_the_message_backs_off(self, repair_env):
        rpc = FakeRPC({"nvme_4n1": _nvme_bdev("1453")}, controllers={"nvme_4"})
        dev = _device("1433", repair_attempts=0)
        recorded = repair_env(dev, rpc)

        device_controller.device_repair(dev.get_id())
        assert recorded["device"] == dev.get_id()
        assert dev.repair_attempts == 1
        assert dev.last_repair_tsc > 0

    def test_a_slot_re_attached_by_address_is_checked_and_detached_again(self, repair_env):
        """The controller was absent, so the repair re-attached the PCI slot
        by address. Only then is there a serial to read; the foreign drive is
        detached again so nothing of it is left under this device's names."""
        class RPC(FakeRPC):
            def bdev_nvme_controller_attach(self, name, pci):
                super().bdev_nvme_controller_attach(name, pci)
                self.bdevs["nvme_4n1"] = _nvme_bdev("1453")

        rpc = RPC({}, controllers=set())
        dev = _device("1433")
        repair_env(dev, rpc)

        assert device_controller.device_repair(dev.get_id()) is False
        assert ("attach", "nvme_4", "0000:00:04.0") in rpc.calls
        assert ("detach", "nvme_4") in rpc.calls


class TestTheStackBuilderIsTheLastLine:
    def test_it_refuses_a_foreign_drive_even_under_force(self, monkeypatch):
        rpc = FakeRPC({"nvme_4n1": _nvme_bdev("1453")}, controllers={"nvme_4"})
        dev = _device("1433")
        snode = _node(rpc, dev)
        monkeypatch.setattr(device_controller, "DBController", lambda: _FakeDB(dev, snode))
        snode.create_alceml = lambda *a, **kw: pytest.fail("an alceml was created on a foreign drive")

        assert device_controller._def_create_device_stack(dev, snode, force=True) is False
