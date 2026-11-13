"""A restart onto another host binds the node's drives there.

StorageNodeOps Migrate relocates a node by restarting it with a new node
address. The target host has never had SPDK on its drives, and when it is
built like the old host its drives sit at the same PCI addresses, so neither
the node's device records (other serials) nor spec.migrate.newSsdPcie
(addresses already in the node's list) say anything new about it. The
restart's own-device rebind is what binds them, by the node's PCI list and
regardless of what the records say about the drives that were there before.

Before #1636 the rebind keyed on drives the kernel driver held, and a target
host whose drives sat on no driver at all got nothing bound: 26.4.0-beta4 on
the Talos lab, 2026-11-12, failed every relocation restart with "No NVMe
devices was found!" until the drives were bound by hand.
"""
from simplyblock_core import storage_node_ops
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

ADDRESSES = ["0000:00:02.0", "0000:00:03.0", "0000:00:04.0", "0000:00:05.0"]


class FakeAgent:
    def __init__(self):
        self.bound = []

    def bind_device_to_spdk(self, pci):
        self.bound.append(pci)
        return True, None


def _device(pci, serial, status=NVMeDevice.STATUS_ONLINE):
    d = NVMeDevice()
    d.pcie_address = pci
    d.serial_number = serial
    d.status = status
    return d


def _relocated_node():
    """The record as the restart sees it on the target: the PCI list cloned
    from the old host, device records still naming the old host's serials."""
    snode = StorageNode()
    snode.uuid = "703754f2-e133-4939-9df4-3e33eea68038"
    snode.ssd_pcie = list(ADDRESSES)
    snode.nvme_devices = [_device(pci, serial) for pci, serial in
                          zip(ADDRESSES[:3], ("1431", "1432", "1433"))]
    return snode


class TestARelocationTargetGetsItsDrivesBound:
    def test_every_address_of_the_node_is_bound_when_spdk_holds_none(self):
        agent = FakeAgent()
        storage_node_ops._rebind_own_devices_to_spdk(
            _relocated_node(), agent, {"spdk_pcie_list": [], "nvme_pcie_list": []})
        assert sorted(agent.bound) == sorted(ADDRESSES)

    def test_a_drive_on_no_driver_at_all_is_still_bound(self):
        """The kernel list is empty too: a fresh host, or one whose drives were
        unbound by an earlier teardown."""
        agent = FakeAgent()
        storage_node_ops._rebind_own_devices_to_spdk(
            _relocated_node(), agent, {"spdk_pcie_list": [], "nvme_pcie_list": []})
        assert "0000:00:05.0" in agent.bound

    def test_the_old_hosts_serials_do_not_matter(self):
        """The records name drives that are not on this host. Binding is by
        the node's PCI list, so they neither block nor select anything."""
        snode = _relocated_node()
        snode.nvme_devices[0].serial_number = "not-on-this-host"
        agent = FakeAgent()
        storage_node_ops._rebind_own_devices_to_spdk(snode, agent, {"spdk_pcie_list": []})
        assert sorted(agent.bound) == sorted(ADDRESSES)

    def test_drives_spdk_already_holds_are_left_alone(self):
        agent = FakeAgent()
        storage_node_ops._rebind_own_devices_to_spdk(
            _relocated_node(), agent, {"spdk_pcie_list": list(ADDRESSES)})
        assert agent.bound == []

    def test_a_device_the_node_gave_up_outside_its_list_is_not_taken_back(self):
        snode = _relocated_node()
        snode.ssd_pcie = ADDRESSES[:3]
        snode.nvme_devices.append(_device("0000:00:05.0", "1434", NVMeDevice.STATUS_REMOVED))
        agent = FakeAgent()
        storage_node_ops._rebind_own_devices_to_spdk(snode, agent, {"spdk_pcie_list": []})
        assert "0000:00:05.0" not in agent.bound

    def test_a_failed_bind_does_not_stop_the_others(self):
        class Flaky(FakeAgent):
            def bind_device_to_spdk(self, pci):
                if pci == "0000:00:03.0":
                    raise RuntimeError("agent timed out")
                return super().bind_device_to_spdk(pci)

        agent = Flaky()
        storage_node_ops._rebind_own_devices_to_spdk(_relocated_node(), agent, {"spdk_pcie_list": []})
        assert sorted(agent.bound) == sorted(a for a in ADDRESSES if a != "0000:00:03.0")
