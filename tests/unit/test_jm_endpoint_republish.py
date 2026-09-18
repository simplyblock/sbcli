"""A restarted device must re-publish where it answers, not keep its old address.

Peers do not discover a device's address. They read `nvmf_ip` off the owner's
FDB record and attach to it (`connect_device` -> `_expected_ips`). So a restart
that re-creates a subsystem and its listeners has to re-publish the record in
the same breath, because the listeners are created from the node's live
`data_nics`.

That drift is invisible until the node's data IPs actually change — which is
exactly what `sn restart --data-nics` and a `--node-addr` move do. Field report
(2026-09-18, 3-node cluster, all nodes shut down then restarted one by one onto
eth1):

    sbctl sn restart <vm12> --data-nics eth1     # 192.168.10.92 -> 10.10.10.92
    sbctl sn restart <vm13> --data-nics eth1     # 192.168.10.93 -> 10.10.10.93

    adding listener for ...:dev:jm_<vm12> on IP 10.10.10.92     <- new, correct
    ...
    Failed to attach controller remote_jm_<vm12> via 192.168.10.92: connection
    error                                                       <- stale record

The node's alceml devices reconnected fine over the new IP; only the JM failed,
and in both directions. `_create_storage_device_stack` refreshes the four nvmf_*
fields for an NVMe device, and `_create_jm_stack_on_raid` returns a freshly
built JMDevice for the RAID layout — but the JM-on-device branch of
`_prepare_cluster_devices_on_restart` mutated the existing record in place and
never touched the endpoint.
"""
import unittest

from simplyblock_core import storage_node_ops
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.nvme_device import JMDevice, NVMeDevice


OLD_NQN = "nqn.2023-02.io.simplyblock:vm12_4421:dev:jm_old"
NEW_NQN = "nqn.2023-02.io.simplyblock:vm12_4421:dev:jm_e1ccb3b9"


class _Node:
    """Only what _publish_device_endpoint reads off a StorageNode."""

    def __init__(self, ips, nvmf_port=4422):
        self.data_nics = [
            IFace({'if_name': f'eth{i}', 'ip4_address': ip})
            for i, ip in enumerate(ips)
        ]
        self.nvmf_port = nvmf_port


class TestPublishDeviceEndpoint(unittest.TestCase):
    def test_single_nic_publishes_that_ip(self):
        dev = JMDevice({'nvmf_ip': '192.168.10.92', 'nvmf_nqn': OLD_NQN})
        storage_node_ops._publish_device_endpoint(dev, _Node(['10.10.10.92']), NEW_NQN)
        self.assertEqual(dev.nvmf_ip, '10.10.10.92')
        self.assertEqual(dev.nvmf_nqn, NEW_NQN)
        self.assertEqual(dev.nvmf_port, 4422)
        self.assertFalse(dev.nvmf_multipath)

    def test_the_reported_stale_jm_address_is_overwritten(self):
        """Regression for the field report in the module docstring.

        The JM record carried 192.168.10.92 while its listener had already moved
        to 10.10.10.92, so the peer attached to the old address and got
        'connection error'.
        """
        jm = JMDevice({'nvmf_ip': '192.168.10.92', 'nvmf_port': 4422})
        storage_node_ops._publish_device_endpoint(jm, _Node(['10.10.10.92']), NEW_NQN)
        self.assertNotIn('192.168.10.92', jm.nvmf_ip)
        self.assertEqual(jm.nvmf_ip, '10.10.10.92')

    def test_multiple_nics_publish_a_comma_list_and_set_multipath(self):
        # _expected_ips splits this back apart on the consuming side.
        dev = NVMeDevice({})
        storage_node_ops._publish_device_endpoint(
            dev, _Node(['10.10.10.92', '10.10.11.92']), NEW_NQN)
        self.assertEqual(dev.nvmf_ip, '10.10.10.92,10.10.11.92')
        self.assertTrue(dev.nvmf_multipath)

    def test_address_less_nics_are_skipped(self):
        dev = NVMeDevice({})
        storage_node_ops._publish_device_endpoint(
            dev, _Node(['', '10.10.10.92', '']), NEW_NQN)
        self.assertEqual(dev.nvmf_ip, '10.10.10.92')
        self.assertFalse(dev.nvmf_multipath)

    def test_no_addressed_nic_yields_empty_rather_than_raising(self):
        # _create_storage_device_stack used to index ip_list[0] here and would
        # have raised IndexError instead.
        dev = NVMeDevice({'nvmf_ip': '192.168.10.92'})
        storage_node_ops._publish_device_endpoint(dev, _Node([]), NEW_NQN)
        self.assertEqual(dev.nvmf_ip, '')

    def test_port_comes_from_the_node(self):
        dev = JMDevice({})
        storage_node_ops._publish_device_endpoint(
            dev, _Node(['10.10.10.93'], nvmf_port=4424), NEW_NQN)
        self.assertEqual(dev.nvmf_port, 4424)


class TestEveryRestartPathRepublishes(unittest.TestCase):
    """All three device-bring-up paths must set the endpoint, so a future one
    cannot quietly skip it again."""

    def test_nvme_and_jm_on_device_paths_call_the_publisher(self):
        import inspect
        for fn in (storage_node_ops._create_storage_device_stack,
                   storage_node_ops._prepare_cluster_devices_on_restart):
            self.assertIn(
                "_publish_device_endpoint", inspect.getsource(fn),
                f"{fn.__name__} re-creates listeners but never re-publishes "
                f"the record peers read")

    def test_jm_on_raid_path_builds_the_endpoint_itself(self):
        import inspect
        src = inspect.getsource(storage_node_ops._create_jm_stack_on_raid)
        self.assertIn("'nvmf_ip': IP", src)


if __name__ == "__main__":
    unittest.main()
