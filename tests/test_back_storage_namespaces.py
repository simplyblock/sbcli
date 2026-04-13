# coding=utf-8

from unittest.mock import MagicMock, call, patch

from simplyblock_core import utils as core_utils
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


def _make_snode(selected_names=None, id_device_by_nqn=False):
    snode = StorageNode()
    snode.uuid = "node-1"
    snode.cluster_id = "cluster-1"
    snode.physical_label = 7
    snode.ssd_nvme_names = list(selected_names or [])
    snode.id_device_by_nqn = id_device_by_nqn
    return snode


def _make_bdev_info(name, pci, nsid, serial="SER123", model="TestModel"):
    return [{
        "name": name,
        "block_size": 4096,
        "num_blocks": 1024,
        "driver_specific": {
            "nvme": [{
                "pci_address": pci,
                "ns_data": {"id": nsid},
                "ctrlr_data": {
                    "model_number": model,
                    "serial_number": serial,
                },
            }]
        },
    }]


def _make_core_group(cpu_mask, isolated):
    core_to_index = {core: idx for idx, core in enumerate(isolated)}
    return {
        "cpu_mask": cpu_mask,
        "isolated": isolated,
        "core_to_index": core_to_index,
        "distribution": [
            isolated[:1],
            isolated[1:2],
            isolated[2:3],
            isolated[3:4],
            [],
            isolated[:2],
            isolated[2:3],
        ],
    }


def test_detect_nvmes_accepts_explicitly_selected_blocked_namespace():
    namespace_entries = {
        "nvme0n1": {
            "pci_address": "0000:00:03.0",
            "numa_node": "0",
            "namespace_id": 1,
            "controller_name": "nvme0",
        },
        "nvme0n2": {
            "pci_address": "0000:00:03.0",
            "numa_node": "0",
            "namespace_id": 2,
            "controller_name": "nvme0",
        },
    }

    with patch("simplyblock_core.utils.get_nvme_pci_devices",
               return_value=(["0000:00:03.0"], ["nvme0n1"])), \
         patch("simplyblock_core.utils.claim_devices_to_nvme"), \
         patch("simplyblock_core.utils._list_nvme_namespaces",
               return_value=namespace_entries), \
         patch("simplyblock_core.utils.pci_utils.ensure_driver") as ensure_driver:
        nvmes = core_utils.detect_nvmes(
            pci_allowed=None,
            pci_blocked=None,
            device_model="",
            size_range="",
            nvme_names=["nvme0n1"],
        )

    assert list(nvmes.keys()) == ["nvme0n1"]
    assert nvmes["nvme0n1"]["namespace_id"] == 1
    ensure_driver.assert_called_with("0000:00:03.0", "nvme")


def test_add_nvme_devices_returns_one_device_per_selected_namespace():
    rpc_client = MagicMock()
    rpc_client.bdev_nvme_controller_list.return_value = []
    rpc_client.bdev_nvme_controller_attach.return_value = ["bdev_ns1", "bdev_ns2"]
    rpc_client.get_bdevs.side_effect = lambda name=None: {
        "bdev_ns1": _make_bdev_info("nvme0n1", "0000:00:03.0", 1),
        "bdev_ns2": _make_bdev_info("nvme0n2", "0000:00:03.0", 2),
    }[name]

    snode = _make_snode(selected_names=["nvme0n2"])
    devices = core_utils.addNvmeDevices(rpc_client, snode, ["0000:00:03.0"])

    assert len(devices) == 1
    dev = devices[0]
    assert dev.namespace_name == "nvme0n2"
    assert dev.namespace_id == 2
    assert dev.serial_number == "SER123:2"
    assert dev.nvme_bdev == "bdev_ns2"
    assert dev.pcie_address == "0000:00:03.0"


def test_add_nvme_devices_uses_pci_and_nsid_identity_when_id_device_by_nqn_is_enabled():
    rpc_client = MagicMock()
    rpc_client.bdev_nvme_controller_list.return_value = []
    rpc_client.bdev_nvme_controller_attach.return_value = ["bdev_ns1", "bdev_ns2"]
    rpc_client.get_bdevs.side_effect = lambda name=None: {
        "bdev_ns1": _make_bdev_info("nvme0n1", "0000:00:03.0", 1),
        "bdev_ns2": _make_bdev_info("nvme0n2", "0000:00:03.0", 2),
    }[name]

    snode = _make_snode(id_device_by_nqn=True)
    devices = core_utils.addNvmeDevices(rpc_client, snode, ["0000:00:03.0"])

    assert [d.serial_number for d in devices] == [
        "0000:00:03.0:1",
        "0000:00:03.0:2",
    ]


def test_generate_configs_keeps_namespaces_from_same_controller_on_one_node():
    detected_nvmes = {
        "nvme0n1": {
            "pci_address": "0000:00:03.0",
            "numa_node": "0",
            "namespace_id": 1,
            "controller_name": "nvme0",
        },
        "nvme0n2": {
            "pci_address": "0000:00:03.0",
            "numa_node": "0",
            "namespace_id": 2,
            "controller_name": "nvme0",
        },
        "nvme1n1": {
            "pci_address": "0000:00:04.0",
            "numa_node": "0",
            "namespace_id": 1,
            "controller_name": "nvme1",
        },
    }
    core_groups = {
        0: [
            _make_core_group("0x0f", [0, 1, 2, 3]),
            _make_core_group("0xf0", [4, 5, 6, 7]),
        ]
    }

    with patch("simplyblock_core.utils.get_numa_cores", return_value={0: list(range(8))}), \
         patch("simplyblock_core.utils.detect_nics", return_value={"eth0": 0}), \
         patch("simplyblock_core.utils.detect_nvmes", return_value=detected_nvmes), \
         patch("simplyblock_core.utils.generate_core_allocation", return_value=core_groups), \
         patch("simplyblock_core.utils.calculate_pool_count", return_value=(128, 64)), \
         patch("simplyblock_core.utils.calculate_minimum_hp_memory", return_value=4096), \
         patch("simplyblock_core.utils.calculate_minimum_sys_memory",
               side_effect=lambda selectors: len(selectors) * 1000), \
         patch("simplyblock_core.utils.node_utils.get_memory_details",
               return_value={"free": 10_000_000, "huge_total": 10_000_000}), \
         patch("simplyblock_core.utils.pci_utils.unbind_driver"), \
         patch("simplyblock_core.utils.regenerate_config",
               side_effect=lambda old_cfg, _new_cfg, _upgrade: old_cfg):
        config, _system_info = core_utils.generate_configs(
            max_lvol=16,
            max_prov=1024,
            sockets_to_use=[0],
            nodes_per_socket=2,
            pci_allowed=None,
            pci_blocked=None,
        )

    assert len(config["nodes"]) == 2

    node0, node1 = config["nodes"]
    assert node0["ssd_pcis"] == ["0000:00:03.0"]
    assert node0["ssd_nvmes"] == ["nvme0n1", "nvme0n2"]
    assert node0["number_of_alcemls"] == 2
    assert node0["sys_memory"] == 2000

    assert node1["ssd_pcis"] == ["0000:00:04.0"]
    assert node1["ssd_nvmes"] == ["nvme1n1"]
    assert node1["number_of_alcemls"] == 1
    assert node1["sys_memory"] == 1000


def test_prepare_cluster_devices_partitions_stops_after_partition_rpc_failure():
    snode = _make_snode()
    snode.num_partitions_per_dev = 1
    snode.jm_percent = 3
    snode.partition_size = 10
    snode.cluster_id = "cluster-1"
    snode.rpc_client = MagicMock(return_value=MagicMock())

    dev1 = NVMeDevice({
        "uuid": "dev-1",
        "status": NVMeDevice.STATUS_ONLINE,
        "nvme_bdev": "bdev_ns1",
        "device_name": "nvme0n1",
        "namespace_name": "nvme0n1",
        "namespace_id": 1,
        "pcie_address": "0000:00:03.0",
    })
    dev2 = NVMeDevice({
        "uuid": "dev-2",
        "status": NVMeDevice.STATUS_ONLINE,
        "nvme_bdev": "bdev_ns2",
        "device_name": "nvme0n2",
        "namespace_name": "nvme0n2",
        "namespace_id": 2,
        "pcie_address": "0000:00:03.0",
    })

    with patch("simplyblock_core.storage_node_ops.DBController") as mock_db_cls, \
         patch("simplyblock_core.storage_node_ops._search_for_partitions",
               side_effect=[[], [], [], []]), \
         patch("simplyblock_core.storage_node_ops._create_device_partitions",
               side_effect=[True, False]) as create_partitions, \
         patch("simplyblock_core.storage_node_ops.device_events.device_create"):
        mock_db_cls.return_value = MagicMock()
        from simplyblock_core import storage_node_ops
        result = storage_node_ops._prepare_cluster_devices_partitions(snode, [dev1, dev2])

    assert result is False
    assert create_partitions.call_args_list == [
        call(snode.rpc_client(), dev1, snode, 1, 3, 10, 1),
        call(snode.rpc_client(), dev2, snode, 1, 3, 10, 2),
    ]
