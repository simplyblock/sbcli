"""SPDK container memory: system (non-hugepage) memory is a 2 GiB base plus
0.7 GiB per TiB of the node's raw capacity (distrib placement maps + alceml),
and it is the container's hard memory limit on k8s (request = limit, with or
without CPU topology) and docker (mem_limit) (2026-11-17)."""
import json
from unittest.mock import patch

import yaml
from jinja2 import Environment, PackageLoader

from simplyblock_core import constants, utils

GiB = 1024 ** 3
TiB = 1024 ** 4


def test_formula_is_base_plus_0_7_gib_per_tib():
    assert utils.sys_memory_for_capacity(0) == 2 * GiB
    assert utils.sys_memory_for_capacity(TiB) == 2 * GiB + int(0.7 * GiB)
    # 250 TiB over 3 nodes: ~83 TiB per node -> ~60 GiB
    per_node = utils.sys_memory_for_capacity(250 * TiB // 3)
    assert 60 * GiB < per_node < 61 * GiB


def test_nvme_capacity_is_summed_over_all_devices():
    listing = {"Devices": [{"Subsystems": [{"Controllers": [
        {"Address": "0000:00:01.0", "Namespaces": [{"PhysicalSize": 4 * TiB}]},
        {"Address": "0000:00:02.0", "Namespaces": [{"PhysicalSize": 4 * TiB}]},
        {"Address": "0000:00:03.0", "Namespaces": [{"PhysicalSize": 4 * TiB}]},  # not ours
    ]}]}]}
    with patch.object(utils, "get_nvme_list_verbose", return_value=json.dumps(listing)):
        assert utils.get_total_capacity_of_nvme_devices(["0000:00:01.0", "0000:00:02.0"]) == 8 * TiB
        assert utils.calculate_minimum_sys_memory(["0000:00:01.0", "0000:00:02.0"]) == \
            utils.sys_memory_for_capacity(8 * TiB)


def _spdk_container_resources(cpu_topology):
    env = Environment(loader=PackageLoader('simplyblock_web', 'templates'),
                      trim_blocks=True, lstrip_blocks=True)
    text = env.get_template('storage_deploy_spdk.yaml.j2').render(
        MEM_MEGA=7168, MEM2_MEGA=4096, CORES=8, CPU_TOPOLOGY_ENABLED=cpu_topology,
        NSOCKET=0, MODE="kubernetes")
    for doc in yaml.safe_load_all(text):
        if not doc or doc.get("kind") != "Pod" and "spec" not in doc:
            continue
        spec = doc.get("spec", {})
        spec = spec.get("template", {}).get("spec", spec)
        for c in spec.get("containers", []):
            if c.get("name") == "spdk-container":
                return c["resources"]
    raise AssertionError("spdk-container not found")


def test_k8s_memory_limit_and_request_always_set():
    for topology in (False, True):
        res = _spdk_container_resources(topology)
        assert res["limits"]["memory"] == "4096Mi"
        assert res["requests"]["memory"] == "4096Mi"
        assert res["limits"]["hugepages-2Mi"] == "7168Mi"


def _gone_then_running():
    """containers.get mock: the old spdk/proxy containers are gone (the two
    probes before the start), afterwards the new ones exist."""
    from unittest.mock import MagicMock

    from docker.errors import NotFound
    calls = {"n": 0}

    def get(name):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise NotFound("gone")
        return MagicMock()
    return get


def test_docker_spdk_container_gets_mem_limit():
    from simplyblock_web.api.internal.storage_node import docker as docker_api

    body = docker_api.SPDKParams(
        rpc_username="u", rpc_password="p", cluster_mode="default", cluster_id="c", server_ip="10.0.0.1",
        ssd_pcie=["0000:00:01.0"], l_cores="0-3", spdk_mem=6 * GiB, system_mem=12 * GiB)
    with patch.object(docker_api, "get_docker_client") as client, \
            patch.object(docker_api.core_utils, "remove_container"), \
            patch.object(docker_api.utils, "get_response"), \
            patch.object(docker_api, "time"):
        client.return_value.containers.get.side_effect = _gone_then_running()
        docker_api.spdk_process_start(body)
        spdk_call = client.return_value.containers.run.call_args_list[0]
    assert spdk_call.kwargs["mem_limit"] == 12 * GiB


def test_docker_mem_limit_never_below_base():
    from simplyblock_web.api.internal.storage_node import docker as docker_api

    body = docker_api.SPDKParams(
        rpc_username="u", rpc_password="p", cluster_mode="default", cluster_id="c", server_ip="10.0.0.1",
        l_cores="0-3", spdk_mem=6 * GiB)
    with patch.object(docker_api, "get_docker_client") as client, \
            patch.object(docker_api.core_utils, "remove_container"), \
            patch.object(docker_api.utils, "get_response"), \
            patch.object(docker_api, "time"):
        client.return_value.containers.get.side_effect = _gone_then_running()
        docker_api.spdk_process_start(body)
        spdk_call = client.return_value.containers.run.call_args_list[0]
    assert spdk_call.kwargs["mem_limit"] == constants.SYS_MEMORY_BASE_BYTES


def test_socket_ramdisk_is_small():
    env = Environment(loader=PackageLoader('simplyblock_web', 'templates'),
                      trim_blocks=True, lstrip_blocks=True)
    text = env.get_template('storage_deploy_spdk.yaml.j2').render(
        MEM_MEGA=7168, MEM2_MEGA=4096, CORES=8, NSOCKET=0, MODE="kubernetes")
    vols = [v for d in yaml.safe_load_all(text) if d
            for v in (d.get("spec", {}).get("template", {}).get("spec", d.get("spec", {})).get("volumes") or [])]
    socket_dir = next(v for v in vols if v["name"] == "socket-dir")
    assert socket_dir["emptyDir"]["sizeLimit"] == "16Mi"


def test_nvme_capacity_reads_subsystem_level_namespaces():
    # newer nvme-cli lists the namespace under the subsystem, not the controller
    listing = {"Devices": [{"Subsystems": [
        {"Namespaces": [{"PhysicalSize": 2 * TiB}],
         "Controllers": [{"Address": "0000:00:01.0"}]},
        {"Namespaces": [{"PhysicalSize": 3 * TiB}],
         "Controllers": [{"Address": "0000:00:02.0", "Namespaces": []}]},
    ]}]}
    with patch.object(utils, "get_nvme_list_verbose", return_value=json.dumps(listing)):
        assert utils.get_total_capacity_of_nvme_devices(["0000:00:01.0", "0000:00:02.0"]) == 5 * TiB


def test_nvme_capacity_counts_each_device_once():
    # a dual-port device shows the same address under two subsystems' controllers
    ns = [{"PhysicalSize": TiB}]
    listing = {"Devices": [{"Subsystems": [
        {"Controllers": [{"Address": "0000:00:01.0", "Namespaces": ns}]},
        {"Controllers": [{"Address": "0000:00:01.0", "Namespaces": ns}]},
    ]}]}
    with patch.object(utils, "get_nvme_list_verbose", return_value=json.dumps(listing)):
        assert utils.get_total_capacity_of_nvme_devices(["0000:00:01.0"]) == TiB


def test_nvme_capacity_is_0_when_nvme_cli_fails():
    with patch.object(utils, "get_nvme_list_verbose", return_value="nvme: command not found"):
        assert utils.get_total_capacity_of_nvme_devices(["0000:00:01.0"]) == 0


def test_capacity_from_node_info_only_counts_the_nodes_devices():
    devices = [{"address": "0000:00:01.0", "size": 4 * TiB},
               {"address": "0000:00:02.0", "size": 4 * TiB},
               {"address": "0000:00:09.0", "size": 4 * TiB}]  # boot disk / not ours
    assert utils.nvme_capacity_from_node_info(devices, ["0000:00:01.0", "0000:00:02.0"]) == 8 * TiB
    assert utils.nvme_capacity_from_node_info(None, ["0000:00:01.0"]) == 0
    assert utils.nvme_capacity_from_node_info(devices, []) == 0
