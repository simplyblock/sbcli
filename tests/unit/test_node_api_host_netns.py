"""The node API on the pod network: what it reads from the host, and how.

On the pod network the agent's own network namespace holds only the pod's eth0,
so the host's NICs, their addresses, and the ping out of a data NIC have to be
read in the host's namespace. SB_HOST_NETNS names that namespace and SB_NODE_NAME
carries the Kubernetes node name, because `hostname` returns the pod's name there.
Both are unset on the host network and in docker mode, where nothing changes.
"""
from unittest.mock import patch

import psutil
import pytest
from flask import Flask

from simplyblock_core import utils
from simplyblock_core.utils import shell
from simplyblock_web.api.internal import _node_info
from simplyblock_web.api.internal.storage_node import docker as agent

HOST_NETNS = "/host-proc/1/ns/net"


def _answer(endpoint, *args):
    """The `results` an endpoint answers with, built inside an application context."""
    with Flask(__name__).app_context():
        response = endpoint(*args)
        return response.get_json()["results"]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("SB_HOST_NETNS", raising=False)
    monkeypatch.delenv("SB_NODE_NAME", raising=False)


def test_a_host_net_command_is_unchanged_when_no_namespace_is_named():
    assert shell.host_net_command(["ip", "-j", "address", "show"]) == ["ip", "-j", "address", "show"]


def test_a_host_net_command_enters_the_named_namespace(monkeypatch):
    monkeypatch.setenv("SB_HOST_NETNS", HOST_NETNS)
    assert shell.host_net_command(["ip", "-j", "address", "show"]) == [
        "nsenter", f"--net={HOST_NETNS}", "ip", "-j", "address", "show"]


def test_the_nic_listing_is_read_in_the_host_namespace(monkeypatch):
    monkeypatch.setenv("SB_HOST_NETNS", HOST_NETNS)
    with patch.object(utils.shell_utils, "run_command", return_value=("[]", "", 0)) as run:
        utils.get_nics_data()
    run.assert_called_once_with(f"nsenter --net={HOST_NETNS} ip -j address show")


def test_the_nic_listing_is_read_in_place_without_a_namespace():
    with patch.object(utils.shell_utils, "run_command", return_value=("[]", "", 0)) as run:
        utils.get_nics_data()
    run.assert_called_once_with("ip -j address show")


def test_a_ping_leaves_through_the_host_namespace(monkeypatch):
    monkeypatch.setenv("SB_HOST_NETNS", HOST_NETNS)
    with patch.object(agent.subprocess, "check_call") as call, \
            patch.object(agent.Path, "read_text", return_value="1\n"):
        _answer(agent.ping_ip, agent.PingQuery(ip="10.10.10.148", ifname="eth1"))
    assert call.call_args.args[0][:3] == ["nsenter", f"--net={HOST_NETNS}", "ping"]


def test_a_ping_runs_in_place_without_a_namespace():
    with patch.object(agent.subprocess, "check_call") as call, \
            patch.object(agent.Path, "read_text", return_value="1\n"):
        _answer(agent.ping_ip, agent.PingQuery(ip="10.10.10.148", ifname="eth1"))
    assert call.call_args.args[0][0] == "ping"


def _nics(**addresses):
    return {name: {"name": name, "ip": ip} for name, ip in addresses.items()}


def test_a_nic_with_an_ipv4_address_is_tcp_capable():
    with patch.object(agent.core_utils, "get_nics_data", return_value=_nics(eth1="10.10.10.147")), \
            patch.object(psutil, "net_if_addrs", return_value={}):
        assert _answer(agent.ifc_is_tcp, agent.NicQuery(nic="eth1")) is True


def test_a_nic_without_an_address_is_not_tcp_capable():
    with patch.object(agent.core_utils, "get_nics_data", return_value=_nics(eth1="")):
        assert _answer(agent.ifc_is_tcp, agent.NicQuery(nic="eth1")) is False


def test_an_unknown_nic_is_not_tcp_capable():
    with patch.object(agent.core_utils, "get_nics_data", return_value=_nics(eth0="192.168.10.147")):
        assert _answer(agent.ifc_is_tcp, agent.NicQuery(nic="eth1")) is False


def test_the_hostname_is_the_first_label_of_the_node_name(monkeypatch):
    monkeypatch.setenv("SB_NODE_NAME", "vm07.simplyblock5.localdomain")
    with patch.object(utils.shell_utils, "run_command", return_value=("agent-pod-x", "", 0)):
        assert utils.get_hostname() == "vm07"


def test_the_hostname_is_asked_of_the_system_without_a_node_name():
    with patch.object(utils.shell_utils, "run_command", return_value=("vm07", "", 0)) as run:
        assert utils.get_hostname() == "vm07"
    run.assert_called_once_with("hostname -s")


def test_the_static_node_info_takes_the_node_name_too(monkeypatch):
    monkeypatch.setenv("SB_NODE_NAME", "vm07.simplyblock5.localdomain")
    monkeypatch.setenv("WITHOUT_CLOUD_INFO", "1")
    _node_info.get_static_node_info.cache_clear()
    with patch.object(_node_info.shell_utils, "run_command", return_value=("agent-pod-x", "", 0)):
        assert _node_info.get_static_node_info()["hostname"] == "vm07"
    _node_info.get_static_node_info.cache_clear()


_SERIALLESS_DISK = (
    '{"blockdevices": [{"name": "vdb", "type": "disk", "size": 1000, "serial": null, "wwn": null,'
    ' "mountpoint": null, "model": "M", "rota": false, "ro": false, "vendor": "V", "pkname": null,'
    ' "partuuid": null}]}'
)


def _synthetic_serial(node_utils_module, pod_hostname):
    """The serial a disk with neither serial nor WWN is given, read with the process's hostname set."""
    with patch.object(node_utils_module.shell_utils, "run_command", return_value=(_SERIALLESS_DISK, "", 0)), \
            patch.object(node_utils_module, "_root_disk_names", return_value=set()), \
            patch.object(node_utils_module, "read_persistent_links", return_value={}), \
            patch.object(node_utils_module, "_read_sysfs", return_value="-1"), \
            patch("socket.gethostname", return_value=pod_hostname):
        return node_utils_module.get_block_devices_info()[0]["serial"]


def test_a_disk_without_a_serial_keeps_its_identity_when_the_pod_is_replaced(monkeypatch):
    """Its synthetic serial is seeded by the hostname, which on the pod network is the pod's name."""
    from simplyblock_web import node_utils

    monkeypatch.setenv("SB_NODE_NAME", "worker-1.example")
    first_pod = _synthetic_serial(node_utils, "agent-abc12")
    replacement = _synthetic_serial(node_utils, "agent-xyz98")

    assert first_pod == replacement


def test_a_disk_without_a_serial_is_seeded_by_the_hostname_when_no_node_name_is_set():
    from simplyblock_web import node_utils

    assert _synthetic_serial(node_utils, "host-a") != _synthetic_serial(node_utils, "host-b")
