"""The host-size floor `sn configure` enforces before laying out cores.

The smallest cluster vCPU budget is 4 and add_node wants one CPU beyond it, so
a 5-CPU host can serve a cluster; the floor must not refuse it at configure.
"""
from unittest.mock import patch

import pytest

from simplyblock_core import storage_node_ops


def _configure(cpu_count):
    with patch.object(storage_node_ops.os, "cpu_count", return_value=cpu_count), \
            patch.object(storage_node_ops.utils, "load_kernel_module"), \
            patch.object(storage_node_ops.utils, "generate_configs", return_value=({}, {})) as generate:
        storage_node_ops.generate_automated_deployment_config(
            max_lvol=10, max_prov=0, sockets_to_use=[0], nodes_per_socket=1,
            pci_allowed=[], pci_blocked=[])
        return generate


def test_a_five_cpu_host_is_configured():
    assert _configure(5).called


def test_a_four_cpu_host_is_refused():
    with pytest.raises(ValueError, match="Minimum 5"):
        _configure(4)
