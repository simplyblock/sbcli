"""The 75-subsystems-per-node limit is enforced at every max_lvol ingress.

Before this, `--max-subsys` was accepted unchecked at `sn configure` /
`sn restart`: the value sized huge pages and landed in the node record, while
volume placement independently clamped the node to MAX_SUBSYSTEMS_PER_NODE
(`lvol_controller.max_subsystems_for_node`). An operator asking for 300 got a
node that reserved memory for 300 and served 75, with nothing saying so.
"""
import argparse
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from simplyblock_cli import cli as cli_module
from simplyblock_cli import clibase
from simplyblock_core import cluster_ops, constants, storage_node_ops
from simplyblock_web import node_configure
from simplyblock_web.api.internal.storage_node.docker import PersistNodeConfigParams

CAP = constants.MAX_SUBSYSTEMS_PER_NODE


def _wrapper():
    """Build a CLI wrapper without entering its run loop."""
    cli_module.CLIWrapper.__init__(w := cli_module.CLIWrapper.__new__(cli_module.CLIWrapper))
    return w


def _restart_args():
    """The restart namespace no longer carries max_lvol / max_prov: those are
    cluster settings and the restart reads them from the cluster."""
    return argparse.Namespace(
        node_id="node-1", max_snap=5000,
        spdk_image=None, spdk_debug=False, reattach_volume=False,
        small_bufsize=0, large_bufsize=0, ssd_pcie=[], node_ip=None,
        force=False, force_lvol_recreate=False, spdk_proxy_image=None,
    )


def test_cap_is_the_documented_product_limit():
    # Lowering/raising this is a product decision — every ingress below and the
    # CI bootstrap defaults are written against it.
    assert CAP == 75


# --- CLI: the node-level knobs are gone -------------------------------------
#
# max-subsys, the huge-page floor and the vCPU count are cluster settings now.
# A per-node value let a cluster drift into nodes with different subsystem
# ceilings and different core budgets.

def test_configure_no_longer_takes_max_subsys():
    w = _wrapper()
    with pytest.raises(SystemExit):
        w.parser.parse_args(["storage-node", "configure", "--max-subsys", str(CAP)])


def test_configure_no_longer_takes_cores_percentage():
    w = _wrapper()
    with pytest.raises(SystemExit):
        w.parser.parse_args(["storage-node", "configure", "--cores-percentage", "50"])


def test_configure_sizes_for_the_product_ceiling():
    """configure runs on the host before it belongs to a cluster, so it cannot
    read the cluster's value. Sizing for the ceiling keeps every cluster
    setting usable; sizing smaller would leave the node unable to honour one."""
    w = _wrapper()
    args = w.parser.parse_args(["storage-node", "configure"])
    with patch.object(clibase.storage_ops, "generate_automated_deployment_config",
                      return_value=True) as gen:
        assert w.storage_node__configure("configure", args) is True
    assert gen.call_args.args[0] == CAP


def test_restart_no_longer_takes_max_subsys():
    w = _wrapper()
    with pytest.raises(SystemExit):
        w.parser.parse_args(["storage-node", "restart", "node-1",
                             "--max-subsys", str(CAP)])


def test_restart_defers_to_the_cluster_value():
    w = _wrapper()
    with patch.object(clibase.storage_ops, "restart_storage_node",
                      return_value=True) as restart:
        assert w.storage_node__restart("restart", _restart_args()) is True
    assert restart.call_args.args[1] == 0, "0 means: adopt the cluster's max_subsys"


# --- cluster level: where the cap is enforced now ---------------------------

def test_cluster_sizing_rejects_above_cap():
    with pytest.raises(ValueError, match=str(CAP)):
        cluster_ops.validate_spdk_sizing(max_subsys=CAP + 1)


def test_cluster_sizing_accepts_the_cap():
    cluster_ops.validate_spdk_sizing(max_subsys=CAP)


def test_cluster_sizing_treats_zero_as_the_product_default():
    cluster_ops.validate_spdk_sizing(max_subsys=0, hugepages_mem=0,
                                     spdk_vcpu_count=0)


def test_cluster_sizing_refuses_negative_values():
    with pytest.raises(ValueError):
        cluster_ops.validate_spdk_sizing(hugepages_mem=-1)
    with pytest.raises(ValueError):
        cluster_ops.validate_spdk_sizing(spdk_vcpu_count=-1)


# --- core: the CLI is not the only caller ----------------------------------

def test_generate_config_rejects_above_cap():
    # Guard sits ahead of every side effect (kernel modules, config file write).
    assert storage_node_ops.generate_automated_deployment_config(
        CAP + 1, 0, [0], 1, [], []) is False


def test_generate_config_rejects_above_cap_for_hugepage_calculation():
    assert storage_node_ops.generate_automated_deployment_config(
        CAP + 1, 0, [0], 1, [], [], calculate_hp_only=True, number_of_devices=1) is False


def test_restart_storage_node_rejects_above_cap_before_touching_the_db():
    with patch.object(storage_node_ops, "DBController") as db:
        assert storage_node_ops.restart_storage_node("node-1", max_lvol=CAP + 1) is False
    db.assert_not_called()


# --- k8s node-configure entrypoint ----------------------------------------

def test_node_configure_rejects_above_cap():
    args = argparse.Namespace(upgrade=False, max_lvol=str(CAP + 1), max_prov="0")
    with pytest.raises(argparse.ArgumentError, match=str(CAP)):
        node_configure.validate_arguments(args)


def test_node_configure_accepts_the_cap():
    args = argparse.Namespace(upgrade=False, max_lvol=str(CAP), max_prov="0",
                              pci_allowed="", pci_blocked="")
    node_configure.validate_arguments(args)  # no raise


# --- node-local config write ----------------------------------------------

def test_persist_node_config_rejects_above_cap():
    with pytest.raises(ValidationError):
        PersistNodeConfigParams(max_lvol=CAP + 1)


def test_persist_node_config_accepts_the_cap():
    assert PersistNodeConfigParams(max_lvol=CAP).max_lvol == CAP
