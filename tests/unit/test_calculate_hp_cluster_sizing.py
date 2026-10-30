"""`sn configure --calculate-hp-only` sized for a stated cluster.

Without --max-subsys/--vcpu-count the calculator sizes for the product
default and the host's own core layout; with them, for the cluster's values.
"""
from unittest.mock import patch

import pytest

from simplyblock_cli import cli as cli_module
from simplyblock_cli import clibase
from simplyblock_core import constants


def _wrapper():
    cli_module.CLIWrapper.__init__(w := cli_module.CLIWrapper.__new__(cli_module.CLIWrapper))
    return w


def _calculate(*extra):
    w = _wrapper()
    args = w.parser.parse_args(["storage-node", "configure", "--calculate-hp-only",
                                "--number-of-devices", "2", *extra])
    with patch.object(clibase.storage_ops, "generate_automated_deployment_config",
                      return_value=True) as gen:
        w.storage_node__configure("configure", args)
    return gen.call_args


def test_the_calculator_takes_the_cluster_sizing():
    call = _calculate("--max-subsys", "30", "--vcpu-count", "4")
    assert call.args[0] == 30
    assert call.kwargs["vcpu_count"] == 4


def test_the_calculator_defaults_to_the_product_default():
    call = _calculate()
    assert call.args[0] == constants.DEFAULT_SUBSYSTEMS_PER_NODE
    assert call.kwargs["vcpu_count"] == 0


def test_the_sizing_flags_need_calculate_hp_only():
    w = _wrapper()
    args = w.parser.parse_args(["storage-node", "configure", "--max-subsys", "30"])
    with pytest.raises(SystemExit):
        w.storage_node__configure("configure", args)
