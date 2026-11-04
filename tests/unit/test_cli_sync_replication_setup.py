"""The CLI forwards --sync-replication (cluster add) and --site (sn add-node).

Driven through the generated ``CLIWrapper.run()`` so the dispatch, including the
defaults it fills in for developer-only arguments, is the real one.
"""
import sys
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_cli import cli as cli_module
from simplyblock_cli import clibase

CLUSTER_ID = "2c1e0a3b-77b2-4a5e-9d0e-6f3b8c2a1d55"


def _run(argv, module_attr, return_value="ok"):
    ops = MagicMock()
    for name in ("add_cluster", "add_node"):
        getattr(ops, name).return_value = return_value
    with patch.object(sys, "argv", ["sbctl", *argv]), \
            patch.object(clibase, module_attr, ops), \
            patch("builtins.print"):
        cli_module.CLIWrapper().run()
    return ops


def _cluster_call(argv):
    ops = _run(["cluster", "add", *argv], "cluster_ops")
    ops.add_cluster.assert_called_once()
    return ops.add_cluster.call_args


def _add_node_call(argv):
    ops = _run(["sn", "add-node", CLUSTER_ID, "10.0.0.10:5000", "eth0", *argv], "storage_ops")
    ops.add_node.assert_called_once()
    return ops.add_node.call_args


@pytest.mark.parametrize("argv, expected", [
    pytest.param(["--ha-type", "ha", "--sync-replication"], True,
                 id="cluster_add_forwards_sync_replication"),
    pytest.param(["--ha-type", "ha"], False, id="cluster_add_defaults_to_no_sync_replication"),
])
def test_cluster_sync_replication_flag(argv, expected):
    call = _cluster_call(argv)
    assert call.kwargs["sync_replication"] is expected


@pytest.mark.parametrize("argv, expected", [
    pytest.param(["--site", "site-a"], "site-a", id="add_node_forwards_site"),
    pytest.param([], None, id="add_node_without_site_passes_none"),
])
def test_add_node_site(argv, expected):
    call = _add_node_call(argv)
    assert call.kwargs["site"] == expected
