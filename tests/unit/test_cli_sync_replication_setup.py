"""The CLI forwards --sync-replication (cluster create / add) and --site (sn add-node).

Driven through the generated ``CLIWrapper.run()`` so the dispatch, including the
defaults it fills in for developer-only arguments, is the real one.
"""
import sys
from unittest.mock import MagicMock, patch

from simplyblock_cli import cli as cli_module
from simplyblock_cli import clibase

CLUSTER_ID = "2c1e0a3b-77b2-4a5e-9d0e-6f3b8c2a1d55"


def _run(argv, module_attr, return_value="ok"):
    ops = MagicMock()
    for name in ("create_cluster", "add_cluster", "add_node"):
        getattr(ops, name).return_value = return_value
    with patch.object(sys, "argv", ["sbctl", *argv]), \
            patch.object(clibase, module_attr, ops), \
            patch("builtins.print"):
        cli_module.CLIWrapper().run()
    return ops


def _cluster_call(subcommand, argv):
    ops = _run(["cluster", subcommand, *argv], "cluster_ops")
    target = ops.create_cluster if subcommand == "create" else ops.add_cluster
    target.assert_called_once()
    return target.call_args


def _add_node_call(argv):
    ops = _run(["sn", "add-node", CLUSTER_ID, "10.0.0.10:5000", "eth0", *argv], "storage_ops")
    ops.add_node.assert_called_once()
    return ops.add_node.call_args


def test_cluster_create_forwards_sync_replication():
    call = _cluster_call("create", ["--ha-type", "ha", "--sync-replication"])
    assert call.kwargs["sync_replication"] is True


def test_cluster_create_defaults_to_no_sync_replication():
    call = _cluster_call("create", ["--ha-type", "ha"])
    assert call.kwargs["sync_replication"] is False


def test_cluster_add_forwards_sync_replication():
    call = _cluster_call("add", ["--ha-type", "ha", "--sync-replication"])
    assert call.kwargs["sync_replication"] is True


def test_cluster_add_defaults_to_no_sync_replication():
    call = _cluster_call("add", ["--ha-type", "ha"])
    assert call.kwargs["sync_replication"] is False


def test_add_node_forwards_site():
    call = _add_node_call(["--site", "site-a"])
    assert call.kwargs["site"] == "site-a"


def test_add_node_without_site_passes_none():
    call = _add_node_call([])
    assert call.kwargs["site"] is None
