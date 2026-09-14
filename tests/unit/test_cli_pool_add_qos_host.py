"""`sbctl pool add --qos-host <node>` forwards the node into qos_host.

The handler passed nine positional arguments to add_pool(), whose ninth
parameter is cr_name -- so the node id landed in Pool.cr_name, qos_host stayed
None, and add_pool auto-picked an arbitrary node instead of honouring the flag.
The mismatch also disabled the "--qos-host without any QoS parameter" guard,
which only fires when qos_host is truthy.
"""

from unittest.mock import MagicMock, patch

from simplyblock_cli import cli as cli_module
from simplyblock_cli import clibase


NODE_ID = "1b0dcd4f-9e21-4b8b-9c04-2a6d7a1c9f00"
CLUSTER_ID = "2c1e0a3b-77b2-4a5e-9d0e-6f3b8c2a1d55"


def _parse(argv):
    cli_module.CLIWrapper.__init__(wrapper := cli_module.CLIWrapper.__new__(cli_module.CLIWrapper))
    return wrapper.parser.parse_args(argv)


def _call_pool_add(argv):
    args = _parse(argv)
    base = clibase.CLIWrapperBase.__new__(clibase.CLIWrapperBase)
    with patch.object(clibase, "pool_controller", MagicMock()) as controller:
        base.storage_pool__add(args.command, args)
    return controller.add_pool.call_args


def test_qos_host_is_passed_as_qos_host():
    call = _call_pool_add([
        "pool", "add", "testpool", CLUSTER_ID,
        "--max-rw-iops", "1000", "--qos-host", NODE_ID,
    ])
    assert call.kwargs.get("qos_host") == NODE_ID


def test_qos_host_does_not_leak_into_cr_name():
    call = _call_pool_add([
        "pool", "add", "testpool", CLUSTER_ID,
        "--max-rw-iops", "1000", "--qos-host", NODE_ID,
    ])
    assert NODE_ID not in call.args[8:], "node id passed in a positional CR slot"
    assert call.kwargs.get("cr_name") is None


def test_omitted_qos_host_stays_none():
    call = _call_pool_add(["pool", "add", "testpool", CLUSTER_ID])
    assert call.kwargs.get("qos_host") is None
