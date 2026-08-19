"""CLI surface for replication targets, policies and volume assignment.

cli.py is generated from cli-reference.yaml, so these tests check that the
commands parse, that every generated dispatch has a handler, and that the
handlers forward to the controller instead of reimplementing anything.
"""
import inspect

import pytest

from simplyblock_cli.cli import CLIWrapper


@pytest.fixture
def cli():
    return CLIWrapper()


def _parse(cli, argv):
    return cli.parser.parse_args(argv)


CLUSTER_COMMANDS = [
    "replication-target-add",
    "replication-target-list",
    "replication-target-remove",
    "replication-target-failover",
    "replication-policy-add",
    "replication-policy-list",
    "replication-policy-remove",
    "replication-policy-failover",
]

VOLUME_COMMANDS = [
    "replication-policy-set",
    "replication-policy-clear",
    "replication-relationship",
]


@pytest.mark.parametrize("command", CLUSTER_COMMANDS)
def test_cluster_command_has_a_handler(cli, command):
    handler = "cluster__" + command.replace("-", "_")
    assert hasattr(cli, handler), f"{command} is generated but {handler} is missing"


@pytest.mark.parametrize("command", VOLUME_COMMANDS)
def test_volume_command_has_a_handler(cli, command):
    handler = "volume__" + command.replace("-", "_")
    assert hasattr(cli, handler), f"{command} is generated but {handler} is missing"


def test_target_add_parses_positionals_and_options(cli):
    args = _parse(cli, ["cluster", "replication-target-add", "CL_SRC", "site-a", "CL_TGT",
                        "--target-pool", "pool_t", "--timeout", "900"])
    assert (args.cluster_id, args.name, args.target_cluster_id) == ("CL_SRC", "site-a", "CL_TGT")
    assert (args.target_pool, args.timeout) == ("pool_t", 900)


def test_policy_add_requires_a_target(cli):
    with pytest.raises(SystemExit):
        _parse(cli, ["cluster", "replication-policy-add", "CL_SRC", "fast"])


def test_policy_add_parses_cadence_mode_and_retention(cli):
    args = _parse(cli, ["cluster", "replication-policy-add", "CL_SRC", "fast",
                        "--target", "site-a", "--interval-min", "5",
                        "--mode", "migration", "--keep", "3"])
    assert (args.name, args.target, args.interval_min) == ("fast", "site-a", 5)
    assert (args.mode, args.keep_replicated) == ("migration", 3)


def test_policy_add_rejects_an_unknown_mode(cli):
    with pytest.raises(SystemExit):
        _parse(cli, ["cluster", "replication-policy-add", "CL_SRC", "bad",
                     "--target", "site-a", "--mode", "sideways"])


def test_volume_policy_set_and_clear_parse(cli):
    set_args = _parse(cli, ["volume", "replication-policy-set", "LV1", "fast"])
    assert (set_args.volume_id, set_args.policy) == ("LV1", "fast")
    clear_args = _parse(cli, ["volume", "replication-policy-clear", "LV1"])
    assert clear_args.volume_id == "LV1"


def test_volume_add_accepts_a_replication_policy(cli):
    """Step 3 of the hierarchy: a policy can be assigned when the volume is created."""
    args = _parse(cli, ["volume", "add", "vol1", "10G", "pool1",
                        "--replication-policy", "fast"])
    assert args.replication_policy == "fast"


def test_read_commands_support_json(cli):
    for argv in (["cluster", "replication-target-list", "--json"],
                 ["cluster", "replication-policy-list", "--json"],
                 ["volume", "replication-relationship", "LV1", "--json"],
                 ["cluster", "replication-target-failover", "T1", "--json"]):
        assert _parse(cli, argv).json is True


def test_handlers_delegate_to_the_controller(cli):
    """The CLI must stay a thin shell over the controller."""
    for name in ([f"cluster__{c.replace('-', '_')}" for c in CLUSTER_COMMANDS]
                 + [f"volume__{c.replace('-', '_')}" for c in VOLUME_COMMANDS]):
        src = inspect.getsource(getattr(cli, name))
        assert "replication_policy_controller." in src, f"{name} bypasses the controller"


def test_failover_output_lists_every_volume(cli):
    """A partial group fail-over must not look like a success."""
    class _Args:
        json = False

    rows = cli._format_failover_results([
        {"lvol_id": "LV1", "status": "failed_over", "target_lvol_id": "T_LV1"},
        {"lvol_id": "LV2", "status": "failed", "detail": "node offline"},
    ], _Args())
    assert "LV1" in rows and "LV2" in rows
    assert "failed_over" in rows and "node offline" in rows


def test_failover_output_is_explicit_when_nothing_matched(cli):
    class _Args:
        json = False

    assert cli._format_failover_results([], _Args()) == "No volumes to fail over"
