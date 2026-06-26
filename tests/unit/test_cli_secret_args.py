"""PR 3 — argparse layer constructs SecretStr at parse time for secret args."""
import argparse

from pydantic import SecretStr

from simplyblock_cli import cli as cli_module


def _build_parser():
    """Build a CLI wrapper without instantiating its run loop."""
    cli_module.CLIWrapper.__init__(wrapper := cli_module.CLIWrapper.__new__(cli_module.CLIWrapper))
    return wrapper.parser


def test_update_secret_arg_is_secretstr_after_parse():
    parser = _build_parser()
    args = parser.parse_args(["cluster", "update-secret", "abc-cluster-id", "supersecret-value"])
    assert isinstance(args.secret, SecretStr)
    assert args.secret.get_secret_value() == "supersecret-value"


def test_control_plane_add_cluster_secret_is_secretstr():
    parser = _build_parser()
    args = parser.parse_args([
        "control-plane", "add",
        "1.2.3.4", "abc-cluster-id", "the-cluster-secret",
    ])
    assert isinstance(args.cluster_secret, SecretStr)
    assert args.cluster_secret.get_secret_value() == "the-cluster-secret"


def test_namespace_repr_masks_secret_value():
    args = argparse.Namespace(
        cluster_id="x",
        secret=SecretStr("supersecret-value"),
    )
    assert "supersecret-value" not in repr(vars(args))
    assert "**********" in repr(vars(args))
