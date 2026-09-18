"""`sbctl cluster index-state` — the operator entry point to the kill switch.

The switch itself (state record plus the clear) needs a real FoundationDB and
lives in ``tests/integration/test_database_indices.py``. What is checkable here
is the surface: that the command parses the way the kill switch is documented,
and that a name like ``LVol.node_id`` resolves to the declaration it claims.
"""
import pytest

from simplyblock_cli import cli as cli_module
from simplyblock_core import index_ops
from simplyblock_core.models.lvol_model import LVol


def _build_parser():
    cli_module.CLIWrapper.__init__(wrapper := cli_module.CLIWrapper.__new__(cli_module.CLIWrapper))
    return wrapper.parser


def test_index_is_optional_so_the_bare_command_lists():
    args = _build_parser().parse_args(["cluster", "index-state"])
    assert args.index is None
    assert args.state is None


def test_set_disabled_parses():
    args = _build_parser().parse_args(
        ["cluster", "index-state", "LVol.node_id", "--set", "disabled"])
    assert args.index == "LVol.node_id"
    assert args.state == "disabled"


def test_ready_is_not_settable():
    """`ready` is what completing a backfill means, not a state to declare."""
    with pytest.raises(SystemExit):
        _build_parser().parse_args(
            ["cluster", "index-state", "LVol.node_id", "--set", "ready"])


def test_resolve_index_returns_the_declaration():
    model_cls, index = index_ops.resolve_index("LVol.node_id")
    assert model_cls is LVol
    assert index.name == "node_id"


def test_resolve_index_rejects_an_unqualified_name():
    with pytest.raises(ValueError):
        index_ops.resolve_index("node_id")


def test_resolve_index_rejects_an_unknown_class():
    with pytest.raises(KeyError):
        index_ops.resolve_index("NotAModel.node_id")


def test_resolve_index_rejects_an_undeclared_index():
    with pytest.raises(KeyError):
        index_ops.resolve_index("LVol.not_an_index")
