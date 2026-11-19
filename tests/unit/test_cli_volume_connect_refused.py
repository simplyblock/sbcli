"""sbctl volume connect while a consistency-group subsystem move holds the
volume's subsystem: the refusal is printed as the command's error, the way a
missing volume is, not as a traceback."""
from types import SimpleNamespace
from unittest import mock

from simplyblock_cli import clibase
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.exceptions import SubsystemMoveInProgressError


def test_a_connect_refused_by_a_move_is_the_commands_error():
    cli = clibase.CLIWrapperBase.__new__(clibase.CLIWrapperBase)
    args = SimpleNamespace(volume_id="LV1", ctrl_loss_tmo=None, host_nqn=None, site=None)

    def refuse(uid, **kw):
        raise SubsystemMoveInProgressError("volume LV1 is in subsystem S, which a move holds")
    with mock.patch.object(lvol_controller, "connect_lvol", refuse):
        out = cli.volume__connect("connect", args)
    assert "move holds" in out
