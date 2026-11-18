"""A promote's connection strings when connect_lvol refuses a volume because
a consistency-group subsystem move holds its subsystem: the promote reports
the refusal as a SyncPromoteRefusedError naming the volume, the way it
reports any other failed connect, instead of letting the exception escape
mid-sequence."""
from unittest import mock

import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.exceptions import (
    SubsystemMoveInProgressError,
    SyncPromoteRefusedError,
)
from simplyblock_core.models.lvol_model import LVol


def test_a_connect_refused_by_a_move_is_a_promote_refusal():
    lv = LVol()
    lv.uuid = "LV1"

    def refuse(uid, **kw):
        raise SubsystemMoveInProgressError("volume LV1 is in subsystem S, which a move holds")
    with mock.patch.object(lvol_controller, "connect_lvol", refuse), \
            pytest.raises(SyncPromoteRefusedError, match="LV1.*move holds") as caught:
        src._connection_strings([lv], "site-a")
    assert caught.value.volumes == ["LV1"]
