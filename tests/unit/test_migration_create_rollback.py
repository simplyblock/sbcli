"""SFAM-2834 follow-up: create_migration writes its LVolMigration record only
at the very end, so a failure after target objects were created left them
behind with nothing for cancel to clean up. The call now takes back exactly
what it created -- and nothing it did not (a batch group's members share one
subsystem nqn)."""

from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.controllers import migration_controller as mc
from simplyblock_core.rpc_client import RPCConnectionError


def test_failure_rolls_back_what_this_call_created():
    tgt_rpc, sec_rpc, node_rpc = MagicMock(), MagicMock(), MagicMock()
    tgt_node, sec_node = MagicMock(), MagicMock()

    def impl(*args):
        undo = args[-1]
        undo.bdev = ("LVS_1/LVOL_1m", tgt_rpc, tgt_node, "LVS_1")
        undo.replicas["secondary"] = (sec_rpc, sec_node)
        undo.crypto_bdevs.append((node_rpc, "crypto_LVOL_1m", "node-b"))
        undo.subsystems.append((node_rpc, "nqn:lvol:1", "node-b"))
        raise RPCConnectionError("Could not reach remote")

    with patch.object(mc, "_create_migration_impl", side_effect=impl), \
            patch.object(mc, "_delete_bdev_blocking") as delete_bdev:
        with pytest.raises(RPCConnectionError):
            mc.create_migration("lvol-1", "node-a")

    node_rpc.subsystem_delete.assert_called_once_with("nqn:lvol:1")
    node_rpc.lvol_crypto_delete.assert_called_once_with("crypto_LVOL_1m")
    delete_bdev.assert_called_once()
    assert delete_bdev.call_args.args == ("LVS_1/LVOL_1m", tgt_rpc)
    assert delete_bdev.call_args.kwargs["secondary_rpc"] is sec_rpc
    assert delete_bdev.call_args.kwargs["tertiary_rpc"] is None


def test_nothing_created_means_nothing_deleted():
    with patch.object(mc, "_create_migration_impl",
                      side_effect=ValueError("preconditions")), \
            patch.object(mc, "_delete_bdev_blocking") as delete_bdev:
        with pytest.raises(ValueError):
            mc.create_migration("lvol-1", "node-a")
    delete_bdev.assert_not_called()


def test_rollback_steps_are_best_effort():
    # One failing teardown step must not stop the next, nor mask the error.
    rpc = MagicMock()
    rpc.subsystem_delete.side_effect = RPCConnectionError("down")

    def impl(*args):
        undo = args[-1]
        undo.subsystems.append((rpc, "nqn:lvol:1", "node-b"))
        undo.bdev = ("LVS_1/LVOL_1m", rpc, MagicMock(), "LVS_1")
        raise ValueError("crypto setup failed")

    with patch.object(mc, "_create_migration_impl", side_effect=impl), \
            patch.object(mc, "_delete_bdev_blocking") as delete_bdev:
        with pytest.raises(ValueError, match="crypto setup failed"):
            mc.create_migration("lvol-1", "node-a")
    delete_bdev.assert_called_once()


def test_success_rolls_nothing_back():
    with patch.object(mc, "_create_migration_impl", return_value=("mig-1", [])), \
            patch.object(mc._CreateUndo, "run") as run:
        assert mc.create_migration("lvol-1", "node-a") == ("mig-1", [])
    run.assert_not_called()
