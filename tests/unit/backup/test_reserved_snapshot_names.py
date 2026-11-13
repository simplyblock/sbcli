"""`is_reserved_snapshot_name`: pure-logic predicate every entry point that
takes a user-supplied snapshot name (CLI, v1, v2) consults before creating
one, so a user can never produce a name the backup subsystem's own internal
callers (`backup_lvol`, `backup.policy._auto_backup_lvol`) reserve for
themselves.
"""
import pytest

from simplyblock_core.controllers.backup import controller as backup_controller


class TestIsReservedSnapshotName:

    @pytest.mark.parametrize("name", [
        "_backup_myvol_123",
        "auto_myvol_123",
        "_backup_",
        "auto_",
    ])
    def test_reserved_prefixes_are_rejected(self, name):
        assert backup_controller.is_reserved_snapshot_name(name)

    @pytest.mark.parametrize("name", [
        "my-snapshot",
        "backup_myvol",  # no leading underscore -- not the reserved prefix
        "automatic-snap",  # starts with "auto" but not "auto_"
        "",
    ])
    def test_ordinary_names_are_not_rejected(self, name):
        assert not backup_controller.is_reserved_snapshot_name(name)

    def test_the_prefixes_backup_lvol_and_the_scheduler_use_are_both_reserved(self):
        """Regression anchor: if either prefix constant is ever renamed, this
        fails instead of the two subsystems silently drifting apart."""
        assert backup_controller.is_reserved_snapshot_name(
            backup_controller.DIRECT_BACKUP_SNAPSHOT_PREFIX + "x")
        assert backup_controller.is_reserved_snapshot_name(
            backup_controller.SCHEDULED_BACKUP_SNAPSHOT_PREFIX + "x")
