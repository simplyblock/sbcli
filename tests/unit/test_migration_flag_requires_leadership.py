"""bdev_lvol_set_migration_flag must never be issued against a non-leader lvstore.

The flag is not a passive metadata bit -- it drives the distrib-level special_io
machinery for the target bdev. Issued against an lvstore the node does not
currently lead, the resulting IO fails, which demotes the lvstore and fences its
client ports. A healthy node goes down, and the control plane reports the last
symptom: "target node offline".

Observed live 2026-09-16 removing node 0d51a544. The flag was set twice on the
same bdev, LVS_7/SNAP_25m:

    18:51:51  set_migration_flag for 'LVS_7/SNAP_25m'   <- create path, guarded
    18:51:59  set_migration_flag for 'LVS_7/SNAP_25m'   <- reuse path, unguarded
    18:51:59  Queued failed IO -> leadership false, groupid 7   (109 ms later)
    18:51:59  block_port 4436 / 4437

_ensure_lvstore_primary_leader already existed and already documented this exact
corruption class, but it was wired to bdev_lvol_create rather than to the flag.
Two consequences, both covered below:

  * the create branch is skipped whenever the bdev is reused on a retry, so the
    check went with it -- the second call above took that path;
  * even on a first pass, create and flag are separate RPCs with secondary and
    tertiary registration in between, so leadership can move after the check
    cleared.

The guard therefore belongs on the flag itself, which is what
set_migration_flag_on_primary enforces for every caller.
"""
import unittest
from unittest.mock import MagicMock

from simplyblock_core.controllers import migration_controller


def _rpc(is_primary=True, is_leader=True, flag_ok=True):
    rpc = MagicMock()
    rpc.bdev_lvol_get_lvstores.return_value = [
        {"lvs_primary": is_primary, "lvs leadership": is_leader}
    ]
    rpc.bdev_lvol_set_migration_flag.return_value = flag_ok
    return rpc


class TestFlagRefusedWhenNotLeader(unittest.TestCase):

    def test_leader_primary_sets_the_flag(self):
        rpc = _rpc()
        ok, err = migration_controller.set_migration_flag_on_primary(
            rpc, "LVS_7", "LVS_7/SNAP_25m", "node-a")
        self.assertTrue(ok)
        self.assertEqual(err, "")
        rpc.bdev_lvol_set_migration_flag.assert_called_once_with("LVS_7/SNAP_25m")

    def test_not_leader_refuses_without_issuing_the_rpc(self):
        """The demotion in the incident above came from the RPC reaching SPDK.
        Refusing after sending it would be no fix at all."""
        rpc = _rpc(is_leader=False)
        ok, err = migration_controller.set_migration_flag_on_primary(
            rpc, "LVS_7", "LVS_7/SNAP_25m", "node-a")
        self.assertFalse(ok)
        self.assertIn("LVS_7/SNAP_25m", err)
        rpc.bdev_lvol_set_migration_flag.assert_not_called()

    def test_not_primary_refuses_without_issuing_the_rpc(self):
        rpc = _rpc(is_primary=False)
        ok, err = migration_controller.set_migration_flag_on_primary(
            rpc, "LVS_7", "LVS_7/SNAP_25m", "node-a")
        self.assertFalse(ok)
        rpc.bdev_lvol_set_migration_flag.assert_not_called()

    def test_unreadable_lvstore_refuses(self):
        """Leadership that cannot be established is not leadership."""
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.side_effect = Exception("connection refused")
        ok, _ = migration_controller.set_migration_flag_on_primary(
            rpc, "LVS_7", "LVS_7/SNAP_25m", "node-a")
        self.assertFalse(ok)
        rpc.bdev_lvol_set_migration_flag.assert_not_called()

    def test_missing_lvstore_refuses(self):
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = []
        ok, _ = migration_controller.set_migration_flag_on_primary(
            rpc, "LVS_7", "LVS_7/SNAP_25m", "node-a")
        self.assertFalse(ok)
        rpc.bdev_lvol_set_migration_flag.assert_not_called()


class TestFlagFailureLeniency(unittest.TestCase):
    """A False return from the RPC means "maybe already flagged" and is benign;
    a leadership failure never is. The two must not be conflated."""

    def test_flag_failure_is_fatal_by_default(self):
        rpc = _rpc(flag_ok=False)
        ok, err = migration_controller.set_migration_flag_on_primary(
            rpc, "LVS_7", "LVS_7/SNAP_25m", "node-a")
        self.assertFalse(ok)
        self.assertIn("failed", err)

    def test_flag_failure_tolerated_when_asked(self):
        rpc = _rpc(flag_ok=False)
        ok, err = migration_controller.set_migration_flag_on_primary(
            rpc, "LVS_7", "LVS_7/SNAP_25m", "node-a", tolerate_flag_failure=True)
        self.assertTrue(ok)
        self.assertEqual(err, "")

    def test_tolerance_does_not_soften_the_leadership_check(self):
        rpc = _rpc(is_leader=False, flag_ok=True)
        ok, _ = migration_controller.set_migration_flag_on_primary(
            rpc, "LVS_7", "LVS_7/SNAP_25m", "node-a", tolerate_flag_failure=True)
        self.assertFalse(ok)
        rpc.bdev_lvol_set_migration_flag.assert_not_called()


class TestReusePathIsGuarded(unittest.TestCase):
    """The regression proper: the second call in the incident reached SPDK
    because the bdev already existed, so the create branch -- and its leadership
    check -- was skipped. Re-flagging an existing bdev must be guarded too."""

    def test_second_flag_on_existing_bdev_still_checks_leadership(self):
        rpc = _rpc()
        ok, _ = migration_controller.set_migration_flag_on_primary(
            rpc, "LVS_7", "LVS_7/SNAP_25m", "node-a")
        self.assertTrue(ok)

        # Leadership moves away between the two calls, exactly as it did live.
        rpc.bdev_lvol_get_lvstores.return_value = [
            {"lvs_primary": True, "lvs leadership": False}
        ]
        rpc.bdev_lvol_set_migration_flag.reset_mock()

        ok, err = migration_controller.set_migration_flag_on_primary(
            rpc, "LVS_7", "LVS_7/SNAP_25m", "node-a")
        self.assertFalse(ok, "re-flag on an existing bdev bypassed the guard")
        self.assertIn("not primary/leader", err)
        rpc.bdev_lvol_set_migration_flag.assert_not_called()


if __name__ == "__main__":
    unittest.main()
