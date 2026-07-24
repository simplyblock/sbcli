# coding=utf-8
"""
test_hublvol_role_assignment.py — regression tests for the duplicate-LVS-role
bug (mass_create_delete_k8s 2026-07-14).

Each LVS must hold a UNIQUE role per node (primary / secondary / tertiary).
The observed violation: the tertiary holder of an LVS was stamped
``role="secondary"`` via ``bdev_lvol_set_lvs_opts`` (e.g. LVS_11 on worker-1
at 12:02:37, corrected to tertiary two seconds later by a topology-correct
call). Two producers:

  1. ``connect_to_hublvol`` had ``role="secondary"`` as its signature
     default, and the activation-mode branch of
     ``recreate_lvstore_on_non_leader`` called it without a role — so every
     activation-mode tertiary recreate stamped "secondary".
  2. ``create_lvstore`` / cluster activation Pass 3 derived the role from
     LIST POSITION over ``[secondary_node_id?, tertiary_node_id?]`` — with
     ``secondary_node_id`` unset (demoted after failover) the tertiary sits
     at index 0 and an index rule marks it "secondary".

The fix makes ``role`` a required keyword-only argument (no silent default)
and derives role/failover from topology back-refs at every call site.
"""

import unittest
from unittest.mock import MagicMock

from simplyblock_core.models.storage_node import StorageNode


class TestConnectToHublvolRoleRequired(unittest.TestCase):

    def test_role_is_required_keyword(self):
        """Calling connect_to_hublvol without role must fail loudly at the
        call site (TypeError), never silently default to 'secondary'."""
        node = StorageNode()
        primary = MagicMock()
        with self.assertRaises(TypeError):
            node.connect_to_hublvol(primary, failover_node=None)

    def test_role_cannot_be_passed_positionally(self):
        """role is keyword-only: a stray third positional argument (the old
        failover_node/role positional order) must not bind to role."""
        node = StorageNode()
        primary = MagicMock()
        with self.assertRaises(TypeError):
            node.connect_to_hublvol(primary, None, "secondary")


if __name__ == "__main__":
    unittest.main()
