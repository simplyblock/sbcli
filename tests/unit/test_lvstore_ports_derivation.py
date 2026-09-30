# coding=utf-8
"""The lvstore_ports map must be complete BEFORE lvols are registered.

Registering an lvol on a non-leader looks its ``lvs_name`` up in
``snode.lvstore_ports`` and refuses to add a listener when the entry is
missing, rather than guessing a port. So a map that is merely eventually
correct is not good enough: the replica serves nothing until something else
repairs it.

Deriving the map from snode's own lvstore plus its two back-reference slots
missed exactly the case a node-removal relocation creates -- the slot that
records the new hosting relationship is committed only AFTER the build
returns. Every relocation of the 2026-09-11 no-FD 2+2 run logged
"INCOMPLETE LVOL REGISTRATION ... running below configured redundancy until
repaired" and stayed that way for ~2 minutes until lvol_monitor healed it.
"""
import unittest
from unittest.mock import MagicMock

from simplyblock_core import storage_node_ops


def _node(node_id, lvstore, subsys_port, hublvol_port=0,
          secondary=None, tertiary=None):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.lvstore = lvstore
    n.lvol_subsys_port = subsys_port
    n.hublvol = MagicMock(nvmf_port=hublvol_port) if hublvol_port else None
    n.lvstore_stack_secondary = secondary
    n.lvstore_stack_tertiary = tertiary
    return n


class TestDeriveLvstorePorts(unittest.TestCase):

    def setUp(self):
        self.primary = _node("prim", "LVS_16", 4442, hublvol_port=4443)
        self.other = _node("other", "LVS_10", 4438, hublvol_port=4439)
        self.db = MagicMock()
        self.db.get_storage_node_by_id.side_effect = lambda i: {
            "prim": self.primary, "other": self.other}[i]

    def test_includes_the_stack_being_built_even_with_no_backref_yet(self):
        """The regression. A relocation calls this while the host's
        secondary/tertiary slot for the incoming primary is still empty."""
        host = _node("host", "LVS_4", 4434, hublvol_port=4435)
        ports = storage_node_ops._derive_lvstore_ports(host, self.primary, self.db)
        self.assertIn(
            "LVS_16", ports,
            "the lvstore this call is building must be in the map before its "
            "lvols are registered, or every listener is refused")
        self.assertEqual(ports["LVS_16"],
                         {"lvol_subsys_port": 4442, "hublvol_port": 4443})

    def test_keeps_the_hosts_own_lvstore(self):
        host = _node("host", "LVS_4", 4434, hublvol_port=4435)
        ports = storage_node_ops._derive_lvstore_ports(host, self.primary, self.db)
        self.assertEqual(ports["LVS_4"],
                         {"lvol_subsys_port": 4434, "hublvol_port": 4435})

    def test_keeps_lvstores_from_both_backref_slots(self):
        host = _node("host", "LVS_4", 4434, hublvol_port=4435,
                     secondary="other", tertiary="prim")
        ports = storage_node_ops._derive_lvstore_ports(host, self.primary, self.db)
        self.assertEqual(sorted(ports), ["LVS_10", "LVS_16", "LVS_4"])

    def test_primary_already_in_a_slot_is_not_duplicated_or_changed(self):
        """Adding primary_node last must be idempotent, not an override."""
        host = _node("host", "LVS_4", 4434, hublvol_port=4435, secondary="prim")
        ports = storage_node_ops._derive_lvstore_ports(host, self.primary, self.db)
        self.assertEqual(sorted(ports), ["LVS_16", "LVS_4"])
        self.assertEqual(ports["LVS_16"],
                         {"lvol_subsys_port": 4442, "hublvol_port": 4443})

    def test_a_node_without_a_hublvol_reports_port_zero(self):
        primary = _node("prim", "LVS_16", 4442)
        host = _node("host", "LVS_4", 4434)
        ports = storage_node_ops._derive_lvstore_ports(host, primary, self.db)
        self.assertEqual(ports["LVS_16"],
                         {"lvol_subsys_port": 4442, "hublvol_port": 0})

    def test_no_primary_node_is_tolerated(self):
        host = _node("host", "LVS_4", 4434, hublvol_port=4435)
        ports = storage_node_ops._derive_lvstore_ports(host, None, self.db)
        self.assertEqual(sorted(ports), ["LVS_4"])

    def test_a_primary_with_no_lvstore_adds_nothing(self):
        primary = _node("prim", "", 0)
        host = _node("host", "LVS_4", 4434, hublvol_port=4435)
        ports = storage_node_ops._derive_lvstore_ports(host, primary, self.db)
        self.assertEqual(sorted(ports), ["LVS_4"])

    def test_a_host_with_no_own_lvstore_still_gets_the_built_stack(self):
        host = _node("host", "", 0)
        ports = storage_node_ops._derive_lvstore_ports(host, self.primary, self.db)
        self.assertEqual(sorted(ports), ["LVS_16"])


if __name__ == "__main__":
    unittest.main()
