"""
test_namespace_matches.py — pins namespace identity matching.

SPDK reports a namespace's ``bdev_name`` as whichever name the bdev was
registered under. For an lvol that is its **raw UUID**, not the
``<lvs>/<lvol>`` alias carried in ``lvol.top_bdev``. Comparing bdev_name
alone therefore reports "no namespace" for a namespace that is present.

Soak 2026-08-11, first outage pair (aa60b24a shutdown + 0baf99e4
network_outage, overlapping): on BOTH recovered nodes every lvol subsystem
ended up with its namespace attached and zero listeners, because
``add_lvol_thread``'s post-condition — the guard added for the 2026-08-09
listener-without-namespace incident — read the namespace as absent and
skipped listener creation. Four of six volumes silently lost a path, the
control plane reported every lvol ``ha``, and fio never errored because the
primary path still served IO. ``lvol_monitor`` detected it and re-refused the
repair on every cycle, so the state was permanent.

The observed shapes are used verbatim below.
"""

import unittest
from unittest.mock import MagicMock

from simplyblock_core.rpc_client import namespace_matches
from simplyblock_core.storage_node_ops import _rpc_subsystem_has_ns

# Exactly as reported by nvmf_get_subsystems on 172.31.98.86 at abort time.
OBSERVED_NS = {
    "nsid": 1,
    "bdev_name": "e5369a9f-6509-41a8-abb4-892439911b2e",
    "uuid": "ac386b9e-a69c-4d4f-9fda-e17547d5b26a",
}
FRIENDLY = "LVS_14/LVOL_22"          # lvol.top_bdev
LVOL_UUID = "ac386b9e-a69c-4d4f-9fda-e17547d5b26a"


class TestNamespaceMatches(unittest.TestCase):

    def test_regression_uuid_registered_bdev_with_friendly_alias(self):
        """The bug: friendly alias != reported bdev_name, so the old
        bdev_name-only comparison said 'absent' for a present namespace."""
        self.assertNotEqual(OBSERVED_NS["bdev_name"], FRIENDLY)
        self.assertTrue(
            namespace_matches(OBSERVED_NS, dev_name=FRIENDLY, nsid=1,
                              uuid=LVOL_UUID))

    def test_uuid_comparison_is_case_insensitive(self):
        self.assertTrue(
            namespace_matches(OBSERVED_NS, dev_name=FRIENDLY, nsid=1,
                              uuid=LVOL_UUID.upper()))

    def test_nsid_mismatch_never_matches(self):
        self.assertFalse(
            namespace_matches(OBSERVED_NS, dev_name=FRIENDLY, nsid=2,
                              uuid=LVOL_UUID))

    def test_conflicting_uuid_disqualifies_a_bdev_name_match(self):
        """Same bdev name carrying a different volume is a real conflict —
        matching on the name alone would accept the wrong namespace."""
        self.assertFalse(
            namespace_matches(OBSERVED_NS,
                              dev_name=OBSERVED_NS["bdev_name"], nsid=1,
                              uuid="deadbeef-0000-0000-0000-000000000000"))

    def test_bdev_name_still_matches_when_ns_has_no_uuid(self):
        """Older/other namespaces report no uuid; fall back to the name."""
        ns = {"nsid": 1, "bdev_name": FRIENDLY}
        self.assertTrue(
            namespace_matches(ns, dev_name=FRIENDLY, nsid=1, uuid=LVOL_UUID))

    def test_bdev_name_mismatch_without_uuid_does_not_match(self):
        ns = {"nsid": 1, "bdev_name": "LVS_9/LVOL_1"}
        self.assertFalse(
            namespace_matches(ns, dev_name=FRIENDLY, nsid=1, uuid=None))

    def test_nsid_only_query_matches_any_namespace_at_that_nsid(self):
        self.assertTrue(namespace_matches(OBSERVED_NS, nsid=1))
        self.assertFalse(namespace_matches(OBSERVED_NS, nsid=7))


class TestRpcSubsystemHasNs(unittest.TestCase):
    """The guard's own entry point, over a subsystem_get payload."""

    def _rpc(self, namespaces):
        rpc = MagicMock()
        rpc.subsystem_get.return_value = {"namespaces": namespaces}
        return rpc

    def test_finds_namespace_registered_under_its_uuid(self):
        rpc = self._rpc([OBSERVED_NS])
        self.assertTrue(_rpc_subsystem_has_ns(
            rpc, "nqn:lvol:ac386b9e", nsid=1, bdev_name=FRIENDLY,
            uuid=LVOL_UUID))

    def test_absent_namespace_is_still_absent(self):
        rpc = self._rpc([])
        self.assertFalse(_rpc_subsystem_has_ns(
            rpc, "nqn:lvol:ac386b9e", nsid=1, bdev_name=FRIENDLY,
            uuid=LVOL_UUID))

    def test_other_volumes_namespace_is_not_a_match(self):
        other = {"nsid": 1, "bdev_name": "x", "uuid": "11111111-2222-3333-4444-555555555555"}
        rpc = self._rpc([other])
        self.assertFalse(_rpc_subsystem_has_ns(
            rpc, "nqn:lvol:ac386b9e", nsid=1, bdev_name=FRIENDLY,
            uuid=LVOL_UUID))

    def test_missing_subsystem_is_not_a_match(self):
        rpc = MagicMock()
        rpc.subsystem_get.return_value = None
        self.assertFalse(_rpc_subsystem_has_ns(
            rpc, "nqn:lvol:ac386b9e", nsid=1, uuid=LVOL_UUID))

    def test_rpc_failure_is_not_a_match(self):
        rpc = MagicMock()
        rpc.subsystem_get.side_effect = RuntimeError("rpc down")
        self.assertFalse(_rpc_subsystem_has_ns(
            rpc, "nqn:lvol:ac386b9e", nsid=1, uuid=LVOL_UUID))


class TestWaitForNamespace(unittest.TestCase):
    """add_ns can report success just before the namespace is observable, so
    the post-condition polls rather than concluding 'empty subsystem' once."""

    def test_polls_until_namespace_surfaces(self):
        from unittest.mock import patch
        from simplyblock_core import storage_node_ops

        rpc = MagicMock()
        rpc.subsystem_get.side_effect = [
            {"namespaces": []},
            {"namespaces": []},
            {"namespaces": [OBSERVED_NS]},
        ]
        with patch.object(storage_node_ops.time, "sleep"):
            self.assertTrue(storage_node_ops._rpc_wait_subsystem_has_ns(
                rpc, "nqn:lvol:ac386b9e", nsid=1, bdev_name=FRIENDLY,
                uuid=LVOL_UUID))
        self.assertEqual(rpc.subsystem_get.call_count, 3)

    def test_gives_up_after_bounded_tries(self):
        from unittest.mock import patch
        from simplyblock_core import storage_node_ops

        rpc = MagicMock()
        rpc.subsystem_get.return_value = {"namespaces": []}
        with patch.object(storage_node_ops.time, "sleep"):
            self.assertFalse(storage_node_ops._rpc_wait_subsystem_has_ns(
                rpc, "nqn:lvol:ac386b9e", nsid=1, bdev_name=FRIENDLY,
                uuid=LVOL_UUID, tries=4))
        self.assertEqual(rpc.subsystem_get.call_count, 4)


class TestAddNsFailureDoesNotCostAListener(unittest.TestCase):
    """The dominant failure path of soak 2026-08-11.

    lvol_monitor's repair calls add_ns for a namespace that is already bound;
    SPDK rejects it with -32602 "Invalid parameters" because nsid 1 is taken.
    add_lvol_thread used to return on that error — before the listener loop —
    so the volume lost a PATH rather than just a namespace, and the repair
    re-failed identically on every monitor cycle. What matters is whether the
    namespace is present now, not whether this particular add reported it.
    """

    def test_present_namespace_survives_a_failed_add(self):
        from unittest.mock import patch
        from simplyblock_core import storage_node_ops

        rpc = MagicMock()
        # add_ns rejects the duplicate ...
        rpc.nvmf_subsystem_add_ns.return_value = None
        # ... but the namespace is plainly there, reported under its raw UUID.
        rpc.subsystem_get.return_value = {"namespaces": [OBSERVED_NS]}

        with patch.object(storage_node_ops.time, "sleep"):
            present = storage_node_ops._rpc_wait_subsystem_has_ns(
                rpc, "nqn:lvol:ac386b9e", nsid=1, bdev_name=FRIENDLY,
                uuid=LVOL_UUID)
        self.assertTrue(
            present,
            "a failed add_ns must not be read as 'namespace absent' when the "
            "namespace is on the subsystem — that is what skipped the listener")

    def test_genuinely_absent_namespace_still_fails(self):
        """The guard must still fail closed: no namespace means no listener."""
        from unittest.mock import patch
        from simplyblock_core import storage_node_ops

        rpc = MagicMock()
        rpc.nvmf_subsystem_add_ns.return_value = None
        rpc.subsystem_get.return_value = {"namespaces": []}

        with patch.object(storage_node_ops.time, "sleep"):
            present = storage_node_ops._rpc_wait_subsystem_has_ns(
                rpc, "nqn:lvol:ac386b9e", nsid=1, bdev_name=FRIENDLY,
                uuid=LVOL_UUID, tries=3)
        self.assertFalse(present)


if __name__ == "__main__":
    unittest.main()
