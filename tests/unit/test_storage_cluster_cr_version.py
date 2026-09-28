"""
test_storage_cluster_cr_version.py — the CR the control plane reads is v1alpha2.

The operator's CRD redesign moved the storage-node knobs off the retired
``StorageNodeSet`` and onto ``StorageCluster``, and made ``v1alpha2`` the stored
version. The control plane kept asking for ``v1alpha1``.

A v1alpha1 request against a v1alpha2-stored object needs the conversion webhook,
and a fresh install deliberately does not run one — everything is written at the
storage version, so nothing needs converting. The two only collide when something
asks for the old version, and then every such read fails::

    conversion webhook for storage.simplyblock.io/v1alpha2, Kind=StorageCluster
    failed: ... service "simplyblock-operator-conversion-webhook-service" not found

That is not cosmetic. ``patch_cr_node_status`` returns False when its read fails,
and the caller turns that into ``Node add result: False`` — so every ``node_add``
failed, retried eleven times, and gave up, on a cluster whose operator and CRDs
were both correct.

``maxParallelNodeAdds`` moved twice over: a different kind, and a different path
within the spec. It seeds the storage MachineConfigPool's initial maxUnavailable
so the first-time CPU-topology reboots roll in one wave, so reading it wrongly
serializes every node reboot of a fresh OpenShift cluster.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants, utils


class TestCRVersion(unittest.TestCase):
    def test_the_control_plane_reads_the_stored_version(self):
        """v1alpha1 is what needs a conversion webhook nobody runs."""
        self.assertEqual(constants.CR_VERSION, "v1alpha2")


class TestMaxParallelNodeAdds(unittest.TestCase):
    """The knob is read from where v1alpha2 keeps it."""

    def _read(self, cr, plural_seen=None):
        api = MagicMock()
        api.get_namespaced_custom_object.return_value = cr
        with patch.object(utils, "load_kube_config_with_fallback"), \
                patch.object(utils.client, "CustomObjectsApi", return_value=api):
            value = utils.get_max_parallel_node_adds_from_cr("a-cluster", "simplyblock")
        if plural_seen is not None:
            plural_seen.update(api.get_namespaced_custom_object.call_args.kwargs)
        return value

    def test_it_is_read_from_the_storage_nodes_block(self):
        """v1alpha2 keeps it at spec.storageNodes.maxParallelNodeAdds."""
        value = self._read({"spec": {"storageNodes": {"maxParallelNodeAdds": 3}}})
        self.assertEqual(value, 3)

    def test_it_asks_for_the_cluster_rather_than_the_retired_kind(self):
        """storagenodesets does not exist in v1alpha2, so asking for one 404s."""
        seen = {}
        self._read({"spec": {"storageNodes": {"maxParallelNodeAdds": 2}}}, seen)
        self.assertEqual(seen.get("plural"), "storageclusters")
        self.assertEqual(seen.get("version"), "v1alpha2")

    def test_an_unset_knob_still_falls_back(self):
        """None is the caller's signal to use NODE_ADD_MAX_PARALLEL."""
        self.assertIsNone(self._read({"spec": {"storageNodes": {}}}))
        self.assertIsNone(self._read({"spec": {}}))

    def test_the_value_is_never_below_one(self):
        """A zero would seed a MachineConfigPool that never rolls a node."""
        self.assertEqual(self._read({"spec": {"storageNodes": {"maxParallelNodeAdds": 0}}}), 1)


if __name__ == "__main__":
    unittest.main()
