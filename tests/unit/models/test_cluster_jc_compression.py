"""Cluster.jc_compression_enabled: defaults on, including for FDB records
written before the field existed."""

import unittest

from simplyblock_core.models.cluster import Cluster


class TestModel(unittest.TestCase):

    def test_default_enabled(self):
        self.assertTrue(Cluster().jc_compression_enabled)

    def test_legacy_record_without_field_stays_enabled(self):
        c = Cluster()
        c.from_dict({"uuid": "cluster-1"})
        self.assertTrue(c.jc_compression_enabled)

    def test_round_trip(self):
        c = Cluster()
        c.jc_compression_enabled = False
        self.assertFalse(Cluster().from_dict(c.to_dict()).jc_compression_enabled)
