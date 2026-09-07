"""Unit tests for raw vs. effective capacity accounting.

The control plane used to mix the two: ``size_prov`` (the sum of provisioned
lvol sizes, EFFECTIVE) was divided by ``size_total`` (summed alceml device
capacity, RAW), so provisioned utilisation came out low by exactly
``(ndcs + npcs) / ndcs``. On a 4+2 cluster that is a factor of 1.5 — enough
that ``prov_cap_crit = 190`` would report 126% for a cluster genuinely at 190%
and never trip.

Everything is effective now, converted once where raw device numbers enter the
system. These tests pin the conversion, pin that the raw numbers survive
alongside it, and pin the provisioned-utilisation arithmetic at node and
cluster level.
"""

from unittest.mock import MagicMock

import pytest

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.stats import DeviceStatObject, NodeStatObject
from simplyblock_core.services import capacity_and_stats_collector as collector
from simplyblock_core.utils import capacity

GiB = 2 ** 30
TiB = 2 ** 40

# Every scheme cluster_ops.SUPPORTED_ERASURE_CODING_SCHEMES allows.
SCHEMES = [(1, 0), (1, 1), (2, 1), (4, 1), (1, 2), (2, 2), (4, 2)]


def _cluster(ndcs=4, npcs=2):
    cl = Cluster()
    cl.uuid = "cl-1"
    cl.distr_ndcs = ndcs
    cl.distr_npcs = npcs
    return cl


class TestConversion:

    @pytest.mark.parametrize("ndcs,npcs", SCHEMES)
    def test_effective_is_the_data_share_of_raw(self, ndcs, npcs):
        raw = 120 * TiB
        assert capacity.to_effective(raw, _cluster(ndcs, npcs)) == raw * ndcs // (ndcs + npcs)

    def test_four_plus_two_loses_a_third(self):
        assert capacity.to_effective(120 * TiB, _cluster(4, 2)) == 80 * TiB

    def test_mirroring_halves(self):
        assert capacity.to_effective(120 * TiB, _cluster(1, 1)) == 60 * TiB

    def test_no_parity_is_an_identity(self):
        assert capacity.to_effective(120 * TiB, _cluster(1, 0)) == 120 * TiB

    @pytest.mark.parametrize("ndcs,npcs", SCHEMES)
    def test_round_trip_never_understates_the_raw_cost(self, ndcs, npcs):
        """to_raw rounds up: a capacity check built on it cannot admit an
        over-commit through a rounding remainder."""
        cl = _cluster(ndcs, npcs)
        effective = 7 * GiB + 12345  # deliberately not a multiple of the stripe
        raw = capacity.to_raw(effective, cl)
        assert capacity.to_effective(raw, cl) >= effective

    def test_arithmetic_is_exact_beyond_float_precision(self):
        """Byte counts above 2**53 are inside the range a large cluster
        reaches; a float round-trip would lose bytes there."""
        raw = (2 ** 60) + 6  # 1 EiB + 6, divisible by 6 for 4+2
        assert capacity.to_effective(raw, _cluster(4, 2)) == raw * 4 // 6

    def test_unset_geometry_falls_back_to_identity(self):
        """distr_ndcs/distr_npcs default to 0 on the model. With no geometry
        known, raw is the best estimate of effective — and must not divide by
        zero."""
        cl = Cluster()
        assert capacity.stripe_geometry(cl) == (1, 0)
        assert capacity.to_effective(120 * TiB, cl) == 120 * TiB
        assert capacity.to_raw(120 * TiB, cl) == 120 * TiB


class TestDeviceRecord:
    """add_device_stats is the single point where raw device numbers become
    effective, so node and cluster records are sums of already-converted
    values."""

    @pytest.fixture(autouse=True)
    def _db(self, monkeypatch):
        db = MagicMock()
        db.get_device_stats.return_value = []
        monkeypatch.setattr(collector, "db", db)
        monkeypatch.setattr(collector, "last_object_record", {})
        monkeypatch.setattr(DeviceStatObject, "write_to_db", lambda self, kv: True)
        return db

    @staticmethod
    def _capacity_dict(npages_nmax, npages_used, page=4096):
        return {
            "res": 1,
            "npages_nmax": npages_nmax,
            "npages_used": npages_used,
            "pba_page_size": page,
        }

    def _record(self, ndcs=4, npcs=2, nmax=(120 * TiB) // 4096, used=(60 * TiB) // 4096):
        device = MagicMock()
        device.get_id.return_value = "dev-1"
        return collector.add_device_stats(
            _cluster(ndcs, npcs), device,
            self._capacity_dict(nmax, used), None)

    def test_reported_sizes_are_effective(self):
        rec = self._record()
        assert rec.size_total == 80 * TiB
        assert rec.size_used == 40 * TiB
        assert rec.size_free == 40 * TiB

    def test_raw_sizes_are_kept_alongside(self):
        rec = self._record()
        assert rec.size_total_raw == 120 * TiB
        assert rec.size_used_raw == 60 * TiB
        assert rec.size_free_raw == 60 * TiB

    def test_free_is_the_effective_difference_not_a_converted_raw_free(self):
        """size_free must equal size_total - size_used exactly, or a node's
        used+free stops adding up to its total after the floor divisions."""
        rec = self._record(ndcs=4, npcs=2, nmax=1001, used=333)
        assert rec.size_free == rec.size_total - rec.size_used

    def test_absolute_utilisation_is_unchanged_by_the_conversion(self):
        """Both operands scale by the same ratio, so size_util was never the
        broken one - pin that so a future change does not "fix" it."""
        assert self._record(ndcs=4, npcs=2).size_util == 50
        assert self._record(ndcs=1, npcs=0).size_util == 50

    def test_no_capacity_reported_leaves_sizes_unset(self):
        device = MagicMock()
        device.get_id.return_value = "dev-1"
        rec = collector.add_device_stats(_cluster(), device, {"res": 0}, None)
        assert rec.size_total == 0
        assert rec.size_total_raw == 0


class TestProvisionedUtilisation:
    """The bug that motivated all of this: effective numerator over raw
    denominator."""

    @pytest.fixture(autouse=True)
    def _db(self, monkeypatch):
        db = MagicMock()
        db.get_node_stats.return_value = []
        db.get_cluster_stats.return_value = []
        monkeypatch.setattr(collector, "db", db)
        monkeypatch.setattr(NodeStatObject, "write_to_db", lambda self, kv: True)
        monkeypatch.setattr(
            collector.ClusterStatObject, "write_to_db", lambda self, kv: True)
        return db

    @staticmethod
    def _device_record(size_total, size_used):
        return DeviceStatObject(data={
            "uuid": "dev-1",
            "size_total": size_total,
            "size_used": size_used,
            "size_free": size_total - size_used,
        })

    @staticmethod
    def _lvol(node_id, size):
        lvol = MagicMock()
        lvol.node_id = node_id
        lvol.size = size
        return lvol

    def test_node_prov_util_is_against_effective_capacity(self):
        """80 TiB effective (from 120 TiB raw at 4+2), 80 TiB provisioned =>
        100%. Against the raw total it read 66%."""
        node = MagicMock()
        node.get_id.return_value = "node-1"
        rec = collector.add_node_stats(
            _cluster(4, 2), node,
            [self._device_record(80 * TiB, 20 * TiB)],
            [self._lvol("node-1", 80 * TiB)])

        assert rec.size_prov == 80 * TiB
        assert rec.size_total == 80 * TiB
        assert rec.size_prov_util == 100

    def test_node_prov_util_counts_only_local_volumes(self):
        node = MagicMock()
        node.get_id.return_value = "node-1"
        rec = collector.add_node_stats(
            _cluster(4, 2), node,
            [self._device_record(80 * TiB, 0)],
            [self._lvol("node-1", 40 * TiB), self._lvol("node-2", 40 * TiB)])

        assert rec.size_prov == 40 * TiB
        assert rec.size_prov_util == 50

    def test_over_commit_now_exceeds_prov_cap_crit(self):
        """A cluster provisioned at 2x its effective capacity must report 200%,
        which is above the default prov_cap_crit of 190. Under the raw
        denominator the same cluster reported 133% and the alarm never fired."""
        cl = _cluster(4, 2)
        node = MagicMock()
        node.get_id.return_value = "node-1"
        rec = collector.add_node_stats(
            cl, node,
            [self._device_record(80 * TiB, 10 * TiB)],
            [self._lvol("node-1", 160 * TiB)])

        assert rec.size_prov_util == 200
        assert rec.size_prov_util > cl.prov_cap_crit

    def test_cluster_prov_util_sums_node_records(self):
        cl = _cluster(4, 2)
        node_records = [
            NodeStatObject(data={
                "uuid": "node-1", "size_total": 40 * TiB,
                "size_used": 10 * TiB, "size_free": 30 * TiB,
                "size_prov": 30 * TiB}),
            NodeStatObject(data={
                "uuid": "node-2", "size_total": 40 * TiB,
                "size_used": 10 * TiB, "size_free": 30 * TiB,
                "size_prov": 50 * TiB}),
        ]
        rec = collector.add_cluster_stats(cl, node_records)

        assert rec.size_total == 80 * TiB
        assert rec.size_prov == 80 * TiB
        assert rec.size_prov_util == 100
        assert rec.size_util == 25
