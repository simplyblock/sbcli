# coding=utf-8
"""
test_latency_outlier.py — unit tests for the per-device latency-deviation
warning in ``simplyblock_core.services.capacity_and_stats_collector``.

The detector flags a device whose windowed-mean **block-size-normalized**
latency (ticks/byte) exceeds ``LATENCY_OUTLIER_FACTOR`` x the cluster average,
with three suppressions:
  * block-size normalization (ticks/byte, not ticks/op) so big-IO devices are
    not falsely slow,
  * load exemption when a device carries >= ``LATENCY_LOAD_EXEMPT_FACTOR`` x the
    cluster-average IOPS or throughput,
  * a per-device window-IO floor (``LATENCY_MIN_WINDOW_IO``) and a per-device
    warn cooldown (``LATENCY_WARN_COOLDOWN_SEC``).
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.services import capacity_and_stats_collector as c


def _dev(uuid, order):
    d = MagicMock()
    d.get_id.return_value = uuid
    d.cluster_device_order = order
    return d


def _rec(iops, block_size, ticks_per_op):
    """A per-cycle device stat record. lat_per_byte = ticks_per_op/block_size
    (independent of iops); throughput = iops*block_size."""
    return {
        "read_io_ps": iops, "write_io_ps": 0,
        "read_bytes_ps": iops * block_size, "write_bytes_ps": 0,
        "read_latency_ps": iops * ticks_per_op, "write_latency_ps": 0,
    }


class TestLatencyOutlierDetector(unittest.TestCase):
    def setUp(self):
        c._latency_window.clear()
        c._latency_last_warn.clear()
        de_p = patch.object(c, "device_events")
        self.de = de_p.start()
        self.addCleanup(de_p.stop)

    def _run_cycles(self, records, cycles=3):
        """Feed the same per-device records for `cycles` ticks (>= MIN_SAMPLES)
        and return how many warnings were issued."""
        for _ in range(cycles):
            c.detect_latency_outliers(records)
        return self.de.device_latency_outlier.call_count

    # --- block-size normalization ----------------------------------------

    def test_large_block_device_not_flagged_when_per_byte_equal(self):
        # C has 32x the per-op latency of A/B but also 32x the block size, so
        # its per-BYTE latency is identical -> must NOT be flagged.
        recs = [
            (_dev("A", 0), _rec(200, 4096, 50)),
            (_dev("B", 1), _rec(200, 4096, 50)),
            (_dev("C", 2), _rec(200, 131072, 1600)),  # 32x bs, 32x ticks/op
        ]
        self.assertEqual(self._run_cycles(recs), 0)

    def test_genuine_per_byte_outlier_warns(self):
        # 4 healthy devices + 1 with 5x per-byte latency (same block size).
        recs = [(_dev(f"n{i}", i), _rec(200, 4096, 50)) for i in range(4)]
        recs.append((_dev("bad", 9), _rec(200, 4096, 250)))  # 5x ticks/op, same bs
        self.assertEqual(self._run_cycles(recs), 1)

    # --- load exemptions --------------------------------------------------

    def test_high_iops_device_exempted(self):
        # 'busy' is a per-byte outlier but carries ~2x+ the cluster-average IOPS
        # -> its latency is load, not degradation -> no warning.
        recs = [(_dev(f"n{i}", i), _rec(200, 4096, 50)) for i in range(4)]
        recs.append((_dev("busy", 9), _rec(2000, 4096, 250)))  # 10x iops, 5x ticks/op
        self.assertEqual(self._run_cycles(recs), 0)

    def test_high_throughput_device_exempted(self):
        # 'bw' is a per-byte outlier but carries ~2x+ the cluster-average
        # throughput (large blocks) -> exempted.
        recs = [(_dev(f"n{i}", i), _rec(200, 4096, 50)) for i in range(4)]
        # bs 131072, ticks/op 4800 -> per-byte 4800/131072 ~= 3x baseline;
        # throughput 200*131072 >> 2x cluster avg.
        recs.append((_dev("bw", 9), _rec(200, 131072, 4800)))
        self.assertEqual(self._run_cycles(recs), 0)

    # --- gating -----------------------------------------------------------

    def test_window_io_floor_excludes_low_volume_device(self):
        # With the floor cranked above any device's window volume, nothing is
        # judged even with a clear outlier present.
        recs = [(_dev(f"n{i}", i), _rec(200, 4096, 50)) for i in range(4)]
        recs.append((_dev("bad", 9), _rec(200, 4096, 250)))
        with patch.object(c, "LATENCY_MIN_WINDOW_IO", 10 ** 12):
            self.assertEqual(self._run_cycles(recs), 0)

    def test_near_idle_cycles_not_sampled(self):
        # Below the per-cycle IO floor -> never accumulates samples -> no judge.
        recs = [(_dev(f"n{i}", i), _rec(10, 4096, 50)) for i in range(3)]
        recs.append((_dev("bad", 9), _rec(10, 4096, 250)))
        self.assertEqual(self._run_cycles(recs), 0)

    def test_min_devices_not_met(self):
        # Only 2 devices with samples -> below LATENCY_MIN_DEVICES -> no judge.
        recs = [
            (_dev("a", 0), _rec(200, 4096, 50)),
            (_dev("bad", 9), _rec(200, 4096, 500)),
        ]
        self.assertEqual(self._run_cycles(recs), 0)

    # --- cooldown ---------------------------------------------------------

    def test_warns_once_then_respects_cooldown(self):
        recs = [(_dev(f"n{i}", i), _rec(200, 4096, 50)) for i in range(4)]
        recs.append((_dev("bad", 9), _rec(200, 4096, 250)))
        # 3 cycles -> 1 warning; further cycles within the cooldown -> no more.
        self._run_cycles(recs, cycles=3)
        self.assertEqual(self.de.device_latency_outlier.call_count, 1)
        self._run_cycles(recs, cycles=5)
        self.assertEqual(self.de.device_latency_outlier.call_count, 1)


if __name__ == "__main__":
    unittest.main()
