# coding=utf-8
"""
test_capacity_churn_soak.py — unit tests for the pure control logic of the
capacity/namespace churn soak (scripts/aws_capacity_namespace_churn_soak.py).

These cover the parts that decide *what* the soak does without touching a
cluster:
- ``parse_size`` / ``human_bytes`` / ``extract_uuid`` helpers.
- ``SoakRunner._pick_volume_size`` — size steering that converges the running
  average to --avg-size while keeping every size in [--min-size, --max-size].
- ``SoakRunner._try_reserve`` — the dual-cap gate (namespace count incl. the
  reserved clone slot, and effective capacity) plus the in-flight worker cap.

If this logic drifts, the soak silently stops honouring its 2500-volume /
14 TB / ~5 GB-average contract, so these are the guard rails.
"""

import importlib.util
import os
import random
import threading
import types
import unittest
from types import SimpleNamespace


def _load_module():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(repo_root, "scripts", "aws_capacity_namespace_churn_soak.py")
    spec = importlib.util.spec_from_file_location("capacity_churn_soak", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


MOD = _load_module()

GiB = 2**30
TiB = 2**40


def _fake_runner(**overrides):
    """Build a minimal stand-in exposing just the attributes the pure methods
    read, so we can call the unbound SoakRunner methods on it."""
    args = SimpleNamespace(
        converge_fraction=0.25,
    )
    runner = SimpleNamespace(
        run_id="testrun",
        state_lock=threading.RLock(),
        volumes={},
        ns_used=0,
        cap_used=0,
        creating_count=0,
        seq_counter=0,
        client_rr=0,
        total_worker_slots=32,
        max_volumes=2500,
        max_total_bytes=14 * TiB,
        avg_target_bytes=5 * GiB,
        min_size_bytes=1 * GiB,
        max_size_bytes=100 * GiB,
        stop_event=threading.Event(),
        clients=[SimpleNamespace(name="client0"), SimpleNamespace(name="client1")],
        args=args,
    )
    for k, v in overrides.items():
        setattr(runner, k, v)
    # Bind the one method that _try_reserve calls internally.
    runner._pick_volume_size = types.MethodType(MOD.SoakRunner._pick_volume_size, runner)
    return runner


class TestHelpers(unittest.TestCase):
    def test_parse_size(self):
        self.assertEqual(MOD.parse_size("1G"), 1 * GiB)
        self.assertEqual(MOD.parse_size("100G"), 100 * GiB)
        self.assertEqual(MOD.parse_size("14T"), 14 * TiB)
        self.assertEqual(MOD.parse_size("512M"), 512 * 2**20)
        self.assertEqual(MOD.parse_size("1024"), 1024)
        self.assertEqual(MOD.parse_size(2048), 2048)
        self.assertEqual(MOD.parse_size("5GiB"), 5 * GiB)

    def test_human_bytes_roundish(self):
        self.assertEqual(MOD.human_bytes(0), "0.0B")
        self.assertTrue(MOD.human_bytes(5 * GiB).endswith("G"))
        self.assertTrue(MOD.human_bytes(14 * TiB).endswith("T"))

    def test_extract_uuid_takes_last_bare_line(self):
        u1 = "11111111-1111-1111-1111-111111111111"
        u2 = "22222222-2222-2222-2222-222222222222"
        text = f"debug line mentions {u1} inline\nnoise\n{u2}\n"
        self.assertEqual(MOD.extract_uuid(text), u2)
        self.assertIsNone(MOD.extract_uuid("no uuid here"))


class TestSizeSteering(unittest.TestCase):
    def test_size_bounds(self):
        runner = _fake_runner()
        for _ in range(2000):
            size = MOD.SoakRunner._pick_volume_size(runner)
            self.assertGreaterEqual(size, runner.min_size_bytes)
            self.assertLessEqual(size, runner.max_size_bytes)

    def test_running_average_converges_to_target(self):
        """Feeding picked sizes back into the live set should steer the
        cumulative average toward --avg-size (5 GiB)."""
        random.seed(1234)
        runner = _fake_runner()
        seq = 0
        for _ in range(2500):
            size = MOD.SoakRunner._pick_volume_size(runner)
            seq += 1
            vol = MOD.VolumeState(seq=seq, name=f"v{seq}", size=size, client_name="client0")
            vol.volume_id = f"id{seq}"
            runner.volumes[vol.volume_id] = vol
            runner.cap_used += size
        avg = runner.cap_used / len(runner.volumes)
        # Converged average should sit close to 5 GiB.
        self.assertGreater(avg, 4.0 * GiB, f"avg too low: {MOD.human_bytes(avg)}")
        self.assertLess(avg, 6.5 * GiB, f"avg too high: {MOD.human_bytes(avg)}")


class TestReservationGating(unittest.TestCase):
    def test_reserves_and_assigns_round_robin(self):
        runner = _fake_runner()
        vol_a, ctx_a = MOD.SoakRunner._try_reserve(runner)
        vol_b, ctx_b = MOD.SoakRunner._try_reserve(runner)
        self.assertEqual(ctx_a.name, "client0")
        self.assertEqual(ctx_b.name, "client1")
        self.assertEqual(runner.ns_used, 2)  # one namespace reserved per original
        self.assertEqual(runner.creating_count, 2)
        self.assertGreater(runner.cap_used, 0)

    def test_blocks_on_volume_count_cap_with_clone_headroom(self):
        # One namespace slot left -> cannot reserve (needs 2: original + clone).
        runner = _fake_runner(ns_used=2499, max_volumes=2500)
        self.assertIsNone(MOD.SoakRunner._try_reserve(runner))
        # Two slots left -> can reserve exactly one volume.
        runner = _fake_runner(ns_used=2498, max_volumes=2500)
        self.assertIsNotNone(MOD.SoakRunner._try_reserve(runner))

    def test_blocks_on_capacity_cap(self):
        # Capacity essentially full: no room for even a min-size volume.
        runner = _fake_runner(cap_used=14 * TiB)
        self.assertIsNone(MOD.SoakRunner._try_reserve(runner))

    def test_blocks_when_inflight_at_worker_cap(self):
        runner = _fake_runner(creating_count=32, total_worker_slots=32)
        self.assertIsNone(MOD.SoakRunner._try_reserve(runner))

    def test_blocks_when_stopping(self):
        runner = _fake_runner()
        runner.stop_event.set()
        self.assertIsNone(MOD.SoakRunner._try_reserve(runner))


if __name__ == "__main__":
    unittest.main()
