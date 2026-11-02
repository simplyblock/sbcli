"""The service-class benchmark's metrics (scripts/svcclass_bench/analysis.py)."""
import importlib.util
import itertools
import pathlib
import sys

import pytest

_PATH = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "svcclass_bench" / "analysis.py"
_spec = importlib.util.spec_from_file_location("svcclass_bench_analysis", _PATH)
analysis = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = analysis      # dataclasses look their module up there
_spec.loader.exec_module(analysis)

T0 = 1_000_000.0


def _rec(vol, snap, start, dur):
    return {"volume": vol, "snapshot": snap, "created_at": T0 + start,
            "replicated_at": T0 + start + dur if dur is not None else 0}


def _iters(durations, period=60, first_start=0):
    recs, t = [], first_start
    for n, d in enumerate(durations):
        recs.append(_rec("v1", f"s{n}", t, d))
        t += max(period, d)
    return analysis.iterations_from_records(recs, T0)["v1"]


class TestSteadyState:

    def test_first_full_copy_is_never_steady(self):
        iters = _iters([300, 20, 21, 19, 20, 22, 20])
        assert analysis.steady_start(iters) == 1

    def test_settling_iterations_are_ramp(self):
        iters = _iters([300, 90, 60, 40, 20, 21, 19, 20, 22, 20])
        assert analysis.steady_start(iters) == 4

    def test_never_settling_is_none(self):
        iters = _iters([300, 10, 40, 10, 40, 10, 40, 10])
        assert analysis.steady_start(iters) is None

    def test_short_transfers_use_the_absolute_floor(self):
        # 2..4 s differs by 50% but only by 1 s around the median
        iters = _iters([300, 3, 2, 4, 3, 3])
        assert analysis.steady_start(iters) == 1

    def test_overrunning_transfers_can_still_be_steady(self):
        iters = _iters([900, 95, 100, 98, 101, 97], period=60)
        assert analysis.steady_start(iters) == 1
        assert all(b.started - a.started >= 95 for a, b in itertools.pairwise(iters[1:]))


class TestReplicationSummary:

    def test_cluster_is_steady_when_its_slowest_volume_is(self):
        recs = [_rec("a", "a0", 0, 200)] + [_rec("a", f"a{i}", 200 + 60 * i, 20) for i in range(1, 8)]
        recs += [_rec("b", "b0", 0, 400)] + [_rec("b", f"b{i}", 400 + 60 * i, 25) for i in range(1, 8)]
        s = analysis.replication_summary(analysis.iterations_from_records(recs, T0), T0)
        assert s["steady_state_reached"]
        assert s["time_to_steady_s"] == 460
        assert s["volumes"]["a"]["initial_full_copy_s"] == 200
        assert s["steady_iteration_s"]["n"] == 14
        assert s["ramp_iteration_s"]["mean"] == pytest.approx(300)
        assert s["steady_period_s"]["p50"] == 60

    def test_unreplicated_and_older_snapshots_are_ignored(self):
        recs = [_rec("a", "old", -500, 10), _rec("a", "open", 0, None)]
        assert analysis.iterations_from_records(recs, T0) == {}

    def test_a_pruned_target_copy_does_not_erase_the_completion_time(self):
        # once retention deletes the target copy, later records say -1
        recs = [_rec("a", "s0", 0, None), _rec("a", "s0", 0, 42),
                {**_rec("a", "s0", 0, None), "replicated_at": -1, "status": "in_deletion"}]
        it, = analysis.iterations_from_records(recs, T0)["a"]
        assert it.duration == 42

    def test_later_record_completes_an_earlier_one(self):
        recs = [_rec("a", "s0", 0, None), _rec("a", "s0", 0, 42)]
        it, = analysis.iterations_from_records(recs, T0)["a"]
        assert it.duration == 42


class TestFio:

    def test_series_are_summed_across_jobs_and_split_by_direction(self, tmp_path):
        d = tmp_path / "client1"
        d.mkdir()
        (d / "v1_iops.1.log").write_text("1000, 100, 0, 4096, 0\n2000, 110, 0, 4096, 0\n")
        (d / "v1_iops.2.log").write_text("1000, 50, 1, 4096, 0\n")
        (d / "v1_bw.1.log").write_text("1000, 2048, 0, 4096, 0\n")
        (d / "unrelated.txt").write_text("x")
        s = analysis.load_fio_series(str(tmp_path), 100.0)
        assert s["read_iops"] == {101: 100.0, 102: 110.0}
        assert s["write_iops"] == {101: 50.0}
        w = analysis.client_window(s, 101, 103)
        assert w["read_iops"] == pytest.approx(105.0)
        assert w["total_iops"] == pytest.approx(130.0)
        assert w["read_mbs"] == pytest.approx(1.0)
