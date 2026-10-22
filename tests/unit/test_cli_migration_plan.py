"""`sbctl cluster migrate*` handlers (docs/online-migration.md)."""
import argparse
from unittest.mock import MagicMock

from simplyblock_cli import clibase
from simplyblock_core.models.migration_plan import MigrationPlan

FORECAST = {"bytes_remaining": 1024, "eta_seconds": 90, "completes_at": 0,
            "throughput_bytes_per_sec": 1024, "throughput_measured": False}


def _plan():
    plan = MigrationPlan()
    plan.uuid = "P1"
    plan.volumes = [{"lvol_id": "v1", "name": "vol1", "state": "replicating",
                     "target_node_id": "T1", "bytes_remaining": 1024, "eta_seconds": 90}]
    return plan


def _args(**kw):
    base = dict(cluster_id="C1", target_cluster_id="C2", pool_id=None, lvol_ids=None,
                delete_source=False, interval_min=None, dry_run=False, json=False)
    base.update(kw)
    return argparse.Namespace(**base)


def _wrapper(monkeypatch):
    ctl = MagicMock()
    ctl.MigrationPlanError = ValueError
    ctl.start_plan.return_value = _plan()
    ctl.build_plan.return_value = _plan()
    ctl.plan_status.return_value = (_plan(), FORECAST)
    ctl.forecast_only.return_value = FORECAST
    monkeypatch.setattr(clibase, "migration_plan_controller", ctl)
    return clibase.CLIWrapperBase.__new__(clibase.CLIWrapperBase), ctl


def test_migrate_passes_the_selection(monkeypatch):
    w, ctl = _wrapper(monkeypatch)
    out = w.cluster__migrate(None, _args(lvol_ids="a, b", delete_source=True))
    kwargs = ctl.start_plan.call_args.kwargs
    assert kwargs["lvol_ids"] == ["a", "b"] and kwargs["delete_source"] is True
    assert "Plan P1" in out and "not yet measured" in out


def test_dry_run_starts_nothing(monkeypatch):
    w, ctl = _wrapper(monkeypatch)
    out = w.cluster__migrate(None, _args(dry_run=True))
    ctl.start_plan.assert_not_called()
    assert "dry run" in out


def test_planning_error_is_reported(monkeypatch):
    w, ctl = _wrapper(monkeypatch)
    ctl.start_plan.side_effect = ValueError("no volume selected")
    assert w.cluster__migrate(None, _args()) == "Error: no volume selected"
