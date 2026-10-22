"""Migration-plan routes (docs/online-migration.md): create (and dry run),
read with live forecast, list, cancel."""
from unittest.mock import MagicMock

import pytest

import simplyblock_web.api.v2.cluster.migration_plan as plan_module
from simplyblock_core.controllers.migration_plan_controller import MigrationPlanError
from simplyblock_core.models.migration_plan import MigrationPlan
from tests.unit.web.api.v2 import _factories as factories

BASE = f'/api/v2/clusters/{factories.CLUSTER_ID}/migration-plans'
TARGET = '22222222-2222-2222-2222-222222222222'
PLAN_ID = '33333333-3333-3333-3333-333333333333'
FORECAST = {"bytes_remaining": 10, "eta_seconds": 5, "completes_at": 100,
            "throughput_bytes_per_sec": 2, "throughput_measured": True}


def _plan(cluster_id=factories.CLUSTER_ID):
    plan = MigrationPlan()
    plan.uuid = PLAN_ID
    plan.cluster_id = cluster_id
    plan.target_cluster_id = TARGET
    plan.volumes = [{"lvol_id": "v1", "name": "vol", "state": "replicating",
                     "target_node_id": "T1", "error": "", "used_bytes": 10,
                     "bytes_remaining": 10, "eta_seconds": 5}]
    return plan


@pytest.fixture()
def controller(monkeypatch):
    mock = MagicMock()
    mock.plan_status.return_value = (_plan(), FORECAST)
    mock.forecast_only.return_value = FORECAST
    monkeypatch.setattr(plan_module, 'migration_plan_controller', mock)
    return mock


def test_create_starts_the_plan(client, db, cluster, controller):
    controller.start_plan.return_value = _plan()
    resp = client.post(f'{BASE}/', json={"target_cluster_id": TARGET, "delete_source": True})
    assert resp.status_code == 201
    body = resp.json()
    assert body["id"] == PLAN_ID
    assert body["volumes"][0]["eta_seconds"] == 5
    assert body["forecast"]["throughput_measured"] is True
    kwargs = controller.start_plan.call_args.kwargs
    assert kwargs["delete_source"] is True and kwargs["lvol_ids"] is None


def test_dry_run_starts_nothing(client, db, cluster, controller):
    controller.build_plan.return_value = _plan()
    resp = client.post(f'{BASE}/', json={"target_cluster_id": TARGET, "dry_run": True})
    assert resp.status_code == 200
    assert resp.json()["id"] is None
    controller.start_plan.assert_not_called()


def test_an_unplannable_request_is_400(client, db, cluster, controller):
    controller.start_plan.side_effect = MigrationPlanError("no volume selected")
    resp = client.post(f'{BASE}/', json={"target_cluster_id": TARGET})
    assert resp.status_code == 400


def test_detail_and_cancel(client, db, cluster, controller):
    assert client.get(f'{BASE}/{PLAN_ID}').json()["status"] == "running"
    assert client.delete(f'{BASE}/{PLAN_ID}').status_code == 204
    controller.cancel_plan.assert_called_once_with(PLAN_ID)


def test_another_clusters_plan_is_404(client, db, cluster, controller):
    controller.plan_status.return_value = (_plan(cluster_id="other"), FORECAST)
    assert client.get(f'{BASE}/{PLAN_ID}').status_code == 404
    assert client.delete(f'{BASE}/{PLAN_ID}').status_code == 404
    controller.cancel_plan.assert_not_called()


def test_list(client, db, cluster, controller):
    controller.list_plans.return_value = [_plan()]
    assert [p["id"] for p in client.get(f'{BASE}/').json()] == [PLAN_ID]
