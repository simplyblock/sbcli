"""Migration plans: move a cluster's volumes to another cluster in one step.

Thin REST surface over migration_plan_controller; see docs/online-migration.md.
"""
from uuid import UUID

from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from simplyblock_core.controllers import migration_plan_controller
from simplyblock_core.controllers.migration_plan_controller import MigrationPlanError

from .. import util
from .._dependencies import Cluster
from .._dtos import MigrationPlanDTO

api = APIRouter(tags=['migration-plans'])


class PlanParams(BaseModel):
    target_cluster_id: UUID
    pool_id: UUID | None = None
    lvol_ids: list[UUID] | None = None
    delete_source: bool = False
    interval_min: util.OptionalUnsigned = None
    #: Plan and forecast only: nothing is started or written.
    dry_run: bool = False


def _plan_of(cluster, plan_id):
    try:
        plan, forecast = migration_plan_controller.plan_status(str(plan_id))
    except KeyError as e:
        raise HTTPException(404, str(e))
    if plan.cluster_id != cluster.get_id():
        raise HTTPException(404, f'migration plan {plan_id} not found')
    return plan, forecast


@api.get('/', name='clusters:migration-plans:list')
def list_plans(cluster: Cluster) -> list[MigrationPlanDTO]:
    out = []
    for plan in migration_plan_controller.list_plans(cluster.get_id()):
        plan, forecast = migration_plan_controller.plan_status(plan.uuid)
        out.append(MigrationPlanDTO.from_model(plan, forecast))
    return out


@api.post('/', name='clusters:migration-plans:create', status_code=201,
          responses={200: {"model": MigrationPlanDTO}})
def create_plan(cluster: Cluster, parameters: PlanParams):
    kwargs = dict(
        pool_id=str(parameters.pool_id) if parameters.pool_id else None,
        lvol_ids=[str(i) for i in parameters.lvol_ids] if parameters.lvol_ids else None,
        delete_source=parameters.delete_source,
        interval_min=parameters.interval_min,
    )
    try:
        if parameters.dry_run:
            plan = migration_plan_controller.build_plan(
                cluster.get_id(), str(parameters.target_cluster_id), **kwargs)
            forecast = migration_plan_controller.forecast_only(plan)
            dto = MigrationPlanDTO.from_model(plan, forecast, dry_run=True)
            return JSONResponse(status_code=200, content=dto.model_dump(mode='json'))
        plan = migration_plan_controller.start_plan(
            cluster.get_id(), str(parameters.target_cluster_id), **kwargs)
    except MigrationPlanError as e:
        raise HTTPException(400, str(e))
    plan, forecast = migration_plan_controller.plan_status(plan.uuid)
    dto = MigrationPlanDTO.from_model(plan, forecast)
    return JSONResponse(status_code=201, content=dto.model_dump(mode='json'))


@api.get('/{plan_id}', name='clusters:migration-plans:detail')
def get_plan(cluster: Cluster, plan_id: UUID) -> MigrationPlanDTO:
    plan, forecast = _plan_of(cluster, plan_id)
    return MigrationPlanDTO.from_model(plan, forecast)


@api.delete('/{plan_id}', name='clusters:migration-plans:cancel', status_code=204,
            responses={204: {"content": None}})
def cancel_plan(cluster: Cluster, plan_id: UUID) -> Response:
    _plan_of(cluster, plan_id)
    migration_plan_controller.cancel_plan(str(plan_id))
    return Response(status_code=204)
