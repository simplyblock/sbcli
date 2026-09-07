from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import replication_policy_controller
from simplyblock_core.controllers.replication_policy_controller import ReplicationConfigError

from .. import util
from .._dependencies import Cluster, ReplicationPolicy, ReplicationTarget
from .._dtos import (
    FailoverResultDTO,
    ReplicationMode,
    ReplicationPolicyDTO,
    ReplicationTargetDTO,
)

api = APIRouter(tags=['replication'])
db = DBController()


class TargetParams(BaseModel):
    target_name: str
    target_cluster_id: UUID
    target_pool_id: UUID | None = None
    timeout_sec: util.Unsigned | None = None


class PolicyParams(BaseModel):
    policy_name: str
    target_id: UUID
    interval_min: util.Unsigned = 1
    mode: ReplicationMode | None = None
    keep_replicated: Annotated[int, Field(ge=2)] | None = None


def _config_error(e: ReplicationConfigError):
    return HTTPException(status_code=400, detail=str(e))


targets_api = APIRouter()


@targets_api.get('/', name='clusters:replication:targets:list')
def list_targets(cluster: Cluster) -> list[ReplicationTargetDTO]:
    return [
        ReplicationTargetDTO.from_model(target)
        for target in replication_policy_controller.list_targets(cluster.get_id())
    ]


@targets_api.post('/', name='clusters:replication:targets:create', status_code=201,
                  responses={201: {"content": None}})
def create_target(request: Request, cluster: Cluster, parameters: TargetParams,
                  response_format: util.CreationResponseFormatParameter = "full") -> Response:
    try:
        target_id = replication_policy_controller.add_target(
            cluster.get_id(), parameters.target_name, str(parameters.target_cluster_id),
            target_pool=str(parameters.target_pool_id) if parameters.target_pool_id else None,
            timeout_sec=parameters.timeout_sec)
    except ReplicationConfigError as e:
        raise _config_error(e)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    target = db.get_replication_target_by_id(target_id)
    return util.creation_response(
        request, response_format,
        entity_id=UUID(target.uuid),
        route_name='clusters:replication:targets:detail',
        route_kwargs={'cluster_id': UUID(cluster.get_id()), 'target_id': UUID(target.uuid)},
        get_full=lambda _: ReplicationTargetDTO.from_model(target),
    )


target_instance_api = APIRouter(prefix='/{target_id}')


@target_instance_api.get('/', name='clusters:replication:targets:detail')
def get_target(cluster: Cluster, target: ReplicationTarget) -> ReplicationTargetDTO:
    return ReplicationTargetDTO.from_model(target)


@target_instance_api.delete('/', name='clusters:replication:targets:delete',
                            status_code=204, responses={204: {"content": None}})
def delete_target(cluster: Cluster, target: ReplicationTarget) -> Response:
    try:
        replication_policy_controller.remove_target(target.get_id())
    except ReplicationConfigError as e:
        raise _config_error(e)
    return Response(status_code=204)


@target_instance_api.post('/failover', name='clusters:replication:targets:failover')
def failover_target(cluster: Cluster, target: ReplicationTarget) -> list[FailoverResultDTO]:
    """Fail over EVERY volume replicating to this target.

    A site loss has to move all volumes at once; doing it volume by volume was
    the only option before. Idempotent per volume and reports one result per
    volume, so a partial failure is visible instead of silent.
    """
    return [
        FailoverResultDTO(**result)
        for result in replication_policy_controller.failover_target(target.get_id())
    ]


policies_api = APIRouter()


@policies_api.get('/', name='clusters:replication:policies:list')
def list_policies(cluster: Cluster) -> list[ReplicationPolicyDTO]:
    return [
        ReplicationPolicyDTO.from_model(policy)
        for policy in replication_policy_controller.list_policies(cluster.get_id())
    ]


@policies_api.post('/', name='clusters:replication:policies:create', status_code=201,
                   responses={201: {"content": None}})
def create_policy(request: Request, cluster: Cluster, parameters: PolicyParams,
                  response_format: util.CreationResponseFormatParameter = "full") -> Response:
    try:
        policy_id = replication_policy_controller.add_policy(
            cluster.get_id(), parameters.policy_name, str(parameters.target_id),
            interval_min=parameters.interval_min, mode=parameters.mode,
            keep_replicated=parameters.keep_replicated)
    except ReplicationConfigError as e:
        raise _config_error(e)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    policy = db.get_replication_policy_by_id(policy_id)
    return util.creation_response(
        request, response_format,
        entity_id=UUID(policy.uuid),
        route_name='clusters:replication:policies:detail',
        route_kwargs={'cluster_id': UUID(cluster.get_id()), 'policy_id': UUID(policy.uuid)},
        get_full=lambda _: ReplicationPolicyDTO.from_model(policy),
    )


policy_instance_api = APIRouter(prefix='/{policy_id}')


@policy_instance_api.get('/', name='clusters:replication:policies:detail')
def get_policy(cluster: Cluster, policy: ReplicationPolicy) -> ReplicationPolicyDTO:
    return ReplicationPolicyDTO.from_model(policy)


@policy_instance_api.delete('/', name='clusters:replication:policies:delete',
                            status_code=204, responses={204: {"content": None}})
def delete_policy(cluster: Cluster, policy: ReplicationPolicy) -> Response:
    try:
        replication_policy_controller.remove_policy(policy.get_id())
    except ReplicationConfigError as e:
        raise _config_error(e)
    return Response(status_code=204)


@policy_instance_api.post('/failover', name='clusters:replication:policies:failover')
def failover_policy(cluster: Cluster, policy: ReplicationPolicy) -> list[FailoverResultDTO]:
    return [
        FailoverResultDTO(**result)
        for result in replication_policy_controller.failover_policy(policy.get_id())
    ]


targets_api.include_router(target_instance_api)
policies_api.include_router(policy_instance_api)
api.include_router(targets_api, prefix='/targets')
api.include_router(policies_api, prefix='/policies')
