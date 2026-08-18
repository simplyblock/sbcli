from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from simplyblock_core.controllers import replication_policy_controller as rpc
from simplyblock_core.controllers.replication_policy_controller import ReplicationConfigError

from .._dependencies import Cluster

api = APIRouter(tags=['replication'])


class ReplicationTargetDTO(BaseModel):
    id: str
    cluster_id: str
    target_name: str
    target_cluster_id: str
    target_pool_uuid: str
    timeout_sec: int
    status: str

    @classmethod
    def from_model(cls, model):
        return cls(
            id=model.get_id(),
            cluster_id=model.cluster_id,
            target_name=model.target_name,
            target_cluster_id=model.target_cluster_id,
            target_pool_uuid=model.target_pool_uuid,
            timeout_sec=model.timeout_sec,
            status=model.status,
        )


class ReplicationPolicyDTO(BaseModel):
    id: str
    cluster_id: str
    policy_name: str
    target_id: str
    interval_min: int
    mode: str
    keep_replicated: int
    status: str

    @classmethod
    def from_model(cls, model):
        return cls(
            id=model.get_id(),
            cluster_id=model.cluster_id,
            policy_name=model.policy_name,
            target_id=model.target_id,
            interval_min=model.interval_min,
            mode=model.mode,
            keep_replicated=model.keep_replicated,
            status=model.status,
        )


class TargetParams(BaseModel):
    target_name: str
    target_cluster_id: str
    target_pool: Optional[str] = None
    timeout_sec: Optional[int] = None


class PolicyParams(BaseModel):
    policy_name: str
    target: str                                   # target id or name
    interval_min: int = 1
    mode: Optional[str] = None                    # failover | migration
    keep_replicated: Optional[int] = Field(None, ge=2)


class FailoverResultDTO(BaseModel):
    lvol_id: str
    status: str                                   # failed_over | skipped | failed
    detail: Optional[str] = None
    target_lvol_id: Optional[str] = None
    connection_strings: Optional[List[str]] = None


def _config_error(e: ReplicationConfigError):
    return HTTPException(status_code=400, detail=str(e))


@api.get('/replication-targets', name='clusters:replication-targets:list')
def list_targets(cluster: Cluster) -> List[ReplicationTargetDTO]:
    return [ReplicationTargetDTO.from_model(t) for t in rpc.list_targets(cluster.get_id())]


@api.post('/replication-targets', name='clusters:replication-targets:create', status_code=201)
def create_target(cluster: Cluster, parameters: TargetParams) -> ReplicationTargetDTO:
    try:
        target_id = rpc.add_target(
            cluster.get_id(), parameters.target_name, parameters.target_cluster_id,
            target_pool=parameters.target_pool, timeout_sec=parameters.timeout_sec)
    except ReplicationConfigError as e:
        raise _config_error(e)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return ReplicationTargetDTO.from_model(rpc.db.get_replication_target_by_id(target_id))


@api.delete('/replication-targets/{target_id}', name='clusters:replication-targets:delete',
            status_code=204, responses={204: {"content": None}})
def delete_target(cluster: Cluster, target_id: str) -> None:
    try:
        rpc.remove_target(target_id)
    except ReplicationConfigError as e:
        raise _config_error(e)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@api.post('/replication-targets/{target_id}/failover',
          name='clusters:replication-targets:failover')
def failover_target(cluster: Cluster, target_id: str) -> List[FailoverResultDTO]:
    """Fail over EVERY volume replicating to this target.

    A site loss has to move all volumes at once; doing it volume by volume was
    the only option before. Idempotent per volume and reports one result per
    volume, so a partial failure is visible instead of silent.
    """
    try:
        results = rpc.failover_target(target_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return [FailoverResultDTO(**r) for r in results]


@api.get('/replication-policies', name='clusters:replication-policies:list')
def list_policies(cluster: Cluster) -> List[ReplicationPolicyDTO]:
    return [ReplicationPolicyDTO.from_model(p) for p in rpc.list_policies(cluster.get_id())]


@api.post('/replication-policies', name='clusters:replication-policies:create', status_code=201)
def create_policy(cluster: Cluster, parameters: PolicyParams) -> ReplicationPolicyDTO:
    try:
        policy_id = rpc.add_policy(
            cluster.get_id(), parameters.policy_name, parameters.target,
            interval_min=parameters.interval_min, mode=parameters.mode,
            keep_replicated=parameters.keep_replicated)
    except ReplicationConfigError as e:
        raise _config_error(e)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return ReplicationPolicyDTO.from_model(rpc.db.get_replication_policy_by_id(policy_id))


@api.delete('/replication-policies/{policy_id}', name='clusters:replication-policies:delete',
            status_code=204, responses={204: {"content": None}})
def delete_policy(cluster: Cluster, policy_id: str) -> None:
    try:
        rpc.remove_policy(policy_id)
    except ReplicationConfigError as e:
        raise _config_error(e)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@api.post('/replication-policies/{policy_id}/failover',
          name='clusters:replication-policies:failover')
def failover_policy(cluster: Cluster, policy_id: str) -> List[FailoverResultDTO]:
    try:
        results = rpc.failover_policy(policy_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return [FailoverResultDTO(**r) for r in results]
