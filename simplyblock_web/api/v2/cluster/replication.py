import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import AnyHttpUrl, BaseModel, Field, SecretStr, model_validator

from simplyblock_core.controllers import lvol_controller, replication_policy_controller
from simplyblock_core.controllers.replication_policy_controller import (
    PeerConnection,
    ReplicationConfigError,
)
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.peer.client import PeerError

from .. import util
from .._dependencies import Cluster, ReplicationPolicy, ReplicationTarget
from .._dtos import (
    FailoverResultDTO,
    ReplicatedGenerationDTO,
    ReplicatedSnapshotDTO,
    ReplicationMode,
    ReplicationPolicyDTO,
    ReplicationRelationshipDTO,
    ReplicationTargetDTO,
)

api = APIRouter(tags=['replication'])
db = DBController()


class TargetParams(BaseModel):
    target_name: str
    target_cluster_id: UUID
    target_pool_id: UUID | None = None
    timeout_sec: util.Unsigned | None = None
    #: Root URL of the control plane that manages target_cluster_id, when that
    #: is not this one. Omitted: the target cluster is in this control plane.
    peer_endpoint: AnyHttpUrl | None = None
    #: The bearer credential the peer accepts for target_cluster_id.
    peer_secret: SecretStr | None = None
    #: PEM CA bundle that signs the peer's certificate; omitted uses the system trust store.
    peer_ca_cert: str | None = None

    @model_validator(mode='after')
    def _peer_fields_together(self) -> 'TargetParams':
        if self.peer_endpoint is None:
            if self.peer_secret is not None or self.peer_ca_cert is not None:
                raise ValueError('peer_secret and peer_ca_cert need peer_endpoint')
        elif self.peer_secret is None or not self.peer_secret.get_secret_value():
            raise ValueError('peer_endpoint needs peer_secret')
        return self

    def peer(self) -> PeerConnection | None:
        if self.peer_endpoint is None or self.peer_secret is None:
            return None
        # AnyHttpUrl appends a '/' to a bare host; store the root as given.
        return PeerConnection(endpoint=str(self.peer_endpoint).rstrip('/'), secret=self.peer_secret,
                              ca_cert=self.peer_ca_cert or '')


class PolicyParams(BaseModel):
    policy_name: str
    target_id: UUID
    interval_min: util.Unsigned = 1
    mode: ReplicationMode | None = None
    keep_replicated: Annotated[int, Field(ge=2)] | None = None
    #: The declared RPO objective in seconds. RPO compliance is computed
    #: against this target rather than the derived lag budget. Omitted means
    #: no declared objective.
    rpo_target_seconds: util.OptionalUnsigned = None
    #: Group the policy's volumes into ONE crash-consistent unit: snapshots are
    #: taken as atomically frozen generations and the members fail over
    #: together. Decided at creation only — the group record is created with
    #: the policy and members pin placement from the first attach.
    consistency_group: bool = False


def _group_for(policy: ReplicationPolicy) -> ConsistencyGroup | None:
    """The policy's consistency group, or None for an ordinary policy —
    resolved here so the DTO can report the pinned placement without the
    model layer knowing about DTOs."""
    if not getattr(policy, 'consistency_group', False):
        return None
    return db.get_consistency_group_for_policy(policy.get_id())


def _config_error(e: ReplicationConfigError):
    return HTTPException(status_code=400, detail=str(e))


@api.get('/relationships/{lvol_id}', name='clusters:replication:relationships:detail')
def get_relationship_by_lvol(cluster: Cluster, lvol_id: UUID) -> ReplicationRelationshipDTO:
    """Replication relationship for a volume, resolvable even when the source volume
    has been deleted (e.g. after replication-commit --delete-source). The CSI driver
    uses this to redirect NodeStageVolume to the active volume on the target cluster.
    Always 404 on a sync-replication cluster: its volumes keep their identity on
    both sites."""
    if cluster.sync_replication:
        raise HTTPException(404, f"Volume {lvol_id} is sync-replicated; it has no replication relationship")
    rel = replication_policy_controller.get_relationship(str(lvol_id))
    if rel is None:
        raise HTTPException(404, f"No replication relationship found for volume {lvol_id}")
    return ReplicationRelationshipDTO(**rel)


@api.get('/relationships/{lvol_id}/latest-snapshot',
         name='clusters:replication:relationships:latest-snapshot')
def get_latest_replicated_snapshot(cluster: Cluster, lvol_id: UUID) -> ReplicatedSnapshotDTO:
    """The volume's newest fully replicated snapshot, on the secondary, as a
    cloneable object. Exists for the volume's whole replicated life: a
    test-failover drill (design §14) resolves its test point through this
    read, without touching the real replication state to find out what it is.
    """
    try:
        snap = lvol_controller.latest_replicated_snapshot(str(lvol_id))
    except KeyError as e:
        raise HTTPException(404, str(e))
    except PeerError as e:
        raise HTTPException(status_code=502, detail=f"Peer control plane: {e}")
    if snap is None:
        raise HTTPException(404, f"Volume {lvol_id} has no replicated snapshot yet")
    return ReplicatedSnapshotDTO.from_model(snap)


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
            timeout_sec=parameters.timeout_sec,
            peer=parameters.peer())
    except ReplicationConfigError as e:
        raise _config_error(e)
    except PeerError as e:
        raise HTTPException(status_code=502, detail=f"Peer control plane: {e}")
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
        ReplicationPolicyDTO.from_model(policy, group=_group_for(policy))
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
            keep_replicated=parameters.keep_replicated,
            consistency_group=parameters.consistency_group,
            rpo_target_seconds=parameters.rpo_target_seconds)
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
        get_full=lambda _: ReplicationPolicyDTO.from_model(policy, group=_group_for(policy)),
    )


policy_instance_api = APIRouter(prefix='/{policy_id}')


@policy_instance_api.get('/', name='clusters:replication:policies:detail')
def get_policy(cluster: Cluster, policy: ReplicationPolicy) -> ReplicationPolicyDTO:
    return ReplicationPolicyDTO.from_model(policy, group=_group_for(policy))


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


@policy_instance_api.get('/latest-generation',
                         name='clusters:replication:policies:latest-generation')
def get_latest_replicated_generation(cluster: Cluster,
                                     policy: ReplicationPolicy) -> ReplicatedGenerationDTO:
    """The consistency group's newest fully replicated generation, every
    member as a cloneable object on the secondary. Refused as a 400 when the
    policy has no consistency group, when no generation is complete for
    every current member yet, or when members are already split across
    generations: the same refusal a real group fail-over applies, so a drill
    never addresses a mixed-generation cut.
    """
    try:
        seq, members = replication_policy_controller.latest_replicated_generation(
            policy.get_id())
    except ReplicationConfigError as e:
        raise _config_error(e)
    except PeerError as e:
        raise HTTPException(status_code=502, detail=f"Peer control plane: {e}")
    return ReplicatedGenerationDTO(
        group_seq=seq,
        members=[ReplicatedSnapshotDTO.from_model(snap) for snap in members.values()],
    )


replicated_groups_api = APIRouter()
logger = logging.getLogger(__name__)


@replicated_groups_api.post('/{source_cluster_id}/{group_id}/failover',
                            name='clusters:replication:replicated-groups:failover')
def failover_replicated_group(cluster: Cluster, source_cluster_id: UUID, group_id: UUID) -> dict:
    """Fail the source cluster's consistency group over onto this cluster, from
    the copies here, with the source cluster cut off (control-plane split).

    Unlike ``consistency-groups/{id}/replication/failover`` this needs no record
    of the group, its volumes, or the source cluster in this control plane:
    every copy carries what its clone needs. The response matches that
    endpoint: ``{"members": [...]}``; 409 when no generation with a copy of
    every member is here, or when a member failed (retryable).
    """
    members = replication_policy_controller.failover_replicated_group(
        cluster.get_id(), f"{source_cluster_id}/{group_id}")
    if not members:
        raise HTTPException(
            409, f'no complete replicated generation of group {source_cluster_id}/{group_id} '
                 f'on cluster {cluster.get_id()}')
    failed = [m for m in members if m.get("status") == "failed"]
    if failed:
        # The member details may name internal topology: log identifiers only.
        logger.error("replicated group fail-over incomplete for %s/%s: failed_members=%d lvol_ids=%s",
                     source_cluster_id, group_id, len(failed), [m.get("lvol_id") for m in failed])
        raise HTTPException(409, 'replicated group fail-over incomplete; retry')
    return {"members": [{
        "lvol_id": m.get("lvol_id", ""),
        "status": m.get("status", ""),
        "target_lvol_id": m.get("target_lvol_id", ""),
        "connection_strings": m.get("connection_strings", []),
        "warnings": m.get("warnings", []),
    } for m in members]}


targets_api.include_router(target_instance_api)
policies_api.include_router(policy_instance_api)
api.include_router(targets_api, prefix='/targets')
api.include_router(policies_api, prefix='/policies')
api.include_router(replicated_groups_api, prefix='/replicated-groups')
