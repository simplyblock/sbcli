"""Standalone consistency-group REST surface (design §10).

A consistency group is born from its first labeled member volume (the volume
create path, `storage_pool/volume`), so this router does not create groups. It
exposes the group summary, its live membership, and its generations: taking one,
listing them with expected-versus-present member counts, reading one, and
deleting one. Detaching a member closes its epoch while preserving the snapshots
prior generations depend on (§8.2).
"""
import builtins
from uuid import UUID

from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import (
    consistency_group_controller,
    lvol_controller,
    replication_policy_controller,
)
from simplyblock_core.controllers.consistency_group_controller import ConsistencyGroupError

from .._dependencies import Cluster, ConsistencyGroupResource
from .._dtos import (
    ConsistencyGroupDTO,
    ConsistencyGroupGenerationDTO,
    ConsistencyGroupGenerationMemberDTO,
    ConsistencyGroupMemberDTO,
    ConsistencyGroupMemberJoinDTO,
    ConsistencyGroupReplicationIntentDTO,
    ConsistencyGroupReplicationStatusDTO,
)

api = APIRouter(tags=['consistency-groups'])
db = DBController()


def _generation_dto(row: dict) -> ConsistencyGroupGenerationDTO:
    return ConsistencyGroupGenerationDTO(
        group_seq=row["group_seq"],
        created_at=row["created_at"],
        expected=row["expected"],
        present=row["present"],
        complete=row["complete"],
        members=[ConsistencyGroupGenerationMemberDTO(**m) for m in row["members"]],
    )


@api.get('/', name='clusters:consistency-groups:list',
         response_model=builtins.list[ConsistencyGroupDTO])
def list(cluster: Cluster, name: str | None = None) -> builtins.list[ConsistencyGroupDTO]:
    """List the cluster's consistency groups, or resolve one by name (§10).

    Returns an empty list when ``name`` matches no group, so a caller can probe
    existence without a 404.
    """
    groups = db.get_consistency_groups(cluster.get_id())
    if name is not None:
        groups = [g for g in groups if g.group_name == name]
    return [ConsistencyGroupDTO.from_model(g) for g in groups]


instance_api = APIRouter(prefix='/{group_id}')


@instance_api.get('/', name='clusters:consistency-groups:detail',
                  response_model=ConsistencyGroupDTO)
def get(cluster: Cluster, group: ConsistencyGroupResource) -> ConsistencyGroupDTO:
    return ConsistencyGroupDTO.from_model(group)


@instance_api.get('/members', name='clusters:consistency-groups:members',
                  response_model=builtins.list[ConsistencyGroupMemberDTO])
def members(cluster: Cluster, group: ConsistencyGroupResource) -> builtins.list[ConsistencyGroupMemberDTO]:
    return [ConsistencyGroupMemberDTO(**m)
            for m in consistency_group_controller.list_members(group)]


@instance_api.post('/members', name='clusters:consistency-groups:members:join',
                   response_model=ConsistencyGroupMemberDTO)
def join_member(cluster: Cluster, group: ConsistencyGroupResource,
                body: ConsistencyGroupMemberJoinDTO) -> ConsistencyGroupMemberDTO:
    """Join an EXISTING volume to the group (design §4.5, Phase 4 late join).

    Validates the pinned placement, the pool, the member cap, and the one-way
    rule; a refusal is a 409 naming the precondition. Idempotent: joining a
    current member returns its membership row unchanged.
    """
    try:
        volume = db.get_lvol_by_id(body.lvol_id)
    except KeyError:
        raise HTTPException(404, f'volume {body.lvol_id} not found')
    try:
        consistency_group_controller.join_existing_volume(group, volume)
    except consistency_group_controller.ConsistencyGroupError as e:
        raise HTTPException(409, str(e))
    fresh = db.get_consistency_group_by_id(group.get_id())
    for m in consistency_group_controller.list_members(fresh):
        if m["lvol_id"] == body.lvol_id:
            return ConsistencyGroupMemberDTO(**m)
    raise HTTPException(500, f'volume {body.lvol_id} joined but is not listed as a member')


@instance_api.delete('/members/{lvol_id}', name='clusters:consistency-groups:members:detach',
                     status_code=204, responses={204: {"content": None}})
def detach_member(cluster: Cluster, group: ConsistencyGroupResource, lvol_id: str) -> Response:
    """Detach a member: close its epoch one-way, preserving prior generations (§8.2)."""
    consistency_group_controller.detach_existing_volume(group, lvol_id)
    return Response(status_code=204)


@instance_api.get('/snapshots', name='clusters:consistency-groups:snapshots:list',
                  response_model=builtins.list[ConsistencyGroupGenerationDTO])
def list_generations(cluster: Cluster, group: ConsistencyGroupResource) -> builtins.list[ConsistencyGroupGenerationDTO]:
    return [_generation_dto(r)
            for r in consistency_group_controller.list_generations(group)]


@instance_api.post('/snapshots', name='clusters:consistency-groups:snapshots:take',
                   response_model=ConsistencyGroupGenerationDTO)
def take_generation(cluster: Cluster, group: ConsistencyGroupResource) -> ConsistencyGroupGenerationDTO:
    """Take one crash-consistent generation across every current member (§5)."""
    _ids, err = consistency_group_controller.create_group_snapshot_for_group(group)
    if err is not None:
        raise HTTPException(422, err)
    fresh = db.get_consistency_group_by_id(group.get_id())
    rows = consistency_group_controller.list_generations(fresh)
    return _generation_dto(rows[-1])


@instance_api.get('/snapshots/{seq}', name='clusters:consistency-groups:snapshots:detail',
                  response_model=ConsistencyGroupGenerationDTO)
def get_generation(cluster: Cluster, group: ConsistencyGroupResource, seq: int) -> ConsistencyGroupGenerationDTO:
    for row in consistency_group_controller.list_generations(group):
        if row["group_seq"] == seq:
            return _generation_dto(row)
    raise HTTPException(404, f'generation {seq} not found')


@instance_api.delete('/snapshots/{seq}', name='clusters:consistency-groups:snapshots:delete',
                     status_code=204, responses={204: {"content": None}})
def delete_generation(cluster: Cluster, group: ConsistencyGroupResource, seq: int) -> Response:
    """Delete one generation and all its member snapshots; never the group (§10)."""
    _deleted, err = consistency_group_controller.delete_generation(group, seq)
    if err is not None:
        raise HTTPException(404, err)
    return Response(status_code=204)


@instance_api.put('/replication', name='clusters:consistency-groups:replication:configure',
                  status_code=204, responses={204: {"content": None}})
def configure_replication(cluster: Cluster, group: ConsistencyGroupResource,
                          body: ConsistencyGroupReplicationIntentDTO) -> Response:
    """Enable or disable group replication (design-csi-addons-replication.md
    §14.4): a policy id attaches the whole group to that group replication
    policy; ``null`` detaches it (the group and its members stay grouped by
    label). A refused attach (not a consistency-group policy, missing policy) is
    a 409.
    """
    if body.replication_policy_id is None:
        consistency_group_controller.detach_group_policy(group)
    else:
        try:
            consistency_group_controller.attach_group_policy(
                group, str(body.replication_policy_id))
        except ConsistencyGroupError as e:
            raise HTTPException(409, str(e))
    return Response(status_code=204)


@instance_api.get('/replication/status', name='clusters:consistency-groups:replication:status',
                  response_model=ConsistencyGroupReplicationStatusDTO)
def replication_status(cluster: Cluster, group: ConsistencyGroupResource) -> ConsistencyGroupReplicationStatusDTO:
    """The group's replication status as one unit: oldest recovery point, worst
    member lag and health, summed backlog (design-csi-addons-replication.md
    §14.4/§14.6). Never 404s -- a group with no replicating member reports
    ``state: not_replicating``.
    """
    infos = []
    for member in consistency_group_controller.list_members(group):
        info = lvol_controller.get_replication_info(member["lvol_id"])
        infos.append(info or {"role": "none", "state": "not_replicating"})
    agg = consistency_group_controller.aggregate_group_replication_info(infos)
    return ConsistencyGroupReplicationStatusDTO.from_info(agg)


@instance_api.post('/replication/failover', name='clusters:consistency-groups:replication:failover')
def replication_failover(cluster: Cluster, group: ConsistencyGroupResource) -> dict:
    """Fail the whole group over as ONE unit through its replication policy
    (design-csi-addons-replication.md §14.4): every member is pinned to the same
    group generation, all-or-nothing. Refuses (412) a group not attached to a
    policy.
    """
    if not group.policy_id:
        raise HTTPException(
            412, f'consistency group {group.get_id()} is not attached to a replication policy')
    return {"members": replication_policy_controller.failover_policy(group.policy_id)}


@instance_api.post('/replication/demote', name='clusters:consistency-groups:replication:demote',
                   status_code=204, responses={204: {"content": None}, 202: {"content": None}})
def replication_demote(cluster: Cluster, group: ConsistencyGroupResource) -> Response:
    """Demote the whole group: fence every member and confirm each one's last
    write replicated (design-csi-addons-replication.md §14.4). Re-drivable, not
    queued: 204 once every member is demoted, 202 (with per-member detail) while
    any is still converging, 500 on a hard failure.
    """
    result = consistency_group_controller.demote_group(group)
    if result.get("error"):
        raise HTTPException(500, result["error"])
    if result["demoted"]:
        return Response(status_code=204)
    return JSONResponse(status_code=202, content=result)


class GroupFailbackParams(BaseModel):
    source_cluster_id: UUID | None = None


@instance_api.post('/replication/failback', name='clusters:consistency-groups:replication:failback',
                   status_code=204, responses={204: {"content": None}})
def replication_failback(cluster: Cluster, group: ConsistencyGroupResource,
                         body: GroupFailbackParams) -> Response:
    """Fail the whole group back: point every member's replication back at the
    source cluster (design-csi-addons-replication.md §14.4). The cutover itself is
    each member's own commit.
    """
    result = consistency_group_controller.failback_group(
        group,
        source_cluster_id=str(body.source_cluster_id) if body.source_cluster_id else None,
    )
    if not result["configured"]:
        raise HTTPException(500, f'failed to configure group fail-back: {result["members"]}')
    return Response(status_code=204)


api.include_router(instance_api)
