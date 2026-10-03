"""Standalone consistency-group REST surface (design §10).

A consistency group is born from its first labeled member volume (the volume
create path, `storage_pool/volume`), so this router does not create groups. It
exposes the group summary, its live membership, and its generations: taking one,
listing them with expected-versus-present member counts, reading one, and
deleting one. Detaching a member closes its epoch while preserving the snapshots
prior generations depend on (§8.2).
"""
import builtins
import logging
from uuid import UUID

from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import (
    cg_colocation,
    consistency_group_controller,
    lvol_controller,
    migration_controller,
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
    ConsistencyGroupJoinPlanDTO,
    ConsistencyGroupColocateDTO,
    ConsistencyGroupMigrationCreateDTO,
    ConsistencyGroupMigrationDTO,
    ConsistencyGroupMigrationItemDTO,
    ConsistencyGroupReplicationIntentDTO,
    ConsistencyGroupReplicationStatusDTO,
)

logger = logging.getLogger(__name__)

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


@instance_api.delete('/', name='clusters:consistency-groups:delete',
                     status_code=204, responses={204: {"content": None}})
def delete(cluster: Cluster, group: ConsistencyGroupResource) -> Response:
    """Delete an EMPTY consistency group. Refused (409) while it still has a
    current member -- detach or hand them off first. Emptied by a hand-off, the
    group is safe to remove; removing it lets the next hand-off mint a fresh,
    correctly node-pinned group instead of reusing a stale record."""
    try:
        consistency_group_controller.delete_group(group)
    except ConsistencyGroupError as e:
        raise HTTPException(409, str(e))
    return Response(status_code=204)


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


@instance_api.post('/members/plan', name='clusters:consistency-groups:members:plan',
                   response_model=ConsistencyGroupJoinPlanDTO)
def plan_member_join(cluster: Cluster, group: ConsistencyGroupResource,
                     body: ConsistencyGroupMemberJoinDTO) -> ConsistencyGroupJoinPlanDTO:
    """The steps joining an EXISTING volume takes, without taking any
    (co-location design §5). A volume off the group's pinned node/LVS needs a
    live migration first: ``migrate`` names the volumes that move with it (its
    subsystem siblings) and the node. 409 when the join can never succeed (the
    volume is in another group or pool, or its siblings are another group's)."""
    try:
        volume = db.get_lvol_by_id(body.lvol_id)
    except KeyError:
        raise HTTPException(404, f'volume {body.lvol_id} not found')
    try:
        plan = cg_colocation.late_join_plan(group, volume)
    except cg_colocation.ColocationError as e:
        raise HTTPException(409, str(e))
    return ConsistencyGroupJoinPlanDTO(steps=plan.steps, target_node_id=plan.target_node_id,
                                       migrate_lvol_ids=plan.migrate_ids, target_nqn=plan.target_nqn)


@instance_api.post('/members/{lvol_id}/colocate', name='clusters:consistency-groups:members:colocate',
                   status_code=204, responses={204: {"content": None}})
def colocate_member(cluster: Cluster, group: ConsistencyGroupResource, lvol_id: str,
                    body: ConsistencyGroupColocateDTO) -> Response:
    """Move a member's namespace into its group's subsystem, flipping a
    non-member out when it is full. 409 names why not now: namespace moves
    disabled, or a connected host and no client device-mapper swap."""
    try:
        volume = db.get_lvol_by_id(lvol_id)
    except KeyError:
        raise HTTPException(404, f'volume {lvol_id} not found')
    try:
        cg_colocation.colocate_member(group, volume, client_swap_ready=body.client_swap_ready)
    except cg_colocation.ColocationError as e:
        raise HTTPException(409, str(e))
    return Response(status_code=204)


def _group_migration_dto(group, created=None) -> ConsistencyGroupMigrationDTO:
    record = created or group.migration or {}
    return ConsistencyGroupMigrationDTO(
        target_node_id=record.get("target_node_id", ""),
        status=migration_controller.group_migration_status(group.get_id()),
        items=[ConsistencyGroupMigrationItemDTO(**it) for it in record.get("items") or []])


@instance_api.get('/migration', name='clusters:consistency-groups:migration',
                  response_model=ConsistencyGroupMigrationDTO)
def get_group_migration(cluster: Cluster, group: ConsistencyGroupResource) -> ConsistencyGroupMigrationDTO:
    return _group_migration_dto(group)


@instance_api.post('/migration', name='clusters:consistency-groups:migration:create',
                   response_model=ConsistencyGroupMigrationDTO)
def create_group_migration(cluster: Cluster, group: ConsistencyGroupResource,
                           body: ConsistencyGroupMigrationCreateDTO) -> ConsistencyGroupMigrationDTO:
    """Pre-create the migration of the group's whole scope to a node: one
    migration per subsystem, all or none. Attach every item's connect strings
    on the client side, then POST .../migration/start."""
    members = [m["lvol_id"] for m in consistency_group_controller.list_members(group)
               if not m.get("removed_seq")]
    if not members:
        raise HTTPException(409, f'consistency group {group.get_id()} has no member')
    try:
        created = migration_controller.create_group_migration(
            members[0], body.target_node_id, host_nqn=body.host_nqn)
    except (migration_controller.MigrationConflictError, migration_controller.PreconditionError) as e:
        raise HTTPException(409, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))
    fresh = db.get_consistency_group_by_id(group.get_id())
    return _group_migration_dto(fresh, created)


@instance_api.post('/migration/start', name='clusters:consistency-groups:migration:start',
                   status_code=204, responses={204: {"content": None}})
def start_group_migration(cluster: Cluster, group: ConsistencyGroupResource) -> Response:
    try:
        migration_controller.start_group_migration(group.get_id())
    except ValueError as e:
        raise HTTPException(409, str(e))
    return Response(status_code=204)


@instance_api.delete('/migration', name='clusters:consistency-groups:migration:cancel',
                     status_code=204, responses={204: {"content": None}})
def cancel_group_migration(cluster: Cluster, group: ConsistencyGroupResource) -> Response:
    migration_controller.cancel_group_migration(group.get_id())
    return Response(status_code=204)


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
    members = consistency_group_controller.list_members(group)
    present = []
    for member in members:
        try:
            present.append(db.get_lvol_by_id(member["lvol_id"]))
        except KeyError:
            pass   # a member whose volume is gone reports not_replicating below

    # ONE cluster-wide bulk read for every member's lag, backlog and recovery
    # point, instead of a get_replication_info per member -- each of which is
    # several unscoped table scans. An 8-member group's N of those exceeded the
    # csi-addons status RPC deadline, so the VGR's lastSyncTime (and Ramen's
    # lastGroupSyncTime) never populated and the DR gate hung even though the
    # data was replicating (root-caused live 2026-10-01).
    bulk = lvol_controller.get_replication_info_bulk(cluster.get_id(), present)

    # Role is the one field the bulk read omits -- it needs _replication_role's
    # unscoped relationship scan. A consistency group promotes/demotes as one
    # unit, so its members share a role: resolve it ONCE from any replicating
    # member rather than per member.
    group_role = "none"
    for lvol in present:
        if lvol.get_id() in bulk:
            group_role = lvol_controller.replication_role(lvol)
            break

    infos = []
    for member in members:
        info = bulk.get(member["lvol_id"])
        infos.append({**info, "role": group_role} if info
                     else {"role": "none", "state": "not_replicating"})
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
    members = replication_policy_controller.failover_group(group)
    # A group fail-over/-back is all-or-nothing: surface any member failure -- or
    # an empty result, which means nothing was promoted at all -- as a non-2xx so
    # the caller (the csi-addons driver) does not read it as success and promote to
    # a group with no clones (silent no-op, live 2026-09-27, both when a member had
    # no common generation and when a fail-back could not resolve its peer group).
    # 409 is retryable while replication catches up to a common generation.
    if not members:
        raise HTTPException(
            409, f'group fail-over promoted no members for {group.get_id()}: '
                 'no members to fail over, or a fail-back could not resolve its peer group')
    failed = [m for m in members if m.get("status") == "failed"]
    if failed:
        # Do not log per-member error detail strings because they may contain
        # sensitive internal topology/state. Log only sanitized identifiers.
        logger.error("group fail-over incomplete for %s: failed_members=%d lvol_ids=%s",
                     group.get_id(), len(failed), [m.get("lvol_id") for m in failed])
        raise HTTPException(
            409, 'group fail-over incomplete; retry while replication converges')
    safe_members = []
    for m in members:
        safe_members.append({
            "lvol_id": m.get("lvol_id", ""),
            "status": m.get("status", ""),
            "target_lvol_id": m.get("target_lvol_id", ""),
            "connection_strings": m.get("connection_strings", []),
            "warnings": m.get("warnings", []),
        })
    return {"members": safe_members}


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
        logger.error("group demote failed for %s: %s", group.get_id(), result["error"])
        raise HTTPException(500, 'group demote failed')
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
