"""Standalone consistency-group REST surface (design §10).

A consistency group is born from its first labeled member volume (the volume
create path, `storage_pool/volume`), so this router does not create groups. It
exposes the group summary, its live membership, and its generations: taking one,
listing them with expected-versus-present member counts, reading one, and
deleting one. Detaching a member closes its epoch while preserving the snapshots
prior generations depend on (§8.2).
"""
import builtins

from fastapi import APIRouter, HTTPException, Response

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import consistency_group_controller

from .._dependencies import Cluster, ConsistencyGroupResource
from .._dtos import (
    ConsistencyGroupDTO,
    ConsistencyGroupGenerationDTO,
    ConsistencyGroupGenerationMemberDTO,
    ConsistencyGroupMemberDTO,
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
        groups = [g for g in groups if g.name == name]
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


@instance_api.delete('/members/{lvol_id}', name='clusters:consistency-groups:members:detach',
                     status_code=204, responses={204: {"content": None}})
def detach_member(cluster: Cluster, group: ConsistencyGroupResource, lvol_id: str) -> Response:
    """Detach a member: close its epoch one-way, preserving prior generations (§8.2)."""
    consistency_group_controller.remove_member_from_group(group, lvol_id)
    # Clear the volume's denormalized group pointer so it reads as a non-member
    # (the migration webhook, §9.5, keys off this); the group's members map
    # keeps the closed epoch for generation history.
    try:
        volume = db.get_lvol_by_id(lvol_id)
        if volume.group_id:
            volume.group_id = ""
            volume.write_to_db(db.kv_store)
    except KeyError:
        pass
    return Response(status_code=204)


@instance_api.get('/snapshots', name='clusters:consistency-groups:snapshots:list',
                  response_model=builtins.list[ConsistencyGroupGenerationDTO])
def list_generations(cluster: Cluster, group: ConsistencyGroupResource) -> builtins.list[ConsistencyGroupGenerationDTO]:
    return [_generation_dto(r)
            for r in consistency_group_controller.list_generations(group)]


@instance_api.post('/snapshots', name='clusters:consistency-groups:snapshots:take',
                   status_code=201, response_model=ConsistencyGroupGenerationDTO)
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


api.include_router(instance_api)
