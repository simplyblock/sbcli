import builtins
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field, RootModel

from simplyblock_core.db_controller import DBController
from simplyblock_core import utils as core_utils
from simplyblock_core.controllers import backup_controller, lvol_controller, snapshot_controller
from simplyblock_core.models.lvol_model import LVol

from ...._dependencies import Cluster, StoragePool, Volume
from ...._dtos import BackupDTO, VolumeDTO, SnapshotDTO
from .... import util
from .replication import (
    api as replication_api,
    apply_policy as apply_replication_policy,
    collection_api as replication_collection_api,
)


api = APIRouter()
db = DBController()


@api.get('/', name='clusters:storage-pools:volumes:list')
def list(request: Request, cluster: Cluster, pool: StoragePool) -> builtins.list[VolumeDTO]:
    data = []
    for lvol in db.get_lvols_by_pool_id(pool.get_id()):
        stat_obj = None
        data.append(VolumeDTO.from_model(lvol, request, cluster.get_id(), stat_obj))
    return data


class _CreateParams(BaseModel):
    name: str
    size: util.Size
    max_rw_iops: util.Unsigned = 0
    max_rw_mbytes: util.Unsigned = 0
    max_r_mbytes: util.Unsigned = 0
    max_w_mbytes: util.Unsigned = 0
    ha_type: Literal['single', 'ha'] | None = None
    host_id: str | None = None
    priority_class: Literal[0, 1] = 0
    namespaced: bool | None = False
    pvc_name: str | None = None
    ndcs: util.Unsigned = 0
    npcs: util.Unsigned = 0
    allowed_hosts: builtins.list[str] | None = None
    fabric: str = "tcp"
    # None → resolved by add_lvol_ha: a shareable default for namespaced
    # volumes, 1 otherwise.
    max_namespace_per_subsys: int | None = None
    do_replicate: bool = False
    replication_cluster_id: str | None = None
    # Optional replication policy (id or name): assigning it at create time
    # configures replication for the volume.
    replication_policy: str | None = None
    encrypt: bool = False


class _CloneParams(BaseModel):
    name: str
    snapshot_id: Annotated[str | None, Field(pattern=core_utils.UUID_PATTERN)]
    size: util.Size = 0
    pvc_name: str | None = None
    pvc_namespace: str | None = None
    delete_snap_on_lvol_delete: bool = False


@api.post('/', name='clusters:storage-pools:volumes:create', status_code=201, responses={201: {"content": None}})
def add(
        request: Request, cluster: Cluster, pool: StoragePool,
        parameters: RootModel[_CreateParams | _CloneParams],
        response_format: util.CreationResponseFormatParameter = "empty",
) -> Response:
    data = parameters.root
    try:
        db.get_lvol_by_name(data.name)
        raise HTTPException(409, f'Volume {data.name} exists')
    except KeyError:
        pass

    if isinstance(data, _CreateParams):
        volume_id_or_false, error = lvol_controller.add_lvol_ha(
            name=data.name,
            size=data.size,
            pool_id_or_name=pool.get_id(),
            use_crypto=data.encrypt,
            max_size=0,
            max_rw_iops=data.max_rw_iops,
            max_rw_mbytes=data.max_rw_mbytes,
            max_r_mbytes=data.max_r_mbytes,
            max_w_mbytes=data.max_w_mbytes,
            host_id_or_name=data.host_id,
            ha_type=data.ha_type if data.ha_type is not None else 'default',
            use_comp=False,
            distr_vuid=0,
            lvol_priority_class=data.priority_class,
            namespaced=data.namespaced,
            pvc_name=data.pvc_name,
            ndcs=data.ndcs,
            npcs=data.npcs,
            allowed_hosts=data.allowed_hosts,
            fabric=data.fabric,
            max_namespace_per_subsys=data.max_namespace_per_subsys,
            do_replicate=data.do_replicate,
            replication_cluster_id=data.replication_cluster_id,
            replication_policy=data.replication_policy,
        )
    elif isinstance(data, _CloneParams):
        volume_id_or_false, error = snapshot_controller.clone(
            data.snapshot_id,
            data.name,
            data.size if data.size is not None else 0,
            pvc_name=data.pvc_name,
            pvc_namespace=data.pvc_namespace,
            delete_snap_on_lvol_delete=data.delete_snap_on_lvol_delete,
        )
    else:
        raise AssertionError('unreachable')

    if volume_id_or_false == False:  # noqa
        raise ValueError(error)

    return util.creation_response(
        request, response_format,
        entity_id=UUID(volume_id_or_false),
        route_name='clusters:storage-pools:volumes:detail',
        route_kwargs={
            'cluster_id': UUID(cluster.get_id()),
            'pool_id': UUID(pool.get_id()),
            'volume_id': UUID(volume_id_or_false),
        },
        get_full=lambda id: VolumeDTO.from_model(db.get_lvol_by_id(str(id)), request, cluster.get_id()),
    )


instance_api = APIRouter(prefix='/{volume_id}')


@instance_api.get('/', name='clusters:storage-pools:volumes:detail')
def get(request: Request, cluster: Cluster, pool: StoragePool, volume: Volume) -> VolumeDTO:
    stat_obj = None
    rep_info = lvol_controller.get_replication_info(volume.get_id())
    return VolumeDTO.from_model(volume, request, cluster.get_id(), stat_obj, rep_info)


class UpdatableLVolParams(BaseModel):
    name: str | None = None
    max_rw_iops: util.Unsigned = 0
    max_rw_mbytes: util.Unsigned = 0
    max_r_mbytes: util.Unsigned = 0
    max_w_mbytes: util.Unsigned = 0
    size: util.Size | None = None
    # Omitted leaves the volume's policy alone; null detaches it.
    replication_policy_id: UUID | None = None


@instance_api.put('/', name='clusters:storage-pools:volumes:update', status_code=204, responses={204: {"content": None}})
def update(cluster: Cluster, pool: StoragePool, volume: Volume, body: UpdatableLVolParams) -> Response:
    updatable_attributes = {'name', 'max_rw_iops', 'max_rw_mbytes', 'max_w_mbytes', 'max_r_mbytes'}
    if ((body.model_fields_set & updatable_attributes) and
            not lvol_controller.set_lvol(uuid=volume.get_id(), **{
        key: value
        for key, value
        in body.model_dump().items()
        if key in updatable_attributes
    })):
        raise ValueError('Failed to update volume')

    if 'size' in body.model_fields_set:
        lvol_controller.resize_lvol(volume.get_id(), body.size)

    if 'replication_policy_id' in body.model_fields_set:
        apply_replication_policy(volume, body.replication_policy_id)

    return Response(status_code=204)


@instance_api.delete('/', name='clusters:storage-pools:volumes:delete', status_code=204, responses={204: {"content": None}})
def delete(cluster: Cluster, pool: StoragePool, volume: Volume) -> Response:
    if volume.status == LVol.STATUS_DELETED:
        raise HTTPException(404, "Volume deleted")

    lvol_controller.delete_lvol(volume)
    return Response(status_code=204)


class _AddHostParams(BaseModel):
    host_nqn: str


@instance_api.post('/hosts', name='clusters:storage-pools:volumes:add-host', status_code=201)
def add_host(cluster: Cluster, pool: StoragePool, volume: Volume, body: _AddHostParams):
    result, error = lvol_controller.add_host_to_lvol(volume.get_id(), body.host_nqn)
    if error:
        raise HTTPException(400, error)
    return result


@instance_api.get('/hosts/{host_nqn}/secret', name='clusters:storage-pools:volumes:get-host-secret')
def get_host_secret(cluster: Cluster, pool: StoragePool, volume: Volume, host_nqn: str):
    # Sanctioned secret-egress endpoint: returns the DH-HMAC-CHAP key for the
    # host so the client can configure NVMe-oF auth. The returned dict carries
    # already-unwrapped plain-text key material — that is the contract.
    result, error = lvol_controller.get_host_secret(volume.get_id(), host_nqn)
    if error:
        raise HTTPException(404, error)
    return result


@instance_api.delete('/hosts/{host_nqn}', name='clusters:storage-pools:volumes:remove-host', status_code=204, responses={204: {"content": None}})
def remove_host(cluster: Cluster, pool: StoragePool, volume: Volume, host_nqn: str) -> Response:
    result, error = lvol_controller.remove_host_from_lvol(volume.get_id(), host_nqn)
    if error:
        raise HTTPException(400, error)
    return Response(status_code=204)


@instance_api.post('/inflate', name='clusters:storage-pools:volumes:inflate', status_code=204, responses={204: {"content": None}})
def inflate(cluster: Cluster, pool: StoragePool, volume: Volume) -> Response:
    if not volume.cloned_from_snap:
        raise HTTPException(400, 'Volume must be cloned')
    if not lvol_controller.inflate_lvol(volume.get_id()):
        raise ValueError('Failed to inflate volume')

    return Response(status_code=204)

@instance_api.get('/connect', name='clusters:storage-pools:volumes:connect')
def connect(cluster: Cluster, pool: StoragePool, volume: Volume, host_nqn: str | None = None):
    details, err = lvol_controller.connect_lvol(volume.get_id(), host_nqn=host_nqn)
    if err:
        return Response(status_code=404, content=err)
    return details


@instance_api.get('/capacity', name='clusters:storage-pools:volumes:capacity')
def capacity(cluster: Cluster, pool: StoragePool, volume: Volume, history: str | None = None):
    records_or_false = lvol_controller.get_capacity(volume.get_id(), history, parse_sizes=False)
    if records_or_false == False:  # noqa
        raise ValueError('Failed to compute capacity')
    return records_or_false


@instance_api.get('/iostats', name='clusters:storage-pools:volumes:iostats')
def iostats(cluster: Cluster, pool: StoragePool, volume: Volume, history: str | None = None):
    records_or_false = lvol_controller.get_io_stats(
        volume.get_id(),
        history,
        parse_sizes=False,
        with_sizes=True
    )
    if records_or_false == False:  # noqa
        raise ValueError('Failed to compute iostats')
    return records_or_false


@instance_api.get('/snapshots', name='clusters:storage-pools:volumes:snapshots:list')
def snapshot(request: Request, cluster: Cluster, pool: StoragePool, volume: Volume) -> builtins.list[SnapshotDTO]:
    return [
        SnapshotDTO.from_model(snapshot, request, cluster_id=cluster.get_id(), pool_id=pool.get_id())
        for snapshot
        in db.get_snapshots()
        if snapshot.lvol is not None and snapshot.lvol.get_id() == volume.get_id()
    ]


class _SnapshotParams(BaseModel):
    name: str
    backup: bool = False


@instance_api.post('/snapshots', name='clusters:storage-pools:volumes:snapshots:create', status_code=201, responses={201: {"content": None}})
def create_snapshot(
        request: Request,
        cluster: Cluster, pool: StoragePool, volume: Volume,
        parameters: _SnapshotParams
) -> Response:
    snapshot_id, err_or_false = snapshot_controller.add(
        volume.get_id(), parameters.name, backup=parameters.backup
    )
    if err_or_false:
        # FIXME: snapshot_controller.add() should raise a named exception directly
        # instead of returning a string that we string-match here
        if 'unique' in str(err_or_false).lower():
            raise HTTPException(409, f'Snapshot {parameters.name} already exists')
        raise ValueError(err_or_false)

    entity_url = request.app.url_path_for(
            'clusters:storage-pools:snapshots:detail',
            cluster_id=cluster.get_id(), pool_id=pool.get_id(), snapshot_id=snapshot_id,
    )
    return Response(status_code=201, headers={'Location': entity_url})


@instance_api.route(
        '/suspend',
        name='clusters:storage-pools:volumes:suspend',
        methods=['GET', 'POST'],  # Support both until all clients have switched to POST
)
def suspend(cluster: Cluster, pool: StoragePool, volume: Volume) -> bool:
    return lvol_controller.suspend_lvol(volume.get_id())

@instance_api.route(
        '/resume',
        name='clusters:storage-pools:volumes:resume',
        methods=['GET', 'POST'],  # Support both until all clients have switched to POST
)
def resume(cluster: Cluster, pool: StoragePool, volume: Volume) -> bool:
    return lvol_controller.resume_lvol(volume.get_id())

@instance_api.post('/clone', name='clusters:storage-pools:volumes:clone', status_code=201, responses={201: {"content": None}})
def clone(
        request: Request, cluster: Cluster, pool: StoragePool, volume: Volume,
        clone_name: str,
        new_size: str | None = None,
        pvc_name: str | None = None,
) -> Response:
    size = None
    if new_size is not None:
        try:
            size = core_utils.parse_size(new_size)
        except Exception:
            raise HTTPException(400, f'Invalid new_size value: {new_size!r}')
    clone_id, error = lvol_controller.clone_lvol(volume.get_id(), clone_name, size, pvc_name)
    if not clone_id:
        raise ValueError(error or 'Failed to clone volume')
    entity_url = request.app.url_path_for(
        'clusters:storage-pools:volumes:detail',
        cluster_id=cluster.get_id(),
        pool_id=pool.get_id(),
        volume_id=clone_id,
    )
    return Response(status_code=201, headers={'Location': str(entity_url)})


@instance_api.get('/backups', name='clusters:storage-pools:volumes:backups:list')
def backups(volume: Volume) -> builtins.list[BackupDTO]:
    rows = db.get_backups_by_lvol_id(volume.get_id())
    rows = sorted(rows, key=lambda b: (b.created_at, b.uuid), reverse=True)
    return [BackupDTO.from_model(b) for b in rows]


@instance_api.delete(
    '/backups',
    name='clusters:storage-pools:volumes:backups:delete',
    status_code=204,
    responses={204: {"content": None}},
)
def delete_backups(cluster: Cluster, pool: StoragePool, volume: Volume) -> Response:
    success, error = backup_controller.delete_backups(volume.get_id())
    if error:
        raise HTTPException(400, error)
    return Response(status_code=204)

api.include_router(replication_collection_api)
instance_api.include_router(replication_api, prefix='/replication')
api.include_router(instance_api)
