from uuid import UUID
from typing import Annotated, List, Literal, Optional, Tuple, Union

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field, StringConstraints, RootModel

from simplyblock_core.db_controller import DBController
from simplyblock_core import utils as core_utils
from simplyblock_core.controllers import lvol_controller, snapshot_controller
from simplyblock_core.models.lvol_model import LVol

from .cluster import Cluster
from .pool import Pool
from .snapshot import instance_api as snapshot_instance_api, SnapshotDTO
from . import util


api = APIRouter(prefix='/volumes')
db = DBController()


class VolumeDTO(BaseModel):
    id: UUID
    name: str
    status: str
    health_check: bool
    nqn: str
    size: util.Unsigned
    cloned_from: Optional[util.UrlPath]
    max_rw_iops: util.Unsigned
    max_rw_mbytes: util.Unsigned
    max_r_mbytes: util.Unsigned
    max_w_mbytes: util.Unsigned
    ha: bool

    @staticmethod
    def from_model(model: LVol, cluster_id: str):
        return VolumeDTO(
            id=UUID(model.get_id()),
            name=model.name,
            status=model.status,
            health_check=model.health_check,
            nqn=model.nqn,
            size=model.size,
            cloned_from=snapshot_instance_api.url_path_for(
                'get',
                cluster_id=cluster_id,
                pool_id=model.pool_uuid,
                snapshot_id=model.cloned_from_snap
            ) if model.cloned_from_snap else None,
            ha=model.ha_type == 'ha',
            max_rw_iops=model.rw_ios_per_sec,
            max_rw_mbytes=model.rw_mbytes_per_sec,
            max_r_mbytes=model.r_mbytes_per_sec,
            max_w_mbytes=model.w_mbytes_per_sec,
        )


@api.get('/')
def list(cluster: Cluster, pool: Pool) -> List[VolumeDTO]:
    return [
        VolumeDTO.from_model(lvol, cluster.get_id())
        for lvol
        in db.get_lvols_by_pool_id(pool.get_id())
    ]


class _CreateParams(BaseModel):
    name: str
    size: util.Size
    crypto_key: Optional[Tuple[str, str]] = None
    max_rw_iops: util.Unsigned = 0
    max_rw_mbytes: util.Unsigned = 0
    max_r_mbytes: util.Unsigned = 0
    max_w_mbytes: util.Unsigned = 0
    ha_type: Optional[Literal['single', 'ha']] = None
    host_id: Optional[str] = None
    lvol_priority_class: Literal[0, 1] = 0
    namespace: Optional[str] = None
    pvc_name: Optional[str] = None


class _CloneParams(BaseModel):
    name: str
    snapshot_id: Annotated[Optional[str], Field(pattern=core_utils.UUID_PATTERN)]
    size: util.Size = 0


@api.post('/', status_code=201, responses={201: {"content": None}})
def add(
        cluster: Cluster, pool: Pool,
        parameters: RootModel[Union[_CreateParams, _CloneParams]]
) -> Response:
    data = parameters.root
    if db.get_lvol_by_name(data.name) is not None:
        raise HTTPException(409, f'Volume {data.name} exists')

    if isinstance(data, _CreateParams):
        volume_id_or_false, error = lvol_controller.add_lvol_ha(
            name=data.name,
            size=data.size,
            pool_id_or_name=pool.get_id(),
            use_crypto=data.crypto_key is not None,
            max_size=0,
            max_rw_iops=data.max_rw_iops,
            max_rw_mbytes=data.max_rw_mbytes,
            max_r_mbytes=data.max_r_mbytes,
            max_w_mbytes=data.max_w_mbytes,
            host_id_or_name=data.host_id,
            ha_type=data.ha_type if data.ha_type is not None else 'default',
            crypto_key1=data.crypto_key[0] if data.crypto_key is not None else None,
            crypto_key2=data.crypto_key[1] if data.crypto_key is not None else None,
            use_comp=False,
            distr_vuid=0,
            lvol_priority_class=data.lvol_priority_class,
            namespace=data.namespace,
            pvc_name=data.pvc_name,
        )
    elif isinstance(data, _CloneParams):
        volume_id_or_false, error = snapshot_controller.clone(
            data.snapshot_id,
            data.name,
            data.size if data.size is not None else 0,
        )
    else:
        raise AssertionError('unreachable')

    if volume_id_or_false == False:  # noqa
        raise ValueError(error)

    entity_url = instance_api.url_path_for(
            'get',
            cluster_id=cluster.get_id(),
            pool_id=pool.get_id(),
            volume_id=volume_id_or_false,
    )
    return Response(status_code=201, headers={'Location': entity_url})


instance_api = APIRouter(prefix='/<volume_id>')


def _lookup_volume(volume_id: Annotated[str, StringConstraints(pattern=core_utils.UUID_PATTERN)]) -> LVol:
    volume = db.get_lvol_by_id(volume_id)
    if volume is None:
        raise HTTPException(404, f'Volume {volume_id} not found')
    return volume


Volume = Annotated[LVol, Depends(_lookup_volume)]


@instance_api.get('/')
def get(cluster: Cluster, pool: Pool, volume: Volume) -> VolumeDTO:
    return VolumeDTO.from_model(volume, cluster.get_id())


class UpdatableLVolParams(BaseModel):
    name: Optional[str] = None
    max_rw_iops: util.Unsigned = 0
    max_rw_mbytes: util.Unsigned = 0
    max_r_mbytes: util.Unsigned = 0
    max_w_mbytes: util.Unsigned = 0
    size: Optional[util.Size] = None


@instance_api.put('/', status_code=204, responses={204: {"content": None}})
def update(cluster: Cluster, pool: Pool, volume: Volume, body: UpdatableLVolParams) -> Response:
    updatable_attributes = {'name', 'max_rw_iops', 'max_rw_mbytes', 'max_w_mbytes', 'max_r_mbytes'}
    if ((body.model_fields_set & updatable_attributes) and
            not lvol_controller.set_lvol(uuid=volume.get_id(), **{
        key: value
        for key, value
        in body.model_dump().items()
        if key in updatable_attributes
    })):
        raise ValueError('Failed to update lvol')

    if 'size' in body.model_fields_set:
        success, msg = lvol_controller.resize_lvol(volume.get_id(), body.size)
        if not success:
            raise HTTPException(400, msg)

    return Response(status_code=204)


@instance_api.delete('/', status_code=204, responses={204: {"content": None}})
def delete(cluster: Cluster, pool: Pool, volume: Volume) -> Response:
    if not lvol_controller.delete_lvol(volume.get_id()):
        raise ValueError('Failed to delete volume')

    return Response(status_code=204)


@instance_api.post('/inflate', status_code=204, responses={204: {"content": None}})
def inflate(cluster: Cluster, pool: Pool, volume: Volume) -> Response:
    if not volume.cloned_from_snap:
        raise HTTPException(400, 'Volume must be cloned')
    if not lvol_controller.inflate_lvol(volume.get_id()):
        raise ValueError('Failed to inflate volume')

    return Response(status_code=204)


@instance_api.get('/connect')
def connect(cluster: Cluster, pool: Pool, volume: Volume):
    details_or_false = lvol_controller.connect_lvol(volume.get_id())
    if details_or_false == False:  # noqa
        raise ValueError('Failed to query connection details')
    return details_or_false


@instance_api.get('/capacity')
def capacity(cluster: Cluster, pool: Pool, volume: Volume, history: Optional[str] = None):
    records_or_false = lvol_controller.get_capacity(volume.get_id(), history, parse_sizes=False)
    if records_or_false == False:  # noqa
        raise ValueError('Failed to compute capacity')
    return records_or_false


@instance_api.get('/iostats')
def iostats(cluster: Cluster, pool: Pool, volume: Volume, history: Optional[str] = None):
    records_or_false = lvol_controller.get_io_stats(
        volume.get_id(),
        history,
        parse_sizes=False,
        with_sizes=True
    )
    if records_or_false == False:  # noqa
        raise ValueError('Failed to compute iostats')
    return records_or_false


@instance_api.get('/snapshots')
def snapshot(cluster: Cluster, pool: Pool, volume: Volume) -> List[SnapshotDTO]:
    return [
        SnapshotDTO.from_model(snapshot, cluster_id=cluster.get_id(), pool_id=pool.get_id())
        for snapshot
        in db.get_snapshots()
        if snapshot.lvol is not None and snapshot.lvol.get_id() == volume.get_id()
    ]


class _SnapshotParams(BaseModel):
    name: str


@instance_api.post('/snapshots', status_code=201, responses={201: {"content": None}})
def create_snapshot(
        cluster: Cluster, pool: Pool, volume: Volume,
        parameters: _SnapshotParams
) -> Response:
    snapshot_id, err_or_false = snapshot_controller.add(
        volume.get_id(), parameters.name
    )
    if err_or_false:
        raise ValueError(err_or_false)

    entity_url = snapshot_instance_api.url_path_for(
            'get',
            cluster_id=cluster.get_id(), pool_id=pool.get_id(), snapshot_id=snapshot_id,
    )
    return Response(status_code=201, headers={'Location': entity_url})


api.include_router(instance_api)
