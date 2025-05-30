from typing import Annotated, Literal, Optional, Tuple, Union

from flask import abort, url_for
from flask_openapi3 import APIBlueprint
from pydantic import BaseModel, Field, RootModel

from simplyblock_core.db_controller import DBController
from simplyblock_core import utils as core_utils
from simplyblock_core.controllers import lvol_controller, snapshot_controller

from .pool import PoolPath
from . import util


api = APIBlueprint('volume', __name__, url_prefix='/volumes')
db = DBController()


@api.get('/')
def list(path: PoolPath):
    return [
        lvol.get_clean_dict()
        for lvol
        in db.get_lvols_by_pool_id(path.pool().get_id())
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


@api.post('/')
def add(path: PoolPath, body: RootModel[Union[_CreateParams, _CloneParams]]):
    data = body.root
    if db.get_lvol_by_name(data.name) is not None:
        abort(409, f'Volume {data.name} exists')

    cluster = path.cluster()
    pool = path.pool()

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

    entity_url = url_for(
            'api.v2.cluster.instance.pool.instance.volume.instance.get',
            cluster_id=cluster.get_id(),
            pool_id=pool.get_id(),
            volume_id=volume_id_or_false,
    )
    return '', 201, {'Location': entity_url}


instance_api = APIBlueprint('instance', __name__, url_prefix='/<volume_id>')


class VolumePath(PoolPath):
    volume_id: str = Field(pattern=core_utils.UUID_PATTERN)

    def volume(self):
        volume = db.get_lvol_by_id(self.volume_id)
        if volume is None:
            abort(404, 'Volume not found')
        return volume


@instance_api.get('/')
def get(path: VolumePath):
    return path.volume().get_clean_dict()


class UpdatableLVolParams(BaseModel):
    name: Optional[str] = None
    max_rw_iops: util.Unsigned = 0
    max_rw_mbytes: util.Unsigned = 0
    max_r_mbytes: util.Unsigned = 0
    max_w_mbytes: util.Unsigned = 0
    size: Optional[util.Size] = None


@instance_api.put('/')
def update(path: VolumePath, body: UpdatableLVolParams):
    volume = path.volume()

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
            abort(400, msg)

    return '', 204


@instance_api.delete('/')
def delete(path: VolumePath):
    volume = path.volume()
    if not lvol_controller.delete_lvol(volume.get_id()):
        raise ValueError('Failed to delete volume')

    return '', 204


@instance_api.post('/inflate')
def inflate(path: VolumePath):
    volume = path.volume()
    if not volume.cloned_from_snap:
        abort(400, 'Volume must be cloned')
    if not lvol_controller.inflate_lvol(volume.get_id()):
        raise ValueError('Failed to inflate volume')

    return '', 204


@instance_api.get('/connect')
def connect(path: VolumePath):
    volume = path.volume()
    details_or_false = lvol_controller.connect_lvol(volume.get_id())
    if details_or_false == False:  # noqa
        raise ValueError('Failed to query connection details')
    return details_or_false


@instance_api.get('/capacity')
def capacity(path: VolumePath, query: util.HistoryQuery):
    volume = path.volume()
    records_or_false = lvol_controller.get_capacity(volume.get_id(), query.history, parse_sizes=False)
    if records_or_false == False:  # noqa
        raise ValueError('Failed to compute capacity')
    return records_or_false


@instance_api.get('/iostats')
def iostats(path: VolumePath, query: util.HistoryQuery):
    volume = path.volume()
    records_or_false = lvol_controller.get_io_stats(
        volume.get_id(),
        query.history,
        parse_sizes=False,
        with_sizes=True
    )
    if records_or_false == False:  # noqa
        raise ValueError('Failed to compute iostats')
    return records_or_false


@instance_api.get('/snapshots')
def snapshot(path: VolumePath):
    volume = path.volume()
    return [
        snapshot.get_clean_dict()
        for snapshot
        in db.get_snapshots()
        if snapshot.lvol.get_id() == volume.get_id()
    ]


class _SnapshotParams(BaseModel):
    name: str


@instance_api.post('/snapshots')
def create_snapshot(path: VolumePath, body: _SnapshotParams):
    pool = path.pool()
    snapshot_id, err_or_false = snapshot_controller.add(
        path.volume().get_id(), body.name
    )
    if err_or_false:
        raise ValueError(err_or_false)
    entity_url = url_for(
            'api.v2.cluster.instance.pool.instance.snapshot.instance.get',
            cluster_id=path.cluster_id, pool_id=pool.get_id(), snapshot_id=snapshot_id,
    )
    return '', 201, {'Location': entity_url}


api.register_api(instance_api)
