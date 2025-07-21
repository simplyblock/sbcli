from typing import Annotated, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import pool_controller
from simplyblock_core import utils as core_utils
from simplyblock_core.models.pool import Pool as PoolModel

from . import util as util
from .cluster import Cluster
from .dtos import StoragePoolDTO


api = APIRouter(prefix='/storage-pools')
db = DBController()


@api.get('/', name='clusters:storage-pools:list')
def list(cluster: Cluster) -> List[StoragePoolDTO]:
    return [
        StoragePoolDTO.from_model(pool)
        for pool
        in db.get_pools()
        if pool.cluster_id == cluster.get_id()
    ]


class StoragePoolParams(BaseModel):
    name: str
    pool_max: util.Unsigned = 0
    volume_max_size: util.Unsigned = 0
    max_rw_iops: util.Unsigned = 0
    max_rw_mbytes: util.Unsigned = 0
    max_r_mbytes: util.Unsigned = 0
    max_w_mbytes: util.Unsigned = 0


@api.post('/', name='clusters:storage-pools:create', status_code=201, responses={201: {"content": None}})
def add(request: Request, cluster: Cluster, parameters: StoragePoolParams) -> Response:
    for pool in db.get_pools():
        if pool.cluster_id == cluster.get_id() and pool.name == parameters.name:
            raise HTTPException(409, f'Pool {parameters.name} already exists')

    id_or_false =  pool_controller.add_pool(
        parameters.name, parameters.pool_max, parameters.volume_max_size, parameters.max_rw_iops, parameters.max_rw_mbytes,
        parameters.max_r_mbytes, parameters.max_w_mbytes, cluster.get_id()
    )

    if not id_or_false:
        raise ValueError('Failed to create pool')

    entity_url = request.app.url_path_for('clusters:storage-pools:detail', cluster_id=cluster.get_id(), pool_id=id_or_false)
    return Response(status_code=201, headers={'Location': entity_url})


instance_api = APIRouter(prefix='/{pool_id}')


def _lookup_storage_pool(pool_id: UUID) -> PoolModel:
    try:
        return db.get_pool_by_id(str(pool_id))
    except KeyError as e:
        raise HTTPException(404, str(e))


StoragePool = Annotated[PoolModel, Depends(_lookup_storage_pool)]


@instance_api.get('/', name='clusters:storage-pools:detail')
def get(cluster: Cluster, pool: StoragePool) -> StoragePoolDTO:
    return StoragePoolDTO.from_model(pool)


@instance_api.delete('/', name='clusters:storage-pools:delete', status_code=204, responses={204: {"content": None}})
def delete(cluster: Cluster, pool: StoragePool) -> Response:
    if pool.status == StoragePool.STATUS_INACTIVE:
        raise HTTPException(400, 'Pool is inactive')

    if not pool_controller.delete_pool(pool.get_id()):
        raise ValueError('Failed to delete pool')

    return Response(status_code=204)


class UpdatableStoragePoolParams(BaseModel):
    name: Optional[str] = None
    max_size: Optional[util.Unsigned] = None
    volume_max_size: Optional[util.Unsigned] = None
    max_rw_iops: Optional[util.Unsigned] = None
    max_rw_mbytes: Optional[util.Unsigned] = None
    max_r_mbytes: Optional[util.Unsigned] = None
    max_w_mbytes: Optional[util.Unsigned] = None


@instance_api.put('/', name='clusters:storage-pools:update', status_code=204, responses={204: {"content": None}})
def update(cluster: Cluster, pool: StoragePool, parameters: UpdatableStoragePoolParams) -> Response:
    names = {
        'max_size': 'pool_max',
        'volume_max_size': 'lvol_max',
    }

    ret, err = pool_controller.set_pool(
        pool.get_id(),
        **{
            names.get(key) or key: value
            for key, value
            in parameters.model_dump().items()
            if key in parameters.model_fields_set
        },
    )
    if err is not None:
        raise ValueError('Failed to update pool')

    return Response(status_code=204)


@instance_api.get('/iostats', name='clusters:storage-pools:iostats')
def iostats(cluster: Cluster, pool: StoragePool, limit: int = 20):
    records = db.get_pool_stats(pool, limit)
    return core_utils.process_records(records, 20)
