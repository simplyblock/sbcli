from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, StringConstraints

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import pool_controller
from simplyblock_core import utils as core_utils
from simplyblock_core.models.pool import Pool as PoolModel

from . import util as util
from .cluster import Cluster
from .dtos import PoolDTO


api = APIRouter(prefix='/pools')
db = DBController()


@api.get('/', name='clusters:pools:list')
def list(cluster: Cluster) -> List[PoolDTO]:
    return [
        PoolDTO.from_model(pool)
        for pool
        in db.get_pools()
        if pool.cluster_id == cluster.get_id()
    ]


class PoolParams(BaseModel):
    name: str
    pool_max: util.Unsigned = 0
    lvol_max: util.Unsigned = 0
    secret: bool = True
    max_rw_iops: util.Unsigned = 0
    max_rw_mbytes: util.Unsigned = 0
    max_r_mbytes: util.Unsigned = 0
    max_w_mbytes: util.Unsigned = 0


@api.post('/', name='clusters:pools:create', status_code=201, responses={201: {"content": None}})
def add(cluster: Cluster, parameters: PoolParams) -> Response:
    for pool in db.get_pools():
        if pool.cluster_id == cluster.get_id() and pool.name == parameters.name:
            raise HTTPException(409, f'Pool {parameters.name} already exists')

    id_or_false =  pool_controller.add_pool(
        parameters.name, parameters.pool_max, parameters.lvol_max, parameters.max_rw_iops, parameters.max_rw_mbytes,
        parameters.max_r_mbytes, parameters.max_w_mbytes, parameters.secret, cluster.get_id()
    )

    if not id_or_false:
        raise ValueError('Failed to create pool')

    entity_url = instance_api.url_path_for('get', cluster_id=cluster.get_id(), pool_id=id_or_false)
    return Response(status_code=201, headers={'Location': entity_url})


instance_api = APIRouter(prefix='/<pool_id>')


def _lookup_pool(pool_id: Annotated[str, StringConstraints(pattern=core_utils.UUID_PATTERN)]) -> PoolModel:
    pool = db.get_pool_by_id(pool_id)
    if pool is None:
        raise HTTPException(404, f'Pool {pool_id} not found')

    return pool


Pool = Annotated[PoolModel, Depends(_lookup_pool)]


@instance_api.get('/', name='clusters:pools:detail')
def get(cluster: Cluster, pool: Pool) -> PoolDTO:
    return PoolDTO.from_model(pool)


@instance_api.delete('/', name='clusters:pools:delete', status_code=204, responses={204: {"content": None}})
def delete(cluster: Cluster, pool: Pool) -> Response:
    if pool.status == Pool.STATUS_INACTIVE:
        raise HTTPException(400, 'Pool is inactive')

    if not pool_controller.delete_pool(pool.get_id()):
        raise ValueError('Failed to delete pool')

    return Response(status_code=204)


class UpdatablePoolParams(BaseModel):
    name: Optional[str] = None
    pool_max: Optional[util.Unsigned] = None
    lvol_max: Optional[util.Unsigned] = None
    max_rw_iops: Optional[util.Unsigned] = None
    max_rw_mbytes: Optional[util.Unsigned] = None
    max_r_mbytes: Optional[util.Unsigned] = None
    max_w_mbytes: Optional[util.Unsigned] = None


@instance_api.put('/', name='clusters:pools:update', status_code=204, responses={204: {"content": None}})
def update(cluster: Cluster, pool: Pool, parameters: UpdatablePoolParams) -> Response:
    ret, err = pool_controller.set_pool(
        pool.get_id(),
        **{
            key: value
            for key, value
            in parameters.model_dump().items()
            if key in parameters.model_fields_set
        },
    )
    if err is not None:
        raise ValueError('Failed to update pool')

    return Response(status_code=204)


@instance_api.get('/iostats', name='clusters:pools:iostats')
def iostats(cluster: Cluster, pool: Pool, limit: int = 20):
    records = db.get_pool_stats(pool, limit)
    return core_utils.process_records(records, 20)
