from typing import Annotated, Optional

from flask import abort, url_for
from flask_openapi3 import APIBlueprint
from pydantic import BaseModel, Field

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import pool_controller
from simplyblock_core import utils as core_utils
from simplyblock_core.models.pool import Pool

from . import util as util
from .cluster import ClusterPath


api = APIBlueprint('pool', __name__, url_prefix='/pools')
db = DBController()


@api.get('/')
def list(path: ClusterPath):
    return [
        pool.get_clean_dict()
        for pool
        in db.get_pools()
        if pool.cluster_id == path.cluster().get_id()
    ]


class PoolParams(BaseModel):
    name: str
    pool_max: int = Field(0, ge=0)
    lvol_max: int = Field(0, ge=0)
    secret: bool = True
    max_rw_iops: int = Field(0, ge=0)
    max_rw_mbytes: int = Field(0, ge=0)
    max_r_mbytes: int = Field(0, ge=0)
    max_w_mbytes: int = Field(0, ge=0)


@api.post('/')
def add(path: ClusterPath, body: PoolParams):
    cluster = path.cluster()

    for pool in db.get_pools():
        if pool.cluster_id == cluster.get_id() and pool.name == body.name:
            abort(409, f'Pool {body.name} already exists')

    id_or_false =  pool_controller.add_pool(
        body.name, body.pool_max, body.lvol_max, body.max_rw_iops, body.max_rw_mbytes,
        body.max_r_mbytes, body.max_w_mbytes, body.secret, cluster.get_id()
    )

    if not id_or_false:
        raise ValueError('Failed to create pool')
    return None, 201, {'Location': url_for('.get', cluster_id=cluster.get_id(), pool_id=id_or_false)}


instance_api = APIBlueprint('instance', __name__, url_prefix='/<pool_id>')


class PoolPath(ClusterPath):
    pool_id: Annotated[str, Field(core_utils.UUID_PATTERN)]

    def pool(self) -> Pool:
        pool = db.get_pool_by_id(self.pool_id)
        if pool is None:
            abort(404)

        return pool


@instance_api.get('/')
def get(path: PoolPath):
    return path.pool().get_clean_dict()


@instance_api.delete('/')
def delete(path: PoolPath):
    pool = path.pool()

    if pool.status == Pool.STATUS_INACTIVE:
        abort(400, 'Pool is inactive')

    if not pool_controller.delete_pool(path.pool().get_id()):
        raise ValueError('Failed to delete pool')


class UpdatablePoolParams(BaseModel):
    name: Optional[str] = None
    pool_max: Optional[int] = Field(None, ge=0)
    lvol_max: Optional[int] = Field(None, ge=0)
    max_rw_iops: Optional[int] = Field(None, ge=0)
    max_rw_mbytes: Optional[int] = Field(None, ge=0)
    max_r_mbytes: Optional[int] = Field(None, ge=0)
    max_w_mbytes: Optional[int] = Field(None, ge=0)


@instance_api.put('/')
def update(path: PoolPath, body: UpdatablePoolParams):
    abort(501)
    ret, err = pool_controller.set_pool(**{
        key: value
        for key, value
        in body.model_dump()
        if key in body.model_fields_set
    })
    if err is not None:
        raise ValueError('Failed to update pool')


class _LimitQuery(BaseModel):
    limit: int = Field(20)


@instance_api.get('/iostats')
def iostats(path: PoolPath, query: _LimitQuery):
    records = db.get_pool_stats(path.pool(), query.limit)
    return core_utils.process_records(records, 20)


@instance_api.get('/lvol')
def lvols(path: PoolPath):
    return [
        lvol.get_clean_dict()
        for lvol
        in db.get_lvols_by_pool_id(path.pool().get_id())
    ]


api.register_api(instance_api)
