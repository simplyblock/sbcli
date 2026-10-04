"""Seeding and reading the index keyspace, shared across the suite."""
import json
import uuid

from simplyblock_core import index_ops
from simplyblock_core.models import indices
from simplyblock_core.models.lvol_model import LVol

CLUSTER = "cluster-idx-1"
POOL = "pool-idx-1"


def make_lvol(name, *, uuid_=None, pool=POOL, node="node-1"):
    lvol = LVol()
    lvol.uuid = uuid_ or str(uuid.uuid4())
    lvol.lvol_name = name
    lvol.pool_uuid = pool
    lvol.node_id = node
    lvol.status = LVol.STATUS_ONLINE
    lvol.create_dt = f"2026-01-01 00:00:{len(name):02d}"
    return lvol


def ready(model_cls):
    index_ops.build_indices([model_cls])


def seed_raw(db, obj):
    """Persist a record the way an older release left it: entity only, no index
    entry derived for it."""
    db.kv_store[obj.get_db_id().encode()] = json.dumps(
        obj.to_dict(unwrap_secrets=True)).encode()
    return obj


def index_keys(db, model_cls):
    prefix = f'{indices.INDEX_PREFIX}{model_cls.__name__}/'.encode()
    return {bytes(k): bytes(v) for k, v in db.kv_store.get_range_startswith(prefix)}


def indexed_ids(db, model_cls, index_name):
    """The entity ids one index holds entries for, read the way a reader does."""
    index = indices.get_index(model_cls, index_name)
    return {
        index.entry_id(model_cls, bytes(key), bytes(value))
        for key, value in db.kv_store.get_range_startswith(index.prefix(model_cls, ()))
    }
