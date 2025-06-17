from typing import Optional

from flask import abort, jsonify
from flask_openapi3 import APIBlueprint
from pydantic import BaseModel, Field

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import snapshot_controller
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core import utils as core_utils

from .pool import PoolPath

api = APIBlueprint('snapshot', __name__, url_prefix='/snapshots')
db = DBController()


@api.get('/')
def list(path: PoolPath):
    return [
        snapshot.get_clean_dict()
        for storage_node
        in db.get_storage_nodes_by_cluster_id(path.cluster_id)
        for snapshot
        in db.get_snapshots_by_node_id(storage_node.get_id())
    ]


instance_api = APIBlueprint('instance', __name__, url_prefix='/<snapshot_id>')


class SnapshotPath(PoolPath):
    snapshot_id: str = Field(pattern=core_utils.UUID_PATTERN)

    def snapshot(self) -> SnapShot:
        snapshot = db.get_snapshot_by_id(self.snapshot_id)
        if snapshot is None:
            abort(404, 'Snapshot does not exist')
        return snapshot


@instance_api.get('/')
def get(path: SnapshotPath):
    return path.snapshot().get_clean_dict()


@instance_api.delete('/')
def delete(path: SnapshotPath):
    if not snapshot_controller.delete(path.snapshot().get_id()):
        raise ValueError('Failed to delete snapshot')
    return '', 204


class _CloneParams(BaseModel):
    name: str
    new_size: Optional[int]


@instance_api.post('/clone')
def clone(path: SnapshotPath, body: _CloneParams):
    clone_id, error = snapshot_controller.clone(
        path.snapshot().get_id(),
        body.name,
        body.new_size if body.new_size is not None else 0
    )
    if error:
        raise ValueError('Failed to clone snapshot')

    return jsonify(clone_id)

api.register_api(instance_api)
