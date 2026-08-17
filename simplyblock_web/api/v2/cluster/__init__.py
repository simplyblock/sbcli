from threading import Thread
from typing import Annotated, List, Literal, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field, computed_field, model_validator
from pydantic.networks import AnyUrl, UrlConstraints

from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.cluster import HashicorpVaultSettings as ModelVaultSettings
from simplyblock_core import cluster_ops
from simplyblock_core.cluster_ops import SUPPORTED_ERASURE_CODING_SCHEMES

from .._dependencies import Cluster
from .backup import api as backup_api
from .storage_pool import api as pool_api
from .storage_node import api as storage_node_api
from .subsystem import api as subsystem_api
from .task import api as task_api
from .._dtos import ClusterDTO
from .. import util as util


api = APIRouter()
db = DBController()


class _ReplicationParams(BaseModel):
    snapshot_replication_target_cluster: str
    snapshot_replication_timeout: int = 0
    target_pool: Optional[str] = None

class _UpdateParams(BaseModel):
    management_image: Optional[str]
    spdk_image: Optional[str]
    restart: bool = Field(False)


class HashicorpVaultSettings(BaseModel):
    base_url: Optional[Annotated[AnyUrl, UrlConstraints(allowed_schemes=["https"])]] = None
    transit_mount: str = "simplyblock/transit"
    kv_mount: str = "simplyblock/kv"
    cert_role: str = "simplyblock-webappapi"


class ClusterParams(BaseModel):
    name: str = ""
    blk_size: Literal[512, 4096] = 512
    page_size_in_blocks: int = Field(2097152, gt=0)
    cap_warn: util.Percent = 0
    cap_crit: util.Percent = 0
    prov_cap_warn: util.Unsigned = 0
    prov_cap_crit: util.Unsigned = 0
    distr_ndcs: int = 1
    distr_npcs: int = 1
    distr_bs: int = 4096
    distr_chunk_bs: int = 4096
    ha_type: Literal['single', 'ha'] = 'ha'
    qpair_count: int = 256
    max_queue_size: int = 128
    inflight_io_threshold: int = 4
    enable_node_affinity: bool = False
    strict_node_anti_affinity: bool = False
    is_single_node: bool = False
    fabric: str = "tcp"
    cr_name: str = ""
    cr_namespace: str = ""
    cr_plural: str = ""
    client_data_nic: str = ""
    nvmf_base_port: int = 4420
    rpc_base_port: int = 8080
    snode_api_port: int = 50001
    backup_config: Optional[BackupConfig] = None
    hashicorp_vault_settings: Optional[HashicorpVaultSettings] = None
    enable_failure_domain: bool = False

    @model_validator(mode="after")
    def validate_erasure_coding_scheme(self):
        if (self.distr_ndcs, self.distr_npcs) not in SUPPORTED_ERASURE_CODING_SCHEMES:
            raise ValueError("Invalid erasure coding scheme selected")
        return self

    @computed_field
    def max_fault_tolerance(self) -> int:
        return self.distr_npcs


@api.get('/', name='clusters:list')
def list() -> List[ClusterDTO]:
    data = []
    for cluster in db.get_clusters():
        stat_obj = None
        ret = db.get_cluster_capacity(cluster, 1)
        if ret:
            stat_obj = ret[0]
        data.append(ClusterDTO.from_model(cluster, stat_obj))
    return data


@api.post('/', name='clusters:create', status_code=201, responses={201: {"content": None}})
def add(request: Request, parameters: ClusterParams, response_format: util.CreationResponseFormatParameter = "full"):
    try:
        params = parameters.model_dump(exclude_none=True)
        if "hashicorp_vault_settings" in params:
            params["hashicorp_vault_settings"] = ModelVaultSettings(params["hashicorp_vault_settings"])
        if parameters.backup_config is not None:
            params["backup_config"] = parameters.backup_config.to_storage_dict()
        cluster_id_or_false = cluster_ops.add_cluster(**params)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    if not cluster_id_or_false:
        raise ValueError('Failed to create cluster')

    cluster = db.get_cluster_by_id(cluster_id_or_false)

    return util.creation_response(
        request, response_format,
        entity_id=UUID(cluster.get_id()),
        route_name='clusters:detail',
        route_kwargs={'cluster_id': UUID(cluster.get_id())},
        get_full=lambda _: ClusterDTO.from_model(cluster),
    )


instance_api = APIRouter(prefix='/{cluster_id}')


@instance_api.get('/', name='clusters:detail')
def get(cluster: Cluster) -> ClusterDTO:
    stat_obj = None
    ret = db.get_cluster_capacity(cluster, 1)
    if ret:
        stat_obj = ret[0]
    return ClusterDTO.from_model(cluster, stat_obj)


class UpdatableClusterParameters(BaseModel):
    name: Optional[str] = None


@instance_api.put('/', name='clusters:update')
def update(cluster: Cluster, parameters: UpdatableClusterParameters):
    if parameters.name is not None:
        cluster_ops.set_name(cluster.get_id(), parameters.name)

    return Response(status_code=204)


@instance_api.get('/backup-config', name='clusters:backup-config:get')
def get_backup_config(cluster: Cluster) -> BackupConfig:
    """The cluster's backup configuration, with credentials masked.

    ``BackupConfig`` carries ``SecretStr``, which FastAPI's JSON serialization
    renders as ``**********``.
    """
    try:
        return cluster.get_backup_config()
    except PreconditionError as e:
        raise HTTPException(404, str(e)) from e


@instance_api.put('/backup-config', name='clusters:backup-config:set',
                  status_code=204, responses={204: {"content": None}})
def set_backup_config(cluster: Cluster, parameters: BackupConfig) -> Response:
    """Replace the cluster's backup configuration.

    Backup configuration used to be settable only at cluster-create time, which
    left no way to correct or complete it -- notably no way to record a region
    on a cluster created before it was mandatory.

    A full replacement rather than a patch: the fields interact (an endpoint
    implies addressing style and TLS expectations), so merging half a config
    into an existing one produces combinations nobody chose.
    """
    cluster_ops.set_backup_config(cluster.get_id(), parameters)
    return Response(status_code=204)


@instance_api.delete('/', name='clusters:delete', status_code=204, responses={204: {"content": None}})
def delete(cluster: Cluster) -> Response:
    try:
        cluster_ops.delete_cluster(cluster.get_id())
    except ValueError as e:
        raise HTTPException(409, str(e)) from e
    return Response(status_code=204)


@instance_api.get('/capacity', name='clusters:capacity')
def capacity(cluster: Cluster, history: Optional[str] = None):
    capacity_or_false = cluster_ops.get_capacity(
            cluster.get_id(), history)
    if not capacity_or_false:
        raise ValueError('Failed to compute capacity')

    return capacity_or_false


@instance_api.get('/iostats', name='clusters:iostats')
def iostats(cluster: Cluster, history: Optional[str] = None):
    iostats_or_false = cluster_ops.get_iostats_history(
            cluster.get_id(), history, with_sizes=True)
    if not iostats_or_false:
        raise ValueError('Failed to compute capacity')

    return iostats_or_false


@instance_api.get('/logs', name='clusters:logs')
def logs(cluster: Cluster, limit: int = 50):
    logs_or_false = cluster_ops.get_logs(
            cluster.get_id(), is_json=True, limit=limit)
    if not logs_or_false:
        raise ValueError('Failed to access logs')

    return logs_or_false


@instance_api.post('/start', name='clusters:start', status_code=202, responses={202: {"content": None}})
def start(cluster: Cluster) -> Response:
    Thread(
        target=cluster_ops.cluster_grace_startup,
        args=(cluster.get_id(),),
    ).start()
    return Response(status_code=202)  # FIXME: Provide URL for checking task status


@instance_api.post('/shutdown', name='clusters:shutdown', status_code=202, responses={202: {"content": None}})
def shutdown(cluster: Cluster) -> Response:
    Thread(
        target=cluster_ops.cluster_grace_shutdown,
        args=(cluster.get_id(),),
    ).start()
    return Response(status_code=202)  # FIXME: Provide URL for checking task status


@instance_api.post('/activate', name='clusters:activate', status_code=202, responses={202: {"content": None}})
def activate(cluster: Cluster) -> Response:
    Thread(
        target=cluster_ops.cluster_activate,
        args=(cluster.get_id(),),
    ).start()
    return Response(status_code=202)  # FIXME: Provide URL for checking task status

@instance_api.post('/addreplication', name='clusters:addreplication', status_code=202, responses={202: {"content": None}})
def cluster_add_replication(cluster: Cluster, parameters: _ReplicationParams) -> Response:
    cluster_ops.add_replication(
        source_cl_id=cluster.get_id(),
        target_cl_id=parameters.snapshot_replication_target_cluster,
        timeout=parameters.snapshot_replication_timeout,
        target_pool=parameters.target_pool
    )
    return Response(status_code=202)

@instance_api.post('/expand', name='clusters:expand', status_code=202, responses={202: {"content": None}})
def expand(cluster: Cluster) -> Response:
    Thread(
        target=cluster_ops.cluster_expand,
        args=(cluster.get_id(),),
    ).start()
    return Response(status_code=202)  # FIXME: Provide URL for checking task status

@instance_api.post('/update', name='clusters:upgrade', status_code=204, responses={204: {"content": None}})
def update_cluster( cluster: Cluster, parameters: _UpdateParams) -> Response:
    cluster_ops.update_cluster(
        cluster_id=cluster.get_id(),
        mgmt_image=parameters.management_image,
        mgmt_only=parameters.spdk_image is None and not parameters.restart,
        spdk_image=parameters.spdk_image,
        restart=parameters.restart
    )
    return Response(status_code=204)


@instance_api.post('/rebalance', name='clusters:rebalance', status_code=204, responses={204: {"content": None}})
def rebalance_cluster( cluster: Cluster) -> Response:
    cluster_ops.rebalance(cluster.get_id())
    return Response(status_code=204)


instance_api.include_router(storage_node_api, prefix='/storage-nodes')
instance_api.include_router(task_api, prefix='/tasks')
instance_api.include_router(pool_api, prefix='/storage-pools')
instance_api.include_router(backup_api, prefix='/backups')
instance_api.include_router(subsystem_api, prefix='/subsystems')
api.include_router(instance_api)
