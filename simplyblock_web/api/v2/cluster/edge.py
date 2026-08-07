# coding=utf-8
"""Edge-cluster API (docs/edge_clusters_spec.md §8): edge-nodes + edge-volumes
under /clusters/{cluster_id}/. DTOs stay local to this module — the _dtos.py
monolith is deliberately not extended."""
import threading
from typing import Annotated, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field

from simplyblock_core.models.cluster import Cluster as ClusterModel
from simplyblock_core import utils as core_utils
from simplyblock_edge import db as edge_db, edge_cluster_ops
from simplyblock_edge.models import EdgeNode, EdgeVolume

from .._dependencies import Cluster
from ..util import Size


def _require_edge(cluster: Cluster) -> ClusterModel:
    if cluster.cluster_type != ClusterModel.TYPE_EDGE:
        raise HTTPException(404, f'Cluster {cluster.get_id()} is not an edge cluster')
    return cluster


EdgeCluster = Annotated[ClusterModel, Depends(_require_edge)]

logger = core_utils.get_logger(__name__)


def _lookup_edge_node(cluster: EdgeCluster, node_id: UUID) -> EdgeNode:
    try:
        return edge_db.get_edge_node_by_id(cluster.get_id(), str(node_id))
    except KeyError as e:
        raise HTTPException(404, str(e))


def _lookup_edge_volume(cluster: EdgeCluster, volume_id: UUID) -> EdgeVolume:
    try:
        return edge_db.get_edge_volume_by_id(cluster.get_id(), str(volume_id))
    except KeyError as e:
        raise HTTPException(404, str(e))


EdgeNodeDep = Annotated[EdgeNode, Depends(_lookup_edge_node)]
EdgeVolumeDep = Annotated[EdgeVolume, Depends(_lookup_edge_volume)]


# ---------------------------------------------------------------------- DTOs

class EdgePartitionDTO(BaseModel):
    device_path: str
    size: int
    status: str

    @staticmethod
    def from_model(partition):
        return EdgePartitionDTO(device_path=partition.device_path,
                                size=partition.size, status=partition.status)


class EdgeNodeDTO(BaseModel):
    uuid: UUID
    hostname: str
    mgmt_ip: str
    data_ip: str
    status: str
    is_primary: bool          # first node added (store index 0)
    # lvs names this node currently LEADS (active/active: normally its own
    # store; after a fail-over the survivor also leads the peer's store).
    leader_of: List[str]
    nvmf_port: int
    partitions: List[EdgePartitionDTO]

    @staticmethod
    def from_model(node: EdgeNode):
        return EdgeNodeDTO(
            uuid=UUID(node.uuid), hostname=node.hostname, mgmt_ip=node.mgmt_ip,
            data_ip=node.get_data_ip(), status=node.status,
            is_primary=node.is_primary, leader_of=list(node.leader_of),
            nvmf_port=node.nvmf_port,
            partitions=[EdgePartitionDTO.from_model(p) for p in node.partitions
                        if p.status != 'removed'])


class EdgeVolumeDTO(BaseModel):
    uuid: UUID
    name: str
    size: int
    nqn: str
    status: str

    @staticmethod
    def from_model(volume: EdgeVolume):
        return EdgeVolumeDTO(uuid=UUID(volume.uuid), name=volume.volume_name,
                             size=volume.size, nqn=volume.nqn, status=volume.status)


class _AddNodeParams(BaseModel):
    hostname: str = Field(min_length=1)
    mgmt_ip: str = Field(min_length=1)
    data_ip: Optional[str] = None
    partitions: List[str] = Field(min_length=1)
    # SPDK vCPUs on this node (1-6); thread placement derives from it
    # (1: everything together; 2: app+lvs / nvmf; 3: one core each;
    # 4-6: extra cores become additional nvmf pollers).
    spdk_cpus: int = Field(default=1, ge=1, le=6)


class _AddDeviceParams(BaseModel):
    device_path: str = Field(min_length=1)


class _ReplaceDeviceParams(BaseModel):
    old_path: str = Field(min_length=1)
    new_path: str = Field(min_length=1)


class _CreateVolumeParams(BaseModel):
    name: str = Field(min_length=1)
    size: Size
    crypto: bool = False


class _ResizeVolumeParams(BaseModel):
    size: Size


# ------------------------------------------------------------ cluster create

create_api = APIRouter()


class _CreateEdgeClusterParams(BaseModel):
    name: str = Field(min_length=1)
    k8s_api_url: str = ""
    k8s_token: str = ""
    k8s_ca_cert: str = ""
    k8s_namespace: str = "simplyblock"


class EdgeClusterCreatedDTO(BaseModel):
    uuid: UUID
    name: str
    status: str
    nqn: str
    # Create-time secret egress: the caller needs it to authenticate as the
    # new cluster (same pattern as hyperscale cluster bootstrap).
    secret: str


@create_api.post('/edge', name='clusters:edge:create', status_code=201)
def create_edge_cluster(parameters: _CreateEdgeClusterParams) -> EdgeClusterCreatedDTO:
    try:
        cluster = edge_cluster_ops.create_edge_cluster(
            parameters.name, k8s_api_url=parameters.k8s_api_url,
            k8s_token=parameters.k8s_token, k8s_ca_cert=parameters.k8s_ca_cert,
            k8s_namespace=parameters.k8s_namespace)
    except ValueError as e:
        raise HTTPException(409, str(e))
    return EdgeClusterCreatedDTO(
        uuid=UUID(cluster.uuid), name=cluster.cluster_name, status=cluster.status,
        nqn=cluster.nqn, secret=cluster.secret.get_secret_value())


# --------------------------------------------------------------- edge-nodes

node_api = APIRouter()


@node_api.get('/', name='clusters:edge-nodes:list')
def list_nodes(cluster: EdgeCluster) -> List[EdgeNodeDTO]:
    return [EdgeNodeDTO.from_model(n)
            for n in edge_db.get_edge_nodes(cluster.get_id())
            if n.status != EdgeNode.STATUS_REMOVED]


@node_api.post('/', name='clusters:edge-nodes:add', status_code=202)
def add_node(cluster: EdgeCluster, parameters: _AddNodeParams) -> Response:
    # Validate the cheap preconditions synchronously so the caller gets a 400
    # instead of a silent background failure; the pod deploy + stack build
    # then runs detached (bounded by the RPC wait timeout).
    nodes = [n for n in edge_db.get_edge_nodes(cluster.get_id())
             if n.status != EdgeNode.STATUS_REMOVED]
    if len(nodes) >= 2:
        raise HTTPException(400, 'Edge clusters support at most 2 nodes')
    if any(n.hostname == parameters.hostname for n in nodes):
        raise HTTPException(400, f'Node {parameters.hostname} is already part of the cluster')
    primary = next((n for n in nodes if n.is_primary), None)
    if primary is not None and primary.lvstore_base:
        raise HTTPException(400, 'Cannot add a node: cluster already has volumes '
                                 'on a single-node layout')

    def _run():
        try:
            edge_cluster_ops.add_edge_node(
                cluster.get_id(), parameters.hostname, parameters.mgmt_ip,
                parameters.partitions, data_ip=parameters.data_ip or "",
                spdk_cpus=parameters.spdk_cpus)
        except Exception:
            logger.exception('Edge node add failed')

    threading.Thread(target=_run, daemon=True).start()
    return Response(status_code=202)


@node_api.get('/{node_id}', name='clusters:edge-nodes:detail')
def get_node(cluster: EdgeCluster, node: EdgeNodeDep) -> EdgeNodeDTO:
    return EdgeNodeDTO.from_model(node)


@node_api.post('/{node_id}/shutdown', name='clusters:edge-nodes:shutdown',
               status_code=204, responses={204: {"content": None}})
def shutdown_node(cluster: EdgeCluster, node: EdgeNodeDep) -> Response:
    edge_cluster_ops.shutdown_node(cluster.get_id(), node.uuid)
    return Response(status_code=204)


@node_api.post('/{node_id}/restart', name='clusters:edge-nodes:restart', status_code=202)
def restart_node(cluster: EdgeCluster, node: EdgeNodeDep) -> dict:
    task_id = edge_cluster_ops.restart_node(cluster.get_id(), node.uuid)
    return {"task_id": task_id}


@node_api.post('/{node_id}/devices', name='clusters:edge-nodes:devices:add', status_code=202)
def add_device(cluster: EdgeCluster, node: EdgeNodeDep, parameters: _AddDeviceParams) -> dict:
    try:
        task_id = edge_cluster_ops.add_device(cluster.get_id(), node.uuid,
                                              parameters.device_path)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"task_id": task_id}


@node_api.put('/{node_id}/devices', name='clusters:edge-nodes:devices:replace', status_code=202)
def replace_device(cluster: EdgeCluster, node: EdgeNodeDep,
                   parameters: _ReplaceDeviceParams) -> dict:
    try:
        task_id = edge_cluster_ops.replace_device(cluster.get_id(), node.uuid,
                                                  parameters.old_path, parameters.new_path)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"task_id": task_id}


@node_api.post('/{node_id}/devices/remove', name='clusters:edge-nodes:devices:remove',
               status_code=204, responses={204: {"content": None}})
def remove_device(cluster: EdgeCluster, node: EdgeNodeDep,
                  parameters: _AddDeviceParams) -> Response:
    try:
        edge_cluster_ops.remove_device(cluster.get_id(), node.uuid,
                                       parameters.device_path)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return Response(status_code=204)


@node_api.post('/{node_id}/devices/restart', name='clusters:edge-nodes:devices:restart',
               status_code=204, responses={204: {"content": None}})
def restart_device(cluster: EdgeCluster, node: EdgeNodeDep,
                   parameters: _AddDeviceParams) -> Response:
    try:
        edge_cluster_ops.restart_device(cluster.get_id(), node.uuid,
                                        parameters.device_path)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return Response(status_code=204)


# ------------------------------------------------------------- edge-volumes

volume_api = APIRouter()


@volume_api.get('/', name='clusters:edge-volumes:list')
def list_volumes(cluster: EdgeCluster) -> List[EdgeVolumeDTO]:
    return [EdgeVolumeDTO.from_model(v)
            for v in edge_db.get_edge_volumes(cluster.get_id())]


@volume_api.post('/', name='clusters:edge-volumes:create', status_code=201)
def create_volume(cluster: EdgeCluster, parameters: _CreateVolumeParams) -> EdgeVolumeDTO:
    try:
        volume = edge_cluster_ops.create_volume(cluster.get_id(), parameters.name,
                                                parameters.size,
                                                crypto=parameters.crypto)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return EdgeVolumeDTO.from_model(volume)


@volume_api.get('/{volume_id}', name='clusters:edge-volumes:detail')
def get_volume(cluster: EdgeCluster, volume: EdgeVolumeDep) -> EdgeVolumeDTO:
    return EdgeVolumeDTO.from_model(volume)


@volume_api.put('/{volume_id}', name='clusters:edge-volumes:resize')
def resize_volume(cluster: EdgeCluster, volume: EdgeVolumeDep,
                  parameters: _ResizeVolumeParams) -> EdgeVolumeDTO:
    try:
        updated = edge_cluster_ops.resize_volume(cluster.get_id(), volume.uuid,
                                                 parameters.size)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return EdgeVolumeDTO.from_model(updated)


@volume_api.delete('/{volume_id}', name='clusters:edge-volumes:delete',
                   status_code=204, responses={204: {"content": None}})
def delete_volume(cluster: EdgeCluster, volume: EdgeVolumeDep) -> Response:
    edge_cluster_ops.delete_volume(cluster.get_id(), volume.uuid)
    return Response(status_code=204)


@volume_api.get('/{volume_id}/connect', name='clusters:edge-volumes:connect')
def connect_volume(cluster: EdgeCluster, volume: EdgeVolumeDep) -> List[dict]:
    try:
        return edge_cluster_ops.get_connect_info(cluster.get_id(), volume.uuid)
    except ValueError as e:
        raise HTTPException(400, str(e))
