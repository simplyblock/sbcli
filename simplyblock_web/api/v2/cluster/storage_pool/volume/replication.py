from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from simplyblock_core.controllers import lvol_controller, replication_policy_controller
from simplyblock_core.controllers.replication_policy_controller import ReplicationConfigError
from simplyblock_core.models.lvol_model import LVol

from .... import util
from ...._dependencies import Cluster, StoragePool, Volume
from ...._dtos import ReplicationMode, ReplicationRelationshipDTO, TaskDTO


api = APIRouter(tags=['replication'])
collection_api = APIRouter(tags=['replication'])


def apply_policy(volume: LVol, policy_id: Optional[UUID]) -> None:
    """Put *volume* under replication policy *policy_id*, or take it out (None).

    Changing policy is detach-then-attach, so the new target receives a FULL
    copy. Detaching stops replication and deletes the internal replication
    snapshots on both sides.
    """
    if policy_id is None:
        try:
            replication_policy_controller.detach_policy(volume.get_id())
        except ReplicationConfigError as e:
            raise HTTPException(409, str(e))  # a cutover is in flight
        except KeyError as e:
            raise HTTPException(404, str(e))
    else:
        try:
            replication_policy_controller.attach_policy(volume.get_id(), str(policy_id))
        except ReplicationConfigError as e:
            raise HTTPException(400, str(e))
        except KeyError as e:
            raise HTTPException(404, str(e))


@api.get('/', name='clusters:storage-pools:volumes:replication:detail')
def get_relationship(cluster: Cluster, pool: StoragePool, volume: Volume) -> ReplicationRelationshipDTO:
    """Resolve a volume to its counterpart on the other cluster.

    Answers "what is the TARGET volume uuid for this SOURCE volume uuid" (and the
    reverse). Before this the ids were only returned by the fail-over or commit
    call itself, so a caller that had not kept them could not find the target
    volume through the API at all -- LVolReplication was exposed nowhere.
    """
    relationship = replication_policy_controller.get_relationship(volume.get_id())
    if relationship is None:
        raise HTTPException(404, 'Volume has no replication relationship')
    return ReplicationRelationshipDTO(**relationship)


class ReplicationStartParams(BaseModel):
    replication_cluster_id: Optional[UUID] = None  # destination; None = cluster default
    mode: Optional[ReplicationMode] = None
    interval_min: Optional[util.Unsigned] = None


@api.post('/start', name='clusters:storage-pools:volumes:replication:start',
          status_code=204, responses={204: {"content": None}})
def start(cluster: Cluster, pool: StoragePool, volume: Volume,
          body: Optional[ReplicationStartParams] = None) -> Response:
    """Start replicating a volume.

    The destination is the request's replication_cluster_id, else the cluster's
    configured target. It used to pass the PATH cluster — the volume's OWN
    cluster — as the destination, which self-targets and never falls back to the
    configured target, so replication could not be started correctly over REST
    at all. mode/interval_min were likewise unreachable.
    """
    parameters = body or ReplicationStartParams()
    if not lvol_controller.replication_start(
            volume.get_id(),
            replication_cluster_id=(
                str(parameters.replication_cluster_id)
                if parameters.replication_cluster_id else None
            ),
            mode=parameters.mode,
            interval_min=parameters.interval_min):
        raise HTTPException(500, 'Failed to start volume snapshot replication')

    return Response(status_code=204)


@api.post('/stop', name='clusters:storage-pools:volumes:replication:stop',
          status_code=204, responses={204: {"content": None}})
def stop(cluster: Cluster, pool: StoragePool, volume: Volume) -> Response:
    if not lvol_controller.replication_stop(volume.get_id()):
        raise HTTPException(500, 'Failed to stop volume snapshot replication')

    return Response(status_code=204)


@api.post('/trigger', name='clusters:storage-pools:volumes:replication:trigger',
          status_code=204, responses={204: {"content": None}})
def trigger(cluster: Cluster, pool: StoragePool, volume: Volume) -> Response:
    if not lvol_controller.replication_trigger(volume.get_id()):
        raise HTTPException(500, 'Failed to start volume snapshot replication')

    return Response(status_code=204)


@api.post('/failover', name='clusters:storage-pools:volumes:replication:failover',
          status_code=204, responses={204: {"content": None}})
def failover(cluster: Cluster, pool: StoragePool, volume: Volume,
             generation: int = 0) -> Response:
    """Bring the volume up on the target cluster.

    The counterpart's id is read back from this volume's replication
    relationship, its connection paths from the target volume's `connect`.

    ``generation`` selects WHICH retained point-in-time to come up on: 0 (the
    default) is the newest, 1 the one before it, and so on through the
    history a retention schedule keeps. Failing over to an older generation
    is the recovery path for a logical corruption, which the newest copy has
    faithfully replicated.
    """
    if generation < 0:
        raise HTTPException(400, 'generation cannot be negative')
    result = lvol_controller.replicate_lvol_on_target_cluster(
        volume.get_id(), generation=generation)
    if isinstance(result, tuple):  # (False, error)
        raise HTTPException(500, str(result[1]))
    if not result:
        raise HTTPException(500, 'Failed to fail the volume over to the target cluster')

    # Consistency groups: an older generation may not match current
    # membership; the operator must SEE that, so warnings turn the empty 204
    # into a 200 with a body (requirement: API response, not only a log).
    if isinstance(result, dict) and result.get("warnings"):
        return JSONResponse(status_code=200,
                            content={"warnings": result["warnings"]})

    return Response(status_code=204)


class CommitParams(BaseModel):
    delete_source: bool = False


@api.post('/commit', name='clusters:storage-pools:volumes:replication:commit',
          status_code=202, responses={202: {"content": None}})
def commit(request: Request, cluster: Cluster, pool: StoragePool, volume: Volume,
           body: Optional[CommitParams] = None) -> Response:
    """Queue the planned cutover. Progress is the returned task.

    delete_source=True instructs the task runner to delete the source volume
    after the cutover succeeds.
    """
    params = body or CommitParams()
    result = lvol_controller.replication_commit(volume.get_id(),
                                                delete_source=params.delete_source)
    if isinstance(result, tuple):  # (False, error)
        raise HTTPException(500, str(result[1]))
    if not result:
        raise HTTPException(500, 'Failed to queue the replication cutover')

    return Response(status_code=202, headers={'Location': str(request.app.url_path_for(
        'clusters:tasks:detail',
        cluster_id=cluster.get_id(), task_id=result['task_id'],
    ))})


class FailbackParams(BaseModel):
    source_cluster_id: Optional[UUID] = None


@api.post('/failback', name='clusters:storage-pools:volumes:replication:failback',
          status_code=204, responses={204: {"content": None}})
def failback(cluster: Cluster, pool: StoragePool, volume: Volume, body: FailbackParams) -> Response:
    """Point replication back at a source cluster. The cutover itself is
    `commit`."""
    result = lvol_controller.replication_failback(
        volume.get_id(),
        source_cluster_id=str(body.source_cluster_id) if body.source_cluster_id else None,
    )
    if isinstance(result, tuple):  # (False, error)
        raise HTTPException(500, str(result[1]))
    if not result:
        raise HTTPException(500, 'Failed to configure fail-back of the volume')

    return Response(status_code=204)


@api.post('/cutover-proceed', name='clusters:storage-pools:volumes:replication:cutover-proceed',
          status_code=204, responses={204: {"content": None}})
def cutover_proceed(cluster: Cluster, pool: StoragePool, volume: Volume) -> Response:
    """Signal that target NVMe paths are connected and cutover may proceed.

    Called by the operator after its preconnect Job succeeds. The task runner
    is suspended waiting for this signal; once set, it advances to the ANA flip.
    """
    try:
        replication_policy_controller.set_cutover_proceed(volume.get_id())
    except KeyError as exc:
        raise HTTPException(404, str(exc))
    return Response(status_code=204)


@api.get('/tasks', name='clusters:storage-pools:volumes:replication:tasks')
def list_tasks(cluster: Cluster, pool: StoragePool, volume: Volume) -> List[TaskDTO]:
    return [TaskDTO.from_model(task) for task in lvol_controller.list_replication_tasks(volume.get_id())]


class ReplicateLVolParams(BaseModel):
    lvol_id: UUID


@collection_api.post('/replicate_lvol_on_source_cluster',
                     name='clusters:storage-pools:replicate_lvol_on_source_cluster',
                     status_code=204, responses={204: {"content": None}})
def replicate_lvol_on_source_cluster(cluster: Cluster, pool: StoragePool,
                                     body: ReplicateLVolParams) -> Response:
    """Rebuild a volume on the source cluster.

    Collection-scoped rather than an operation on `/{volume_id}`: the volume is
    typically gone from the source cluster by the time this is called, so the
    controller falls back to resolving the id through the replication records.
    """
    result = lvol_controller.replicate_lvol_on_source_cluster(
        str(body.lvol_id), cluster.get_id(), pool.get_id())
    if isinstance(result, tuple):  # (False, error)
        raise HTTPException(500, str(result[1]))
    if not result:
        raise HTTPException(500, 'Failed to rebuild the volume on the source cluster')

    return Response(status_code=204)
