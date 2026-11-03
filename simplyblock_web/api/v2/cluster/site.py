"""The sites of a sync-replication cluster: the site-level fence a stretched
cluster's CSI driver issues for csi-addons' ``NetworkFence``
(design-stretched-sync-replication-ramen §10.2), its unfence confirmation,
and the site's record.

Every route answers 400 on a cluster without sync replication and for a site
the cluster does not have (no node of it).
"""
from uuid import UUID

from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from simplyblock_core.controllers import sync_replication_controller

from .. import util
from .._dependencies import Cluster
from .._dtos import SiteFenceTaskDTO, SiteRecordDTO

api = APIRouter(tags=['sync-replication'])

instance_api = APIRouter(prefix='/{site}')


@instance_api.get('/status', name='clusters:sites:status')
def status(cluster: Cluster, site: str) -> SiteRecordDTO:
    """The site's record: whether the cluster records it as lost and in
    which state (``draining`` / ``down`` by the storage-node monitor,
    ``fencing`` / ``done`` by a fence), its latest fence task, its nodes."""
    with util.sync_http_errors():
        record = sync_replication_controller.site_record(cluster.get_id(), site)
    return SiteRecordDTO.from_record(record)


@instance_api.post('/fence', name='clusters:sites:fence', status_code=200,
                   responses={202: {"model": SiteFenceTaskDTO,
                                    "description": "The fence runs: call again (Location: the task)"},
                              409: {"description": "Refused while the site's loss is not proven "
                                                   "(detail.message; detail.task_id names a failed "
                                                   "fence task, reported once)"}})
def fence(request: Request, cluster: Cluster, site: str) -> JSONResponse:
    """Fence the site as a whole: the backend's disaster site steps, run once
    for the site (sync_replication_controller.fence_site) - the liveness
    proof of each of its nodes, then the site recorded lost, its devices
    unavailable, its nodes offline, every volume of every store led from it
    fenced there. Leadership does not move: a forced promote on the
    surviving site opens the volumes afterwards.

    Idempotent and resumable: 202 with the task while the fence runs (the
    call that queues it included - call again), 200 with the site's record
    once the cluster records the site ``done``, 409 while the fence is
    refused - a node of the site may still run, another site is lost, the
    monitor drains the site, a site activation runs, no other site serves,
    or the last fence task failed its proof (reported once, with its
    ``task_id``; the next call queues a new attempt)."""
    with util.sync_http_errors():
        result = sync_replication_controller.fence_site(cluster.get_id(), site)
        record = sync_replication_controller.site_record(cluster.get_id(), site) if result.done else None
    if record is None:
        return JSONResponse(
            status_code=202, content=jsonable_encoder(SiteFenceTaskDTO(task_id=UUID(result.task_id))),
            headers={'Location': str(request.app.url_path_for(
                'clusters:tasks:detail', cluster_id=cluster.get_id(), task_id=result.task_id))})
    return JSONResponse(status_code=200, content=jsonable_encoder(SiteRecordDTO.from_record(record)))


@instance_api.post('/unfence', name='clusters:sites:unfence',
                   responses={409: {"description": "The cluster still records the site as lost"}})
def unfence(cluster: Cluster, site: str) -> SiteRecordDTO:
    """A confirmation, not an action: the site returns by the backend's own
    site activation once its nodes are back. 200 with the site's record once
    the cluster no longer records the site as lost, 409 while it does."""
    with util.sync_http_errors():
        record = sync_replication_controller.confirm_site_unfenced(cluster.get_id(), site)
    return SiteRecordDTO.from_record(record)


api.include_router(instance_api)
