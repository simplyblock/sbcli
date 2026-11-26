"""The stretched control plane of a cluster (docs/stretched-control-plane.md):
its status, the planned leadership move and the operator's takeover.

These routes are the ones a standby or disconnected control plane always
answers itself (the web app's middleware never redirects a path that contains
``/control-plane``): a takeover is issued against the surviving site.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from simplyblock_core.controllers import control_plane_controller
from simplyblock_core.exceptions import (
    ControlPlaneError,
    ControlPlaneNotReadyError,
    ControlPlaneTakeoverRefusedError,
)

from .. import util
from .._dependencies import Cluster
from .._dtos import ControlPlaneStatusDTO, SiteReadinessDTO

api = APIRouter(tags=['control-plane'])


class LeaderSiteParams(BaseModel):
    #: The site whose control plane should lead.
    site: str = Field(min_length=1)
    #: Move even while a readiness gate is red (audited).
    force: bool = False


class TakeoverParams(BaseModel):
    #: The surviving site, which takes the lead.
    site: str = Field(min_length=1)
    #: The leading site the operator confirmed down (out of band); the
    #: command probes it and refuses while anything there answers.
    confirm_site_down: str = Field(min_length=1)
    #: Who confirmed it; recorded in the cluster event.
    operator: str = ""


def _control_plane_errors(e: Exception) -> HTTPException:
    if isinstance(e, ControlPlaneNotReadyError):
        return util.sync_error(409, str(e), problems=e.problems)
    if isinstance(e, ControlPlaneTakeoverRefusedError):
        return util.sync_error(409, str(e), alive=e.alive)
    if isinstance(e, ControlPlaneError):
        return util.sync_error(400, str(e))
    raise e


@api.get('/', name='clusters:control-plane:status')
def status(cluster: Cluster) -> ControlPlaneStatusDTO:
    """The control plane's mode on this site, the lease, and the readiness
    of every site (``failback_ready`` per site: what a planned move gates
    on)."""
    return ControlPlaneStatusDTO.from_status(control_plane_controller.control_plane_status(cluster.get_id()))


@api.post('/leader-site', name='clusters:control-plane:leader-site',
          responses={400: {"description": "Not a stretched cluster, or no control plane on the site"},
                     409: {"description": "The site is not ready (detail.problems); pass force"}})
def set_leader_site(cluster: Cluster, parameters: LeaderSiteParams) -> SiteReadinessDTO:
    """The planned move: refused while a readiness gate of the site is red
    unless ``force``; the lease then drains on the holder and moves within
    a few seconds."""
    try:
        with util.sync_http_errors():
            readiness = control_plane_controller.move_leader_site(
                cluster.get_id(), parameters.site, force=parameters.force)
    except ControlPlaneError as e:
        raise _control_plane_errors(e) from e
    return SiteReadinessDTO.from_readiness(readiness)


@api.post('/takeover', name='clusters:control-plane:takeover',
          responses={400: {"description": "Not a stretched cluster, a wrong site, or a confirmation "
                                          "that does not name the leading site"},
                     409: {"description": "The confirmed site still answers (detail.alive)"}})
def takeover(cluster: Cluster, parameters: TakeoverParams) -> ControlPlaneStatusDTO:
    """The unplanned move after the leading site is down. FoundationDB must
    answer on the surviving site: when the coordinator majority was lost
    with the leader, the FDB recovery comes first."""
    try:
        with util.sync_http_errors():
            result = control_plane_controller.takeover(
                cluster.get_id(), parameters.site, parameters.confirm_site_down,
                operator=parameters.operator)
    except ControlPlaneError as e:
        raise _control_plane_errors(e) from e
    return ControlPlaneStatusDTO.from_status(result)
