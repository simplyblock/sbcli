"""Service (QoS) classes of a cluster: /clusters/{cluster_id}/qos.

A cluster schedules data-plane IO in 8 classes: Default, internal metadata and
migration, up to 5 user classes, and one reserved for snapshot replication.
User classes are configured here declaratively, and so is the share the
replication class gets.
"""
import builtins
from typing import Annotated

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field

from simplyblock_core import constants
from simplyblock_core.controllers import qos_controller

from .._dependencies import Cluster

api = APIRouter()

_MAX_REPLICATION_WEIGHT = 100 - constants.qos_class_meta_and_migration_weight_percent - 1


class QOSClassParams(BaseModel):
    name: Annotated[str, Field(min_length=1, max_length=64, pattern=r'^[A-Za-z][A-Za-z0-9_.-]*$')]
    weight: Annotated[int, Field(gt=0, le=1000)]


class QOSConfigParams(BaseModel):
    #: Relative weight of the Default class; user classes are relative to it.
    default_weight: Annotated[int, Field(gt=0, le=1000)]
    classes: Annotated[builtins.list[QOSClassParams],
                       Field(max_length=constants.QOS_MAX_USER_CLASS_ID)] = []
    #: Percent of IO scheduling given to replication traffic; None keeps it.
    replication_weight: Annotated[int, Field(ge=0, le=_MAX_REPLICATION_WEIGHT)] | None = None


class QOSClassDTO(BaseModel):
    id: int
    name: str
    weight: int


class QOSConfigDTO(BaseModel):
    default_weight: int | None
    classes: builtins.list[QOSClassDTO]
    replication_weight: int
    #: False only on a cluster whose legacy 6th user class holds the
    #: reserved id; replication then runs in Default.
    replication_class_available: bool
    #: Per-class shares (percent) sent to the data plane, indexed by the class
    #: carried in the IO address: 0 Default, 1 metadata/migration, class id k
    #: at k+1, 7 replication.
    weights: builtins.list[int]


@api.get('/', name='clusters:qos:get')
def get(cluster: Cluster) -> QOSConfigDTO:
    return QOSConfigDTO(**qos_controller.get_config(cluster.get_id()))


@api.put('/', name='clusters:qos:set', status_code=204, responses={204: {"content": None}})
def set(cluster: Cluster, parameters: QOSConfigParams) -> Response:
    """Make the cluster's service classes exactly the ones given.

    Classes not listed are removed (refused with 409 while a volume uses one),
    and existing ones keep their id. A request that passes the schema but not
    the controller's checks (for example duplicate names) is answered with 400
    by the general PreconditionError handler. Works on an active cluster: the weights
    reach the online nodes before this returns, and nodes that are not online
    get them when they next start.
    """
    try:
        failed = qos_controller.set_config(
            cluster.get_id(), parameters.default_weight,
            [c.model_dump() for c in parameters.classes],
            parameters.replication_weight)
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    if failed:
        raise HTTPException(503, f"configuration saved, but the weights could not be "
                                 f"sent to nodes {', '.join(failed)}; they get them on restart")
    return Response(status_code=204)
