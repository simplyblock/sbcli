from typing import List, Optional

from fastapi import APIRouter, Query

from simplyblock_core.controllers import alerts_controller
from simplyblock_core.db_controller import DBController

from .._dependencies import Cluster
from .._dtos import AlertDTO, AlertSeverity, AlertStatus


api = APIRouter()
db = DBController()


@api.get('/', name='clusters:alerts:list')
def list(
        cluster: Cluster,
        severity: Optional[AlertSeverity] = Query(
            None, description="Only return alerts of this severity"),
        history: bool = Query(
            False,
            description="Also return alerts that have already resolved"),
        history_seconds: Optional[int] = Query(
            None, ge=1,
            description="Limit the history to alerts resolved within this many "
                        "seconds. Implies history=true."),
        status: Optional[AlertStatus] = Query(
            None, description="Only return alerts in this state"),
) -> List[AlertDTO]:
    """The conditions in this cluster that currently need an operator.

    This is not the event log. An alert appears only while it is still true
    and disappears on its own once it is not: the node comes back ONLINE, the
    device comes back, the cluster leaves degraded. Conditions an operator
    caused on purpose -- a node they shut down, a device they removed -- are
    not alerts and are not listed.

    By default only what is wrong NOW is returned -- every entry has
    ``status: firing``. Pass ``history=true`` to also get the ones that have
    since resolved, each with its ``resolved_at``, or ``history_seconds=N``
    for just the recent past. Either way both transitions are written to the
    cluster event log as ALERT_RAISED / ALERT_RESOLVED, so a resolution
    reaches an operator whether or not anyone asks for history here.

    Critical sorts before warning, and firing before resolved.
    """
    alerts = alerts_controller.get_alerts(
        cluster.get_id(), include_history=history,
        history_seconds=history_seconds)
    if severity is not None:
        alerts = [a for a in alerts if a['severity'] == severity]
    if status is not None:
        alerts = [a for a in alerts if a.get('status') == status]
    return [AlertDTO.from_alert(alert) for alert in alerts]
