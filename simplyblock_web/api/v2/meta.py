import fdb
from fastapi import APIRouter
from fastapi.responses import PlainTextResponse, Response

from simplyblock_core.db_controller import DBController

api = APIRouter()


@api.get('/health', status_code=204, response_class=Response)
def health() -> Response:
    """Liveness probe: succeeds whenever the process can serve requests."""
    return Response(status_code=204)


@api.get('/fdb-datacenters')
def fdb_datacenters() -> dict:
    """The FoundationDB data centers (``dcid``) with processes in the database.
    A stretched control plane's operator reads it to learn that the other
    site's region has joined (docs/stretched-control-plane.md)."""
    from simplyblock_core.controllers import control_plane_controller
    return {"datacenters": control_plane_controller.current_fdb_datacenters()}


@api.get('/ready', status_code=204, response_class=Response)
def ready() -> Response:
    """Readiness probe: succeeds when the FoundationDB backend is reachable."""
    db = DBController()
    if db.kv_store is None:
        return Response("FDB not ready", status_code=503)

    try:
        tr = db.kv_store.create_transaction()
        tr.options.set_timeout(1000)
        tr.get(b'\x00')
        tr.commit().wait()
    except fdb.FDBError:  # type: ignore[attr-defined]
        return PlainTextResponse("FDB not ready", status_code=503)

    return Response(status_code=204)
