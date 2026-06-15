from fastapi import APIRouter
from fastapi.responses import PlainTextResponse, Response
import fdb

from simplyblock_core.db_controller import DBController


api = APIRouter()


@api.get('/health', status_code=204, response_class=Response)
def health() -> Response:
    """Liveness probe: succeeds whenever the process can serve requests."""
    return Response(status_code=204)


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
