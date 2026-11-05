"""A failed ``BaseModel.write_to_db`` raises; it does not end the process.

``write_to_db`` used to call ``exit(1)`` on any FDB error. In the web API that
``SystemExit`` escaped the request into the event loop and killed it, while the
half-shut-down process kept its listen socket open: a replica that looked
Running, accepted connections, and served none of them until it was restarted
by hand. The trigger was a transaction timeout (1031) while FDB recovered from
disk I/O stalls -- a transient error that should have failed one request.
"""

import uuid

import fdb
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.alert_state import AlertState
from simplyblock_core.models.lvol_model import LVol

TRANSACTION_TIMED_OUT = 1031


@pytest.fixture()
def db():
    return DBController()


@pytest.fixture()
def commit_times_out(monkeypatch):
    """Every commit fails the way it did during the FDB recovery.

    1031 is not retryable, so the binding's ``on_error`` raises it again --
    the same path the incident took out of ``@fdb.transactional``.
    """
    def timed_out(self):
        raise fdb.FDBError(TRANSACTION_TIMED_OUT)

    monkeypatch.setattr(fdb.impl.Transaction, "commit", timed_out)


def _alert_state():
    state = AlertState()
    state.uuid = str(uuid.uuid4())
    return state


def _lvol():
    lvol = LVol()
    lvol.uuid = str(uuid.uuid4())
    lvol.lvol_name = "vol-1"
    lvol.pool_uuid = str(uuid.uuid4())
    return lvol


@pytest.mark.parametrize("make", [_alert_state, _lvol], ids=["plain-set", "indexed-transaction"])
def test_a_timed_out_write_raises_the_driver_error(db, commit_times_out, make):
    record = make()

    with pytest.raises(fdb.FDBError) as raised:
        record.write_to_db(db.kv_store)

    assert raised.value.code == TRANSACTION_TIMED_OUT


def test_a_failed_write_is_a_500_and_the_server_keeps_serving(db, monkeypatch):
    app = FastAPI()

    @app.post("/write")
    def write():
        _lvol().write_to_db(db.kv_store)
        return {"written": True}

    @app.get("/live")
    def live():
        return {"live": True}

    client = TestClient(app, raise_server_exceptions=False)

    def timed_out(self):
        raise fdb.FDBError(TRANSACTION_TIMED_OUT)

    with monkeypatch.context() as m:
        m.setattr(fdb.impl.Transaction, "commit", timed_out)
        assert client.post("/write").status_code == 500

    assert client.get("/live").status_code == 200
    assert client.post("/write").status_code == 200
