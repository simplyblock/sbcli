# coding=utf-8
"""Unit tests for simplyblock_lib.api (middleware + util) via a minimal app."""
import logging
from uuid import UUID

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from pydantic import BaseModel, TypeAdapter, ValidationError

from simplyblock_lib.api.middleware import AccessLogMiddleware
from simplyblock_lib.api.util import (
    Percent,
    Port,
    Size,
    UrlPath,
    creation_response,
)

ENTITY_ID = UUID("00000000-0000-0000-0000-000000000001")


class _Thing(BaseModel):
    uuid: UUID
    name: str


def _make_app():
    app = FastAPI()

    @app.get('/things/{thing_id}', name='things:detail')
    def get_thing(thing_id: UUID):
        return _Thing(uuid=thing_id, name="thing")

    @app.post('/things')
    def create_thing(request: Request, response_format: str = "identifier"):
        return creation_response(
            request=request,
            response_format=response_format,  # type: ignore[arg-type]
            entity_id=ENTITY_ID,
            route_name='things:detail',
            route_kwargs={'thing_id': ENTITY_ID},
            get_full=lambda uid: _Thing(uuid=uid, name="thing"),
        )

    return app


# ------------------------------------------------------------ typed scalars

def test_size_parses_units():
    adapter = TypeAdapter(Size)
    assert adapter.validate_python("1GiB") == 2 ** 30
    assert adapter.validate_python(4096) == 4096


def test_size_rejects_garbage():
    with pytest.raises(ValidationError):
        TypeAdapter(Size).validate_python("garbage")  # parse_size returns -1 → ge=0 fails


def test_percent_and_port_bounds():
    assert TypeAdapter(Percent).validate_python(100) == 100
    with pytest.raises(ValidationError):
        TypeAdapter(Percent).validate_python(101)
    assert TypeAdapter(Port).validate_python(65535) == 65535
    with pytest.raises(ValidationError):
        TypeAdapter(Port).validate_python(65536)


def test_url_path_annotation_accepts_strings():
    """Parity note: the UrlPath annotation carries a bare callable, which
    pydantic ignores — the validator has never been active (v2 DTOs store
    absolute URLs from request.url_for in UrlPath-typed fields, which the
    validator would reject if wired). The refactor preserves that behavior."""
    adapter = TypeAdapter(UrlPath)
    assert adapter.validate_python("/some/path") == "/some/path"
    assert adapter.validate_python("https://example.com/path") == "https://example.com/path"


def test_url_path_validator_function_rejects_full_urls():
    from simplyblock_lib.api.util import _validate_url_path
    assert _validate_url_path("/some/path") == "/some/path"
    with pytest.raises(ValueError):
        _validate_url_path("https://example.com/path")
    with pytest.raises(ValueError):
        _validate_url_path("/path?query=1")
    with pytest.raises(ValueError):
        _validate_url_path(42)


# -------------------------------------------------------- creation_response

@pytest.mark.parametrize("fmt,expect_body", [
    ("empty", b""),
    ("identifier", f'"{ENTITY_ID}"'.encode()),
])
def test_creation_response_formats(fmt, expect_body):
    client = TestClient(_make_app())
    response = client.post(f'/things?response_format={fmt}')
    assert response.status_code == 201
    assert response.headers["Location"] == f'/things/{ENTITY_ID}'
    assert response.content == expect_body


def test_creation_response_full():
    client = TestClient(_make_app())
    response = client.post('/things?response_format=full')
    assert response.status_code == 201
    assert response.json() == {"uuid": str(ENTITY_ID), "name": "thing"}


# --------------------------------------------------------------- middleware

def test_access_log_logs_path_but_never_query_string(caplog):
    logger = logging.getLogger("test.access")
    logger.propagate = True
    app = _make_app()
    app.add_middleware(AccessLogMiddleware, logger=logger)
    client = TestClient(app)

    with caplog.at_level(logging.INFO, logger="test.access"):
        client.get(f'/things/{ENTITY_ID}?secret=hunter2')

    records = [r for r in caplog.records if r.name == "test.access"]
    assert len(records) == 1
    record = records[0]
    assert record.message == f'GET /things/{ENTITY_ID}'
    assert 'hunter2' not in record.message
    assert record.status_code == 200
    assert record.client_ip


def test_web_reexports_are_the_lib_objects():
    """simplyblock_web.api.v2.util must remain a facade over the lib."""
    from simplyblock_lib.api import util as lib_util
    from simplyblock_web.api.v2 import util as web_util
    assert web_util.creation_response is lib_util.creation_response
    assert web_util.Size is lib_util.Size
