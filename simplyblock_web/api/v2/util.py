from typing import Annotated, Any, Literal
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from urllib.parse import urlparse
from uuid import UUID

from fastapi import HTTPException, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, BeforeValidator, Field

from simplyblock_core import utils as core_utils
from simplyblock_core.exceptions import (
    SyncGateError, SyncGroupMemberError, SyncPromoteFailedError, SyncPromoteRefusedError,
    SyncReplicationSiteError, SyncReplicationUnsupportedError, SyncSiteOfflineError,
)


Unsigned = Annotated[int, Field(ge=0)]
OptionalUnsigned = Annotated[int | None, Field(ge=0)]
Size = Annotated[Unsigned, BeforeValidator(core_utils.parse_size)]
Percent = Annotated[int, Field(ge=0, le=100)]
Port = Annotated[int, Field(ge=0, lt=65536)]
# Records spell an unset reference as an empty string rather than omitting it.
OptionalUUID = Annotated[UUID | None, BeforeValidator(lambda value: value or None)]


def _validate_url_path(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError('Path must be a string')

    parsed = urlparse(value)
    for attribute in ['scheme', 'netloc', 'query', 'fragment']:
        if getattr(parsed, attribute):
            raise ValueError(f'{attribute} must not be set')

    return value

UrlPath = Annotated[str, _validate_url_path]

CreationResponseFormat = Literal["empty", "full", "identifier"]
CreationResponseFormatParameter = Annotated[CreationResponseFormat, Query(alias="response-format")]


def creation_response(
    request: Request,
    response_format: CreationResponseFormat,
    entity_id: UUID,
    route_name: str,
    route_kwargs: dict[str, UUID | str],
    get_full: Callable[[UUID], BaseModel],
    extra_headers: dict[str, str] | None = None,
) -> Response:
    headers = {"Location": str(request.app.url_path_for(route_name, **route_kwargs))}
    if extra_headers:
        headers.update(extra_headers)

    if response_format == "empty":
        return Response(status_code=201, headers=headers)
    elif response_format == "identifier":
        return JSONResponse(content=str(entity_id), status_code=201, headers=headers)
    elif response_format == "full":
        return JSONResponse(content=jsonable_encoder(get_full(entity_id)), status_code=201, headers=headers)
    else:
        raise ValueError(f"Unknown response format: {response_format!r}")


#: The ``site`` query parameter of the sync-replication routes: the site the
#: caller acts from. Required on a sync-replication cluster (require_site),
#: ignored on any other.
SiteParameter = Annotated[str | None, Query(description='Sync replication: the site the caller acts from')]


def sync_error(status_code: int, message: str, **extra: Any) -> HTTPException:
    """A sync-replication refusal: ``{"detail": {"message": ..., **extra}}``."""
    return HTTPException(status_code, {"message": message, **extra})


def require_site(site: str | None) -> str:
    """The ``site`` of a request on a sync-replication cluster; a missing or
    empty one is a 400."""
    if not site:
        raise sync_error(400, 'site is required on a sync-replication cluster')
    return site


@contextmanager
def sync_http_errors() -> Iterator[None]:
    """Answer the sync-replication refusals of the controllers as HTTP: an
    unsupported operation or a bad site 400, a gate or a refused promote /
    demote 409 (retryable, never a code csi-addons escalates to force on), a
    site that is not online 412 (csi-addons escalates to a forced promote);
    a promote whose task failed 409 with its ``task_id`` (reported once).
    A failed ANA RPC (``SyncAnaError``) is left to the 500 handler."""
    try:
        yield
    except (SyncReplicationUnsupportedError, SyncReplicationSiteError) as e:
        raise sync_error(400, str(e)) from e
    except SyncGateError as e:
        raise sync_error(409, str(e), problems=e.problems) from e
    except SyncPromoteFailedError as e:
        raise sync_error(409, str(e), volumes=e.volumes, task_id=e.task_id) from e
    except (SyncPromoteRefusedError, SyncGroupMemberError) as e:
        raise sync_error(409, str(e), volumes=e.volumes) from e
    except SyncSiteOfflineError as e:
        raise sync_error(412, str(e)) from e
