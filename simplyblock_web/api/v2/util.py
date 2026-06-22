from typing import Annotated, Any, Callable, Literal
from urllib.parse import urlparse
from uuid import UUID

from fastapi import Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, BeforeValidator, Field

from simplyblock_core import utils as core_utils


Unsigned = Annotated[int, Field(ge=0)]
Size = Annotated[Unsigned, BeforeValidator(core_utils.parse_size)]
Percent = Annotated[int, Field(ge=0, le=100)]
Port = Annotated[int, Field(ge=0, lt=65536)]


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
    route_kwargs: dict[str, UUID],
    get_full: Callable[[UUID], BaseModel],
    extra_headers: dict[str, str] | None = None,
) -> Response:
    headers = {"Location": str(request.app.url_path_for(route_name, **route_kwargs))}
    if extra_headers:
        headers.update(extra_headers)

    match response_format:
        case "empty":
            return Response(status_code=201, headers=headers)
        case "identifier":
            return JSONResponse(content=str(entity_id), status_code=201, headers=headers)
        case "full":
            return JSONResponse(content=jsonable_encoder(get_full(entity_id)), status_code=201, headers=headers)
