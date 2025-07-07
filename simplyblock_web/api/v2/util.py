from typing import Annotated, Any
from urllib.parse import urlparse

from simplyblock_core import utils as core_utils

from pydantic import BeforeValidator, Field


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
