from typing import Annotated, Optional

from simplyblock_core import utils as core_utils

from pydantic import BaseModel, BeforeValidator, Field


Unsigned = Annotated[int, Field(ge=0)]
Size = Annotated[Unsigned, BeforeValidator(core_utils.parse_size)]
Percent = Annotated[int, Field(ge=0, le=100)]


class HistoryQuery(BaseModel):
    history: Optional[str]
