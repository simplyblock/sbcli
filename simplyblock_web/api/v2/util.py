from typing import Optional

from pydantic import BaseModel


class HistoryQuery(BaseModel):
    history: Optional[str]
