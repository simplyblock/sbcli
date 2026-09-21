"""A throwaway model to declare indices against.

Keeping the machinery's tests off the shipped models means a field rename in
production cannot quietly change what they assert; ``test_declarations`` is the
one module that reads the real declarations.
"""
from simplyblock_core.models.base_model import BaseModel


class Sample(BaseModel):
    pool: str = ""
    label: str = ""
    seq: int = 0
    children: list = None  # type: ignore[assignment]
