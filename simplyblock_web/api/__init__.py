from fastapi import APIRouter

from . import v1
from . import v2

public = APIRouter()
public.include_router(v2.api, prefix='/v2')

__all__ = ['public', 'v1']
