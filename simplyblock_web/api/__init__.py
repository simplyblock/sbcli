from fastapi import FastAPI
from fastapi.middleware.wsgi import WSGIMiddleware

from . import v1
from . import v2

public = FastAPI()
public.mount('/v1', WSGIMiddleware(v1.api))
public.include_router(v2.api, prefix='/v2')


__all__ = ['public']
