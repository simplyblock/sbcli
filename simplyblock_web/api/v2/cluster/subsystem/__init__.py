from fastapi import APIRouter

from .migration import api as migration_api

api = APIRouter(prefix='/{nqn}')
api.include_router(migration_api, prefix='/migrations')
