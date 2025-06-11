from flask_openapi3 import APIBlueprint

from . import internal
from . import v1
from . import v2

public_api = APIBlueprint('Simplyblock API', __name__)
public_api.register_api(v1.api, path_prefix='/v1')
public_api.register_api(v2.api, path_prefix='/v2')

__all__ = ['public_api', 'internal']
