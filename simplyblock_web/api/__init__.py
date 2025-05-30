from flask_openapi3 import APIBlueprint

from . import v1
from . import v2

public_api = APIBlueprint('api', __name__, url_prefix='api')
public_api.register_blueprint(v1.api)
public_api.register_api(v2.api)

__all__ = ['public_api']
