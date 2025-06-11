from functools import wraps
import re

from flask import abort, request

from simplyblock_core.db_controller import DBController


TOKEN_PATTERN = re.compile(r'^Bearer (?P<token>.*)$')
_db = DBController()

api_token_required_scheme = {
    'type': 'http',
    'scheme': 'bearer',
    'bearerFormat': 'Cluster secret',
}


def api_token_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        raw_token = request.headers.get('Authorization')
        if raw_token is None:
            abort(401)

        if (match := TOKEN_PATTERN.match(raw_token)) is None:
            abort(401)

        token = match.group('token')

        cluster_id = next((
            cluster.id
            for cluster
            in _db.get_clusters()
            if cluster.secret == token
        ), None)
        if (cluster_id is None) or (request.view_args.get('cluster_id') != cluster_id):
            abort(401)

        return func(*args, **kwargs)

    return wrapper
