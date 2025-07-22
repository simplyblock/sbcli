#!/usr/bin/env python
# encoding: utf-8

import logging

from fastapi import FastAPI, Request, Response
from fastapi.middleware.wsgi import WSGIMiddleware
from fastapi.responses import RedirectResponse
import uvicorn

from simplyblock_web.api import public, v1
from simplyblock_core import constants, utils as core_utils

logger = core_utils.get_logger(__name__)
logger.setLevel(constants.LOG_WEB_LEVEL)
logging.getLogger().setLevel(constants.LOG_WEB_LEVEL)


core_utils.init_sentry_sdk()


app = FastAPI()
app.include_router(public, prefix='/api')
app.mount('/api/v1', WSGIMiddleware(v1.api))  # For some reason this fails if done in `api/__init__.py`


@app.route('/', methods=['GET'])
@app.route('/cluster/{full_path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/mgmtnode/{full_path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/device/{full_path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/lvol/{full_path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/snapshot/{full_path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/storagenode/{full_path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/pool/{full_path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
def redirect_legacy(request: Request) -> Response:
    redirect_url = f'/api/v1/{request.url.path}'
    if (query_params := str(request.query_params)):
        redirect_url += f'?{query_params}'
    return RedirectResponse(url=redirect_url, status_code=308)


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=5000, log_level='debug', forwarded_allow_ips='192.168.1.0/24')
