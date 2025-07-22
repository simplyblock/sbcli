import functools
from pathlib import Path
from urllib.parse import urlparse

import pytest
import requests

from simplyblock_web.test import util


def api_call(entrypoint, secret, method, path, *, fail=True, data=None, log_func=lambda msg: None):
    response = requests.request(
        method,
        f'{entrypoint}/api/v2{path}',
        headers={'Authorization': f'Bearer {secret}'},
        json=data,
    )

    log_func(f'{method} {path} -> {response.status_code}')

    if response.status_code == 422:
        log_func(response.json())

    if fail:
        response.raise_for_status()

    if response.status_code == 201:
        location = response.headers.get('Location')
        path = Path(urlparse(location).path)
        entity_id = path.parts[-1]
        return entity_id

    try:
        return response.json() if response.text else None
    except requests.exceptions.JSONDecodeError:
        log_func("Failed to decode content as JSON:")
        log_func(response.text)
        if fail:
            raise


@pytest.fixture(scope='module')
def call(request):
    options = request.config.option

    return functools.partial(
            api_call,
            options.entrypoint,
            options.secret,
            log_func=print,
    )


@pytest.fixture(scope='module')
def storage_pool(call, cluster):
    pool_uuid = call('POST', f'/clusters/{cluster}/storage-pools', data={'name': 'poolX', 'secret': False})
    yield pool_uuid
    call('DELETE', f'/clusters/{cluster}/storage-pools/{pool_uuid}')


@pytest.fixture(scope='module')
def volume(call, cluster, storage_pool):
    volume_uuid = call('POST', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes', data={
        'name': 'volumeX',
        'size': '2G',
    })
    yield volume_uuid
    call('DELETE', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume_uuid}')
    util.await_deletion(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume_uuid}')
