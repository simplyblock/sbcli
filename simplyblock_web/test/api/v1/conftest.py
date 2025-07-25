import functools

import pytest
import requests

from simplyblock_web.test import util


def api_call(entrypoint, cluster, secret, method, path, *, fail=True, data=None, log_func=lambda msg: None):
    response = requests.request(
        method,
        f'{entrypoint}{path}',
        headers={'Authorization': f'{cluster} {secret}'},
        json=data,
    )

    if fail:
        response.raise_for_status()

    try:
        result = response.json()
    except requests.exceptions.JSONDecodeError:
        log_func("Failed to decode content as JSON:")
        log_func(response.text)
        if fail:
            raise

    if not result['status']:
        raise ValueError(result.get('error', 'Request failed'))

    log_func(f'{method} {path}' + (f" -> {result['results']}" if method == 'POST' else ''))

    return result['results']


@pytest.fixture(scope='module')
def call(request):
    options = request.config.option

    return functools.partial(
            api_call,
            options.entrypoint,
            options.cluster,
            options.secret,
            log_func=print,
    )


@pytest.fixture(scope='module')
def pool(call, cluster):
    pool_uuid = call('POST', '/pool', data={'name': 'poolX', 'cluster_id': cluster, 'no_secret': True})
    yield pool_uuid
    call('DELETE', f'/pool/{pool_uuid}')


@pytest.fixture(scope='module')
def lvol(call, cluster, pool):
    pool_name = call('GET', f'/pool/{pool}')[0]['pool_name']
    lvol_uuid = call('POST', '/lvol', data={
        'name': 'lvolX',
        'size': '1G',
        'pool': pool_name}
    )
    yield lvol_uuid
    call('DELETE', f'/lvol/{lvol_uuid}')
    util.await_deletion(call, f'/lvol/{lvol_uuid}')
