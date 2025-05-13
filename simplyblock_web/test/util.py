import re
import time

import requests


uuid_regex = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')


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


def await_deletion(call, resource, timeout=120):
    for i in range(timeout):
        try:
            call('GET', resource)
            time.sleep(1)
        except ValueError:
            return
        except requests.exceptions.HTTPError:
            return

    raise TimeoutError('Failed to await deletion')


def list(call, type):
    return [
        obj['uuid']
        for obj
        in call('GET', f'/{type}/')
    ]
