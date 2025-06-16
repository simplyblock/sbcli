from pathlib import Path
import time
import re
from urllib.parse import urlparse

import requests


uuid_regex = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')


def api_call(entrypoint, secret, method, path, *, fail=True, data=None, log_func=lambda msg: None):
    response = requests.request(
        method,
        f'{entrypoint}/api/v2{path}',
        headers={'Authorization': f'Bearer {secret}'},
        json=data,
    )

    log_func(f'{method} {path} -> {response.status_code}')
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
