import time
import re

import requests


uuid_regex = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')


OPTIONS = ['entrypoint', 'cluster', 'secret']


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


def list_ids(call, path):
    return [
        item['id']
        for item
        in call('GET', path)
    ]
