import re

import pytest
from requests.exceptions import HTTPError

from simplyblock_web.test import util
from simplyblock_web.test.api.v2.util import list_ids


def test_pool(call, cluster):
    pool_uuid = call('POST', f'/clusters/{cluster}/storage-pools', data={'name': 'poolX'})
    assert re.match(util.uuid_regex, pool_uuid)

    assert call('GET', f'/clusters/{cluster}/storage-pools/{pool_uuid}')['id'] == pool_uuid
    assert pool_uuid in list_ids(call, f'/clusters/{cluster}/storage-pools')

    call('DELETE', f'/clusters/{cluster}/storage-pools/{pool_uuid}')

    assert pool_uuid not in list_ids(call, f'/clusters/{cluster}/storage-pools')

    with pytest.raises(HTTPError):
        call('GET', f'/clusters/{cluster}/storage-pools/{pool_uuid}')


def test_pool_duplicate(call, cluster, storage_pool):
    with pytest.raises(HTTPError):
        call('POST', f'/clusters/{cluster}/storage-pools', data={'name': 'poolX'})


def test_pool_delete_missing(call, cluster):
    with pytest.raises(HTTPError):
        call('DELETE', f'/clusters/{cluster}/storage-pools/invalid_uuid')


def test_pool_update(call, cluster, storage_pool):
    params = {
        'name': 'poolY',
        'max_size': 1,
        'volume_max_size': 1,
        'max_rw_iops': 1,
        'max_rw_mbytes': 1,
        'max_r_mbytes': 1,
        'max_w_mbytes': 1,
    }

    call('PUT', f'/clusters/{cluster}/storage-pools/{storage_pool}', data=params)

    pool = call('GET', f'/clusters/{cluster}/storage-pools/{storage_pool}')
    for field, value in params.items():
        assert pool[field] == value


def test_pool_io_stats(call, cluster, storage_pool):
    call('GET', f'/clusters/{cluster}/storage-pools/{storage_pool}/iostats')
    # TODO match expected schema


def test_pool_io_stats_history(call, cluster, storage_pool):
    call('GET', f'/clusters/{cluster}/storage-pools/{storage_pool}/iostats?limit=10')
    # TODO match expected schema
