import re

import pytest
from requests.exceptions import HTTPError

import util


def test_api(call):
    assert call('GET', '/') == "Live"


def test_pool(call, cluster):
    pool_uuid = call('POST', '/pool', data={'name': 'poolX', 'cluster_id': cluster, 'no_secret': True})
    assert re.match(util.uuid_regex, pool_uuid)

    assert call('GET', f'/pool/{pool_uuid}')[0]['uuid'] == pool_uuid
    assert pool_uuid in util.list(call, 'pool')

    call('DELETE', f'/pool/{pool_uuid}')

    assert pool_uuid not in util.list(call, 'pool')

    with pytest.raises(HTTPError):
        call('GET', f'/pool/{pool_uuid}')


def test_pool_duplicate(call, cluster, pool):
    with pytest.raises(HTTPError):
        call('POST', '/pool', data={'name': 'poolX', 'cluster_id': cluster, 'no_secret': True})


def test_pool_delete_missing(call):
    with pytest.raises(HTTPError):
        call('DELETE', '/pool/invalid_uuid')


def test_pool_update(call, cluster, pool):
    values = [
        ('name', 'pool_name', 'poolY'),
        ('pool_max', 'pool_max_size', 1),
        ('lvol_max', 'lvol_max_size', 1),
        ('max_rw_iops', 'max_rw_ios_per_sec', 1),
        ('max_rw_mbytes', 'max_rw_mbytes_per_sec', 1),
        ('max_r_mbytes', 'max_r_mbytes_per_sec', 1),
        ('max_w_mbytes', 'max_w_mbytes_per_sec', 1),
    ]

    call('PUT', f'/pool/{pool}', data={
        parameter: value
        for parameter, _, value
        in values
    })

    pool = call('GET', f'/pool/{pool}')[0]
    for _, field, value in values:
        assert pool[field] == value


def test_pool_io_stats(call, pool):
    io_stats = call('GET', f'/pool/iostats/{pool}')
    assert io_stats['object_data']['uuid'] == pool
    # TODO match expected schema


def test_pool_io_stats_history(call, pool):
    io_stats = call('GET', f'/pool/iostats/{pool}/history/10m')
    assert io_stats['object_data']['uuid'] == pool
    # TODO match expected schema


def test_pool_capacity(call, pool):
    call('GET', f'/pool/capacity/{pool}')
    # TODO match expected schema


@pytest.mark.skip(reason="Known faulty")
def test_pool_capacity_history(call, pool):
    call('GET', f'/pool/capacity/{pool}/history/10m')
    # TODO match expected schema
