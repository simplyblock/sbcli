import re

import pytest
from requests.exceptions import HTTPError

import util


def test_pool(call, cluster):
    pool_uuid = call('POST', f'/clusters/{cluster}/pools', data={'name': 'poolX'})
    assert re.match(util.uuid_regex, pool_uuid)

    assert call('GET', f'/clusters/{cluster}/pools/{pool_uuid}')[0]['uuid'] == pool_uuid
    assert pool_uuid in util.list(call, 'pool')

    call('DELETE', f'/clusters/{cluster}/pools/{pool_uuid}')

    assert pool_uuid not in util.list(call, 'pool')

    with pytest.raises(HTTPError):
        call('GET', f'/clusters/{cluster}/pools/{pool_uuid}')


def test_pool_duplicate(call, cluster, pool):
    with pytest.raises(HTTPError):
        call('POST', f'/clusters/{cluster}/pools', data={'name': 'poolX', 'cluster_id': cluster, 'no_secret': True})


def test_pool_delete_missing(call, cluster):
    with pytest.raises(HTTPError):
        call('DELETE', f'/clusters/{cluster}/pools/invalid_uuid')


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

    call('PUT', f'/clusters/{cluster}/pools/{pool}', data={
        parameter: value
        for parameter, _, value
        in values
    })

    pool = call('GET', f'/clusters/{cluster}/pools/{pool}')[0]
    for _, field, value in values:
        assert pool[field] == value


def test_pool_io_stats(call, cluster, pool):
    io_stats = call('GET', f'/clusters/{cluster}/pools/{pool}/iostats')
    assert io_stats['object_data']['uuid'] == pool
    # TODO match expected schema


def test_pool_io_stats_history(call, cluster, pool):
    io_stats = call('GET', f'/clusters/{cluster}/pools/{pool}/iostats?history=10m')
    assert io_stats['object_data']['uuid'] == pool
    # TODO match expected schema


def test_pool_capacity(call, cluster, pool):
    call('GET', f'/clusters/{cluster}/pools/{pool}/capacity')
    # TODO match expected schema


def test_pool_capacity_history(call, cluster, pool):
    call('GET', f'/clusters/{cluster}/pool/{pool}/capacity?history=10m')
    # TODO match expected schema
