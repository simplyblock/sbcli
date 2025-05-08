import re

import pytest
from requests.exceptions import HTTPError

import util


@pytest.mark.timeout(120)
def test_lvol(call, cluster, pool):
    pool_name = call('GET', f'/pool/{pool}')[0]['pool_name']
    lvol_uuid = call('POST', '/lvol', data={
        'name': 'lvolX',
        'size': '1G',
        'pool': pool_name}
    )
    assert re.match(util.uuid_regex, lvol_uuid)

    assert call('GET', f'/lvol/{lvol_uuid}')[0]['uuid'] == lvol_uuid
    assert lvol_uuid in util.list(call, 'lvol')

    call('DELETE', f'/lvol/{lvol_uuid}')

    util.await_deletion(call, f'/lvol/{lvol_uuid}')

    assert lvol_uuid not in util.list(call, 'lvol')

    with pytest.raises(HTTPError):
        call('GET', f'/lvol/{lvol_uuid}')


def test_lvol_get(call, cluster, pool, lvol):
    pool_name = call('GET', f'/pool/{pool}')[0]['pool_name']
    lvol_details = call('GET', f'/lvol/{lvol}')

    assert len(lvol_details) == 1
    assert lvol_details[0]['lvol_name'] == 'lvolX'
    assert lvol_details[0]['lvol_type'] == 'lvol'
    assert lvol_details[0]['uuid'] == lvol
    assert lvol_details[0]['pool_name'] == pool_name
    assert lvol_details[0]['pool_uuid'] == pool
    assert lvol_details[0]['size'] == 10 ** 9
    # TODO assert schema


def test_lvol_update(call, cluster, pool, lvol):
    call('PUT', f'/lvol/{lvol}', data={
        'name': 'lvol2',
        'max-rw-iops': 1,
        'max-rw-mbytes': 1,
        'max-r-mbytes': 1,
        'max-w-mbytes': 1
    })
    lvol_details = call('GET', f'/lvol/{lvol}')
    assert lvol_details[0]['rw_ios_per_sec'] == 1
    assert lvol_details[0]['rw_mbytes_per_sec'] == 1
    assert lvol_details[0]['r_mbytes_per_sec'] == 1
    assert lvol_details[0]['w_mbytes_per_sec'] == 1


def test_resize(call, cluster, pool, lvol):
    call('PUT', f'/lvol/resize/{lvol}', data={'size': '2G'})
    call('GET', f'/lvol/{lvol}')[0]['size'] == (2 * 2 ** 30)

    with pytest.raises(ValueError):
        call('PUT', f'/lvol/resize/{lvol}', data={'size': '1G'})


def test_iostats(call, cluster, pool, lvol):
    call('GET', f'/lvol/iostats/{lvol}')
    # TODO check schema


def test_iostats_history(call, cluster, pool, lvol):
    call('GET', f'/lvol/iostats/{lvol}/history/1h')
    # TODO check schema


def test_capacity(call, cluster, pool, lvol):
    call('GET', f'/lvol/capacity/{lvol}')
    # TODO check schema


def test_capacity_history(call, cluster, pool, lvol):
    call('GET', f'/lvol/capacity/{lvol}/history/1h')
    # TODO check schema


def test_get_connection_strings(call, cluster, pool, lvol):
    call('GET', f'/lvol/connect/{lvol}')
    # TODO check schema
