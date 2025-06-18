import re

import pytest
from requests.exceptions import HTTPError

import util



@pytest.mark.timeout(120)
def test_lvol(call, cluster, pool):
    lvol_uuid = call('POST', f'/clusters/{cluster}/pools/{pool}/volumes', data={
        'name': 'lvolX',
        'size': '1G',
    })
    assert re.match(util.uuid_regex, lvol_uuid)

    assert call('GET', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol_uuid}')['uuid'] == lvol_uuid
    assert lvol_uuid in util.list_ids(call, f'/clusters/{cluster}/pools/{pool}/volumes')

    call('DELETE', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol_uuid}')

    util.await_deletion(call, f'/clusters/{cluster}/pools/{pool}/volumes/{lvol_uuid}')

    assert lvol_uuid not in util.list_ids(call, f'/clusters/{cluster}/pools/{pool}/volumes')

    with pytest.raises(HTTPError):
        call('GET', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol_uuid}')


def test_lvol_get(call, cluster, pool, lvol):
    pool_name = call('GET', f'/clusters/{cluster}/pools/{pool}')['pool_name']
    lvol_details = call('GET', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol}')

    assert lvol_details['lvol_name'] == 'lvolX'
    assert lvol_details['lvol_type'] == 'lvol'
    assert lvol_details['uuid'] == lvol
    assert lvol_details['pool_name'] == pool_name
    assert lvol_details['pool_uuid'] == pool
    assert lvol_details['size'] == 2 * 10 ** 9
    # TODO assert schema


def test_lvol_update(call, cluster, pool, lvol):
    call('PUT', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol}', data={
        'name': 'lvol2',
        'max-rw-iops': 1,
        'max-rw-mbytes': 1,
        'max-r-mbytes': 1,
        'max-w-mbytes': 1
    })
    lvol_details = call('GET', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol}')
    print(lvol_details)
    assert lvol_details['rw_ios_per_sec'] == 1
    assert lvol_details['rw_mbytes_per_sec'] == 1
    assert lvol_details['r_mbytes_per_sec'] == 1
    assert lvol_details['w_mbytes_per_sec'] == 1


def test_resize(call, cluster, pool, lvol):
    call('PUT', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol}', data={'size': '3G'})
    call('GET', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol}')['size'] == (3 * 2 ** 30)

    with pytest.raises(HTTPError):
        call('PUT', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol}', data={'size': '1G'})


def test_iostats(call, cluster, pool, lvol):
    call('GET', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol}/iostats')
    # TODO check schema


def test_iostats_history(call, cluster, pool, lvol):
    call('GET', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol}/iostats?history=1h')
    # TODO check schema


def test_capacity(call, cluster, pool, lvol):
    call('GET', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol}/capacity')
    # TODO check schema


def test_capacity_history(call, cluster, pool, lvol):
    call('GET', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol}/capacity?history=1h')
    # TODO check schema


def test_get_connection_strings(call, cluster, pool, lvol):
    call('GET', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol}/connect')
    # TODO check schema
