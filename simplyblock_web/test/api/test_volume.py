import re

import pytest
from requests.exceptions import HTTPError

import util



@pytest.mark.timeout(120)
def test_volume(call, cluster, storage_pool):
    volume_uuid = call('POST', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes', data={
        'name': 'volumeX',
        'size': '1G',
    })
    assert re.match(util.uuid_regex, volume_uuid)

    assert call('GET', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume_uuid}')['id'] == volume_uuid
    assert volume_uuid in util.list_ids(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes')

    call('DELETE', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume_uuid}')

    util.await_deletion(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume_uuid}')

    assert volume_uuid not in util.list_ids(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes')

    with pytest.raises(HTTPError):
        call('GET', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume_uuid}')


def test_volume_get(call, cluster, storage_pool, volume):
    volume_details = call('GET', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume}')

    assert volume_details['name'] == 'volumeX'
    assert volume_details['id'] == volume
    assert volume_details['size'] == 2 * 10 ** 9
    # TODO assert schema


def test_volume_update(call, cluster, storage_pool, volume):
    call('PUT', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume}', data={
        'name': 'volume2',
        'max_rw_iops': 1,
        'max_rw_mbytes': 1,
        'max_r_mbytes': 1,
        'max_w_mbytes': 1
    })
    volume_details = call('GET', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume}')
    print(volume_details)
    assert volume_details['max_rw_iops'] == 1
    assert volume_details['max_rw_mbytes'] == 1
    assert volume_details['max_r_mbytes'] == 1
    assert volume_details['max_w_mbytes'] == 1


def test_resize(call, cluster, storage_pool, volume):
    call('PUT', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume}', data={'size': '3G'})
    call('GET', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume}')['size'] == (3 * 2 ** 30)

    with pytest.raises(HTTPError):
        call('PUT', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume}', data={'size': '1G'})


def test_iostats(call, cluster, storage_pool, volume):
    call('GET', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume}/iostats')
    # TODO check schema


def test_iostats_history(call, cluster, storage_pool, volume):
    call('GET', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume}/iostats?history=1h')
    # TODO check schema


def test_capacity(call, cluster, storage_pool, volume):
    call('GET', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume}/capacity')
    # TODO check schema


def test_capacity_history(call, cluster, storage_pool, volume):
    call('GET', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume}/capacity?history=1h')
    # TODO check schema


def test_get_connection_strings(call, cluster, storage_pool, volume):
    call('GET', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume}/connect')
    # TODO check schema
