import pytest

from simplyblock_web.test import util
from simplyblock_web.test.api.v2.util import list_ids


@pytest.mark.timeout(120)
def test_snapshot_delete(call, cluster, storage_pool):
    volume_uuid = call(
            'POST',
            f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes',
            data={'name': 'volumeX', 'size': '1G'}
    )

    snapshot_uuid = call(
           'POST',
           f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume_uuid}/snapshots',
           data={'name': 'snapX'},
    )

    call('DELETE', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume_uuid}')
    util.await_deletion(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume_uuid}')
    assert volume_uuid not in list_ids(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes')

    clone_uuid = call(
            'POST',
            f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes',
            data={'name': 'cloneX', 'snapshot_id': snapshot_uuid},
    )

    call('DELETE', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{clone_uuid}')
    util.await_deletion(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{clone_uuid}')
    assert clone_uuid not in list_ids(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes')

    call('DELETE', f'/clusters/{cluster}/storage-pools/{storage_pool}/snapshots/{snapshot_uuid}')
    assert snapshot_uuid not in list_ids(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/snapshots')


@pytest.mark.timeout(120)
def test_snapshot_softdelete(call, cluster, storage_pool):
    volume_uuid = call(
            'POST',
            f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes',
            data={'name': 'volumeX', 'size': '1G'},
    )

    snapshot_uuid = call(
            'POST',
            f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume_uuid}/snapshots',
            data={'name': 'snapX'},
    )

    call('DELETE', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume_uuid}')
    util.await_deletion(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume_uuid}')
    assert volume_uuid not in list_ids(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes')

    clone_uuid = call(
            'POST',
            f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes',
            data={'name': 'cloneX', 'snapshot_id': snapshot_uuid},
    )

    call('DELETE', f'/clusters/{cluster}/storage-pools/{storage_pool}/snapshots/{snapshot_uuid}')
    # Snapshot still present due to existing clone

    call('DELETE', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{clone_uuid}')
    util.await_deletion(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{clone_uuid}')
    assert clone_uuid not in list_ids(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes')
    assert snapshot_uuid not in list_ids(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/snapshots')
