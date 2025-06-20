import util


import pytest


@pytest.mark.timeout(120)
def test_snapshot_delete(call, cluster, pool):
    lvol_uuid = call(
            'POST',
            f'/clusters/{cluster}/pools/{pool}/volumes',
            data={'name': 'lvolX', 'size': '1G'}
    )

    snapshot_uuid = call(
           'POST',
           f'/clusters/{cluster}/pools/{pool}/volumes/{lvol_uuid}/snapshots',
           data={'name': 'snapX'},
    )

    call('DELETE', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol_uuid}')
    util.await_deletion(call, f'/clusters/{cluster}/pools/{pool}/volumes/{lvol_uuid}')
    assert lvol_uuid not in util.list_ids(call, f'/clusters/{cluster}/pools/{pool}/volumes')

    clone_uuid = call(
            'POST',
            f'/clusters/{cluster}/pools/{pool}/snapshots/{snapshot_uuid}/clone',
            data={'name': 'cloneX'},
    )

    call('DELETE', f'/clusters/{cluster}/pools/{pool}/volumes/{clone_uuid}')
    util.await_deletion(call, f'/clusters/{cluster}/pools/{pool}/volumes/{clone_uuid}')
    assert clone_uuid not in util.list_ids(call, f'/clusters/{cluster}/pools/{pool}/volumes')

    call('DELETE', f'/clusters/{cluster}/pools/{pool}/snapshots/{snapshot_uuid}')
    assert snapshot_uuid not in util.list_ids(call, f'/clusters/{cluster}/pools/{pool}/snapshots')


@pytest.mark.timeout(120)
def test_snapshot_softdelete(call, cluster, pool):
    lvol_uuid = call(
            'POST',
            f'/clusters/{cluster}/pools/{pool}/volumes',
            data={'name': 'lvolX', 'size': '1G'},
    )

    snapshot_uuid = call(
            'POST',
            f'/clusters/{cluster}/pools/{pool}/volumes/{lvol_uuid}/snapshots',
            data={'snapshot_name': 'snapX'},
    )

    call('DELETE', f'/clusters/{cluster}/pools/{pool}/volumes/{lvol_uuid}')
    util.await_deletion(call, f'/clusters/{cluster}/pools/{pool}/volumes/{lvol_uuid}')
    assert lvol_uuid not in util.list_ids(call, f'/clusters/{cluster}/pools/{pool}/volumes')

    clone_uuid = call(
            'POST',
            f'/clusters/{cluster}/pools/{pool}/snapshots/{snapshot_uuid}/clone',
            data={'name': 'cloneX'},
    )

    call('DELETE', f'/clusters/{cluster}/pools/{pool}/snapshots/{snapshot_uuid}')
    # Snapshot still present due to existing clone

    call('DELETE', f'/clusters/{cluster}/pools/{pool}/volumes/{clone_uuid}')
    util.await_deletion(call, f'/clusters/{cluster}/pools/{pool}/volumes/{clone_uuid}')
    assert clone_uuid not in util.list_ids(call, f'/clusters/{cluster}/pools/{pool}/volumes')
    assert snapshot_uuid not in util.list_ids(call, f'/clusters/{cluster}/pools/{pool}/snapshots')
