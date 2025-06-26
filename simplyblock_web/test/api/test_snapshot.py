import util


import pytest


@pytest.mark.timeout(120)
def test_snapshot_delete(call, cluster, pool):
    pool_name = call('GET', f'/pool/{pool}')[0]['pool_name']
    lvol_uuid = call('POST', '/lvol', data={'name': 'lvolX', 'size': '1G', 'pool': pool_name})

    snapshot_uuid = call('POST', '/snapshot', data={'lvol_id': lvol_uuid, 'snapshot_name': 'snapX'})

    call('DELETE', f'/lvol/{lvol_uuid}')
    util.await_deletion(call, f'/lvol/{lvol_uuid}')
    assert lvol_uuid not in util.list(call, 'lvol')

    clone_uuid = call('POST', '/snapshot/clone', data={'snapshot_id': snapshot_uuid, 'clone_name': 'cloneX'})

    call('DELETE', f'/lvol/{clone_uuid}')
    util.await_deletion(call, f'/lvol/{clone_uuid}')
    assert clone_uuid not in util.list(call, 'lvol')

    call('DELETE', f'/snapshot/{snapshot_uuid}')
    assert snapshot_uuid not in util.list(call, 'snapshot')


@pytest.mark.timeout(120)
def test_snapshot_softdelete(call, cluster, pool):
    pool_name = call('GET', f'/pool/{pool}')[0]['pool_name']
    lvol_uuid = call('POST', '/lvol', data={'name': 'lvolX', 'size': '1G', 'pool': pool_name})

    snapshot_uuid = call('POST', '/snapshot', data={'lvol_id': lvol_uuid, 'snapshot_name': 'snapX'})

    call('DELETE', f'/lvol/{lvol_uuid}')
    util.await_deletion(call, f'/lvol/{lvol_uuid}')
    assert lvol_uuid not in util.list(call, 'lvol')

    clone_uuid = call('POST', '/snapshot/clone', data={'snapshot_id': snapshot_uuid, 'clone_name': 'cloneX'})

    call('DELETE', f'/snapshot/{snapshot_uuid}')
    # Snapshot still present due to existing clone

    call('DELETE', f'/lvol/{clone_uuid}')
    util.await_deletion(call, f'/lvol/{clone_uuid}')
    assert clone_uuid not in util.list(call, 'lvol')
    assert snapshot_uuid not in util.list(call, 'snapshot')
