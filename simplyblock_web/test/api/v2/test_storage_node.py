import time

from requests import HTTPError
import pytest


def _check_status_transition(expected_statuses, f, interval=.5):
    for expected_status, next_expected_status in zip(expected_statuses[:-1], expected_statuses[1:]):
        while (status := f()['status']) == expected_status:
            print(f"Status {status} matched expectation {expected_status}")
            time.sleep(interval)

        assert status == next_expected_status
        print(f"Status {status} matched next expected status {next_expected_status}")


def test_storage_node_get(call, cluster):
    nodes = call('GET', f'/clusters/{cluster}/storage-nodes')

    for node in nodes:
        call('GET', f"/clusters/{cluster}/storage-nodes/{node['id']}")


def test_capacity(call, cluster):
    node_uuid = call('GET', f'/clusters/{cluster}/storage-nodes')[0]['id']
    call('GET', f'/clusters/{cluster}/storage-nodes/{node_uuid}/capacity')
    call('GET', f'/clusters/{cluster}/storage-nodes/{node_uuid}/capacity?history=10m')


def test_iostats(call, cluster):
    node_uuid = call('GET', f'/clusters/{cluster}/storage-nodes')[0]['id']
    call('GET', f'/clusters/{cluster}/storage-nodes/{node_uuid}/iostats')
    call('GET', f'/clusters/{cluster}/storage-nodes/{node_uuid}/iostats?history=10m')


def test_port(call, cluster):
    node_uuid = call('GET', f'/clusters/{cluster}/storage-nodes')[0]['id']
    port_id = call('GET', f'/clusters/{cluster}/storage-nodes/{node_uuid}/nics')[0]['ID']
    call('GET', f'/clusters/{cluster}/storage-nodes/{node_uuid}/nics/{port_id}/iostats')


@pytest.mark.timeout(20)
def test_suspend_resume(call, cluster):
    node = call('GET', f'/clusters/{cluster}/storage-nodes')[0]
    assert node['status'] == 'online'
    node_uuid = node['id']

    call('POST', f'/clusters/{cluster}/storage-nodes/{node_uuid}/suspend')
    assert call('GET', f'/clusters/{cluster}/storage-nodes/{node_uuid}')['status'] == 'suspended'

    call('POST', f'/clusters/{cluster}/storage-nodes/{node_uuid}/resume')
    assert call('GET', f'/clusters/{cluster}/storage-nodes/{node_uuid}')['status'] == 'online'


@pytest.mark.timeout(240)
def test_restart(call, cluster):
    node = call('GET', f'/clusters/{cluster}/storage-nodes')[0]
    assert node['status'] == 'online'
    node_uuid = node['id']

    call('POST', f'/clusters/{cluster}/storage-nodes/{node_uuid}/suspend')
    assert call('GET', f'/clusters/{cluster}/storage-nodes/{node_uuid}')['status'] == 'suspended'

    call('POST', f'/clusters/{cluster}/storage-nodes/{node_uuid}/restart', data={'force': True})
    _check_status_transition(
        ['online', 'in_restart', 'online'],
        lambda: call('GET', f'/clusters/{cluster}/storage-nodes/{node_uuid}'),
    )
    _check_status_transition(
        ['active', 'degraded', 'active'],
        lambda: call('GET', f'/clusters/{cluster}/'),
    )


@pytest.mark.xfail
def test_shutdown_unsuspended(call, cluster):
    node = call('GET', f'/clusters/{cluster}/storage-nodes')[0]
    assert node['status'] == 'online'
    node_uuid = node['id']

    with pytest.raises(HTTPError):
        call('POST', f'/clusters/{cluster}/storage-nodes/{node_uuid}/shutdown')


@pytest.mark.timeout(120)
def test_shutdown(call, cluster):
    node = call('GET', f'/clusters/{cluster}/storage-nodes')[0]
    assert node['status'] == 'online'
    node_uuid = node['id']

    call('POST', f'/clusters/{cluster}/storage-nodes/{node_uuid}/suspend')
    assert call('GET', f'/clusters/{cluster}/storage-nodes/{node_uuid}')['status'] == 'suspended'

    call('POST', f'/clusters/{cluster}/storage-nodes/{node_uuid}/shutdown?force=true')
    _check_status_transition(
        ['suspended', 'in_shutdown', 'offline'],
        lambda: call('GET', f'/clusters/{cluster}/storage-nodes/{node_uuid}'),
        interval=.1,
    )

    call('POST', f'/clusters/{cluster}/storage-nodes/{node_uuid}/restart/')
    _check_status_transition(
        ['offline', 'in_restart', 'online'],
        lambda: call('GET', f'/clusters/{cluster}/storage-nodes/{node_uuid}'),
        interval=.1,
    )


@pytest.mark.xfail
def test_add():
    pass
