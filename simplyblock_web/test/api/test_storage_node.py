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


def test_storage_node_get(call):
    nodes = call('GET', '/storagenode')

    for node in nodes:
        call('GET', f"/storagenode/{node['uuid']}")


def test_capacity(call):
    node_uuid = call('GET', '/storagenode')[0]['uuid']
    call('GET', f'/storagenode/capacity/{node_uuid}')
    call('GET', f'/storagenode/capacity/{node_uuid}/history/10m')


def test_iostats(call):
    node_uuid = call('GET', '/storagenode')[0]['uuid']
    call('GET', f'/storagenode/iostats/{node_uuid}')
    call('GET', f'/storagenode/iostats/{node_uuid}/history/10m')


def test_port(call):
    node_uuid = call('GET', '/storagenode')[0]['uuid']
    port_id = call('GET', f'/storagenode/port/{node_uuid}')[0]['ID']
    call('GET', f'/storagenode/port-io-stats/{port_id}')


@pytest.mark.timeout(20)
def test_suspend_resume(call):
    node = call('GET', '/storagenode')[0]
    assert node['status'] == 'online'
    node_uuid = node['uuid']

    call('GET', f'/storagenode/suspend/{node_uuid}')
    assert call('GET', '/storagenode')[0]['status'] == 'suspended'

    call('GET', f'/storagenode/resume/{node_uuid}')
    assert call('GET', '/storagenode')[0]['status'] == 'online'


@pytest.mark.timeout(30)
def test_restart(call):
    node = call('GET', '/storagenode')[0]
    assert node['status'] == 'online'
    node_uuid = node['uuid']

    call('PUT', '/storagenode/restart/', data={'uuid': node_uuid, 'force': True})
    _check_status_transition(
        ['online', 'in_restart', 'online'],
        lambda: call('GET', f'/storagenode/{node_uuid}')[0],
    )


@pytest.mark.xfail
def test_shutdown_unsuspended(call):
    node = call('GET', '/storagenode')[0]
    assert node['status'] == 'online'
    node_uuid = node['uuid']

    with pytest.raises(HTTPError):
        call('GET', f'/storagenode/shutdown/{node_uuid}')



@pytest.mark.timeout(40)
def test_shutdown(call):
    node = call('GET', '/storagenode')[0]
    assert node['status'] == 'online'
    node_uuid = node['uuid']

    call('GET', f'/storagenode/suspend/{node_uuid}')
    assert call('GET', f'/storagenode/{node_uuid}')[0]['status'] == 'suspended'

    call('GET', f'/storagenode/shutdown/{node_uuid}')
    _check_status_transition(
        ['suspended', 'in_shutdown', 'offline'],
        lambda: call('GET', f'/storagenode/{node_uuid}')[0],
        interval=.1,
    )

    call('PUT', '/storagenode/restart/', data={'uuid': node_uuid})
    _check_status_transition(
        ['offline', 'in_restart', 'online'],
        lambda: call('GET', f'/storagenode/{node_uuid}')[0],
        interval=.1,
    )


@pytest.mark.xfail
def test_add():
    pass
