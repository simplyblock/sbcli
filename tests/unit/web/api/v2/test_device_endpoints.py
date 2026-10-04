"""Unit tests for /api/v2/.../storage-nodes/{id}/devices endpoints (device_controller mocked)."""

import pytest

from simplyblock_core.models.nvme_device import NVMeDevice
from tests.unit.web.api.v2 import _factories as factories
from tests.unit.web.api.v2._factories import CLUSTER_ID, DEVICE_ID, STORAGE_NODE_ID

BASE = f'/api/v2/clusters/{CLUSTER_ID}/storage-nodes/{STORAGE_NODE_ID}/devices'


HEALTH_INFO = {
    'model_number': 'test-model',
    'serial_number': 'SN0001',
    'firmware_revision': '1.0',
    'traddr': '0000:00:1e.0',
    'critical_warning': 0,
    'temperature_celsius': 40,
    'available_spare_percentage': 100,
    'available_spare_threshold_percentage': 10,
    'percentage_used': 3,
    'data_units_read': 123456,
    'data_units_written': 654321,
    'host_read_commands': 1000,
    'host_write_commands': 2000,
    'controller_busy_time': 42,
    'power_cycles': 5,
    'power_on_hours': 8760,
    'unsafe_shutdowns': 1,
    'media_errors': 0,
    'num_err_log_entries': 0,
    'warning_temperature_time_minutes': 0,
    'critical_composite_temperature_time_minutes': 0,
}


class TestListDevices:

    def test_returns_devices_of_node(self, client, db, device):
        response = client.get(f'{BASE}/')

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == DEVICE_ID
        assert body['storage_node_id'] == STORAGE_NODE_ID
        assert body['serial_number'] == 'SN0001'


class TestGetDevice:

    def test_returns_device(self, client, db, device):
        response = client.get(f'{BASE}/{DEVICE_ID}/')

        assert response.status_code == 200
        assert response.json()['id'] == DEVICE_ID

    def test_unknown_device_returns_404(self, client, db, storage_node):
        response = client.get(f'{BASE}/{DEVICE_ID}/')

        assert response.status_code == 404

    def test_new_device_is_serializable(self, client, db, device):
        """A detected-but-not-added device has no cluster map slot or listener."""
        device.status = NVMeDevice.STATUS_NEW
        device.cluster_device_order = -1
        device.nvmf_ip = ''

        response = client.get(f'{BASE}/{DEVICE_ID}/')

        assert response.status_code == 200
        body = response.json()
        assert body['cluster_device_order'] is None
        assert body['nvmf_ips'] == []


NEW_DEVICE_ID = 'abcdabcd-abcd-abcd-abcd-abcdabcdabcd'


class TestAddDevice:

    def test_add(self, client, device, device_controller):
        device.status = NVMeDevice.STATUS_NEW
        device_controller.add_device.return_value = DEVICE_ID

        response = client.post(f'{BASE}/{DEVICE_ID}/add')

        assert response.status_code == 204
        device_controller.add_device.assert_called_once_with(DEVICE_ID)

    def test_already_online_is_a_noop(self, client, device, device_controller):
        response = client.post(f'{BASE}/{DEVICE_ID}/add')

        assert response.status_code == 204
        device_controller.add_device.assert_not_called()

    def test_wrong_status_conflicts(self, client, device, device_controller):
        device.status = NVMeDevice.STATUS_FAILED

        response = client.post(f'{BASE}/{DEVICE_ID}/add')

        assert response.status_code == 409
        device_controller.add_device.assert_not_called()

    def test_raises_on_failure(self, client, device, device_controller):
        device.status = NVMeDevice.STATUS_NEW
        device_controller.add_device.return_value = False

        with pytest.raises(ValueError):
            client.post(f'{BASE}/{DEVICE_ID}/add')


class TestReplaceDevice:

    @pytest.fixture()
    def failed_device(self, device):
        device.status = NVMeDevice.STATUS_FAILED_AND_MIGRATED
        return device

    def test_returns_new_device_id(self, client, db, failed_device, device_controller):
        device_controller.new_device_from_failed.return_value = NEW_DEVICE_ID

        response = client.post(f'{BASE}/{DEVICE_ID}/replace')

        assert response.status_code == 201
        assert response.json() == NEW_DEVICE_ID
        assert response.headers['location'] == f'{BASE}/{NEW_DEVICE_ID}/'
        device_controller.new_device_from_failed.assert_called_once_with(DEVICE_ID)

    def test_full_response_format_serializes_the_new_device(
            self, client, db, failed_device, device_controller):
        device_controller.new_device_from_failed.return_value = NEW_DEVICE_ID
        # As new_device_from_failed leaves it: no cluster map slot, no NVMe-oF
        # listener yet. Both are what the DTO has to tolerate.
        db.get_storage_device_by_id.return_value = factories.make_device(
            uuid=NEW_DEVICE_ID,
            status=NVMeDevice.STATUS_NEW,
            cluster_device_order=-1,
            nvmf_ip='',
        )

        response = client.post(
            f'{BASE}/{DEVICE_ID}/replace', params={'response-format': 'full'})

        assert response.status_code == 201
        body = response.json()
        assert body['id'] == NEW_DEVICE_ID
        assert body['status'] == NVMeDevice.STATUS_NEW
        assert body['cluster_device_order'] is None
        assert body['nvmf_ips'] == []

    def test_wrong_status_conflicts(self, client, db, device, device_controller):
        response = client.post(f'{BASE}/{DEVICE_ID}/replace')

        assert response.status_code == 409
        device_controller.new_device_from_failed.assert_not_called()

    def test_already_replaced_conflicts(self, client, db, failed_device, device_controller):
        failed_device.serial_number = 'SN0001_failed'

        response = client.post(f'{BASE}/{DEVICE_ID}/replace')

        assert response.status_code == 409
        device_controller.new_device_from_failed.assert_not_called()

    def test_raises_on_failure(self, client, db, failed_device, device_controller):
        device_controller.new_device_from_failed.return_value = False

        with pytest.raises(ValueError):
            client.post(f'{BASE}/{DEVICE_ID}/replace')


class TestDeviceActions:

    def test_remove(self, client, device, device_controller):
        device_controller.device_remove.return_value = True

        response = client.post(f'{BASE}/{DEVICE_ID}/remove', params={'force': True})

        assert response.status_code == 204
        # The cause is not decoration: it is what marks the removal as
        # operator-initiated so device self-repair never undoes it.
        device_controller.device_remove.assert_called_once_with(
            DEVICE_ID, True,
            cause=device_controller.CAUSE_ADMIN_REMOVE)

    def test_restart(self, client, device, device_controller):
        device_controller.restart_device.return_value = True

        response = client.post(f'{BASE}/{DEVICE_ID}/restart')

        assert response.status_code == 204
        device_controller.restart_device.assert_called_once_with(DEVICE_ID, False)

    def test_reset(self, client, device, device_controller):
        device_controller.reset_storage_device.return_value = True

        response = client.post(f'{BASE}/{DEVICE_ID}/reset')

        assert response.status_code == 204
        device_controller.reset_storage_device.assert_called_once_with(DEVICE_ID)

    def test_fail(self, client, device, device_controller):
        device_controller.device_set_failed.return_value = True

        response = client.post(f'{BASE}/{DEVICE_ID}/fail')

        assert response.status_code == 204
        device_controller.device_set_failed.assert_called_once_with(DEVICE_ID)

    def test_fail_raises_on_failure(self, client, device, device_controller):
        device_controller.device_set_failed.return_value = False

        with pytest.raises(ValueError):
            client.post(f'{BASE}/{DEVICE_ID}/fail')


class TestDeviceStats:

    def test_capacity(self, client, device, device_controller):
        device_controller.get_device_capacity.return_value = [{'date': 1}]

        response = client.get(f'{BASE}/{DEVICE_ID}/capacity', params={'history': '5'})

        assert response.status_code == 200
        device_controller.get_device_capacity.assert_called_once_with(
            DEVICE_ID, '5', parse_sizes=False)

    def test_iostats(self, client, device, device_controller):
        device_controller.get_device_iostats.return_value = [{'date': 1}]

        response = client.get(f'{BASE}/{DEVICE_ID}/iostats')

        assert response.status_code == 200
        device_controller.get_device_iostats.assert_called_once_with(
            DEVICE_ID, None, parse_sizes=False)


class TestDeviceHealthInfo:

    def test_returns_health_info(self, client, device, device_controller):
        device_controller.get_device_health_info.return_value = dict(HEALTH_INFO)

        response = client.get(f'{BASE}/{DEVICE_ID}/health-info')

        assert response.status_code == 200
        body = response.json()
        assert body['id'] == DEVICE_ID
        assert body['model_number'] == 'test-model'
        assert body['serial_number'] == 'SN0001'
        assert body['temperature_celsius'] == 40
        assert body['percentage_used'] == 3
        assert body['critical_composite_temperature_time_minutes'] == 0
        device_controller.get_device_health_info.assert_called_once_with(DEVICE_ID)

    def test_unknown_device_returns_404(self, client, db, storage_node, device_controller):
        response = client.get(f'{BASE}/{DEVICE_ID}/health-info')

        assert response.status_code == 404
        device_controller.get_device_health_info.assert_not_called()

    def test_missing_health_info_raises(self, client, device, device_controller):
        device_controller.get_device_health_info.return_value = None

        with pytest.raises(ValueError):
            client.get(f'{BASE}/{DEVICE_ID}/health-info')


class TestWatchDevices:

    def test_list_dispatches_watch_devices(self, client, device, device_controller, watch_stream):
        device_controller.watch_devices.return_value = watch_stream([device])

        response = client.get(f'{BASE}/?watch=true')

        assert response.status_code == 200
        assert response.headers['content-type'].startswith('text/event-stream')
        assert 'event: snapshot' in response.text
        assert DEVICE_ID in response.text
        device_controller.watch_devices.assert_called_once_with(CLUSTER_ID, STORAGE_NODE_ID)

    def test_detail_dispatches_watch_device(self, client, device, device_controller, watch_stream):
        device_controller.watch_device.return_value = watch_stream([device])

        response = client.get(f'{BASE}/{DEVICE_ID}/?watch=true')

        assert response.status_code == 200
        assert response.headers['content-type'].startswith('text/event-stream')
        assert 'event: snapshot' in response.text
        assert DEVICE_ID in response.text
        device_controller.watch_device.assert_called_once_with(
            CLUSTER_ID, STORAGE_NODE_ID, DEVICE_ID)
