# coding=utf-8
"""Unit tests for /api/v2/.../storage-nodes/{id}/devices endpoints (device_controller mocked)."""

import pytest

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


class TestDeviceActions:

    def test_remove(self, client, device, device_controller):
        device_controller.device_remove.return_value = True

        response = client.post(f'{BASE}/{DEVICE_ID}/remove', params={'force': True})

        assert response.status_code == 204
        device_controller.device_remove.assert_called_once_with(DEVICE_ID, True)

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
