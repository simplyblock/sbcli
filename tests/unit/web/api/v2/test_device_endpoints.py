# coding=utf-8
"""Unit tests for /api/v2/.../storage-nodes/{id}/devices endpoints (device_controller mocked)."""

from tests.unit.web.api.v2._factories import CLUSTER_ID, DEVICE_ID, STORAGE_NODE_ID

BASE = f'/api/v2/clusters/{CLUSTER_ID}/storage-nodes/{STORAGE_NODE_ID}/devices'


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
