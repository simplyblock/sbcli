# coding=utf-8
"""Unit tests for /api/v2/.../snapshots endpoints (snapshot_controller mocked)."""

from tests.unit.web.api.v2 import _factories as factories
from tests.unit.web.api.v2._factories import CLUSTER_ID, POOL_ID, SNAPSHOT_ID

BASE = f'/api/v2/clusters/{CLUSTER_ID}/storage-pools/{POOL_ID}/snapshots'


class TestListSnapshots:

    def test_returns_snapshots_of_pool(self, client, db, snapshot):
        response = client.get(f'{BASE}/')

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == SNAPSHOT_ID
        assert body['name'] == 'snapshot-1'
        assert body['lvol'] is None
        db.get_snapshots_by_pool_id.assert_called_once_with(POOL_ID)

    def test_snapshot_of_volume_links_to_volume(self, client, db, snapshot):
        snapshot.lvol = factories.make_volume()

        response = client.get(f'{BASE}/')

        assert response.status_code == 200


class TestGetSnapshot:

    def test_returns_snapshot(self, client, db, snapshot):
        response = client.get(f'{BASE}/{SNAPSHOT_ID}/')

        assert response.status_code == 200
        assert response.json()['id'] == SNAPSHOT_ID
        db.get_snapshot_by_id.assert_called_once_with(SNAPSHOT_ID)

    def test_snapshot_of_other_pool_returns_404(self, client, db, snapshot):
        snapshot.pool_uuid = '22222222-2222-2222-2222-222222222223'

        response = client.get(f'{BASE}/{SNAPSHOT_ID}/')

        assert response.status_code == 404


class TestDeleteSnapshot:

    def test_deletes_snapshot(self, client, snapshot, snapshot_controller):
        snapshot_controller.delete.return_value = True

        response = client.delete(f'{BASE}/{SNAPSHOT_ID}/')

        assert response.status_code == 204
        snapshot_controller.delete.assert_called_once_with(SNAPSHOT_ID)
