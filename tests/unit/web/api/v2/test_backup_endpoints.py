# coding=utf-8
"""Unit tests for /api/v2/clusters/{id}/backups endpoints (backup_controller mocked)."""

from tests.unit.web.api.v2 import _factories as factories
from tests.unit.web.api.v2._factories import (
    BACKUP_ID,
    CLUSTER_ID,
    POLICY_ID,
    SNAPSHOT_ID,
    VOLUME_ID,
)

BASE = f'/api/v2/clusters/{CLUSTER_ID}/backups'


class TestListBackups:

    def test_returns_backups_newest_first(self, client, db, backup):
        older = factories.make_backup(
            uuid='99999999-9999-9999-9999-999999999998', created_at=1600000000)
        db.get_backups.return_value = [older, backup]

        response = client.get(f'{BASE}/')

        assert response.status_code == 200
        assert [entry['id'] for entry in response.json()] == [BACKUP_ID, older.uuid]
        db.get_backups.assert_called_once_with(CLUSTER_ID)


class TestCreateBackup:

    def test_backs_up_snapshot(self, client, db, cluster, backup_controller):
        backup_controller.backup_snapshot.return_value = (BACKUP_ID, None)

        response = client.post(f'{BASE}/', json={'snapshot_id': SNAPSHOT_ID})

        assert response.status_code == 201
        backup_controller.backup_snapshot.assert_called_once_with(
            SNAPSHOT_ID, cluster_id=CLUSTER_ID)
        assert response.headers['X-Backup-Id'] == BACKUP_ID
        assert response.headers['Location'].endswith(f'/backups/{BACKUP_ID}/')

    def test_error_returns_400(self, client, db, cluster, backup_controller):
        backup_controller.backup_snapshot.return_value = (None, 'snapshot not found')

        response = client.post(f'{BASE}/', json={'snapshot_id': SNAPSHOT_ID})

        assert response.status_code == 400


class TestGetBackup:

    def test_returns_backup(self, client, db, backup):
        response = client.get(f'{BASE}/{BACKUP_ID}/')

        assert response.status_code == 200
        body = response.json()
        assert body['id'] == BACKUP_ID
        assert body['snapshot_id'] == SNAPSHOT_ID
        db.get_backup_by_id.assert_called_once_with(BACKUP_ID)

    def test_backup_of_other_cluster_returns_404(self, client, db, backup):
        backup.cluster_id = '11111111-1111-1111-1111-111111111112'

        response = client.get(f'{BASE}/{BACKUP_ID}/')

        assert response.status_code == 404


class TestRestoreBackup:

    def test_restores_backup(self, client, db, cluster, backup_controller):
        backup_controller.restore_backup.return_value = (VOLUME_ID, None)

        response = client.post(f'{BASE}/restore', json={
            'backup_id': BACKUP_ID,
            'lvol_name': 'restored-volume',
            'pool': 'pool-1',
        })

        assert response.status_code == 202
        assert response.json() == {'lvol_id': VOLUME_ID}
        backup_controller.restore_backup.assert_called_once_with(
            BACKUP_ID, 'restored-volume', 'pool-1',
            cluster_id=CLUSTER_ID, target_node_id=None)


class TestBackupPolicies:

    def test_list_policies(self, client, db, backup_policy):
        response = client.get(f'{BASE}/backup-policies/')

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == POLICY_ID
        assert body['name'] == 'policy-1'
        db.get_backup_policies.assert_called_once_with(CLUSTER_ID)

    def test_create_policy(self, client, db, cluster, backup_controller):
        backup_controller.add_policy.return_value = (POLICY_ID, None)

        response = client.post(f'{BASE}/backup-policies/', json={
            'name': 'policy-1',
            'versions': 7,
            'schedule': '15m,4',
        })

        assert response.status_code == 201
        backup_controller.add_policy.assert_called_once_with(
            CLUSTER_ID, 'policy-1', max_versions=7, max_age='', schedule='15m,4')
        assert response.headers['X-Policy-Id'] == POLICY_ID

    def test_delete_policy(self, client, db, backup_policy, backup_controller):
        backup_controller.remove_policy.return_value = (True, None)

        response = client.delete(f'{BASE}/backup-policies/{POLICY_ID}')

        assert response.status_code == 204
        backup_controller.remove_policy.assert_called_once_with(POLICY_ID)
