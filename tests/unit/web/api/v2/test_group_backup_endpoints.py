"""v2 REST endpoints for consistency-group backup (design §8).

The routers run through TestClient; the control-plane logic (group_backup
controller) and the database are mocked, so these assert the HTTP contract: which
controller call a request makes, and how a controller result or error maps to a
status code.
"""
from tests.unit.web.api.v2 import _factories as factories
from tests.unit.web.api.v2._factories import (
    CLUSTER_ID,
    CONSISTENCY_GROUP_ID,
    GROUP_BACKUP_ID,
    VOLUME_ID,
)

BASE = f'/api/v2/clusters/{CLUSTER_ID}/group-backups'
CG_BASE = f'/api/v2/clusters/{CLUSTER_ID}/consistency-groups/{CONSISTENCY_GROUP_ID}'


class TestListGroupBackups:
    def test_lists_the_clusters_group_backups(self, client, db, group_backup):
        response = client.get(f'{BASE}/')
        assert response.status_code == 200
        assert [g['id'] for g in response.json()] == [GROUP_BACKUP_ID]


class TestGetGroupBackup:
    def test_returns_the_group_backup_with_its_members(self, client, db, group_backup):
        response = client.get(f'{BASE}/{GROUP_BACKUP_ID}/')
        assert response.status_code == 200
        body = response.json()
        assert body['id'] == GROUP_BACKUP_ID
        assert body['group_seq'] == 7
        assert body['members'][0]['lvol_id'] == VOLUME_ID

    def test_404_when_the_group_backup_belongs_to_another_cluster(self, client, db, cluster):
        other = factories.make_group_backup(cluster_id='00000000-0000-0000-0000-000000000000')
        db.get_group_backup_by_id.return_value = other
        response = client.get(f'{BASE}/{GROUP_BACKUP_ID}/')
        assert response.status_code == 404


class TestRestoreGroupBackup:
    def test_restores_into_a_new_group_and_returns_the_ids(
            self, client, db, group_backup, group_backup_controller):
        group_backup_controller.restore_group_backup.return_value = (
            f'{CLUSTER_ID}/new-cg', ['lvol-a', 'lvol-b'], None)

        response = client.post(
            f'{BASE}/{GROUP_BACKUP_ID}/restore',
            json={'into_group_name': 'restored-cg', 'pool': 'pool-1'})

        assert response.status_code == 202
        assert response.json() == {
            'group_id': f'{CLUSTER_ID}/new-cg', 'lvol_ids': ['lvol-a', 'lvol-b']}
        args, kwargs = group_backup_controller.restore_group_backup.call_args
        assert args[0] == group_backup.get_id()
        assert args[1] == 'restored-cg'
        assert args[2] == 'pool-1'

    def test_a_restore_error_is_422(self, client, db, group_backup, group_backup_controller):
        group_backup_controller.restore_group_backup.return_value = (
            None, None, 'consistency group restored-cg already exists')
        response = client.post(
            f'{BASE}/{GROUP_BACKUP_ID}/restore',
            json={'into_group_name': 'restored-cg', 'pool': 'pool-1'})
        assert response.status_code == 422
        assert 'already exists' in response.json()['detail']


class TestDeleteGroupBackup:
    def test_deletes_the_group_backup(self, client, db, group_backup, group_backup_controller):
        group_backup_controller.delete_group_backup.return_value = (True, None)
        response = client.delete(f'{BASE}/{GROUP_BACKUP_ID}/')
        assert response.status_code == 204
        group_backup_controller.delete_group_backup.assert_called_once_with(
            group_backup.get_id())

    def test_a_delete_error_is_400(self, client, db, group_backup, group_backup_controller):
        group_backup_controller.delete_group_backup.return_value = (False, 'bucket unreachable')
        response = client.delete(f'{BASE}/{GROUP_BACKUP_ID}/')
        assert response.status_code == 400


class TestCreateGroupBackupUnderGroup:
    def _group(self, db):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()

    def test_takes_a_fresh_generation_by_default(
            self, client, db, cluster, group_backup, group_backup_controller):
        self._group(db)
        group_backup_controller.create_group_backup.return_value = (group_backup.get_id(), None)

        response = client.post(f'{CG_BASE}/backups', json={})

        assert response.status_code == 201
        assert response.json()['id'] == GROUP_BACKUP_ID
        _, kwargs = group_backup_controller.create_group_backup.call_args
        assert kwargs['group_seq'] is None

    def test_backs_up_a_named_generation(
            self, client, db, cluster, group_backup, group_backup_controller):
        self._group(db)
        group_backup_controller.create_group_backup.return_value = (group_backup.get_id(), None)

        response = client.post(f'{CG_BASE}/backups', json={'group_seq': 3})

        assert response.status_code == 201
        _, kwargs = group_backup_controller.create_group_backup.call_args
        assert kwargs['group_seq'] == 3

    def test_a_create_error_is_422(self, client, db, cluster, group_backup_controller):
        self._group(db)
        group_backup_controller.create_group_backup.return_value = (
            None, 'consistency group has no member snapshots at generation 3')
        response = client.post(f'{CG_BASE}/backups', json={'group_seq': 3})
        assert response.status_code == 422
        assert 'no member snapshots' in response.json()['detail']
