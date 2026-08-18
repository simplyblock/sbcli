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
        backup_controller.restore_backup.return_value = VOLUME_ID

        response = client.post(f'{BASE}/restore', json={
            'backup_id': BACKUP_ID,
            'lvol_name': 'restored-volume',
            'pool': 'pool-1',
        })

        assert response.status_code == 202
        assert response.json() == {'lvol_id': VOLUME_ID}
        backup_controller.restore_backup.assert_called_once_with(
            BACKUP_ID, 'restored-volume', 'pool-1', target_node_id=None,
            s3_credentials=None)

    def test_passes_bucket_credentials_through(self, client, db, cluster,
                                               backup_controller):
        """Restoring another cluster's bucket needs credentials for it."""
        backup_controller.restore_backup.return_value = VOLUME_ID

        response = client.post(f'{BASE}/restore', json={
            'backup_id': BACKUP_ID,
            'lvol_name': 'restored-volume',
            'pool': 'pool-1',
            's3_credentials': {'access_key_id': 'AKIA', 'secret_access_key': 'shh'},
        })

        assert response.status_code == 202
        credentials = backup_controller.restore_backup.call_args.kwargs['s3_credentials']
        assert credentials.access_key_id.get_secret_value() == 'AKIA'

    def test_a_precondition_error_is_not_mapped_here(self, client, db, cluster,
                                                     backup_controller):
        """app.py maps PreconditionError to 400 for the whole API; a second,
        disagreeing mapping in this one router made it inconsistent with itself.

        (This test app deliberately mounts only the routers, so an unhandled
        exception surfaces here instead of reaching that handler.)
        """
        import pytest
        from simplyblock_core.exceptions import PreconditionError
        backup_controller.restore_backup.side_effect = PreconditionError('node offline')

        with pytest.raises(PreconditionError):
            client.post(f'{BASE}/restore', json={
                'backup_id': BACKUP_ID,
                'lvol_name': 'restored-volume',
                'pool': 'pool-1',
            })


class TestImportBackups:
    """The body is a union of two shapes, not one model with everything optional."""

    _MANIFEST = {
        'schema_version': 1,
        'backup_id': BACKUP_ID,
        's3_id': 7,
        'created_at': 100,
        'completed_at': 200,
        'size': 4096,
        'encryption': {'encrypted': False},
        'location': {'bucket_name': 'backups', 'region': 'eu-central-1'},
        'source': {'cluster_id': CLUSTER_ID, 'node_id': 'node-1'},
        'volume': {'lvol_id': VOLUME_ID, 'lvol_name': 'vol',
                   'snapshot_id': SNAPSHOT_ID, 'snapshot_name': 'snap',
                   'size': 4096},
        'dataplane': {},
    }

    _BUCKET = {'bucket_name': 'backups', 'region': 'eu-central-1'}

    def test_inline_manifests_are_validated_by_the_body_type(
            self, client, db, cluster, backup_controller):
        backup_controller.import_backups.return_value = 1

        response = client.post(f'{BASE}/import', json={'metadata': [self._MANIFEST]})

        assert response.status_code == 200
        assert response.json() == {'imported': 1}
        (manifests,), kwargs = backup_controller.import_backups.call_args
        assert [m.backup_id for m in manifests] == [BACKUP_ID]

    def test_a_malformed_manifest_is_rejected_before_the_controller(
            self, client, db, cluster, backup_controller):
        response = client.post(
            f'{BASE}/import',
            json={'metadata': [{**self._MANIFEST, 's3_id': 'not-an-int'}]})

        assert response.status_code == 422
        backup_controller.import_backups.assert_not_called()

    def test_a_bucket_reads_the_manifests_itself(
            self, client, db, cluster, backup_controller):
        backup_controller.import_from_bucket.return_value = 3

        response = client.post(f'{BASE}/import', json={'bucket': self._BUCKET})

        assert response.status_code == 200
        assert response.json() == {'imported': 3}
        backup_controller.import_backups.assert_not_called()

    def test_naming_both_sources_is_rejected(
            self, client, db, cluster, backup_controller):
        """extra="forbid" on both arms is what makes the union decide."""
        response = client.post(f'{BASE}/import', json={
            'metadata': [self._MANIFEST], 'bucket': self._BUCKET})

        assert response.status_code == 422

    def test_naming_neither_source_is_rejected(
            self, client, db, cluster, backup_controller):
        response = client.post(f'{BASE}/import', json={})

        assert response.status_code == 422

    def test_an_unreadable_bucket_is_a_bad_request_not_a_bad_gateway(
            self, client, db, cluster, backup_controller):
        """Nothing here proxies for S3, and the bucket came from the request."""
        from simplyblock_core.controllers.backup.manifest import ManifestError
        backup_controller.import_from_bucket.side_effect = ManifestError('no such bucket')

        response = client.post(f'{BASE}/import', json={'bucket': self._BUCKET})

        assert response.status_code == 400
        assert 'no such bucket' in response.json()['detail']

    def test_a_precondition_error_is_not_mapped_here(
            self, client, db, cluster, backup_controller):
        """It used to become a 409, contradicting app.py, which maps every
        PreconditionError to 400 for the whole API. The endpoint lets it through.

        (This test app deliberately mounts only the routers, so an unhandled
        exception surfaces here instead of reaching that handler.)
        """
        import pytest
        from simplyblock_core.exceptions import PreconditionError
        backup_controller.import_backups.side_effect = PreconditionError('already exists')

        with pytest.raises(PreconditionError):
            client.post(f'{BASE}/import', json={'metadata': [self._MANIFEST]})


class TestDiscoverBackups:

    def test_returns_the_manifests_the_bucket_holds(
            self, client, db, backup_controller):
        from simplyblock_core.controllers.backup.manifest import BackupManifest
        backup_controller.discover_backups.return_value = [
            BackupManifest.model_validate(TestImportBackups._MANIFEST)]

        response = client.post(
            f'{BASE}/discover',
            json={'bucket_name': 'backups', 'region': 'eu-central-1'})

        assert response.status_code == 200
        assert [entry['backup_id'] for entry in response.json()] == [BACKUP_ID]

    def test_credentials_are_masked_in_the_response_of_a_failure(
            self, client, db, backup_controller):
        from simplyblock_core.controllers.backup.manifest import ManifestError
        backup_controller.discover_backups.side_effect = ManifestError('unreachable')

        response = client.post(f'{BASE}/discover', json={
            'bucket_name': 'backups', 'region': 'eu-central-1',
            'credentials': {'access_key_id': 'AKIA', 'secret_access_key': 's3cr3t'},
        })

        assert response.status_code == 400
        assert 's3cr3t' not in response.text


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
