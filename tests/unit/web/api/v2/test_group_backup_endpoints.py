"""v2 REST endpoints for consistency-group backup (design §8).

The routers run through TestClient; the control-plane logic (group_backup
controller) and the database are mocked, so these assert the HTTP contract: which
controller call a request makes, and how a controller result or error maps to a
status code.
"""
from datetime import UTC, datetime
from typing import Any

from simplyblock_core.controllers.backup.manifest import GroupBackupExport
from tests.unit.web.api.v2 import _factories as factories
from tests.unit.web.api.v2._factories import (
    BACKUP_ID,
    CLUSTER_ID,
    CONSISTENCY_GROUP_ID,
    GROUP_BACKUP_ID,
    SNAPSHOT_ID,
    STORAGE_NODE_ID,
    VOLUME_ID,
)

BASE = f'/api/v2/clusters/{CLUSTER_ID}/backups/groups'
CG_BASE = f'/api/v2/clusters/{CLUSTER_ID}/consistency-groups/{CONSISTENCY_GROUP_ID}'

#: The source consistency group, as the group manifest spells it.
CG_COMPOSITE = f'{CLUSTER_ID}/{CONSISTENCY_GROUP_ID}'

#: A member backup manifest, the shape a single-volume export carries.
_MEMBER_MANIFEST: dict[str, Any] = {
    'schema_version': 1,
    'backup_id': BACKUP_ID,
    's3_id': 7,
    'created_at': 100,
    'completed_at': 200,
    'size': 4096,
    'source': {'cluster_id': CLUSTER_ID, 'node_id': STORAGE_NODE_ID},
    'volume': {'lvol_id': VOLUME_ID, 'lvol_name': 'vol',
               'snapshot_id': SNAPSHOT_ID, 'snapshot_name': 'snap',
               'size': 4096},
    'dataplane': {},
}

#: The group manifest, naming the member above.
_GROUP_MANIFEST: dict[str, Any] = {
    'schema_version': 1,
    'group_backup_id': GROUP_BACKUP_ID,
    'created_at': 100,
    'completed_at': 200,
    'size': 4096,
    'source': {'cluster_id': CLUSTER_ID, 'consistency_group_id': CG_COMPOSITE,
               'consistency_group_name': 'app-group-cg', 'generation': 7},
    'member_backup_ids': [BACKUP_ID],
    'encrypted': False,
}

_LOCATION: dict[str, str] = {'bucket_name': 'backups', 'region': 'eu-central-1'}


def _group_export() -> dict[str, Any]:
    return {
        'schema_version': 1,
        'group_manifest': _GROUP_MANIFEST,
        'members': {'schema_version': 1, 'groups': [
            {'location': _LOCATION, 'manifests': [_MEMBER_MANIFEST]}]},
    }


def _group_export_model() -> GroupBackupExport:
    """A real export model, for the export endpoint's response_model to render."""
    return GroupBackupExport.model_validate(_group_export())


class TestListGroupBackups:
    def test_lists_the_clusters_group_backups(self, client, db, group_backup):
        response = client.get(f'{BASE}/')
        assert response.status_code == 200
        assert [g['id'] for g in response.json()] == [GROUP_BACKUP_ID]


class TestGetGroupBackup:
    def test_returns_the_group_backup_with_its_member_backup_ids(self, client, db, group_backup):
        """The members are named by their backup ids only. Each member backup
        carries its own volume, and GET .../members/ returns the records. Ids are
        bare UUIDs, as on every other DTO: the cluster is cluster_id."""
        response = client.get(f'{BASE}/{GROUP_BACKUP_ID}/')
        assert response.status_code == 200
        body = response.json()
        assert body['id'] == GROUP_BACKUP_ID
        assert body['group_seq'] == 7
        assert body['consistency_group_id'] == CONSISTENCY_GROUP_ID
        assert body['member_backup_ids'] == [BACKUP_ID]
        assert 'members' not in body

    def test_encryption_is_reported_by_the_member_backups_not_the_group(
            self, client, db, group_backup):
        """Each member backup carries its own ``encrypted``; the group does not
        repeat it."""
        body = client.get(f'{BASE}/{GROUP_BACKUP_ID}/').json()
        assert 'encrypted' not in body

    def test_times_are_utc_datetimes_and_an_uncommitted_one_has_no_completion(
            self, client, db, group_backup):
        body = client.get(f'{BASE}/{GROUP_BACKUP_ID}/').json()
        assert datetime.fromisoformat(body['created_at']) == datetime.fromtimestamp(1700000000, tz=UTC)
        assert body['completed_at'] is None

    def test_a_committed_group_backup_reports_when_it_completed(self, client, db, cluster):
        db.get_group_backup_by_id.return_value = factories.make_group_backup(
            status='completed', completed_at=1700000100)
        body = client.get(f'{BASE}/{GROUP_BACKUP_ID}/').json()
        assert datetime.fromisoformat(body['completed_at']) == datetime.fromtimestamp(1700000100, tz=UTC)

    def test_a_group_backup_without_an_error_reports_none(self, client, db, group_backup):
        body = client.get(f'{BASE}/{GROUP_BACKUP_ID}/').json()
        assert body['error_message'] is None

    def test_a_failed_group_backup_reports_its_error(self, client, db, cluster):
        db.get_group_backup_by_id.return_value = factories.make_group_backup(
            status='failed', error_message='member vol-1 could not be backed up')
        body = client.get(f'{BASE}/{GROUP_BACKUP_ID}/').json()
        assert body['error_message'] == 'member vol-1 could not be backed up'

    def test_404_when_the_group_backup_belongs_to_another_cluster(self, client, db, cluster):
        other = factories.make_group_backup(cluster_id='00000000-0000-0000-0000-000000000000')
        db.get_group_backup_by_id.return_value = other
        response = client.get(f'{BASE}/{GROUP_BACKUP_ID}/')
        assert response.status_code == 404


class TestRestoreGroupBackup:
    def test_restores_into_a_new_group_and_returns_the_members(
            self, client, db, group_backup, group_backup_controller):
        members = [{'lvol_id': 'lvol-a', 'member_name': 'db'},
                   {'lvol_id': 'lvol-b', 'member_name': 'data'}]
        group_backup_controller.restore_group_backup.return_value = (
            f'{CLUSTER_ID}/new-cg', members, None)

        response = client.post(
            f'{BASE}/{GROUP_BACKUP_ID}/restore',
            json={'into_group_name': 'restored-cg', 'pool': 'pool-1'})

        assert response.status_code == 202
        assert response.json() == {
            'group_id': f'{CLUSTER_ID}/new-cg', 'members': members}
        args, kwargs = group_backup_controller.restore_group_backup.call_args
        assert args[0] == group_backup.get_id()
        assert args[1] == 'restored-cg'
        assert args[2] == 'pool-1'

    def test_a_restore_error_is_409(self, client, db, group_backup, group_backup_controller):
        group_backup_controller.restore_group_backup.return_value = (
            None, None, 'consistency group restored-cg already exists')
        response = client.post(
            f'{BASE}/{GROUP_BACKUP_ID}/restore',
            json={'into_group_name': 'restored-cg', 'pool': 'pool-1'})
        # 409, never 422: a string detail under 422 is undecodable to the
        # generated clients, which type 422 as FastAPI's field-error list.
        assert response.status_code == 409
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


class TestCreateGroupBackup:
    """POST /backups/groups/ backs up one generation of the group the body names."""

    def _group(self, db, **attrs):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group(**attrs)

    def test_takes_a_fresh_generation_by_default(
            self, client, db, cluster, group_backup, group_backup_controller):
        self._group(db)
        group_backup_controller.create_group_backup.return_value = (group_backup.get_id(), None)

        response = client.post(f'{BASE}/', json={'consistency_group_id': CONSISTENCY_GROUP_ID})

        assert response.status_code == 201
        assert response.json()['id'] == GROUP_BACKUP_ID
        args, kwargs = group_backup_controller.create_group_backup.call_args
        assert args[0] == CG_COMPOSITE
        assert kwargs['group_seq'] is None

    def test_backs_up_a_named_generation(
            self, client, db, cluster, group_backup, group_backup_controller):
        self._group(db)
        group_backup_controller.create_group_backup.return_value = (group_backup.get_id(), None)

        response = client.post(f'{BASE}/', json={'consistency_group_id': CONSISTENCY_GROUP_ID, 'group_seq': 3})

        assert response.status_code == 201
        _, kwargs = group_backup_controller.create_group_backup.call_args
        assert kwargs['group_seq'] == 3

    def test_a_create_error_is_409(self, client, db, cluster, group_backup_controller):
        self._group(db)
        group_backup_controller.create_group_backup.return_value = (
            None, 'consistency group has no member snapshots at generation 3')
        response = client.post(f'{BASE}/', json={'consistency_group_id': CONSISTENCY_GROUP_ID, 'group_seq': 3})
        assert response.status_code == 409

    def test_an_unknown_group_is_404(self, client, db, cluster, group_backup_controller):
        db.get_consistency_group_by_id.side_effect = KeyError('ConsistencyGroup not found')
        response = client.post(f'{BASE}/', json={'consistency_group_id': CONSISTENCY_GROUP_ID})
        assert response.status_code == 404
        group_backup_controller.create_group_backup.assert_not_called()

    def test_a_group_of_another_cluster_is_404(self, client, db, cluster, group_backup_controller):
        self._group(db, cluster_id='00000000-0000-4000-8000-000000000000')
        response = client.post(f'{BASE}/', json={'consistency_group_id': CONSISTENCY_GROUP_ID})
        assert response.status_code == 404
        group_backup_controller.create_group_backup.assert_not_called()

    def test_the_group_is_required(self, client, db, cluster, group_backup_controller):
        response = client.post(f'{BASE}/', json={})
        assert response.status_code == 422
        group_backup_controller.create_group_backup.assert_not_called()


class TestGroupBackupMembers:
    def test_lists_the_member_backups(self, client, db, group_backup):
        member = factories.make_backup(group_backup_id=group_backup.get_id(), group_seq=7)
        db.get_backups_by_group_backup_id.return_value = [member]

        response = client.get(f'{BASE}/{GROUP_BACKUP_ID}/members/')

        assert response.status_code == 200
        body = response.json()
        assert [m['id'] for m in body] == [BACKUP_ID]
        assert body[0]['group_seq'] == 7
        db.get_backups_by_group_backup_id.assert_called_once_with(group_backup.get_id())

    def test_404_for_a_group_backup_of_another_cluster(self, client, db, cluster):
        db.get_group_backup_by_id.return_value = factories.make_group_backup(
            cluster_id='00000000-0000-4000-8000-000000000000')
        response = client.get(f'{BASE}/{GROUP_BACKUP_ID}/members/')
        assert response.status_code == 404


class TestGroupBackupRoutes:
    """Group backups live under /backups/, beside routes whose last segment is a
    backup id. /backups/groups/ must reach the group routes, not be read as a
    backup id that is not a UUID."""

    def test_groups_is_not_read_as_a_backup_id(self, client, db, group_backup):
        response = client.get(f'/api/v2/clusters/{CLUSTER_ID}/backups/groups/')
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_the_old_paths_are_gone(self, client, db, cluster, group_backup):
        assert client.get(f'/api/v2/clusters/{CLUSTER_ID}/group-backups/').status_code == 404
        assert client.post(f'{CG_BASE}/backups', json={}).status_code in (404, 405)


class TestExportGroupBackup:
    def test_exports_the_group_backup_as_a_unit(
            self, client, db, group_backup, group_backup_controller):
        group_backup_controller.export_group_backup.return_value = _group_export_model()

        response = client.get(f'{BASE}/export?group_backup_id={GROUP_BACKUP_ID}')

        assert response.status_code == 200
        body = response.json()
        assert body['group_manifest']['group_backup_id'] == GROUP_BACKUP_ID
        assert [m['backup_id'] for m in body['members']['groups'][0]['manifests']] == [
            BACKUP_ID]
        group_backup_controller.export_group_backup.assert_called_once_with(
            group_backup.get_id(), cluster_id=CLUSTER_ID)

    def test_404_for_an_unknown_group_backup(self, client, db, cluster):
        db.get_group_backup_by_id.side_effect = KeyError('nope')
        response = client.get(f'{BASE}/export?group_backup_id={GROUP_BACKUP_ID}')
        assert response.status_code == 404

    def test_404_when_the_group_backup_belongs_to_another_cluster(
            self, client, db, cluster):
        db.get_group_backup_by_id.return_value = factories.make_group_backup(
            cluster_id='00000000-0000-0000-0000-000000000000')
        response = client.get(f'{BASE}/export?group_backup_id={GROUP_BACKUP_ID}')
        assert response.status_code == 404


class TestImportGroupBackup:
    """The body is a union of two shapes, as the single-volume import is."""

    def test_inline_export_is_validated_by_the_body_type(
            self, client, db, cluster, group_backup_controller):
        group_backup_controller.import_group_backup.return_value = f'{CLUSTER_ID}/{GROUP_BACKUP_ID}'

        response = client.post(f'{BASE}/import', json={'metadata': _group_export()})

        assert response.status_code == 200
        assert response.json() == {
            'imported': 1, 'group_backup_ids': [f'{CLUSTER_ID}/{GROUP_BACKUP_ID}']}
        (export,), kwargs = group_backup_controller.import_group_backup.call_args
        assert str(export.group_manifest.group_backup_id) == GROUP_BACKUP_ID
        assert kwargs['cluster_id'] == CLUSTER_ID

    def test_a_bucket_reads_the_group_backups_itself(
            self, client, db, cluster, group_backup_controller):
        group_backup_controller.import_group_backup_from_bucket.return_value = [
            f'{CLUSTER_ID}/gb-a', f'{CLUSTER_ID}/gb-b']

        response = client.post(f'{BASE}/import', json={'bucket': _LOCATION})

        assert response.status_code == 200
        assert response.json()['imported'] == 2
        group_backup_controller.import_group_backup.assert_not_called()

    def test_naming_both_sources_is_rejected(
            self, client, db, cluster, group_backup_controller):
        response = client.post(f'{BASE}/import', json={
            'metadata': _group_export(), 'bucket': _LOCATION})
        assert response.status_code == 422

    def test_naming_neither_source_is_rejected(
            self, client, db, cluster, group_backup_controller):
        response = client.post(f'{BASE}/import', json={})
        assert response.status_code == 422

    def test_an_unreadable_bucket_is_a_bad_request(
            self, client, db, cluster, group_backup_controller):
        from simplyblock_core.controllers.backup.manifest import ManifestError
        group_backup_controller.import_group_backup_from_bucket.side_effect = \
            ManifestError('no such bucket')

        response = client.post(f'{BASE}/import', json={'bucket': _LOCATION})

        assert response.status_code == 400
        assert 'no such bucket' in response.json()['detail']

    def test_a_torn_export_is_a_bad_request(
            self, client, db, cluster, group_backup_controller):
        group_backup_controller.import_group_backup.side_effect = ValueError(
            'group export is missing member manifests')

        response = client.post(f'{BASE}/import', json={'metadata': _group_export()})

        assert response.status_code == 400
        assert 'missing member manifests' in response.json()['detail']

    def test_a_precondition_error_is_not_mapped_here(
            self, client, db, cluster, group_backup_controller):
        """app.py maps PreconditionError to 400 for the whole API; the endpoint
        lets it through rather than mapping it a second, disagreeing way."""
        import pytest

        from simplyblock_core.exceptions import PreconditionError
        group_backup_controller.import_group_backup.side_effect = PreconditionError(
            'already exists')

        with pytest.raises(PreconditionError):
            client.post(f'{BASE}/import', json={'metadata': _group_export()})


class TestDiscoverGroupBackups:
    def test_returns_the_group_backups_the_bucket_holds(
            self, client, db, group_backup_controller):
        from simplyblock_core.controllers.backup.manifest import GroupBackupManifest
        group_backup_controller.discover_group_backups.return_value = [
            GroupBackupManifest.model_validate(_GROUP_MANIFEST)]

        response = client.post(f'{BASE}/discover', json=_LOCATION)

        assert response.status_code == 200
        assert [g['group_backup_id'] for g in response.json()] == [GROUP_BACKUP_ID]

    def test_credentials_are_masked_in_a_failure_response(
            self, client, db, group_backup_controller):
        from simplyblock_core.controllers.backup.manifest import ManifestError
        group_backup_controller.discover_group_backups.side_effect = \
            ManifestError('unreachable')

        response = client.post(f'{BASE}/discover', json={
            **_LOCATION,
            'credentials': {'access_key_id': 'AKIA', 'secret_access_key': 's3cr3t'}})

        assert response.status_code == 400
        assert 's3cr3t' not in response.text
