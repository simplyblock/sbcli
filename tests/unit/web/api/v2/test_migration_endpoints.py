# coding=utf-8
"""Unit tests for /api/v2/.../volumes/{id}/migrations endpoints (migration_controller mocked)."""

from simplyblock_core import constants

from tests.unit.web.api.v2._factories import (
    CLUSTER_ID,
    MIGRATION_ID,
    POOL_ID,
    VOLUME_ID,
)

BASE = f'/api/v2/clusters/{CLUSTER_ID}/storage-pools/{POOL_ID}/volumes/{VOLUME_ID}/migrations'
TARGET_NODE_ID = '44444444-4444-4444-4444-444444444445'


class TestListMigrations:

    def test_returns_migrations_of_cluster(self, client, db, volume, migration):
        response = client.get(f'{BASE}/')

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == MIGRATION_ID
        assert body['lvol_id'] == VOLUME_ID
        db.get_migrations.assert_called_once_with(CLUSTER_ID)


class TestCreateMigration:

    def test_calls_create_migration(self, client, db, volume, migration, migration_controller):
        migration_controller.create_migration.return_value = (MIGRATION_ID, [])

        response = client.post(f'{BASE}/', json={'target_node_id': TARGET_NODE_ID})

        assert response.status_code == 201
        migration_controller.create_migration.assert_called_once_with(
            VOLUME_ID, TARGET_NODE_ID,
            ctrl_loss_tmo=constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO,
            host_nqn=None,
        )
        assert response.json()['id'] == MIGRATION_ID
        assert response.headers['Location'].endswith(f'/migrations/{MIGRATION_ID}/')

    def test_conflict_returns_400(self, client, db, volume, migration_controller):
        migration_controller.create_migration.side_effect = ValueError('already migrating')

        response = client.post(f'{BASE}/', json={'target_node_id': TARGET_NODE_ID})

        assert response.status_code == 400


class TestGetMigration:

    def test_returns_migration(self, client, db, migration):
        response = client.get(f'{BASE}/{MIGRATION_ID}/')

        assert response.status_code == 200
        assert response.json()['id'] == MIGRATION_ID
        db.get_migration_by_id.assert_called_once_with(MIGRATION_ID)

    def test_migration_of_other_volume_returns_404(self, client, db, migration):
        migration.lvol_id = '33333333-3333-3333-3333-333333333334'

        response = client.get(f'{BASE}/{MIGRATION_ID}/')

        assert response.status_code == 404


class TestContinueMigration:

    def test_starts_migration(self, client, db, migration, migration_controller):
        migration_controller.start_migration.return_value = MIGRATION_ID

        response = client.post(
            f'{BASE}/{MIGRATION_ID}/continue', json={'max_retries': 5})

        assert response.status_code == 200
        assert response.json() == {'migration_id': MIGRATION_ID}
        migration_controller.start_migration.assert_called_once_with(
            migration_id=MIGRATION_ID, max_retries=5, deadline_seconds=14400)


class TestCancelMigration:

    def test_cancels_migration(self, client, db, migration, migration_controller):
        migration_controller.cancel_migration.return_value = None

        response = client.delete(f'{BASE}/{MIGRATION_ID}/')

        assert response.status_code == 200
        assert response.json() == {'status': 'cancelled'}
        migration_controller.cancel_migration.assert_called_once_with(MIGRATION_ID)

    def test_cancel_inactive_returns_400(self, client, db, migration, migration_controller):
        migration_controller.cancel_migration.side_effect = ValueError('Migration is not active')

        response = client.delete(f'{BASE}/{MIGRATION_ID}/')

        assert response.status_code == 400
