"""Unit tests for /api/v2/.../subsystems/{nqn}/migrations endpoints (migration_controller mocked).

Covers the merged single-lvol / batch (shared-namespace) migration endpoint:
create picks migration_controller.create_migration vs. create_batch_migration
based on the resolved lvol's max_namespace_per_subsys, with no explicit batch
flag from the caller. continue/cancel/detail dispatch the same way based on
whether the resolved id is an LVolMigration or LVolMigrationGroup.
"""

from simplyblock_core import constants
from simplyblock_core.models.lvol_migration_group import LVolMigrationGroup

from tests.unit.web.api.v2._factories import (
    CLUSTER_ID,
    MIGRATION_ID,
    VOLUME_ID,
    VOLUME_NQN,
)

BASE = f'/api/v2/clusters/{CLUSTER_ID}/subsystems/{VOLUME_NQN}/migrations'
TARGET_NODE_ID = '44444444-4444-4444-4444-444444444445'
GROUP_ID = 'cccccccc-cccc-cccc-cccc-cccccccccccc'


def _make_group(**attrs):
    group = LVolMigrationGroup()
    group.uuid = GROUP_ID
    group.cluster_id = CLUSTER_ID
    group.source_node_id = '44444444-4444-4444-4444-444444444444'
    group.target_node_id = TARGET_NODE_ID
    group.target_nqn = VOLUME_NQN
    group.phase = LVolMigrationGroup.PHASE_SNAP_COPY
    group.status = LVolMigrationGroup.STATUS_RUNNING
    for name, value in attrs.items():
        setattr(group, name, value)
    return group


class TestListMigrations:

    def test_returns_single_migrations_of_subsystem(self, client, db, volume, migration):
        response = client.get(f'{BASE}/')

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == MIGRATION_ID
        assert body['lvol_id'] == VOLUME_ID
        db.get_migrations.assert_called_once_with(CLUSTER_ID)

    def test_returns_batch_groups_of_subsystem(self, client, db, volume, migration):
        migration.migration_group_id = GROUP_ID  # worker — excluded, group represents it
        group = _make_group()
        db.get_migration_groups.return_value = [group]

        response = client.get(f'{BASE}/')

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == GROUP_ID


class TestCreateMigration:

    def test_calls_create_migration_for_lone_lvol(self, client, db, volume, migration, migration_controller):
        migration_controller.create_migration.return_value = (MIGRATION_ID, [])

        response = client.post(f'{BASE}/', json={'target_node_id': TARGET_NODE_ID})

        assert response.status_code == 201
        migration_controller.create_migration.assert_called_once_with(
            VOLUME_ID, TARGET_NODE_ID,
            ctrl_loss_tmo=constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO,
            host_nqn=None,
        )
        migration_controller.create_batch_migration.assert_not_called()
        assert response.json()['id'] == MIGRATION_ID
        assert response.headers['Location'].endswith(f'/migrations/{MIGRATION_ID}/')

    def test_calls_create_batch_migration_for_shared_subsystem(
        self, client, db, volume, migration_controller,
    ):
        volume.max_namespace_per_subsys = 4
        migration_controller.create_batch_migration.return_value = (GROUP_ID, [])
        db.get_migration_group_by_id.return_value = _make_group()

        response = client.post(f'{BASE}/', json={'target_node_id': TARGET_NODE_ID})

        assert response.status_code == 201
        migration_controller.create_batch_migration.assert_called_once_with(
            VOLUME_ID, TARGET_NODE_ID,
            ctrl_loss_tmo=constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO,
            host_nqn=None,
        )
        migration_controller.create_migration.assert_not_called()
        assert response.json()['id'] == GROUP_ID

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

    def test_migration_of_other_subsystem_returns_404(self, client, db, volume, migration):
        volume.nqn = 'nqn.2023-02.io.simplyblock:other-volume'

        response = client.get(f'{BASE}/{MIGRATION_ID}/')

        assert response.status_code == 404

    def test_returns_batch_group(self, client, db, cluster):
        db.get_migration_group_by_id.return_value = _make_group()

        response = client.get(f'{BASE}/{GROUP_ID}/')

        assert response.status_code == 200
        assert response.json()['id'] == GROUP_ID


class TestContinueMigration:

    def test_starts_migration(self, client, db, migration, migration_controller):
        migration_controller.start_migration.return_value = MIGRATION_ID

        response = client.post(
            f'{BASE}/{MIGRATION_ID}/continue', json={'max_retries': 5})

        assert response.status_code == 200
        assert response.json() == {'migration_id': MIGRATION_ID}
        migration_controller.start_migration.assert_called_once_with(
            migration_id=MIGRATION_ID, max_retries=5, deadline_seconds=14400)

    def test_starts_batch_migration(self, client, db, cluster, migration_controller):
        db.get_migration_group_by_id.return_value = _make_group()
        migration_controller.start_batch_migration.return_value = GROUP_ID

        response = client.post(
            f'{BASE}/{GROUP_ID}/continue', json={'max_retries': 5})

        assert response.status_code == 200
        assert response.json() == {'migration_id': GROUP_ID}
        migration_controller.start_batch_migration.assert_called_once_with(
            group_id=GROUP_ID, max_retries=5, deadline_seconds=14400)


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

    def test_cancels_batch_migration(self, client, db, cluster, migration_controller):
        db.get_migration_group_by_id.return_value = _make_group()
        migration_controller.cancel_batch_migration.return_value = None

        response = client.delete(f'{BASE}/{GROUP_ID}/')

        assert response.status_code == 200
        migration_controller.cancel_batch_migration.assert_called_once_with(GROUP_ID)


class TestCleanupMigrationTarget:

    def test_cleans_up_migration(self, client, db, migration, migration_controller):
        migration_controller.cleanup_migration_target.return_value = {
            'deleted': [], 'not_found': [], 'skipped': [], 'errors': [],
        }

        response = client.post(f'{BASE}/{MIGRATION_ID}/cleanup-target')

        assert response.status_code == 200
        migration_controller.cleanup_migration_target.assert_called_once_with(MIGRATION_ID)

    def test_batch_group_returns_400(self, client, db, cluster):
        db.get_migration_group_by_id.return_value = _make_group()

        response = client.post(f'{BASE}/{GROUP_ID}/cleanup-target')

        assert response.status_code == 400
