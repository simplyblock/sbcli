# coding=utf-8
"""Unit tests for /api/v2/.../volumes endpoints (lvol/snapshot controllers mocked)."""

from simplyblock_core import utils as core_utils
from simplyblock_core.models.lvol_model import LVol

from tests.unit.web.api.v2._factories import (
    CLUSTER_ID,
    POOL_ID,
    SNAPSHOT_ID,
    STORAGE_NODE_ID,
    VOLUME_ID,
)

BASE = f'/api/v2/clusters/{CLUSTER_ID}/storage-pools/{POOL_ID}/volumes'


class TestListVolumes:

    def test_returns_volumes_of_pool(self, client, db, volume):
        response = client.get(f'{BASE}/')

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == VOLUME_ID
        assert body['name'] == 'volume-1'
        assert body['storage_node_id'] == STORAGE_NODE_ID
        assert body['pool_uuid'] == POOL_ID
        db.get_lvols_by_pool_id.assert_called_once_with(POOL_ID)


class TestCreateVolume:

    def test_calls_add_lvol_ha(self, client, db, pool, lvol_controller):
        db.get_lvol_by_name.side_effect = KeyError('LVol not found')
        lvol_controller.add_lvol_ha.return_value = (VOLUME_ID, None)

        response = client.post(f'{BASE}/', json={'name': 'volume-1', 'size': '10G'})

        assert response.status_code == 201
        lvol_controller.add_lvol_ha.assert_called_once_with(
            name='volume-1',
            size=core_utils.parse_size('10G'),
            pool_id_or_name=POOL_ID,
            use_crypto=False,
            max_size=0,
            max_rw_iops=0,
            max_rw_mbytes=0,
            max_r_mbytes=0,
            max_w_mbytes=0,
            host_id_or_name=None,
            ha_type='default',
            use_comp=False,
            distr_vuid=0,
            lvol_priority_class=0,
            namespaced=False,
            pvc_name=None,
            ndcs=0,
            npcs=0,
            allowed_hosts=None,
            fabric='tcp',
            max_namespace_per_subsys=1,
            do_replicate=False,
            replication_cluster_id=None,
        )
        assert response.headers['Location'].endswith(f'/volumes/{VOLUME_ID}/')

    def test_clone_parameters_call_snapshot_clone(self, client, db, pool, lvol_controller, snapshot_controller):
        db.get_lvol_by_name.side_effect = KeyError('LVol not found')
        snapshot_controller.clone.return_value = (VOLUME_ID, None)

        response = client.post(f'{BASE}/', json={'name': 'clone-1', 'snapshot_id': SNAPSHOT_ID})

        assert response.status_code == 201
        snapshot_controller.clone.assert_called_once_with(
            SNAPSHOT_ID, 'clone-1', 0,
            pvc_name=None, pvc_namespace=None, delete_snap_on_lvol_delete=False,
        )
        lvol_controller.add_lvol_ha.assert_not_called()

    def test_existing_name_returns_409(self, client, db, pool, volume, lvol_controller):
        db.get_lvol_by_name.return_value = volume

        response = client.post(f'{BASE}/', json={'name': 'volume-1', 'size': '10G'})

        assert response.status_code == 409
        lvol_controller.add_lvol_ha.assert_not_called()


class TestGetVolume:

    def test_returns_volume_with_replication_info(self, client, db, volume, lvol_controller):
        lvol_controller.get_replication_info.return_value = {'source': True}

        response = client.get(f'{BASE}/{VOLUME_ID}/')

        assert response.status_code == 200
        body = response.json()
        assert body['id'] == VOLUME_ID
        assert body['rep_info'] == {'source': True}
        lvol_controller.get_replication_info.assert_called_once_with(VOLUME_ID)
        db.get_lvol_by_id.assert_called_once_with(VOLUME_ID)

    def test_volume_of_other_pool_returns_404(self, client, db, volume, lvol_controller):
        volume.pool_uuid = '22222222-2222-2222-2222-222222222223'

        response = client.get(f'{BASE}/{VOLUME_ID}/')

        assert response.status_code == 404


class TestUpdateVolume:

    def test_updates_qos_attributes_and_resizes(self, client, volume, lvol_controller):
        lvol_controller.set_lvol.return_value = True

        response = client.put(f'{BASE}/{VOLUME_ID}/', json={'name': 'renamed', 'size': '20G'})

        assert response.status_code == 204
        lvol_controller.set_lvol.assert_called_once_with(
            uuid=VOLUME_ID, name='renamed',
            max_rw_iops=0, max_rw_mbytes=0, max_r_mbytes=0, max_w_mbytes=0,
        )
        lvol_controller.resize_lvol.assert_called_once_with(
            VOLUME_ID, core_utils.parse_size('20G'))

    def test_size_only_update_skips_set_lvol(self, client, volume, lvol_controller):
        response = client.put(f'{BASE}/{VOLUME_ID}/', json={'size': '20G'})

        assert response.status_code == 204
        lvol_controller.set_lvol.assert_not_called()


class TestDeleteVolume:

    def test_deletes_volume(self, client, volume, lvol_controller):
        response = client.delete(f'{BASE}/{VOLUME_ID}/')

        assert response.status_code == 204
        lvol_controller.delete_lvol.assert_called_once_with(volume)

    def test_deleted_volume_returns_404(self, client, volume, lvol_controller):
        volume.status = LVol.STATUS_DELETED

        response = client.delete(f'{BASE}/{VOLUME_ID}/')

        assert response.status_code == 404
        lvol_controller.delete_lvol.assert_not_called()


class TestConnectVolume:

    def test_returns_connection_details(self, client, volume, lvol_controller):
        details = [{'connect': 'nvme connect ...'}]
        lvol_controller.connect_lvol.return_value = (details, None)

        response = client.get(f'{BASE}/{VOLUME_ID}/connect')

        assert response.status_code == 200
        assert response.json() == details
        lvol_controller.connect_lvol.assert_called_once_with(VOLUME_ID, host_nqn=None)

    def test_error_returns_404(self, client, volume, lvol_controller):
        lvol_controller.connect_lvol.return_value = (None, 'volume offline')

        response = client.get(f'{BASE}/{VOLUME_ID}/connect')

        assert response.status_code == 404


class TestVolumeStats:

    def test_capacity(self, client, volume, lvol_controller):
        lvol_controller.get_capacity.return_value = [{'date': 1}]

        response = client.get(f'{BASE}/{VOLUME_ID}/capacity', params={'history': '5'})

        assert response.status_code == 200
        lvol_controller.get_capacity.assert_called_once_with(VOLUME_ID, '5', parse_sizes=False)

    def test_iostats(self, client, volume, lvol_controller):
        lvol_controller.get_io_stats.return_value = [{'date': 1}]

        response = client.get(f'{BASE}/{VOLUME_ID}/iostats')

        assert response.status_code == 200
        lvol_controller.get_io_stats.assert_called_once_with(
            VOLUME_ID, None, parse_sizes=False, with_sizes=True)


class TestCreateVolumeSnapshot:

    def test_calls_snapshot_add(self, client, volume, snapshot_controller):
        snapshot_controller.add.return_value = (SNAPSHOT_ID, None)

        response = client.post(
            f'{BASE}/{VOLUME_ID}/snapshots', json={'name': 'snapshot-1'})

        assert response.status_code == 201
        snapshot_controller.add.assert_called_once_with(VOLUME_ID, 'snapshot-1', backup=False)
        assert response.headers['Location'].endswith(f'/snapshots/{SNAPSHOT_ID}/')

    def test_duplicate_name_returns_409(self, client, volume, snapshot_controller):
        snapshot_controller.add.return_value = (None, 'Snapshot name must be unique')

        response = client.post(
            f'{BASE}/{VOLUME_ID}/snapshots', json={'name': 'snapshot-1'})

        assert response.status_code == 409


class TestVolumeHosts:

    def test_add_host(self, client, volume, lvol_controller):
        lvol_controller.add_host_to_lvol.return_value = ({'added': True}, None)

        response = client.post(
            f'{BASE}/{VOLUME_ID}/hosts',
            json={'host_nqn': 'nqn.2014-08.org.nvmexpress:host-1'},
        )

        assert response.status_code == 201
        lvol_controller.add_host_to_lvol.assert_called_once_with(
            VOLUME_ID, 'nqn.2014-08.org.nvmexpress:host-1')

    def test_remove_host(self, client, volume, lvol_controller):
        lvol_controller.remove_host_from_lvol.return_value = (True, None)

        response = client.delete(
            f'{BASE}/{VOLUME_ID}/hosts/nqn.2014-08.org.nvmexpress:host-1')

        assert response.status_code == 204
        lvol_controller.remove_host_from_lvol.assert_called_once_with(
            VOLUME_ID, 'nqn.2014-08.org.nvmexpress:host-1')
