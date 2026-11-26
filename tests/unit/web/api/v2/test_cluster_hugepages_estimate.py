"""Unit tests for POST /api/v2/clusters/sizing/hugepages-estimate.

design-preprovisioned-hugepages.md S:7 -- stateless, no cluster required.
"""

from simplyblock_core import utils as core_utils


class TestHugePagesEstimate:

    def test_matches_calculate_minimum_hp_memory_with_no_known_devices(self, client):
        response = client.post(
            '/api/v2/clusters/sizing/hugepages-estimate',
            json={'max_subsys': 40, 'vcpu_count': 4})

        assert response.status_code == 200
        small, large = core_utils.calculate_pool_count(0, 0, 4, 4, 40)
        expected = core_utils.calculate_minimum_hp_memory(small, large, 40, 0, 4)
        assert response.json() == {
            'minimum_hp_memory_mib': core_utils.convert_size(expected, 'MiB', round_up=True)}

    def test_needs_no_cluster_or_db_access(self, client, db):
        response = client.post(
            '/api/v2/clusters/sizing/hugepages-estimate',
            json={'max_subsys': 10, 'vcpu_count': 4})

        assert response.status_code == 200
        db.get_cluster_by_id.assert_not_called()

    def test_rejects_negative_max_subsys(self, client):
        response = client.post(
            '/api/v2/clusters/sizing/hugepages-estimate',
            json={'max_subsys': -1, 'vcpu_count': 4})

        assert response.status_code == 422

    def test_rejects_negative_vcpu_count(self, client):
        response = client.post(
            '/api/v2/clusters/sizing/hugepages-estimate',
            json={'max_subsys': 40, 'vcpu_count': -1})

        assert response.status_code == 422

    def test_larger_sizing_requires_more_memory(self, client):
        small_body = client.post(
            '/api/v2/clusters/sizing/hugepages-estimate',
            json={'max_subsys': 10, 'vcpu_count': 4}).json()
        large_body = client.post(
            '/api/v2/clusters/sizing/hugepages-estimate',
            json={'max_subsys': 500, 'vcpu_count': 64}).json()

        assert large_body['minimum_hp_memory_mib'] > small_body['minimum_hp_memory_mib']
