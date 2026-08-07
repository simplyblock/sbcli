# coding=utf-8
"""Unit tests for the v2 edge routers (mounted standalone, auth bypassed —
auth is attached at the api/v2 package level and covered by test_auth)."""
import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from simplyblock_core.models.cluster import Cluster
from simplyblock_edge import edge_cluster_ops
from simplyblock_web.api.v2.cluster import edge as edge_router


@pytest.fixture()
def client(kv, spdk, fake_k8s):
    app = FastAPI()
    instance_api = APIRouter(prefix='/clusters/{cluster_id}')
    instance_api.include_router(edge_router.node_api, prefix='/edge-nodes')
    instance_api.include_router(edge_router.volume_api, prefix='/edge-volumes')
    app.include_router(instance_api)
    return TestClient(app)


@pytest.fixture()
def cluster(kv, spdk, fake_k8s):
    cluster = edge_cluster_ops.create_edge_cluster("edge-api")
    edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    return cluster


def test_non_edge_cluster_is_404(client, kv):
    hyper = Cluster()
    hyper.uuid = "11111111-1111-1111-1111-111111111111"
    hyper.cluster_name = "hyper"
    hyper.write_to_db(kv)
    response = client.get(f'/clusters/{hyper.uuid}/edge-nodes/')
    assert response.status_code == 404


def test_list_and_detail_nodes(client, cluster):
    response = client.get(f'/clusters/{cluster.uuid}/edge-nodes/')
    assert response.status_code == 200
    nodes = response.json()
    assert len(nodes) == 1
    assert nodes[0]["hostname"] == "worker-1"
    assert nodes[0]["is_primary"] is True

    detail = client.get(f'/clusters/{cluster.uuid}/edge-nodes/{nodes[0]["uuid"]}')
    assert detail.status_code == 200
    assert detail.json()["partitions"][0]["device_path"] == "/dev/sdb1"


def test_add_node_validations(client, cluster):
    duplicate = client.post(f'/clusters/{cluster.uuid}/edge-nodes/', json={
        "hostname": "worker-1", "mgmt_ip": "10.0.0.1", "partitions": ["/dev/sdb1"]})
    assert duplicate.status_code == 400

    missing_partitions = client.post(f'/clusters/{cluster.uuid}/edge-nodes/', json={
        "hostname": "worker-2", "mgmt_ip": "10.0.0.2", "partitions": []})
    assert missing_partitions.status_code == 422


def test_volume_crud_and_connect(client, cluster):
    created = client.post(f'/clusters/{cluster.uuid}/edge-volumes/',
                          json={"name": "pvc-1", "size": "1GiB"})
    assert created.status_code == 201
    volume = created.json()
    assert volume["size"] == 2 ** 30

    duplicate = client.post(f'/clusters/{cluster.uuid}/edge-volumes/',
                            json={"name": "pvc-1", "size": "1GiB"})
    assert duplicate.status_code == 400

    listed = client.get(f'/clusters/{cluster.uuid}/edge-volumes/')
    assert [v["name"] for v in listed.json()] == ["pvc-1"]

    connect = client.get(f'/clusters/{cluster.uuid}/edge-volumes/{volume["uuid"]}/connect')
    assert connect.status_code == 200
    assert connect.json()[0]["nqn"] == volume["nqn"]

    resized = client.put(f'/clusters/{cluster.uuid}/edge-volumes/{volume["uuid"]}',
                         json={"size": "2GiB"})
    assert resized.status_code == 200
    assert resized.json()["size"] == 2 ** 31

    deleted = client.delete(f'/clusters/{cluster.uuid}/edge-volumes/{volume["uuid"]}')
    assert deleted.status_code == 204
    assert client.get(f'/clusters/{cluster.uuid}/edge-volumes/').json() == []


def test_device_endpoints(client, cluster, kv, spdk, fake_k8s):
    node_id = client.get(f'/clusters/{cluster.uuid}/edge-nodes/').json()[0]["uuid"]

    # single-partition single-node: replace rejected
    replace = client.put(f'/clusters/{cluster.uuid}/edge-nodes/{node_id}/devices',
                         json={"old_path": "/dev/sdb1", "new_path": "/dev/sdz1"})
    assert replace.status_code == 400

    add = client.post(f'/clusters/{cluster.uuid}/edge-nodes/{node_id}/devices',
                      json={"device_path": "/dev/sdz1"})
    assert add.status_code == 400  # needs raid5 (3+ partitions)


def test_restart_returns_task(client, cluster):
    node_id = client.get(f'/clusters/{cluster.uuid}/edge-nodes/').json()[0]["uuid"]
    response = client.post(f'/clusters/{cluster.uuid}/edge-nodes/{node_id}/restart')
    assert response.status_code == 202
    assert response.json()["task_id"]
