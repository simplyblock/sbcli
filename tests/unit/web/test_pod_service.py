"""The Service the agent keeps in front of an SPDK pod on the pod network, and the pod
spec of such a pod: off the host network with no Multus annotation, its proxy bound to
its own address, labelled so the Service selects it and not another cluster's pod."""

from unittest.mock import MagicMock

from kubernetes.client import ApiException

import tests.unit.web.test_network_attachments as attachments_tests
from simplyblock_web.api.internal.storage_node import network_attachments, pod_service


def _core(existing=None):
    core = MagicMock()
    if existing is None:
        core.read_namespaced_service.side_effect = ApiException(status=404)
    else:
        core.read_namespaced_service.return_value = existing
    core.create_namespaced_service.return_value.spec.cluster_ip = "10.103.252.64"
    return core


def _existing(ports, cluster_ip="10.103.252.64"):
    service = MagicMock()
    service.spec.cluster_ip = cluster_ip
    service.spec.ports = [MagicMock(port=p) for p in ports]
    return service


class TestEnsuringTheService:
    def test_a_missing_service_is_created_selecting_the_pod_of_this_cluster_with_the_rpc_port_among_the_ports(self):
        core = _core()

        address = pod_service.ensure(core, "simplyblock", 4426, "15260308-01ab", [4432, 4433])

        assert address == "10.103.252.64"
        body = core.create_namespaced_service.call_args.kwargs["body"]
        assert body.metadata.name == "snode-spdk-svc-4426-152603"
        assert body.spec.selector == {"app": "spdk-app-4426", "simplyblock.io/cluster": "15260308-01ab"}
        assert [p.port for p in body.spec.ports] == [4426, 4432, 4433]
        assert all(p.target_port == p.port for p in body.spec.ports)

    def test_an_existing_service_with_the_right_ports_is_left_alone_and_its_address_answered(self):
        core = _core(_existing([4426, 4432]))

        assert pod_service.ensure(core, "simplyblock", 4426, "c1", [4432]) == "10.103.252.64"
        core.create_namespaced_service.assert_not_called()
        core.patch_namespaced_service.assert_not_called()

    def test_new_ports_are_patched_onto_the_existing_service_keeping_its_address(self):
        core = _core(_existing([4426, 4432]))

        pod_service.ensure(core, "simplyblock", 4426, "c1", [4432, 4434, 4435])

        patched = core.patch_namespaced_service.call_args.args[2]
        assert [p["port"] for p in patched["spec"]["ports"]] == [4426, 4432, 4434, 4435]
        # a strategic merge would keep a port the node no longer serves on
        assert core.patch_namespaced_service.call_args.kwargs["_content_type"] == "application/merge-patch+json"

    def test_a_port_the_node_stopped_serving_on_is_dropped(self):
        core = _core(_existing([4426, 4432, 9090]))

        pod_service.ensure(core, "simplyblock", 4426, "c1", [4432])

        patched = core.patch_namespaced_service.call_args.args[2]
        assert [p["port"] for p in patched["spec"]["ports"]] == [4426, 4432]

    def test_a_read_error_other_than_not_found_propagates(self):
        core = MagicMock()
        core.read_namespaced_service.side_effect = ApiException(status=500)

        try:
            pod_service.ensure(core, "simplyblock", 4426, "c1", [])
        except ApiException as e:
            assert e.status == 500
        else:
            raise AssertionError("a server error was swallowed")


class TestDeletingTheService:
    def test_the_service_is_deleted_by_its_name(self):
        core = MagicMock()

        pod_service.delete(core, "simplyblock", 4426, "15260308-01ab")

        core.delete_namespaced_service.assert_called_once_with("snode-spdk-svc-4426-152603", "simplyblock")

    def test_a_service_already_gone_is_fine(self):
        core = MagicMock()
        core.delete_namespaced_service.side_effect = ApiException(status=404)

        pod_service.delete(core, "simplyblock", 4426, "c1")


class TestThePodOnThePodNetwork:
    def test_it_is_off_the_host_network_with_no_annotation_and_carries_the_cluster_label(self):
        pod = attachments_tests._render(POD_NETWORK=True, FULL_CLUSTER_ID="15260308-01ab")

        assert not pod["spec"].get("hostNetwork")
        assert pod["spec"]["dnsPolicy"] == "ClusterFirst"
        assert "k8s.v1.cni.cncf.io/networks" not in (pod["metadata"].get("annotations") or {})
        assert pod["metadata"]["labels"]["simplyblock.io/cluster"] == "15260308-01ab"

    def test_its_proxy_binds_the_pods_own_address(self):
        pod = attachments_tests._render(POD_NETWORK=True, SERVER_IP=network_attachments.server_ip([], pod_network=True))

        env = {e["name"]: e for c in pod["spec"]["containers"] if c["name"] == "spdk-proxy-container" for e in c["env"]}
        assert env["SERVER_IP"]["value"] == network_attachments.POD_IP_REFERENCE
        assert env["POD_IP"]["valueFrom"]["fieldRef"]["fieldPath"] == "status.podIP"

    def test_a_host_network_pod_binds_the_control_planes_value_as_before(self):
        assert network_attachments.server_ip([]) == ""

    def test_a_pod_without_attachments_reports_as_soon_as_it_runs(self):
        assert network_attachments.report(attachments_tests._Pod(status=""), []) == {"pod_ip": "10.42.1.5", "interfaces": []}
        assert network_attachments.report(attachments_tests._Pod(phase="Pending"), []) is None
