"""apply_config on a Kubernetes worker sizes the huge-page pool from what
Kubernetes says the pool's pages are claimed by, not from what the host carried
the first time simplyblock saw it.

Regression: e2e run 44678939560 (lblk, 2026-11-20). The test bootstrap reserved
half of a 31 GiB worker as huge pages, 7958 pages that no pod requested. The init
Job recorded them as the host's own and apply_config added simplyblock's 3990 on
top: 12028 pages, 23.5 GiB. The kubelet takes the whole pool out of the node's
memory, used or not, so 7.6 GiB was left for pods, and the SPDK pod's 2188 MiB did
not fit beside the 5.9 GiB the control-plane pods on that worker requested. It
stayed Pending on 'Insufficient memory' until the run timed out.

A pod can use only the huge pages it requests (the kubelet sets every container's
hugetlb limit to its request), so the requests of the pods on the node are what
the pool is claimed by. simplyblock's own storage pods are left out of that sum:
their pages are the ones being sized.
"""
import os
from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client import (
    ApiException,
    V1Container,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1PodStatus,
    V1ResourceRequirements,
)

from simplyblock_core import utils
from simplyblock_web.api.internal.storage_node import kubernetes as k8s_node

WORKER = "githubactions-lblk-storage-node-1"

#: The node config the e2e run's worker carried: 6.5 GiB of huge pages for SPDK,
#: which apply_config turns into 3990 pages on NUMA node 0.
NODE_CONFIG = {"huge_page_memory": 6980607384, "max_size": 0, "socket": 0}

#: What the bootstrap reserved on that worker before simplyblock arrived.
HOST_RESERVED = 7958


class _Host:
    """One NUMA node's huge-page count and simplyblock's records, under a directory."""

    def __init__(self, root):
        self.records = os.path.join(root, "run")
        self.count_file = os.path.join(root, "nr_hugepages")
        os.makedirs(self.records)

    def _count_path(self, node, page_size_kb=2048):
        return self.count_file

    def count(self):
        with open(self.count_file) as f:
            return int(f.read())

    def set_count(self, pages):
        with open(self.count_file, "w") as f:
            f.write(f"{pages}\n")

    def _kernel_write(self, node, pages, page_size_kb=2048):
        self.set_count(int(pages))

    def record_baseline(self, pages):
        """What the init Job writes before it calls apply_config."""
        self.record("baseline", pages)

    def record(self, kind, pages):
        with open(os.path.join(self.records, f"hugepages_{kind}_node0"), "w") as f:
            f.write(str(pages))


def _container(name, hugepages=None, restart_policy=None):
    requests = {"memory": "256Mi"}
    if hugepages:
        requests["hugepages-2Mi"] = hugepages
    return V1Container(name=name, restart_policy=restart_policy,
                       resources=V1ResourceRequirements(requests=requests, limits=dict(requests)))


def _pod(name, hugepages=None, phase="Running", labels=None, init_containers=None):
    return V1Pod(
        metadata=V1ObjectMeta(name=name, namespace="simplyblock", labels=labels or {}),
        spec=V1PodSpec(node_name=WORKER, containers=[_container("c", hugepages)], init_containers=init_containers),
        status=V1PodStatus(phase=phase))


@pytest.fixture
def host(tmp_path):
    h = _Host(str(tmp_path))
    h.set_count(HOST_RESERVED)
    h.record_baseline(HOST_RESERVED)
    with patch.object(utils, "_HUGEPAGES_BASELINE_DIR", h.records), \
            patch.object(utils, "_nr_hugepages_path", h._count_path), \
            patch.object(utils, "_write_nr_hugepages", h._kernel_write):
        yield h


def _apply_config(pods=None, error=None):
    core = MagicMock()
    if error is not None:
        core.list_pod_for_all_namespaces.side_effect = error
    else:
        core.list_pod_for_all_namespaces.return_value = MagicMock(items=pods or [])
    with patch.dict(os.environ, {"HOSTNAME": WORKER}), \
            patch.object(k8s_node.core_utils, "get_k8s_core_client", return_value=core), \
            patch.object(k8s_node.core_utils, "load_config", return_value={"nodes": [NODE_CONFIG]}), \
            patch.object(k8s_node.core_utils, "validate_config", return_value=True), \
            patch.object(k8s_node.utils, "get_response", side_effect=lambda d, error=None, http_code=None: (d, error)):
        k8s_node.apply_config()
    return core


def test_unclaimed_host_pages_cover_simplyblock_and_the_pool_is_left_alone(host):
    """The e2e run: 7958 pages, none requested by a pod, 3990 needed."""
    core = _apply_config(pods=[_pod("prometheus-0"), _pod("webappapi")])

    assert host.count() == HOST_RESERVED
    core.list_pod_for_all_namespaces.assert_called_once_with(field_selector=f"spec.nodeName={WORKER}")


def test_pages_other_pods_request_are_kept_beside_simplyblocks(host):
    """2000 pages claimed by another workload plus 3990 for simplyblock still
    fit in the 7958 the host carries."""
    _apply_config(pods=[_pod("dpdk-app", hugepages="4000Mi")])

    assert host.count() == HOST_RESERVED


def test_the_pool_grows_when_other_pods_and_simplyblock_do_not_fit(host):
    """6000 pages claimed by another workload plus 3990 for simplyblock is more
    than the 7958 the host carries: the pool grows to cover both."""
    _apply_config(pods=[_pod("dpdk-app", hugepages="12000Mi")])

    assert host.count() == utils.adjust_hugepages(6000 + 3990)


def test_simplyblocks_own_storage_pods_are_not_counted_as_claims(host):
    """A storage pod of this host, left over from the previous attempt, holds
    pages that are the ones being sized: counting it would grow the pool by
    simplyblock's need twice."""
    spdk = _pod("snode-spdk-pod-4420-9290ee", hugepages="7170Mi", labels={"role": "simplyblock-storage-node"})
    _apply_config(pods=[spdk])

    assert host.count() == HOST_RESERVED


def test_finished_pods_claim_nothing(host):
    _apply_config(pods=[_pod("old-job", hugepages="12000Mi", phase="Succeeded"),
                        _pod("failed-job", hugepages="12000Mi", phase="Failed")])

    assert host.count() == HOST_RESERVED


def test_without_the_kubernetes_accounting_the_host_reservation_is_kept(host):
    """When the pods cannot be listed, nothing says who uses the host's pages:
    they stay the host's, and simplyblock's go on top as before."""
    _apply_config(error=ApiException(status=403, reason="Forbidden"))

    assert host.count() == utils.adjust_hugepages(HOST_RESERVED + 3990)


def test_a_claim_above_the_hosts_own_share_is_kept_and_survives_the_release(host):
    """The host reserved nothing itself, simplyblock grew the pool to 4096
    earlier, and a pod has since claimed 2000 of it: 4096 does not hold the
    claim beside simplyblock's 3990, so the pool grows. The claim also becomes
    the host's share, so giving simplyblock's pages back later leaves it."""
    host.set_count(4096)
    host.record_baseline(0)
    host.record("sb", 4096)

    _apply_config(pods=[_pod("dpdk-app", hugepages="4000Mi")])

    assert host.count() == utils.adjust_hugepages(2000 + 3990)
    assert utils.release_hugepages() == {0: 2000}
    assert host.count() == 2000


def test_a_decimal_quantity_is_read_like_a_binary_one(host):
    """4194304k is 4194304000 bytes, exactly 2000 pages of 2 MiB: a valid
    hugepages-2Mi request, counted like 4000Mi."""
    _apply_config(pods=[_pod("dpdk-app", hugepages="4194304k")])

    assert host.count() == HOST_RESERVED


def test_a_native_sidecar_claims_beside_the_containers_it_runs_with(host):
    """An init container with restartPolicy Always runs for the pod's whole
    life: 6000Mi beside the app's 6000Mi is 6000 pages, which with
    simplyblock's 3990 no longer fit in 7958."""
    sidecar = _container("proxy", hugepages="6000Mi", restart_policy="Always")
    _apply_config(pods=[_pod("dpdk-app", hugepages="6000Mi", init_containers=[sidecar])])

    assert host.count() == utils.adjust_hugepages(6000 + 3990)


def test_an_init_container_that_ends_claims_only_while_it_runs(host):
    """A plain init container finishes before the app starts: the pod claims
    the larger of the two, 3000 pages, which fit beside simplyblock's 3990."""
    init = _container("setup", hugepages="6000Mi")
    _apply_config(pods=[_pod("dpdk-app", hugepages="6000Mi", init_containers=[init])])

    assert host.count() == HOST_RESERVED
