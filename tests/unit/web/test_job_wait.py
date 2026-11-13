"""A failed node-preparation Job reports why it failed.

The jobs that prepare a storage node refuse on purpose in a few cases, such as
masters whose isolated cores or reserved CPUs differ, and print the reason.
That reason is only in the failed pod's log, so the Job wait carries its last
lines in the error the node add reports, instead of a bare "Job failed".
"""

from types import SimpleNamespace
from unittest import mock

import pytest

from simplyblock_web import node_utils_k8s


def _job(failed=0, succeeded=0):
    return SimpleNamespace(status=SimpleNamespace(failed=failed, succeeded=succeeded))


def _pods(*names):
    return SimpleNamespace(items=[SimpleNamespace(metadata=SimpleNamespace(name=n)) for n in names])


def test_a_failed_job_reports_its_pods_last_log_lines():
    batch = mock.Mock()
    batch.read_namespaced_job.return_value = _job(failed=1)
    core = mock.Mock()
    core.list_namespaced_pod.return_value = _pods("snode-core-master-2-abcde")
    core.read_namespaced_pod_log.return_value = (
        "[INFO] Isolated cores to apply: 4,5\n"
        "[ERROR] master-2 is a master and needs cores 4,5 isolated, but MachineConfig "
        "simplyblock-master-isolated applies [\"isolcpus=2,3\"] to every master\n")
    with mock.patch.object(node_utils_k8s, "get_k8s_batch_client", return_value=batch), \
            mock.patch.object(node_utils_k8s, "get_k8s_core_client", return_value=core):
        with pytest.raises(RuntimeError) as failure:
            node_utils_k8s.wait_for_job_completion("snode-core-master-2", "simplyblock")
    assert "Job 'snode-core-master-2' failed" in str(failure.value)
    assert "needs cores 4,5 isolated" in str(failure.value)
    core.list_namespaced_pod.assert_called_once_with("simplyblock", label_selector="job-name=snode-core-master-2")


def test_a_failed_job_whose_log_cannot_be_read_still_fails():
    batch = mock.Mock()
    batch.read_namespaced_job.return_value = _job(failed=1)
    core = mock.Mock()
    core.list_namespaced_pod.side_effect = node_utils_k8s.ApiException(status=500)
    with mock.patch.object(node_utils_k8s, "get_k8s_batch_client", return_value=batch), \
            mock.patch.object(node_utils_k8s, "get_k8s_core_client", return_value=core):
        with pytest.raises(RuntimeError, match="Job 'snode-core-master-2' failed"):
            node_utils_k8s.wait_for_job_completion("snode-core-master-2", "simplyblock")


def test_a_succeeded_job_reads_no_log():
    batch = mock.Mock()
    batch.read_namespaced_job.return_value = _job(succeeded=1)
    core = mock.Mock()
    with mock.patch.object(node_utils_k8s, "get_k8s_batch_client", return_value=batch), \
            mock.patch.object(node_utils_k8s, "get_k8s_core_client", return_value=core):
        assert node_utils_k8s.wait_for_job_completion("snode-core-master-2", "simplyblock") is True
    core.list_namespaced_pod.assert_not_called()
