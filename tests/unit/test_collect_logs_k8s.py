# Tests for the Kubernetes collection in scripts/collect_logs.py: it must work
# from inside the admin-control pod, which has the Python client but no kubectl.
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from kubernetes.client.rest import ApiException

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "collect_logs.py"


@pytest.fixture(scope="module")
def cl():
    spec = importlib.util.spec_from_file_location("collect_logs_under_test", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _pod(name, containers):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        spec=SimpleNamespace(
            init_containers=None,
            containers=[SimpleNamespace(name=c) for c in containers],
        ),
    )


def _api(pods):
    api = mock.Mock()
    api.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
    api.read_namespaced_pod.side_effect = lambda name, namespace: next(
        p for p in pods if p.metadata.name == name)

    def read_log(name, namespace, container, timestamps, previous=False, **kw):
        if previous:
            raise ApiException(status=400)
        return f"2026-11-17T19:00:00.000000000Z {name}/{container} line\n"

    api.read_namespaced_pod_log.side_effect = read_log
    return api


FROM, TO = "2026-11-17T17:30:00+00:00", "2026-11-17T20:30:00+00:00"


def test_kubectl_is_not_needed(cl, monkeypatch):
    # The admin-control pod has no kubectl binary; any subprocess call to it
    # silently produced empty logs.
    monkeypatch.setattr(cl.subprocess, "run", mock.Mock(side_effect=FileNotFoundError("kubectl")))
    api = _api([_pod("simplyblock-tasks-abc-1", ["tasks-runner-restart"])])
    out = Path(cl.tempfile.mkdtemp())
    cl.collect_k8s_pod_logs(api, "ns", "simplyblock-tasks-abc-1", out, FROM, TO)
    text = (out / "simplyblock-tasks-abc-1_tasks-runner-restart.log").read_text()
    assert "simplyblock-tasks-abc-1/tasks-runner-restart line" in text
    assert "(no previous logs)" in text


def test_control_plane_and_spdk_pods_are_collected(cl, tmp_path):
    # The restart service and the SPDK pods were only fetched from Graylog, so
    # a cluster without monitoring produced no logs for either.
    pods = [
        _pod("simplyblock-tasks-abc-1", ["tasks-runner-restart", "tasks-runner-migration"]),
        _pod("simplyblock-webappapi-abc-1", ["webappapi"]),
        _pod("simplyblock-monitoring-abc-1", ["storage-node-monitor"]),
        _pod("simplyblock-admin-control-abc-1", ["simplyblock-control"]),
        _pod("snode-spdk-pod-4422-2e111c", ["spdk-container", "spdk-proxy-container"]),
        _pod("simplyblock-csi-node-x", ["csi-node"]),
    ]
    api = _api(pods)
    with mock.patch.object(cl, "collect_k8s_csi_dmesg"):
        cl.collect_k8s_namespace(api, "ns", tmp_path, FROM, TO)
    files = {p.name for p in tmp_path.rglob("*.log")}
    for expected in (
        "simplyblock-tasks-abc-1_tasks-runner-restart.log",
        "simplyblock-webappapi-abc-1_webappapi.log",
        "simplyblock-monitoring-abc-1_storage-node-monitor.log",
        "simplyblock-admin-control-abc-1_simplyblock-control.log",
        "snode-spdk-pod-4422-2e111c_spdk-container.log",
        "snode-spdk-pod-4422-2e111c_spdk-proxy-container.log",
        "simplyblock-csi-node-x_csi-node.log",
    ):
        assert expected in files, expected


def test_dmesg_forbidden_does_not_abort(cl, tmp_path):
    api = _api([_pod("simplyblock-csi-node-x", ["csi-node"])])
    with mock.patch.object(cl, "k8s_stream", side_effect=ApiException(status=403)):
        cl.collect_k8s_csi_dmesg(api, "ns", "simplyblock-csi-node-x", tmp_path, FROM, TO)
    assert "could not exec" in (tmp_path / "simplyblock-csi-node-x_csi-node_dmesg.log").read_text()


def test_namespace_defaults_to_the_pods_own(cl, tmp_path):
    # The customer's control plane runs in simplyblock-operator, not simplyblock.
    f = tmp_path / "namespace"
    f.write_text("simplyblock-operator")
    assert cl._default_namespace(str(f)) == "simplyblock-operator"
    assert cl._default_namespace(str(tmp_path / "missing")) == "simplyblock"


def test_task_list_is_in_the_sbctl_snapshots(cl):
    assert any(args[:2] == ["cluster", "list-tasks"] for _, args, _, _ in cl.sbctl_snapshots("cid"))


