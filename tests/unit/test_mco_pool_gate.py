"""The agent starts SPDK on OpenShift only once the node runs the render that
carries the CPU-topology kubelet settings, and it reads that render from the
pool MCO actually put the node in.

A master node never leaves the master pool: MCO keeps it there even when a
custom pool selects it, and logs "defaulting to master". The topology job
therefore gives the master pool the storage KubeletConfig, and the gate has to
wait on the master pool's render, not the storage pool's, which a master never
reaches.
"""

import re
from types import SimpleNamespace
from unittest import mock

import pytest
from kubernetes.client import ApiException
from tenacity import wait_none

from simplyblock_core.utils import storage_mco_names
from simplyblock_web.api.internal.storage_node.kubernetes import (
    _converged_on_pool,
    _kubelet_configs_owned_by,
    _mco_pool_name,
    _pool_kubelet_config_name,
    _pool_kubelet_configs,
    _wait_converged_on_pool,
)

MASTER_LABEL = "node-role.kubernetes.io/master"
KC = "storage-kubelet-ac85c3"


def _pool(spec_name, status_name, sources):
    return {
        "spec": {"configuration": {"name": spec_name, "source": [{"name": s} for s in sources]}},
        "status": {"configuration": {"name": status_name}},
    }


def _annotations(current, state="Done"):
    return {
        "machineconfiguration.openshift.io/currentConfig": current,
        "machineconfiguration.openshift.io/state": state,
    }


def test_a_worker_is_gated_on_the_cluster_storage_pool():
    assert _mco_pool_name({"node-role.kubernetes.io/worker": ""}, "ac85c3") == "storage-ac85c3"


def test_a_master_is_gated_on_the_master_pool():
    labels = {MASTER_LABEL: "", "node-role.kubernetes.io/worker": ""}
    assert _mco_pool_name(labels, "ac85c3") == "master"


def test_a_control_plane_label_alone_is_not_the_master_pool():
    """The master pool selects node-role.kubernetes.io/master; a node without
    it is not kept there whatever else it carries."""
    assert _mco_pool_name({"node-role.kubernetes.io/control-plane": ""}, "ac85c3") == "storage-ac85c3"


def test_a_storage_pool_node_on_the_pool_render_has_converged():
    pool = _pool("rendered-storage-ac85c3-aaa", "rendered-storage-ac85c3-aaa", [])
    assert _converged_on_pool("storage-ac85c3", pool, _annotations("rendered-storage-ac85c3-aaa"), False, None)


def test_a_storage_pool_node_on_an_identical_older_render_has_converged():
    """Same content hash under a previous cluster's pool name: no reboot due."""
    pool = _pool("rendered-storage-ac85c3-aaa", "rendered-storage-ac85c3-aaa", [])
    assert _converged_on_pool("storage-ac85c3", pool, _annotations("rendered-storage-old111-aaa"), False, None)


def test_a_cordoned_node_has_not_converged():
    pool = _pool("rendered-storage-ac85c3-aaa", "rendered-storage-ac85c3-aaa", [])
    assert not _converged_on_pool("storage-ac85c3", pool, _annotations("rendered-storage-ac85c3-aaa"), True, None)


def test_a_node_mid_update_has_not_converged():
    pool = _pool("rendered-storage-ac85c3-aaa", "rendered-storage-ac85c3-aaa", [])
    annotations = _annotations("rendered-storage-ac85c3-aaa", state="Working")
    assert not _converged_on_pool("storage-ac85c3", pool, annotations, False, None)


def test_a_master_still_on_the_old_render_has_not_converged_while_the_pool_rolls():
    """The master pool's status names the old render until every master has
    rebooted, so a master that has not rebooted yet matches it. The target is
    the spec's render."""
    pool = _pool("rendered-master-new", "rendered-master-old", ["00-master", "99-master-generated-kubelet"])
    assert not _converged_on_pool("master", pool, _annotations("rendered-master-old"), False,
                                  {"99-master-generated-kubelet"})


def test_a_master_on_the_target_render_with_the_kubelet_config_has_converged():
    pool = _pool("rendered-master-new", "rendered-master-old", ["00-master", "99-master-generated-kubelet-1"])
    assert _converged_on_pool("master", pool, _annotations("rendered-master-new"), False,
                              {"99-master-generated-kubelet-1"})


def test_a_master_render_without_the_kubelet_config_has_not_converged():
    """Before the topology job's KubeletConfig reaches the master pool, the
    master already runs the pool's target render; that is not the settings."""
    pool = _pool("rendered-master-old", "rendered-master-old", ["00-master", "01-master-kubelet"])
    assert not _converged_on_pool("master", pool, _annotations("rendered-master-old"), False, set())


def test_another_kubelet_configs_generated_config_is_not_the_settings():
    """After a rerun the master render still carries the previous cluster's
    generated kubelet config, under the name this cluster's will take."""
    pool = _pool("rendered-master-old", "rendered-master-old", ["00-master", "99-master-generated-kubelet"])
    assert not _converged_on_pool("master", pool, _annotations("rendered-master-old"), False, set())


def test_the_generated_configs_of_a_kubelet_config_are_found_by_owner():
    def mc(name, owner=None, kind="KubeletConfig"):
        refs = [{"kind": kind, "name": owner}] if owner else []
        return {"metadata": {"name": name, "ownerReferences": refs}}

    items = [
        mc("00-master"),
        mc("99-master-generated-kubelet", KC),
        mc("99-storage-ac85c3-generated-kubelet", KC),
        mc("99-master-generated-kubelet-1", "site-tuning"),
        mc("99-master-generated-registries", KC, kind="ContainerRuntimeConfig"),
    ]
    assert _kubelet_configs_owned_by(items, KC) == {
        "99-master-generated-kubelet", "99-storage-ac85c3-generated-kubelet"}


def test_the_storage_pool_takes_the_cluster_kubelet_config():
    assert _pool_kubelet_config_name("storage-ac85c3", "ac85c3") == KC


def test_the_master_pool_takes_a_kubelet_config_of_its_own():
    """MCO evaluates a KubeletConfig when it is created, never when a pool's
    labels change, so the masters get one that selects the master pool from
    the start: the topology job's storage-kubelet-<id>-master."""
    assert _pool_kubelet_config_name("master", "ac85c3") == KC + "-master"


def test_the_names_of_a_clusters_storage_objects_come_from_one_place():
    names = storage_mco_names("ac85c3d4-1ce7-428e-82d9-e212828594ba")
    assert names.pool == "storage-ac85c3"
    assert names.machine_config == "storage-mc-ac85c3"
    assert names.kubelet_config == KC
    assert names.kubelet_label == "storage-cpumanager-ac85c3"
    assert names.master_kubelet_config == KC + "-master"
    assert re.match(names.master_kubelet_config_pattern, "storage-kubelet-old111-master")
    assert not re.match(names.master_kubelet_config_pattern, KC)


def test_a_master_gate_without_its_kubelet_configs_is_an_error():
    """Leaving them out must not read as "none rendered yet" and wait the
    whole 40 minutes."""
    pool = _pool("rendered-master-new", "rendered-master-new", ["99-master-generated-kubelet"])
    with pytest.raises(ValueError):
        _converged_on_pool("master", pool, _annotations("rendered-master-new"), False, None)


def _co(sources, machine_configs):
    co = mock.Mock()
    co.get_cluster_custom_object.side_effect = lambda **kw: _get(kw, sources, machine_configs)
    return co


def _get(kw, sources, machine_configs):
    if kw["plural"] == "machineconfigpools":
        return _pool("rendered-master-new", "rendered-master-old", sources)
    if kw["name"] not in machine_configs:
        raise ApiException(status=404)
    owner = machine_configs[kw["name"]]
    return {"metadata": {"name": kw["name"], "ownerReferences": [{"kind": "KubeletConfig", "name": owner}]}}


def test_only_the_pools_generated_kubelet_configs_are_read():
    """Every MachineConfig, rendered payloads included, every 10 seconds for
    40 minutes is far more than the gate needs: the candidates are named in
    the pool's sources."""
    co = _co([], {"99-master-generated-kubelet": "site-tuning", "99-master-generated-kubelet-1": KC + "-master"})
    pool = _pool("rendered-master-new", "rendered-master-old",
                 ["00-master", "99-master-generated-kubelet", "99-master-generated-kubelet-1", "99-master-ssh"])
    assert _pool_kubelet_configs(co, "master", pool, KC + "-master") == {"99-master-generated-kubelet-1"}
    read = sorted(call.kwargs["name"] for call in co.get_cluster_custom_object.call_args_list)
    assert read == ["99-master-generated-kubelet", "99-master-generated-kubelet-1"]
    co.list_cluster_custom_object.assert_not_called()


def test_a_generated_config_deleted_meanwhile_is_skipped():
    co = _co([], {})
    pool = _pool("rendered-master-new", "rendered-master-old", ["99-master-generated-kubelet"])
    assert _pool_kubelet_configs(co, "master", pool, KC + "-master") == set()


def _node(current, state="Done", unschedulable=False, labels=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(labels=labels or {MASTER_LABEL: ""},
                                 annotations=_annotations(current, state)),
        spec=SimpleNamespace(unschedulable=unschedulable))


def test_the_gate_waits_until_the_node_runs_the_render():
    co = _co(["99-master-generated-kubelet"], {"99-master-generated-kubelet": KC + "-master"})
    core = mock.Mock()
    # One read for the labels that pick the pool, then one per poll.
    core.read_node.side_effect = [_node("rendered-master-old"), _node("rendered-master-old"),
                                  _node("rendered-master-old"), _node("rendered-master-new")]
    assert _wait_converged_on_pool(co, core, "master-3", "ac85c3", wait=wait_none())
    assert core.read_node.call_count == 1 + 3


def test_the_gate_gives_up_after_its_attempts():
    co = _co(["99-master-generated-kubelet"], {"99-master-generated-kubelet": KC + "-master"})
    core = mock.Mock()
    core.read_node.return_value = _node("rendered-master-old")
    assert not _wait_converged_on_pool(co, core, "master-3", "ac85c3", wait=wait_none(), attempts=4)
    assert core.read_node.call_count == 1 + 4


def test_a_missing_pool_degrades_to_waiting_for_the_uncordon():
    co = mock.Mock()
    co.get_cluster_custom_object.side_effect = ApiException(status=404)
    core = mock.Mock()
    core.read_node.side_effect = [_node("x"), _node("x", unschedulable=True), _node("x", unschedulable=False)]
    assert _wait_converged_on_pool(co, core, "master-3", "ac85c3", wait=wait_none())
