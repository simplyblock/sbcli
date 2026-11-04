"""The agent starts SPDK on OpenShift only once the node runs the render that
carries the CPU-topology kubelet settings, and it reads that render from the
pool MCO actually put the node in.

A master node never leaves the master pool: MCO keeps it there even when a
custom pool selects it, and logs "defaulting to master". The topology job
therefore gives the master pool the storage KubeletConfig, and the gate has to
wait on the master pool's render, not the storage pool's, which a master never
reaches.
"""

from simplyblock_web.api.internal.storage_node.kubernetes import (
    _converged_on_pool,
    _kubelet_configs_owned_by,
    _mco_pool_name,
    _pool_kubelet_config_name,
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
    assert _converged_on_pool("storage-ac85c3", pool, _annotations("rendered-storage-ac85c3-aaa"), False)


def test_a_storage_pool_node_on_an_identical_older_render_has_converged():
    """Same content hash under a previous cluster's pool name: no reboot due."""
    pool = _pool("rendered-storage-ac85c3-aaa", "rendered-storage-ac85c3-aaa", [])
    assert _converged_on_pool("storage-ac85c3", pool, _annotations("rendered-storage-old111-aaa"), False)


def test_a_cordoned_node_has_not_converged():
    pool = _pool("rendered-storage-ac85c3-aaa", "rendered-storage-ac85c3-aaa", [])
    assert not _converged_on_pool("storage-ac85c3", pool, _annotations("rendered-storage-ac85c3-aaa"), True)


def test_a_node_mid_update_has_not_converged():
    pool = _pool("rendered-storage-ac85c3-aaa", "rendered-storage-ac85c3-aaa", [])
    annotations = _annotations("rendered-storage-ac85c3-aaa", state="Working")
    assert not _converged_on_pool("storage-ac85c3", pool, annotations, False)


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
