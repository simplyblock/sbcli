"""A node configuration that could not be generated must stop the pod.

`generate_automated_deployment_config` reports failure by returning
``(False, False)``: no device matched the filters, the sockets did not
validate, the memory did not add up. `main` discarded that return, so the
process exited 0 and the storage-node pod's init container counted as
successful. The pod then started on whatever `/etc/simplyblock/sn_config_file`
the host already had — a previous deployment's, in the case this was written
for, naming lblk devices for a cluster running in nvme mode. Nothing said so
until `node_add` refused the node twenty minutes later, and the message named
the device class rather than the configure that never ran.
"""
import argparse
from unittest.mock import patch

import pytest

from simplyblock_web import node_configure


def _configure_args(**overrides):
    """The namespace `main` builds for a k8s storage node, as the operator's
    init container invokes it: a device filter and nothing else."""
    args = argparse.Namespace(
        upgrade=False,
        max_lvol="30", max_prov="0", nodes_per_socket="1", sockets_to_use="0",
        pci_allowed="0000:01:00.0", pci_blocked="", nvme_names="",
        device_model="", size_range="", force=False, lblk=False,
        blk_names="", blk_names_exclude="", blk_serials="",
        jm_percent="3", force_format=False,
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


@pytest.mark.parametrize("result", [(False, False), False, None])
def test_a_generation_that_failed_exits_non_zero(result):
    """Every falsy shape the generator reports failure with stops the process."""
    with patch.object(node_configure, "parse_arguments", return_value=_configure_args()), \
         patch.object(node_configure, "validate_arguments"), \
         patch.object(node_configure, "_is_pod_present_for_node", return_value=False), \
         patch.object(node_configure, "generate_automated_deployment_config",
                      return_value=result) as generated:
        with pytest.raises(SystemExit) as exit_info:
            node_configure.main()

    assert generated.called, "the generator was never reached"
    assert exit_info.value.code != 0, (
        "the configure failed and the process exited 0, so the init container "
        "succeeded and the pod started on the host's previous configuration"
    )


def test_a_generation_that_succeeded_does_not_exit_non_zero():
    """The guard must not turn a working configure into a crash loop."""
    with patch.object(node_configure, "parse_arguments", return_value=_configure_args()), \
         patch.object(node_configure, "validate_arguments"), \
         patch.object(node_configure, "_is_pod_present_for_node", return_value=False), \
         patch.object(node_configure, "generate_automated_deployment_config",
                      return_value=({"nodes": [{}]}, {"host": {}})):
        try:
            node_configure.main()
        except SystemExit as exit_info:  # pragma: no cover - only on a regression
            assert exit_info.code in (0, None), f"a successful configure exited {exit_info.code}"


def test_a_previous_configuration_is_discarded_before_regenerating(tmp_path):
    """A regeneration must not be able to inherit the file it is replacing.

    The generator writes the whole document on success, so a successful run
    already replaces it. The failure path is the one that mattered: it wrote
    nothing, so the host kept a configuration no run of this deployment
    produced, and the node_add that read it was refused for a device class
    nobody had asked for.
    """
    config = tmp_path / "sn_config_file"
    config.write_text('{"nodes": [{"ssd_pcis": [], "lblk_devices": [{"name": "nvme0n1"}]}]}')
    read_only = tmp_path / "sn_config_file_read_only"
    read_only.write_text(config.read_text())

    with patch.object(node_configure.constants, "NODES_CONFIG_FILE", str(config)), \
         patch.object(node_configure, "parse_arguments", return_value=_configure_args()), \
         patch.object(node_configure, "validate_arguments"), \
         patch.object(node_configure, "_is_pod_present_for_node", return_value=False), \
         patch.object(node_configure, "generate_automated_deployment_config",
                      return_value=(False, False)):
        with pytest.raises(SystemExit):
            node_configure.main()

    assert not config.exists(), (
        "the configure failed and a previous deployment's configuration is still on the host"
    )
    assert not read_only.exists(), "the read-only copy outlived the configuration it mirrors"


def test_nothing_to_discard_is_not_an_error(tmp_path):
    """A first install has no previous configuration, and that is the ordinary case."""
    with patch.object(node_configure.constants, "NODES_CONFIG_FILE", str(tmp_path / "absent")), \
         patch.object(node_configure, "parse_arguments", return_value=_configure_args()), \
         patch.object(node_configure, "validate_arguments"), \
         patch.object(node_configure, "_is_pod_present_for_node", return_value=False), \
         patch.object(node_configure, "generate_automated_deployment_config",
                      return_value=({"nodes": [{}]}, {"host": {}})):
        node_configure.main()


def test_a_skipped_generation_keeps_the_configuration(tmp_path):
    """A pod already present skips the generation, so the file it would have
    rewritten is the file that pod is running on and must survive."""
    config = tmp_path / "sn_config_file"
    config.write_text('{"nodes": []}')

    with patch.object(node_configure.constants, "NODES_CONFIG_FILE", str(config)), \
         patch.object(node_configure, "parse_arguments", return_value=_configure_args()), \
         patch.object(node_configure, "validate_arguments"), \
         patch.object(node_configure, "_is_pod_present_for_node", return_value=True), \
         patch.object(node_configure, "generate_automated_deployment_config") as generated:
        with pytest.raises(SystemExit) as exit_info:
            node_configure.main()

    assert exit_info.value.code in (0, None)
    assert not generated.called, "the generation ran despite a pod already being present"
    assert config.exists(), "the configuration a running pod depends on was discarded"
