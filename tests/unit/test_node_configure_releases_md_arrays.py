"""The configure stops the unused software-RAID arrays holding its devices before it generates.

Regression: 2026-11-17-unused-md-array-members. The operator drafts the members
of an MD RAID array nothing uses, and the generation refused each of them as
held by the array, because nothing on the worker took the array apart. The
release has to run before the generation, on the devices the configure was
given, and a release that fails has to stop the configure rather than let the
generation fail later naming the holder.
"""
import argparse
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_web import md_release, node_configure


def _configure_args(**overrides):
    args = argparse.Namespace(
        upgrade=False,
        max_lvol="30", max_prov="0", nodes_per_socket="1", sockets_to_use="0",
        pci_allowed="0000:00:04.0", pci_blocked="", nvme_names="",
        device_model="", size_range="", force=False, lblk=False,
        blk_names="", blk_names_exclude="", blk_serials="",
        jm_percent="3", lblk_journal_device=False,
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _run(args, release, generate):
    with patch.object(node_configure, "parse_arguments", return_value=args), \
         patch.object(node_configure, "validate_arguments"), \
         patch.object(node_configure, "_is_pod_present_for_node", return_value=False), \
         patch.object(node_configure, "_discard_previous_config"), \
         patch.object(node_configure.md_release, "selected_block_devices",
                      side_effect=lambda _host, pci_addresses, blk_paths: sorted(pci_addresses + blk_paths)), \
         patch.object(node_configure.md_release, "release_idle_arrays", release), \
         patch.object(node_configure, "generate_automated_deployment_config", generate):
        node_configure.main()


@pytest.mark.parametrize("args, selected", [
    pytest.param(_configure_args(), ["0000:00:04.0"], id="nvme"),
    pytest.param(_configure_args(pci_allowed="", lblk=True,
                                 blk_names="/dev/disk/by-id/a,/dev/disk/by-id/b"),
                 ["/dev/disk/by-id/a", "/dev/disk/by-id/b"], id="lblk"),
])
def test_the_arrays_are_released_before_the_generation(args, selected):
    order = MagicMock()
    order.release.return_value = ["md0"]
    order.generate.return_value = True

    _run(args, order.release, order.generate)

    calls = [name for name, *_ in order.mock_calls]
    assert calls == ["release", "generate"], (
        f"the generation ran before the arrays holding its devices were stopped: {calls}")
    assert order.release.call_args.args[0] == selected


def test_a_release_that_failed_stops_the_configure():
    release = MagicMock(side_effect=md_release.ReleaseError("md0 still holds nvme0n1"))
    generate = MagicMock(return_value=True)

    with pytest.raises(SystemExit) as exit_info:
        _run(_configure_args(), release, generate)

    assert exit_info.value.code != 0
    assert not generate.called, "the generation ran on devices an array still holds"
