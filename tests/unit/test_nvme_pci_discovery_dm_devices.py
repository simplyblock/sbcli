"""Regression tests for ``utils.get_nvme_pci_devices`` on hosts with device-mapper devices.

A dm device is listed by ``lsblk`` under its mapped name (``sb-<uuid>``) but has no
``/dev/sb-<uuid>`` node, so querying it by that name fails. Discovery used to treat that
failure as "no NVMe devices with class 0108" and the node configuration was never generated.
"""

import subprocess

from simplyblock_core import utils

# (kernel name, display name, mountpoint, partition count) as lsblk --nodeps would list them.
_HOST_BLOCK_DEVICES = [
    ("nvme0n1", "nvme0n1", "", 0),
    ("nvme1n1", "nvme1n1", "", 1),
    ("dm-2", "sb-ed9f1e84-ac72-499b-9f36-6c03b6c7f7d6", "", 0),
]
_NVME_PCI = ["0000:00:02.0", "0000:00:03.0"]


def _fake_check_output(broken=()):
    """A stand-in for ``subprocess.check_output`` that answers like lsblk and lspci on one host."""

    def check_output(cmd, *args, **kwargs):
        if isinstance(cmd, str) and cmd.startswith("lspci"):
            return "".join(f"{pci} Non-Volatile memory controller [0108]: QEMU NVMe Ctrl\n" for pci in _NVME_PCI)
        if cmd[:2] == ["lsblk", "-dn"]:
            column = cmd[cmd.index("-o") + 1].split(",")[0]
            lines = []
            for kname, name, mountpoint, _ in _HOST_BLOCK_DEVICES:
                lines.append(f"{kname if column == 'KNAME' else name} {mountpoint}".rstrip())
            return "\n".join(lines) + "\n"
        if cmd[:2] == ["lsblk", "-n"]:
            path = cmd[2]
            for kname, _, _, partitions in _HOST_BLOCK_DEVICES:
                if path == f"/dev/{kname}" and kname not in broken:
                    return "\n".join([kname] + [f"{kname}p{i + 1}" for i in range(partitions)]) + "\n"
            raise subprocess.CalledProcessError(32, cmd, output="")
        raise AssertionError(f"unexpected command {cmd!r}")

    return check_output


def test_dm_device_does_not_hide_nvme_controllers(monkeypatch):
    # Regression: a dm-linear volume layer (sb-<uuid>) on the host made discovery report no NVMe.
    monkeypatch.setattr(utils.subprocess, "check_output", _fake_check_output())

    pci_addresses, blocked = utils.get_nvme_pci_devices()

    assert pci_addresses == _NVME_PCI
    assert "nvme1n1" in blocked
    assert "nvme0n1" not in blocked


def test_unqueryable_device_is_treated_as_blocked(monkeypatch):
    # Regression: one block device that lsblk cannot query must not abort the whole discovery,
    # and since its partitions are unknown it must not be offered for formatting.
    monkeypatch.setattr(utils.subprocess, "check_output", _fake_check_output(broken={"nvme0n1"}))

    pci_addresses, blocked = utils.get_nvme_pci_devices()

    assert pci_addresses == _NVME_PCI
    assert "nvme0n1" in blocked
    assert "nvme1n1" in blocked
