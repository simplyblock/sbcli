# coding=utf-8
"""Regression tests for hugepage reservation across node restarts.

`set_hugepages_if_needed` writes an *absolute* `nr_hugepages` computed as
"the user's own pre-existing reservation + what simplyblock needs". The user's
reservation is not observable once simplyblock has written to the node, so it
is remembered in two files under `_HUGEPAGES_BASELINE_DIR`:

  hugepages_baseline_node{N}  the user's reservation
  hugepages_sb_node{N}        the total simplyblock last wrote to the kernel

If either is lost, the already-inflated `nr_hugepages` is mistaken for the
user's reservation and simplyblock's requirement is added on top of itself --
every `sn restart` then grows the reservation by the full requirement. That is
what these tests pin down: a restart must be a no-op, and the state must live
somewhere that survives a container restart (a host tmpfs, not `/tmp`).
"""

import inspect
import pathlib
import re
import subprocess

import pytest

from simplyblock_core import storage_node_ops, utils

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
TEMPLATE_DIR = REPO_ROOT / "simplyblock_web" / "templates"


NEEDED = 7168          # 14 GiB of 2 MiB pages, a typical single-node requirement
NEEDED_TOTAL = 7680    # adjust_hugepages(7168) -- rounded up with headroom


class FakeHost:
    """A single-NUMA-node host whose nr_hugepages lives in a temp file."""

    def __init__(self, tmp_path, monkeypatch, nr_hugepages=0):
        self.state_dir = tmp_path / "run" / "simplyblock"
        self._nr_file = tmp_path / "nr_hugepages"
        self._nr_file.write_text(str(nr_hugepages))
        self.writes: list = []

        real_open = open

        def _open(path, *args, **kwargs):
            if isinstance(path, str) and path.startswith("/sys/devices/system/node/"):
                return real_open(self._nr_file, *args, **kwargs)
            return real_open(path, *args, **kwargs)

        # `open` and `subprocess` are shadowed in the utils module namespace
        # only, so the real sysfs is never touched and the `tee` shell-out is
        # intercepted instead of executed.
        monkeypatch.setattr(utils, "open", _open, raising=False)
        monkeypatch.setattr(utils, "subprocess", self, raising=True)
        monkeypatch.setattr(utils, "_HUGEPAGES_BASELINE_DIR", str(self.state_dir))

    def run(self, cmd, shell=False, check=False):
        """Stand in for `subprocess.run("echo N | sudo tee <sysfs path>")`."""
        match = re.match(r"echo (-?\d+) \| sudo tee (\S+)", cmd)
        assert match is not None, f"unexpected command: {cmd}"
        value = int(match.group(1))
        self.writes.append(value)
        if value < 0:
            # The kernel rejects a negative write with "Invalid argument".
            raise subprocess.CalledProcessError(1, cmd)
        self._nr_file.write_text(str(value))

    @property
    def nr_hugepages(self):
        return int(self._nr_file.read_text().strip())

    def set_nr_hugepages(self, value):
        self._nr_file.write_text(str(value))

    def state(self, name):
        path = self.state_dir / name
        return int(path.read_text().strip()) if path.exists() else None

    def drop_state(self):
        """Simulate losing the state directory (container restart, /tmp sweep)."""
        for path in self.state_dir.glob("hugepages_*"):
            path.unlink()


@pytest.fixture
def host(tmp_path, monkeypatch):
    return FakeHost(tmp_path, monkeypatch)


def test_state_dir_survives_a_container_restart():
    """The state must not live in the container or in a swept directory.

    `/tmp` is pruned by systemd-tmpfiles-clean (10-day TTL on RHEL family), and
    a path the storage-node API pod does not host-mount is destroyed with the
    container. Either loses the baseline while nr_hugepages stays inflated.
    """
    assert utils._HUGEPAGES_BASELINE_DIR == "/var/run/simplyblock"


@pytest.mark.parametrize(
    "template",
    ["storage_init_job.yaml.j2", "storage_deploy_spdk.yaml.j2"],
)
def test_k8s_templates_host_mount_the_state_dir(template):
    """The k8s pods must bind the state dir to the same path on the host.

    The storage-node init job captures the user's baseline and the API pod
    reads it back. If either resolves `_HUGEPAGES_BASELINE_DIR` inside its own
    container instead of on the host, the two never see the same file and the
    baseline is re-captured -- from an already-inflated nr_hugepages -- on the
    next restart.
    """
    text = (TEMPLATE_DIR / template).read_text()
    state_dir = utils._HUGEPAGES_BASELINE_DIR

    assert f"path: {state_dir}\n" in text, f"{template} does not hostPath {state_dir}"
    assert f"mountPath: {state_dir}\n" in text, f"{template} does not mount {state_dir}"


def test_init_job_writes_the_baseline_into_the_mounted_state_dir():
    text = (TEMPLATE_DIR / "storage_init_job.yaml.j2").read_text()
    match = re.search(r'baseline_file="([^"]+)"', text)

    assert match is not None, "init job no longer captures a hugepage baseline"
    assert match.group(1).startswith(utils._HUGEPAGES_BASELINE_DIR + "/")


def test_docker_snode_api_container_bind_mounts_the_state_dir():
    """Docker mounts are fixed for a container's lifetime, and `cluster upgrade`
    never recreates SNodeAPI -- so a wrong path here is only fixed by `sn deploy`.
    """
    source = inspect.getsource(storage_node_ops.start_storage_node_api_container)
    state_dir = utils._HUGEPAGES_BASELINE_DIR

    assert f"'{state_dir}:{state_dir}'" in source


def test_first_deploy_treats_the_current_reservation_as_the_user_s(host):
    utils.set_hugepages_if_needed(0, NEEDED)

    assert host.nr_hugepages == NEEDED_TOTAL
    assert host.state("hugepages_baseline_node0") == 0
    assert host.state("hugepages_sb_node0") == NEEDED_TOTAL


def test_restart_does_not_grow_the_reservation(host):
    """The reported fault: every `sn restart` added the requirement again."""
    utils.set_hugepages_if_needed(0, NEEDED)
    after_deploy = host.nr_hugepages

    for _ in range(3):
        utils.set_hugepages_if_needed(0, NEEDED)
        assert host.nr_hugepages == after_deploy

    # Only the initial deploy should have written to the kernel at all.
    assert host.writes == [NEEDED_TOTAL]


def test_sb_file_records_the_written_total_not_the_raw_requirement(host):
    """Drift guard: the delta on the next run is measured against what we wrote.

    Storing the unrounded requirement made the rounding headroom look like a
    manual user addition on the following restart.
    """
    utils.set_hugepages_if_needed(0, NEEDED)

    assert host.state("hugepages_sb_node0") == NEEDED_TOTAL
    assert host.state("hugepages_sb_node0") != NEEDED


def test_a_pre_existing_user_reservation_is_added_once(tmp_path, monkeypatch):
    host = FakeHost(tmp_path, monkeypatch, nr_hugepages=2000)

    utils.set_hugepages_if_needed(0, NEEDED)
    assert host.state("hugepages_baseline_node0") == 2000
    after_deploy = host.nr_hugepages
    assert after_deploy > 2000 + NEEDED

    utils.set_hugepages_if_needed(0, NEEDED)
    assert host.nr_hugepages == after_deploy


def test_manual_user_addition_survives_repeated_restarts(host):
    """A user adding pages between restarts must not be reverted or re-added."""
    utils.set_hugepages_if_needed(0, NEEDED)
    host.set_nr_hugepages(host.nr_hugepages + 1000)

    utils.set_hugepages_if_needed(0, NEEDED)
    assert host.state("hugepages_baseline_node0") == 1000
    absorbed = host.nr_hugepages
    assert absorbed > NEEDED_TOTAL

    # Second restart: the addition is already in the baseline, so it is neither
    # dropped (reverting the user's change) nor counted twice.
    utils.set_hugepages_if_needed(0, NEEDED)
    assert host.nr_hugepages == absorbed
    assert host.state("hugepages_baseline_node0") == 1000


def test_reboot_reapplies_the_requirement_without_a_negative_write(host):
    """A reboot zeroes nr_hugepages while the state files survive.

    Reading that as "the user removed pages" drove the baseline negative, and
    `echo -512 | tee nr_hugepages` fails, so the node never got its pages.
    """
    utils.set_hugepages_if_needed(0, NEEDED)
    host.set_nr_hugepages(0)  # reboot: runtime allocation gone, state kept

    utils.set_hugepages_if_needed(0, NEEDED)

    assert host.nr_hugepages == NEEDED_TOTAL
    assert host.state("hugepages_baseline_node0") == 0
    assert all(value >= 0 for value in host.writes)


def test_a_smaller_requirement_shrinks_the_reservation(host):
    """Over-provisioned nodes must be able to give pages back."""
    utils.set_hugepages_if_needed(0, NEEDED)
    assert host.nr_hugepages == NEEDED_TOTAL

    utils.set_hugepages_if_needed(0, 3000)

    assert host.nr_hugepages < NEEDED_TOTAL
    assert host.nr_hugepages >= 3000


def test_the_written_total_never_drops_below_the_requirement(host):
    utils.set_hugepages_if_needed(0, NEEDED)
    host.drop_state()
    host.set_nr_hugepages(0)

    utils.set_hugepages_if_needed(0, NEEDED)

    assert host.nr_hugepages >= NEEDED
