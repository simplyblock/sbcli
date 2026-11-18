"""Stopping a software-RAID array nothing uses, so the configure can take its members.

Regression: 2026-11-17-unused-md-array-members. The operator's discovery drafts
the members of an MD RAID array that nothing mounts, holds, or formatted, and
the configure refused every one of them as "held by md0", because the array was
still assembled. The configure is where the array is taken apart: it is the step
on the worker that runs before the storage node takes the devices, at node add
and at every regeneration after it.

Each test builds a sysfs tree the way the kernel lays it out and drives the
release through a host whose kernel side (stopping an array, erasing a
superblock) is simulated by moving the same entries the kernel would move.
"""
import os

import pytest

from simplyblock_web import md_release


class FakeHost(md_release.Host):
    """A host over a sysfs tree under tmp_path, whose kernel answers the way
    the real one does: stopping an array removes it from its members' holders,
    and erasing the superblock is recorded."""

    def __init__(self, root, *, content=None, busy=(), mounted=(), swaps=(), uuids=None):
        super().__init__(sysfs=str(root / "sys"), proc=str(root / "proc"), dev=str(root / "dev"),
                         state=str(root / "state"))
        self.content = content or {}
        self.busy = set(busy)
        self.uuids = uuids if uuids is not None else {"nvme0n1": UUID, "nvme0n2": UUID}
        self.wiped = []
        self.stopped = []
        os.makedirs(root / "proc" / "1", exist_ok=True)
        (root / "proc" / "1" / "mountinfo").write_text(
            "25 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw\n"
            + "".join(f"40 25 {dev} / /mnt/x rw - ext4 /dev/md0 rw\n" for dev in mounted))
        (root / "proc" / "swaps").write_text(
            "Filename\tType\tSize\tUsed\tPriority\n"
            + "".join(f"{s}\tpartition\t1\t0\t-2\n" for s in swaps))

    def probe_content(self, name):
        return self.content.get(name, "")

    def exclusive_free(self, name):
        return name not in self.busy

    def stop_array(self, name):
        super().stop_array(name)
        self.stopped.append(name)
        for member in os.listdir(self.block(name, "slaves")):
            os.rmdir(self.block(member, "holders", name))
        for member in os.listdir(self.block(name, "slaves")):
            os.rmdir(self.block(name, "slaves", member))

    def wipe_raid_superblock(self, name):
        self.wiped.append(name)
        self.uuids.pop(name, None)

    def member_uuid(self, name):
        return self.uuids.get(name, "")


UUID = "6f1c2a3b:4d5e6f70:81920a1b:2c3d4e5f"


def _disk(root, name):
    os.makedirs(root / "sys" / "class" / "block" / name / "holders")


def _array(root, name, members, dev="9:0"):
    base = root / "sys" / "class" / "block" / name
    os.makedirs(base / "holders")
    os.makedirs(base / "md")
    (base / "md" / "array_state").write_text("clean\n")
    (base / "md" / "level").write_text("raid0\n")
    (base / "dev").write_text(dev + "\n")
    for member in members:
        os.makedirs(base / "slaves" / member)
        os.makedirs(root / "sys" / "class" / "block" / member / "holders" / name)


@pytest.fixture
def gke_like(tmp_path):
    """Two namespaces assembled into md0, the shape GKE gives local SSDs."""
    _disk(tmp_path, "nvme0n1")
    _disk(tmp_path, "nvme0n2")
    _array(tmp_path, "md0", ["nvme0n1", "nvme0n2"])
    return tmp_path


def test_an_array_nothing_uses_is_stopped_and_its_members_wiped(gke_like):
    host = FakeHost(gke_like)

    released = md_release.release_idle_arrays(["nvme0n1", "nvme0n2"], host)

    assert released == ["md0"]
    assert (gke_like / "sys/class/block/md0/md/array_state").read_text() == "clear"
    assert host.stopped == ["md0"]
    assert sorted(host.wiped) == ["nvme0n1", "nvme0n2"], (
        "the superblocks were left, so the next boot assembles the array again "
        "over whatever the storage node wrote")
    for member in ("nvme0n1", "nvme0n2"):
        assert os.listdir(gke_like / "sys/class/block" / member / "holders") == []


@pytest.mark.parametrize("use", [
    pytest.param(dict(mounted=["9:0"]), id="mounted"),
    pytest.param(dict(swaps=["/dev/md0"]), id="swap"),
    pytest.param(dict(content={"md0": "ext4"}), id="formatted"),
    pytest.param(dict(busy=["md0"]), id="busy"),
])
def test_an_array_in_use_is_left_alone(gke_like, use):
    host = FakeHost(gke_like, **use)

    released = md_release.release_idle_arrays(["nvme0n1", "nvme0n2"], host)

    assert released == []
    assert host.stopped == [] and host.wiped == []
    assert (gke_like / "sys/class/block/md0/md/array_state").read_text() == "clean\n"


def test_an_array_holding_another_device_is_left_alone(gke_like):
    os.makedirs(gke_like / "sys/class/block/md0/holders/dm-0")
    host = FakeHost(gke_like)

    assert md_release.release_idle_arrays(["nvme0n1", "nvme0n2"], host) == []
    assert host.stopped == []


def test_an_array_with_a_member_the_configure_does_not_take_is_left_alone(gke_like):
    """Stopping it would take a disk out of an array on behalf of a deployment
    that never asked for that disk."""
    host = FakeHost(gke_like)

    assert md_release.release_idle_arrays(["nvme0n1"], host) == []
    assert host.stopped == []


def test_a_selection_without_arrays_touches_nothing(tmp_path):
    _disk(tmp_path, "nvme0n1")
    host = FakeHost(tmp_path)

    assert md_release.release_idle_arrays(["nvme0n1"], host) == []
    assert host.stopped == [] and host.wiped == []


def test_an_array_that_will_not_stop_fails_the_configure(gke_like):
    """A member still held after the stop is a member the generation refuses,
    and a configure that carried on would fail later naming the holder rather
    than the stop that did not happen."""

    class Stubborn(FakeHost):
        def stop_array(self, name):
            md_release.Host.stop_array(self, name)
            self.stopped.append(name)

    host = Stubborn(gke_like)
    with pytest.raises(md_release.ReleaseError, match="md0"):
        md_release.release_idle_arrays(["nvme0n1", "nvme0n2"], host)


def test_the_selection_names_every_namespace_of_a_controller(tmp_path):
    """GKE puts two local SSDs behind one controller, so the operator names one
    PCI address for both, and both are members of the array."""
    ctrl = tmp_path / "sys/bus/pci/devices/0000:00:04.0/nvme/nvme0"
    os.makedirs(ctrl / "nvme0n1")
    os.makedirs(ctrl / "nvme0n2")
    # With native multipath the controller carries the path, nvme1c1n1, and the
    # block device is the head, nvme1n1.
    os.makedirs(tmp_path / "sys/bus/pci/devices/0000:00:05.0/nvme/nvme1/nvme1c1n1")
    host = FakeHost(tmp_path)

    names = md_release.selected_block_devices(
        host, pci_addresses=["0000:00:04.0", "00:05.0"], blk_paths=[])

    assert names == ["nvme0n1", "nvme0n2", "nvme1n1"]


def test_the_selection_resolves_persistent_names(tmp_path):
    os.makedirs(tmp_path / "dev" / "disk" / "by-id")
    (tmp_path / "dev" / "nvme0n1").write_text("")
    link = tmp_path / "dev" / "disk" / "by-id" / "google-local-nvme-ssd-0"
    link.symlink_to(tmp_path / "dev" / "nvme0n1")
    host = FakeHost(tmp_path)

    assert md_release.selected_block_devices(host, pci_addresses=[], blk_paths=[str(link)]) == ["nvme0n1"]


def test_the_selection_takes_no_name_the_generation_would_refuse(tmp_path):
    """The release runs before the generation validates its selectors, so a
    selector the generation refuses must select nothing here either: a kernel
    path, a path that does not exist, and a PCI address that is a pattern
    rather than an address."""
    os.makedirs(tmp_path / "dev")
    (tmp_path / "dev" / "nvme0n1").write_text("")
    for ctrl in ("0000:00:04.0", "0000:00:05.0"):
        os.makedirs(tmp_path / f"sys/bus/pci/devices/{ctrl}/nvme/nvme0/nvme0n1")
    host = FakeHost(tmp_path)

    names = md_release.selected_block_devices(
        host, pci_addresses=["0000:*", "0000:00:0?.0"],
        blk_paths=[str(tmp_path / "dev" / "nvme0n1"), str(tmp_path / "dev" / "disk" / "by-id" / "missing")])

    assert names == []


def test_a_member_added_after_the_check_is_never_wiped(gke_like):
    """The check said every member is a device this node takes. A member the
    array gained after that is one nobody checked, and its superblock is not
    this configure's to erase."""

    class Growing(FakeHost):
        def probe_content(self, name):
            if not os.path.exists(self.block("sdz")):
                os.makedirs(self.block("sdz", "holders", "md0"))
                os.makedirs(self.block("md0", "slaves", "sdz"))
            return super().probe_content(name)

    host = Growing(gke_like)

    assert md_release.release_idle_arrays(["nvme0n1", "nvme0n2"], host) == []
    assert host.stopped == [] and host.wiped == []


def test_an_array_udev_reassembles_is_stopped_again(gke_like):
    """udev assembles incrementally on the change event a stop produces, so the
    first stop can come back before the wipe; the next attempt is the ordinary
    answer to that, not a failure."""

    class Reassembling(FakeHost):
        def stop_array(self, name):
            if not self.stopped:
                md_release.Host.stop_array(self, name)
                self.stopped.append(name)
                return  # the array is back before the wipe
            super().stop_array(name)

    host = Reassembling(gke_like)

    assert md_release.release_idle_arrays(["nvme0n1", "nvme0n2"], host) == ["md0"]
    assert host.stopped == ["md0", "md0"]
    assert sorted(host.wiped) == ["nvme0n1", "nvme0n2"]


def test_a_teardown_interrupted_after_the_stop_is_finished_by_the_next_run(gke_like):
    """Once the array is stopped nothing holds its members, so a run that died
    before the wipe leaves superblocks no holder points at, and the next boot
    assembles the array over whatever the storage node wrote. The next
    configure finishes the wipe from the record the first one left."""

    class Crashing(FakeHost):
        def wipe_raid_superblock(self, name):
            raise KeyboardInterrupt  # the pod was killed

    with pytest.raises(KeyboardInterrupt):
        md_release.release_idle_arrays(["nvme0n1", "nvme0n2"], Crashing(gke_like))
    assert os.listdir(gke_like / "sys/class/block/nvme0n1/holders") == []

    host = FakeHost(gke_like, uuids={"nvme0n1": UUID, "nvme0n2": UUID, "nvme1n1": "another"})
    _disk(gke_like, "nvme1n1")
    assert md_release.release_idle_arrays(["nvme0n1", "nvme0n2", "nvme1n1"], host) == []
    assert sorted(host.wiped) == ["nvme0n1", "nvme0n2"], (
        "the interrupted wipe was not finished, or a superblock of another array was erased")

    # Finished work is not done twice.
    again = FakeHost(gke_like, uuids={})
    md_release.release_idle_arrays(["nvme0n1", "nvme0n2"], again)
    assert again.wiped == []
