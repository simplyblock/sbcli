"""The LIO data path of the mock arrays (storage_arrays.testing.lio): a fake's
volumes, hosts, and mappings mirrored into an iSCSI target, here into the
in-memory backend. Every vendor fake with iSCSI runs the same scenario through
its own adapter, which is what the control plane would send it."""
from __future__ import annotations

import pytest

from simplyblock_core.array.san import host_wwn
from simplyblock_core.storage_arrays.base import HostSpec, Initiator, Protocol
from simplyblock_core.storage_arrays.testing import FakeTransport
from simplyblock_core.storage_arrays.testing.lio import LioMirror, MemoryLio

from .conftest import VENDORS, Harness, fake_module

GIB = 1 << 30
ISCSI_VENDORS = [v for v in VENDORS if v != "dell-powerflex"]
UUID = "6f1d3a52-8c4b-4e0a-9d27-3b8e4c1f7a90"


class Bed:
    def __init__(self, vendor: str):
        self.setup = fake_module(vendor).contract_setup()
        self.harness = Harness(self.setup)
        self.client = self.harness.client
        self.backend = MemoryLio()
        self.mirror = LioMirror(self.setup.fake, self.setup.config, self.setup.credential,
                                self.backend, transport=FakeTransport(self.setup.fake),
                                session_kwargs={"sleep": lambda s: None})
        groups = self.setup.initiators[Protocol.ISCSI]
        self.iqn_a, self.iqn_b = groups[0][0].address.lower(), groups[1][0].address.lower()

    def host(self, name: str, iqn: str) -> str:
        return self.client.ensure_host(HostSpec(
            name=name, initiators=(Initiator(protocol=Protocol.ISCSI, address=iqn),)))


@pytest.fixture(params=ISCSI_VENDORS)
def bed(request) -> Bed:
    b = Bed(request.param)
    yield b
    b.client.close()
    b.mirror.close()


def test_volumes_hosts_and_mappings_reach_the_target(bed):
    vol = bed.client.create_volume(UUID, 10 * GIB)
    host_a = bed.host("sbtest-a", bed.iqn_a)
    bed.mirror.reconcile()
    lun = bed.backend.volumes[bed.mirror.key(vol.id)]
    assert lun.size_bytes == vol.size_bytes
    # The host sees the identity the plug-in reports for the LUN.
    assert lun.serial == host_wwn(vol, Protocol.ISCSI)
    # A registered host can log in, and sees nothing yet.
    assert bed.backend.acls[bed.iqn_a] == {}

    bed.client.map_volume(vol.id, host_a)
    bed.mirror.reconcile()
    (mapped,) = bed.backend.acls[bed.iqn_a].values()
    assert mapped == bed.mirror.key(vol.id)

    # Multi-attach: a second host sees the same LUN.
    host_b = bed.host("sbtest-b", bed.iqn_b)
    bed.client.map_volume(vol.id, host_b)
    bed.mirror.reconcile()
    assert list(bed.backend.acls[bed.iqn_b].values()) == [bed.mirror.key(vol.id)]

    # Fence: the unmapped host loses the LUN, the other keeps it.
    bed.client.unmap_volume(vol.id, host_a)
    bed.mirror.reconcile()
    assert bed.backend.acls[bed.iqn_a] == {}
    assert list(bed.backend.acls[bed.iqn_b].values()) == [bed.mirror.key(vol.id)]

    grown = bed.client.resize_volume(vol.id, 20 * GIB)
    bed.mirror.reconcile()
    assert bed.backend.volumes[bed.mirror.key(vol.id)].size_bytes == grown.size_bytes

    bed.client.delete_volume(vol.id)
    bed.mirror.reconcile()
    assert bed.mirror.key(vol.id) not in bed.backend.volumes
    assert bed.backend.acls[bed.iqn_b] == {}


def test_reconcile_is_idempotent(bed):
    vol = bed.client.create_volume(UUID, 10 * GIB)
    bed.client.map_volume(vol.id, bed.host("sbtest-a", bed.iqn_a))
    bed.mirror.reconcile()
    before = bed.backend.ops
    bed.mirror.reconcile()
    assert bed.backend.ops == before


def test_allowed_initiators_can_log_in_before_they_are_registered():
    b = Bed("pure-flasharray")
    b.mirror.allow = ["iqn.2026-01.io.sb:not-registered"]
    b.mirror.reconcile()
    assert b.backend.acls["iqn.2026-01.io.sb:not-registered"] == {}


def test_lun_numbers_follow_the_array():
    b = Bed("infinidat-infinibox")   # reports a LUN number per mapping
    vol = b.client.create_volume(UUID, 10 * GIB)
    m = b.client.map_volume(vol.id, b.host("sbtest-a", b.iqn_a))
    b.mirror.reconcile()
    assert list(b.backend.acls[b.iqn_a]) == [m.lun]
