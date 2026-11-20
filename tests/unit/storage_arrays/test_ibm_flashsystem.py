"""IBM Storage Virtualize specifics beyond the shared contract suite."""
from __future__ import annotations

import uuid

import pytest

from simplyblock_core.storage_arrays.base import (
    ArrayConflictError,
    ArrayNotFoundError,
    ArrayRequestError,
    ArrayUnsupportedError,
    HostSpec,
    Initiator,
    Protocol,
)
from simplyblock_core.storage_arrays.client import ArrayClient
from simplyblock_core.storage_arrays.testing import FakeTransport
from simplyblock_core.storage_arrays.testing.ibm_flashsystem import contract_setup

GIB = 1 << 30


class _Provider:
    def __init__(self, credential):
        self.credential = credential
        self.invalidated = 0

    def resolve(self, ref, context):
        return self.credential

    def invalidate(self, ref=None):
        self.invalidated += 1


def _client(**options):
    setup = contract_setup()
    if options:
        setup.config = setup.config.model_copy(
            update={"options": {**setup.config.options, **options}})
    client = ArrayClient(setup.config, _Provider(setup.credential),  # type: ignore[arg-type]
                         transport=FakeTransport(setup.fake),
                         session_kwargs={"sleep": lambda s: None})
    return client, setup.fake


def _cmds(fake, name):
    return [opts for cmd, _, opts in fake.commands if cmd == name]


def test_get_info():
    client, _ = _client()
    info = client.get_info()
    assert info.model == "IBM FlashSystem 7300"
    assert info.version == "8.6.0.4"
    assert info.serial == "000002042140A0B2"


def test_mkvolume_parameters_thin_default():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), 10 * GIB + 1)
    (opts,) = _cmds(fake, "mkvolume")
    assert opts == {"name": vol.name, "pool": "Pool0", "size": 10 * GIB + 512,
                    "unit": "b", "thin": True}
    assert vol.size_bytes == 10 * GIB + 512
    assert vol.wwn == "naa." + fake.vdisks[vol.id]["vdisk_UID"].lower()
    # capacity must be read in bytes, never in the human-readable form
    assert all(o.get("bytes") is True for o in _cmds(fake, "lsvdisk"))


def test_thick_volume_and_io_group():
    client, fake = _client(thin=False, io_group="io_grp1")
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    (opts,) = _cmds(fake, "mkvolume")
    assert "thin" not in opts and opts["iogrp"] == "io_grp1"
    assert fake.vdisks[vol.id]["IO_group_name"] == "io_grp1"


def test_rest_v1_root():
    client, fake = _client(rest_root="/rest/v1")
    client.create_volume(str(uuid.uuid4()), GIB)
    assert all(path.startswith("/rest/v1/") for _, path in fake.requests)


def test_resize_sends_delta():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), 8 * GIB)
    client.resize_volume(vol.id, 20 * GIB)
    assert _cmds(fake, "expandvdisksize") == [{"size": 12 * GIB, "unit": "b"}]


def test_unknown_pool_is_not_found():
    client, _ = _client(pool="NoSuchPool")
    with pytest.raises(ArrayNotFoundError, match="CMMVC5754E"):
        client.create_volume(str(uuid.uuid4()), GIB)


def test_cmmvc_codes_are_mapped():
    client, fake = _client()
    fake.fail_next(500, b"CMMVC6035E The action failed as the object already exists.",
                   path="mkvolume")
    with pytest.raises(ArrayConflictError, match="CMMVC6035E"):
        client.create_volume(str(uuid.uuid4()), GIB)
    fake.fail_next(500, b"CMMVC5786E The action failed because the cluster is not in a "
                        b"stable state.", path="mkvolume")
    with pytest.raises(ArrayRequestError) as e:
        client.create_volume(str(uuid.uuid4()), GIB)
    assert e.value.status == 500 and "CMMVC5786E" in str(e.value)


def test_expired_token_is_403_and_relogs_in():
    client, fake = _client()
    client.get_info()
    fake.expire_sessions()
    client.get_info()
    assert [p for _, p in fake.requests].count("/rest/auth") == 2


def test_volume_id_reuse_is_caught_by_identifier():
    client, _ = _client()
    old = client.create_volume(str(uuid.uuid4()), GIB)
    assert client.delete_volume(old.id)
    new = client.create_volume(str(uuid.uuid4()), GIB)
    assert new.id == old.id  # the array hands out the lowest free id again
    with pytest.raises(ArrayConflictError, match="does not match"):
        client.delete_volume(old.id, expected_identifier=old.identifier)
    assert client.get_volume(new.id) == new


def test_multi_host_mapping_uses_force_and_single_row_quirk():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    a = client.ensure_host(HostSpec(name="sbtest-a", initiators=(
        Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:a"),)))
    b = client.ensure_host(HostSpec(name="sbtest-b", initiators=(
        Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:b"),)))
    m = client.map_volume(vol.id, a)
    assert m.lun == 0
    # one mapping: lsvdiskhostmap answers with a bare dict
    assert client.list_mappings(vol.id) == [m]
    client.map_volume(vol.id, b)
    assert all(o.get("force") is True for o in _cmds(fake, "mkvdiskhostmap"))
    assert {x.host_id for x in client.list_mappings(vol.id)} == {a, b}


def test_fc_host_wwpns_are_colon_separated_and_forced():
    client, fake = _client()
    client.ensure_host(HostSpec(name="sbtest-fc", initiators=(
        Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:2b"),
        Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:2c"))))
    (opts,) = _cmds(fake, "mkhost")
    assert opts == {"name": "sbtest-fc", "fcwwpn": "21000024FF4C1A2B:21000024FF4C1A2C",
                    "force": True}
    # re-run with the same ports: nothing is re-added
    client.ensure_host(HostSpec(name="sbtest-fc", initiators=(
        Initiator(protocol=Protocol.FC, address="21000024ff4c1a2b"),)))
    assert _cmds(fake, "addhostport") == []


def test_fc_port_of_other_host_conflicts():
    client, fake = _client()
    wwpn = Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:2b")
    client.ensure_host(HostSpec(name="sbtest-one", initiators=(wwpn,)))
    with pytest.raises(ArrayConflictError, match="registered to another host"):
        client.ensure_host(HostSpec(name="sbtest-two", initiators=(wwpn,)))
    assert "sbtest-two" not in fake.hosts


def test_nvme_host_protocol():
    client, fake = _client()
    client.ensure_host(HostSpec(name="sbtest-nvme", initiators=(
        Initiator(protocol=Protocol.NVME_TCP, address="nqn.2014-08.org.nvmexpress:uuid:x"),)))
    assert fake.hosts["sbtest-nvme"]["protocol"] == "tcpnvme"
    with pytest.raises(ArrayUnsupportedError, match="one transport"):
        client.ensure_host(HostSpec(name="sbtest-mixed", initiators=(
            Initiator(protocol=Protocol.NVME_TCP, address="nqn.2014-08.org.nvmexpress:uuid:y"),
            Initiator(protocol=Protocol.NVME_RDMA, address="nqn.2014-08.org.nvmexpress:uuid:z"))))


def test_delete_host_with_mappings_conflicts():
    client, _ = _client()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    host = client.ensure_host(HostSpec(name="sbtest-busy", initiators=(
        Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:busy"),)))
    client.map_volume(vol.id, host)
    with pytest.raises(ArrayConflictError, match="CMMVC5871E"):
        client.delete_host(host)
