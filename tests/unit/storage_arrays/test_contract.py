"""The contract every array adapter must meet, run against each vendor fake
(and, with SB_ARRAY_LIVE_CONFIG, against one real array or simulator)."""
from __future__ import annotations

import uuid

import pytest

from simplyblock_core.storage_arrays.base import (
    ArrayAuthError,
    ArrayConflictError,
    ArrayError,
    ArrayRequestError,
    HostSpec,
    Protocol,
)
from simplyblock_core.storage_arrays.client import MANAGED_PREFIX, round_size

GIB = 1 << 30


def _uuid() -> str:
    return str(uuid.uuid4())


def _protocols(h):
    return [p for p in h.setup.initiators if p in h.client.capabilities.protocols]


def _host(h, protocol, which=0, name=None):
    return HostSpec(name=name or f"sbtest-{protocol.value}-{which}-{uuid.uuid4().hex[:6]}",
                    initiators=h.setup.initiators[protocol][which])


@pytest.fixture()
def cleanup(harness):
    volumes, hosts = [], []
    yield volumes, hosts
    for vid in volumes:
        try:
            harness.client.delete_volume(vid)
        except ArrayError:
            pass
    for host in hosts:
        try:
            harness.client.delete_host(host)
        except ArrayError:
            pass


def test_adapter_declares_itself(harness):
    cls = harness.client.adapter_cls
    assert cls.vendor == harness.client.vendor
    assert cls.display_name
    caps = cls.capabilities
    assert caps.protocols
    assert caps.size_granularity_bytes > 0 and caps.min_size_bytes > 0
    assert caps.max_name_len >= len(MANAGED_PREFIX) + 16
    assert set(_protocols(harness)), "fake provides no initiators for a supported protocol"


def test_get_info(harness):
    info = harness.client.get_info()
    assert info.vendor == harness.client.vendor
    assert harness.provider.resolved[0].startswith("array/")


def test_create_get_find(harness, cleanup):
    vol_uuid = _uuid()
    vol = harness.client.create_volume(vol_uuid, 10 * GIB + 1)
    cleanup[0].append(vol.id)
    caps = harness.client.capabilities
    assert vol.name == harness.client.volume_name(vol_uuid)
    assert vol.name.startswith(MANAGED_PREFIX)
    assert len(vol.name) <= caps.max_name_len
    assert vol.size_bytes == round_size(10 * GIB + 1, caps.size_granularity_bytes,
                                        caps.min_size_bytes)
    assert vol.identifier and vol.identifier == vol.identifier.lower()
    assert vol.wwn is None or vol.wwn.startswith(("naa.", "eui."))
    assert vol.nguid is None or vol.nguid.startswith("eui.")

    got = harness.client.get_volume(vol.id)
    assert got == vol
    assert harness.client.find_volume(vol_uuid) == vol


def test_missing_volume_is_none(harness):
    assert harness.client.find_volume(_uuid()) is None


def test_get_volume_of_deleted_is_none(harness, cleanup):
    vol = harness.client.create_volume(_uuid(), GIB)
    assert harness.client.delete_volume(vol.id) is True
    assert harness.client.get_volume(vol.id) is None
    assert harness.client.delete_volume(vol.id) is False


def test_create_is_idempotent(harness, cleanup):
    vol_uuid = _uuid()
    first = harness.client.create_volume(vol_uuid, 4 * GIB)
    cleanup[0].append(first.id)
    again = harness.client.create_volume(vol_uuid, 4 * GIB)
    assert again.id == first.id and again.identifier == first.identifier


def test_create_rerun_with_larger_size_grows(harness, cleanup):
    vol_uuid = _uuid()
    first = harness.client.create_volume(vol_uuid, 8 * GIB)
    cleanup[0].append(first.id)
    again = harness.client.create_volume(vol_uuid, 16 * GIB)
    assert again.id == first.id and again.size_bytes >= 16 * GIB


def test_resize(harness, cleanup):
    vol = harness.client.create_volume(_uuid(), 8 * GIB)
    cleanup[0].append(vol.id)
    grown = harness.client.resize_volume(vol.id, 24 * GIB, expected_identifier=vol.identifier)
    assert grown.size_bytes >= 24 * GIB
    assert grown.identifier == vol.identifier
    assert harness.client.get_volume(vol.id).size_bytes == grown.size_bytes
    # same size: no-op
    assert harness.client.resize_volume(vol.id, grown.size_bytes) == grown
    with pytest.raises(ArrayConflictError, match="shrink"):
        harness.client.resize_volume(vol.id, 8 * GIB)


def test_host_and_mapping_lifecycle(harness, cleanup):
    for protocol in _protocols(harness):
        vol = harness.client.create_volume(_uuid(), GIB * 8)
        cleanup[0].append(vol.id)
        spec_a = _host(harness, protocol, 0)
        spec_b = _host(harness, protocol, 1)
        host_a = harness.client.ensure_host(spec_a)
        cleanup[1].append(host_a)
        assert harness.client.ensure_host(spec_a) == host_a, "ensure_host not idempotent"
        host_b = harness.client.ensure_host(spec_b)
        cleanup[1].append(host_b)
        assert host_a != host_b

        m_a = harness.client.map_volume(vol.id, host_a, expected_identifier=vol.identifier)
        assert m_a.host_id == host_a and m_a.volume_id == vol.id
        if harness.setup.has_lun_numbers:
            assert isinstance(m_a.lun, int) and m_a.lun >= 0
        assert harness.client.map_volume(vol.id, host_a) == m_a, "map not idempotent"
        m_b = harness.client.map_volume(vol.id, host_b)  # multi-attach

        hosts = {m.host_id for m in harness.client.list_mappings(vol.id)}
        assert hosts == {host_a, host_b}

        harness.client.unmap_volume(vol.id, host_a)
        harness.client.unmap_volume(vol.id, host_a)  # idempotent
        assert [m.host_id for m in harness.client.list_mappings(vol.id)] == [m_b.host_id]

        # delete unmaps the remaining host first
        assert harness.client.delete_volume(vol.id, expected_identifier=vol.identifier)
        cleanup[0].remove(vol.id)
        assert harness.client.get_volume(vol.id) is None

        harness.client.delete_host(host_a)
        harness.client.delete_host(host_a)  # absent: no-op
        harness.client.delete_host(host_b)
        cleanup[1].clear()


def test_initiator_owned_by_another_host_conflicts(harness, cleanup):
    protocol = _protocols(harness)[0]
    first = _host(harness, protocol, 0)
    cleanup[1].append(harness.client.ensure_host(first))
    other = HostSpec(name=f"sbtest-other-{uuid.uuid4().hex[:6]}", initiators=first.initiators)
    with pytest.raises(ArrayConflictError):
        cleanup[1].append(harness.client.ensure_host(other))


def test_existing_host_gets_missing_initiators(harness, cleanup):
    protocol = _protocols(harness)[0]
    a, b = harness.setup.initiators[protocol]
    name = f"sbtest-grow-{uuid.uuid4().hex[:6]}"
    host = harness.client.ensure_host(HostSpec(name=name, initiators=a))
    cleanup[1].append(host)
    limit = harness.client.capabilities.max_initiators_per_host
    if limit is not None and len(a + b) > limit:
        with pytest.raises(ArrayError, match="at most"):
            harness.client.ensure_host(HostSpec(name=name, initiators=a + b))
        return
    assert harness.client.ensure_host(HostSpec(name=name, initiators=a + b)) == host
    # b's initiators now belong to that host: claiming them elsewhere conflicts
    with pytest.raises(ArrayConflictError):
        cleanup[1].append(harness.client.ensure_host(
            HostSpec(name=f"sbtest-x-{uuid.uuid4().hex[:6]}", initiators=b)))


def test_wrong_identifier_is_refused(harness, cleanup):
    vol = harness.client.create_volume(_uuid(), GIB)
    cleanup[0].append(vol.id)
    for action in (
        lambda: harness.client.delete_volume(vol.id, expected_identifier="naa.600000000000"),
        lambda: harness.client.resize_volume(vol.id, 4 * GIB, expected_identifier="naa.6000"),
    ):
        with pytest.raises(ArrayConflictError, match="does not match"):
            action()
    assert harness.client.get_volume(vol.id) is not None


def test_volume_of_another_record_is_refused(harness, cleanup):
    """A reused array id/WWN must not pass for the recorded volume: the
    name derived from the recorded UUID pins it."""
    vol_uuid = _uuid()
    vol = harness.client.create_volume(vol_uuid, GIB)
    cleanup[0].append(vol.id)
    with pytest.raises(ArrayConflictError, match="name does not match"):
        harness.client.delete_volume(vol.id, vol.identifier, volume_uuid=_uuid())
    with pytest.raises(ArrayConflictError, match="name does not match"):
        harness.client.resize_volume(vol.id, 4 * GIB, volume_uuid=_uuid())
    assert harness.client.delete_volume(vol.id, vol.identifier, volume_uuid=vol_uuid)
    cleanup[0].remove(vol.id)


def test_foreign_volume_is_refused(harness):
    if harness.live:
        pytest.skip("live tier never touches foreign volumes")
    vid = harness.setup.create_foreign_volume("customer-db01", 4 * GIB)
    for action in (lambda: harness.client.delete_volume(vid),
                   lambda: harness.client.resize_volume(vid, 8 * GIB)):
        with pytest.raises(ArrayConflictError, match="not created by simplyblock"):
            action()
    assert harness.client.get_volume(vid) is not None


def test_session_expiry_is_transparent(harness, cleanup):
    if harness.live:
        pytest.skip("cannot expire a real array's sessions")
    harness.client.get_info()
    harness.fake.expire_sessions()
    vol = harness.client.create_volume(_uuid(), GIB)
    cleanup[0].append(vol.id)
    assert harness.client.get_volume(vol.id) == vol


def test_bad_credential_raises_auth_error_after_one_retry(harness):
    harness.provider.credential = harness.setup.bad_credential
    with pytest.raises(ArrayAuthError):
        harness.client.get_info()
    assert harness.provider.invalidated == 1
    assert len(harness.provider.resolved) == 2


def test_rotated_credential_is_picked_up(harness):
    if harness.live:
        pytest.skip("needs control of the array's credential")
    harness.client.get_info()
    new = {k: "rotated-" + harness.setup.credential.get(k).get_secret_value()
           for k in harness.setup.credential.field_names}
    harness.fake.set_credential(**new)
    harness.fake.expire_sessions()
    from pydantic import SecretStr

    from simplyblock_core.credentials import Credential
    harness.provider.credential = Credential({k: SecretStr(v) for k, v in new.items()})
    # The client still holds the old credential; the array rejects it, the
    # client invalidates once, resolves the rotated secret and succeeds.
    harness.client.get_info()
    assert harness.provider.invalidated == 1


def test_server_error_does_not_leak_credentials(harness):
    if harness.live:
        pytest.skip("fault injection needs the fake")
    harness.client.get_info()
    harness.fake.fail_next(500, {"error": "internal"}, times=50)
    with pytest.raises(ArrayRequestError) as e:
        harness.client.create_volume(_uuid(), GIB)
    text = str(e.value)
    for name in harness.setup.credential.field_names:
        assert harness.setup.credential.get(name).get_secret_value() not in text


def test_unsupported_protocol_is_refused_before_calling_the_array(harness):
    unsupported = [p for p in Protocol if p not in harness.client.capabilities.protocols]
    if not unsupported:
        pytest.skip("adapter supports every protocol")
    from simplyblock_core.storage_arrays.base import Initiator
    p = unsupported[0]
    address = "21:00:00:24:ff:4c:1a:99" if p in (Protocol.FC, Protocol.NVME_FC) else "x"
    with pytest.raises(ArrayError, match="does not support"):
        harness.client.ensure_host(HostSpec(name="sbtest-unsup",
                                            initiators=(Initiator(protocol=p, address=address),)))


def test_delete_host_with_mappings_is_refused(harness, cleanup):
    """Deleting a host must never silently drop a live mapping."""
    protocol = _protocols(harness)[0]
    vol = harness.client.create_volume(_uuid(), GIB)
    cleanup[0].append(vol.id)
    host = harness.client.ensure_host(_host(harness, protocol, 0))
    cleanup[1].append(host)
    harness.client.map_volume(vol.id, host)
    try:
        harness.client.delete_host(host)
    except ArrayConflictError:
        pass  # refused: the expected outcome on arrays that own host objects
    # Either way (refused, or a no-op as for PowerFlex SDCs), the mapping survives.
    assert [m.host_id for m in harness.client.list_mappings(vol.id)] == [host]
