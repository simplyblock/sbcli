"""The SAN array plugins (simplyblock_core.array.san): every storage_arrays
adapter driven through the multi-attach ArrayPlugin contract, against its
vendor fake."""
from __future__ import annotations

import pytest

from simplyblock_core import array, multi_attach
from simplyblock_core.array import san
from simplyblock_core.storage_arrays.base import Protocol
from simplyblock_core.storage_arrays.testing import FakeTransport

from .conftest import VENDORS, fake_module

GIB = 1 << 30
LUN = "simplyblock-node-a-s0"

_KEY = {
    Protocol.ISCSI: "iscsi_iqn",
    Protocol.FC: "fc_wwpns",
    Protocol.NVME_TCP: "nvme_hostnqn",
    Protocol.NVME_RDMA: "nvme_hostnqn",
    Protocol.NVME_FC: "nvme_hostnqn",
    Protocol.SDC: "sdc_guid",
}


class Bed:
    def __init__(self, vendor: str):
        self.vendor = vendor
        self.setup = fake_module(vendor).contract_setup()
        protos = list(self.setup.initiators)
        self.protocol = Protocol.ISCSI if Protocol.ISCSI in protos else protos[0]
        self.creds = {n: self.setup.credential.get(n) for n in self.setup.credential.field_names}

    def plugin(self, cluster_id: str = "cluster-1", **extra) -> san.SanArrayPlugin:
        options = {**self.setup.config.options, "protocol": self.protocol.value, **extra}
        p = array.plugin_class(self.vendor)(endpoint=self.setup.config.endpoint, options=options,
                                            cluster_id=cluster_id)
        assert isinstance(p, san.SanArrayPlugin)
        p.transport = FakeTransport(self.setup.fake)
        p.session_kwargs = {"sleep": lambda s: None}
        p.connect(self.setup.config.endpoint, self.creds, options)
        return p

    def initiators(self, which: int) -> dict:
        group = self.setup.initiators[self.protocol][which]
        key = _KEY[self.protocol]
        addresses = [i.address for i in group]
        value = addresses if key == "fc_wwpns" else addresses[0]
        return {"hostname": f"node-{which}", key: value}


@pytest.fixture(params=VENDORS)
def bed(request) -> Bed:
    return Bed(request.param)


def test_registered_as_builtin_plugins():
    assert set(san.SAN_PLUGINS) == set(VENDORS)
    for name, path in san.SAN_PLUGINS.items():
        assert array.BUILTIN_PLUGINS[name] == path
        assert array.plugin_class(name).vendor == name


def test_lifecycle_multi_attach_fence_grow_delete(bed):
    p = bed.plugin()
    caps = p.capabilities()
    assert bed.protocol.value in caps["protocols"]
    host_a = p.ensure_host("node-a", bed.initiators(0))
    assert p.ensure_host("node-a", bed.initiators(0)) == host_a        # idempotent

    lun = p.create_lun(LUN, 10 * GIB, {"simplyblock-host": "node-a"})
    assert lun["size"] >= 10 * GIB
    assert lun["wwn"] and not lun["wwn"].startswith("naa.")
    assert p.create_lun(LUN, 10 * GIB, {})["id"] == lun["id"]          # retried create

    p.attach(lun["id"], host_a)
    p.attach(lun["id"], host_a)                                         # idempotent
    view = p.get(lun["id"])
    assert view["hosts"] == [host_a]
    assert multi_attach.normalize_wwn(view["wwn"]) == multi_attach.normalize_wwn(lun["wwn"])

    host_b = p.ensure_host("node-b", bed.initiators(1))
    p.attach(lun["id"], host_b)
    assert sorted(p.get(lun["id"])["hosts"]) == sorted([host_a, host_b])
    p.detach(lun["id"], host_b)
    assert p.get(lun["id"])["hosts"] == [host_a]

    assert p.fence(lun["id"], host_a)
    assert p.get(lun["id"])["hosts"] == []
    assert p.unfence(lun["id"], host_a)
    assert p.get(lun["id"])["hosts"] == [host_a]

    p.resize(lun["id"], 20 * GIB)
    assert p.get(lun["id"])["size"] >= 20 * GIB

    p.delete_lun(lun["id"])
    with pytest.raises(array.ArrayError):
        p.get(lun["id"])
    p.delete_lun(lun["id"])                                              # gone: succeeds
    p.detach(lun["id"], host_a)                                          # gone: succeeds


def test_other_clusters_luns_are_never_touched(bed):
    mine = bed.plugin("cluster-1")
    theirs = bed.plugin("cluster-2")
    lun = mine.create_lun(LUN, 10 * GIB, {})
    # Same requested name, other cluster: a different LUN.
    assert theirs.create_lun(LUN, 10 * GIB, {})["id"] != lun["id"]
    for op in (lambda: theirs.get(lun["id"]), lambda: theirs.delete_lun(lun["id"]),
               lambda: theirs.resize(lun["id"], 20 * GIB),
               lambda: theirs.attach(lun["id"], "h"), lambda: theirs.detach(lun["id"], "h")):
        with pytest.raises(array.ForeignLunError):
            op()
    assert mine.get(lun["id"])["size"] >= 10 * GIB

    foreign = bed.setup.create_foreign_volume("prod-db-01", 5 * GIB)
    with pytest.raises(array.ForeignLunError):
        mine.delete_lun(foreign)


def test_connect_validation(bed):
    with pytest.raises(array.ArrayError, match="protocol"):
        bed.plugin(protocol="")
    with pytest.raises(array.ArrayError, match="unknown protocol"):
        bed.plugin(protocol="carrier-pigeon")
    unsupported = [p for p in Protocol if p.value not in
                   bed.plugin().capabilities()["protocols"]]
    if unsupported:
        with pytest.raises(array.ArrayError, match="does not support"):
            bed.plugin(protocol=unsupported[0].value)


def test_host_without_initiator_of_the_protocol(bed):
    p = bed.plugin()
    with pytest.raises(array.ArrayError, match="reports no"):
        p.ensure_host("node-a", {"hostname": "node-a"})


def test_list_luns_falls_back_to_recorded(bed):
    with pytest.raises(NotImplementedError):
        bed.plugin().list_luns()


def test_names():
    assert len(san.lun_uuid("c", "x")) == 36
    a, b = san.lun_uuid("c1", LUN), san.lun_uuid("c2", LUN)
    assert a != b and a == san.lun_uuid("c1", LUN)
    assert san.host_object_name("node-a.example.com", 63) == "sb-node-a-example-com"
    long = san.host_object_name("x" * 80, 31)
    assert len(long) == 31 and long.startswith("sb-x")
    assert san.host_object_name("x" * 80, 31) == long
    assert san.host_object_name("y" * 80, 31) != long


# NetApp creates SCSI LUNs unless told otherwise; an NVMe protocol without
# protocol_family would create LUNs the NVMe hosts never see.
def test_netapp_protocol_family_follows_the_protocol():
    bed = Bed("netapp-ontap")
    bed.protocol = Protocol.NVME_TCP
    assert bed.plugin().client.config.options["protocol_family"] == "nvme"
    bed.protocol = Protocol.ISCSI
    assert bed.plugin().client.config.options.get("protocol_family", "scsi") == "scsi"
    # A stated family that contradicts the protocol is refused.
    bed.protocol = Protocol.NVME_TCP
    with pytest.raises(array.ArrayError, match="protocol_family"):
        bed.plugin(protocol_family="scsi")
