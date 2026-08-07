# coding=utf-8
"""Shared mock factories for unit tests."""

from unittest.mock import MagicMock


def make_mock_cluster(cluster_id="cluster-1", **attrs):
    """Build a MagicMock Cluster with safe defaults for unit tests.

    ``hashicorp_vault_settings`` is set to ``None`` so callers of
    ``create_kms_connection`` take the LocalKMS branch instead of trying
    to read TLS material from disk via the HCP branch (a MagicMock's
    auto-created attribute would otherwise be truthy).
    """
    cluster = MagicMock()
    cluster.get_id.return_value = cluster_id
    cluster.hashicorp_vault_settings = None
    for name, value in attrs.items():
        setattr(cluster, name, value)
    return cluster


# ---------------------------------------------------------------------------
# Edge-cluster fakes (shared by tests/unit/edge/ and tests/integration/edge/).
# ---------------------------------------------------------------------------

from simplyblock_core.rpc_client import RPCException  # noqa: E402


class FakeSpdk:
    """Stateful stand-in for one edge node's SPDK proxy: tracks bdevs, raids,
    subsystems, lvstores. ``fail`` makes named methods raise; ``alive=False``
    makes every call raise (dead pod). ``reset()`` simulates a pod restart."""

    def __init__(self):
        self.bdevs = set()
        self.raids = {}          # raid name -> list of base bdevs
        self.subsystems = {}     # nqn -> {'namespaces': [...], 'listen_addresses': [...]}
        self.transports = []
        self.lvstores = {}       # lvs name -> base bdev
        self.calls = []
        self.fail = set()
        self.alive = True

    def reset(self):
        self.__init__()

    def _rec(self, method, **kwargs):
        self.calls.append((method, kwargs))
        if not self.alive:
            raise RPCException("connection error")
        if method in self.fail:
            raise RPCException(f"{method} failed (injected)")

    def called(self, method):
        return [c for c in self.calls if c[0] == method]

    # -- liveness / inventory
    def get_version(self):
        self._rec("get_version")
        return "25.05-edge"

    def get_bdevs(self, name=None, all_bdevs=False):
        self._rec("get_bdevs", name=name)
        if name is not None:
            return [{"name": name}] if name in self.bdevs else None
        return [{"name": b} for b in self.bdevs]

    # -- aio
    def bdev_aio_create(self, name, filename, block_size=4096):
        self._rec("bdev_aio_create", name=name, filename=filename)
        self.bdevs.add(name)
        return name

    def bdev_aio_delete(self, name):
        self._rec("bdev_aio_delete", name=name)
        self.bdevs.discard(name)
        return True

    # -- raid
    def bdev_raid_create(self, name, bdevs_list, raid_level="0", strip_size_kb=4,
                         superblock=False):
        self._rec("bdev_raid_create", name=name, bdevs_list=list(bdevs_list),
                  raid_level=raid_level)
        self.raids[name] = list(bdevs_list)
        self.bdevs.add(name)
        return True

    def bdev_raid_add_base_bdev(self, raid_bdev, base_bdev):
        self._rec("bdev_raid_add_base_bdev", raid_bdev=raid_bdev, base_bdev=base_bdev)
        if base_bdev in self.raids.get(raid_bdev, []):
            raise RPCException("base bdev already in raid")
        self.raids.setdefault(raid_bdev, []).append(base_bdev)
        return True

    def bdev_raid_remove_base_bdev(self, base_bdev):
        self._rec("bdev_raid_remove_base_bdev", base_bdev=base_bdev)
        for members in self.raids.values():
            if base_bdev in members:
                members.remove(base_bdev)
                return True
        raise RPCException("base bdev not found")

    def bdev_raid_get_bdevs(self):
        self._rec("bdev_raid_get_bdevs")
        return [{"name": name, "base_bdevs_list": [{"name": m} for m in members]}
                for name, members in self.raids.items()]

    def detach_backing_device(self, bdev):
        """Test helper: simulate the backing disk vanishing (EBS force-detach)
        — the bdev disappears and every raid ejects it."""
        self.bdevs.discard(bdev)
        for members in self.raids.values():
            if bdev in members:
                members.remove(bdev)

    # -- remote leg
    def bdev_nvme_attach_controller(self, name, nqn, traddr, trsvcid, trtype,
                                    multipath=False, **kwargs):
        self._rec("bdev_nvme_attach_controller", name=name, nqn=nqn,
                  traddr=traddr, trsvcid=trsvcid)
        self.bdevs.add(f"{name}n1")
        return [f"{name}n1"]

    def bdev_nvme_detach_controller(self, name):
        self._rec("bdev_nvme_detach_controller", name=name)
        self.bdevs.discard(f"{name}n1")
        return True

    def bdev_examine(self, name):
        self._rec("bdev_examine", name=name)
        return True

    # -- transport / subsystems
    def transport_list(self, trtype=None):
        self._rec("transport_list", trtype=trtype)
        return [t for t in self.transports if trtype is None or t == trtype] or None

    def transport_create(self, trtype, qpair_count=6, shared_bufs=24576):
        self._rec("transport_create", trtype=trtype)
        self.transports.append(trtype)
        return True

    def subsystem_get(self, nqn):
        self._rec("subsystem_get", nqn=nqn)
        return self.subsystems.get(nqn)

    def subsystem_create(self, nqn, serial_number, model_number, min_cntlid=1,
                         max_namespaces=32, allow_any_host=True):
        self._rec("subsystem_create", nqn=nqn)
        self.subsystems[nqn] = {"nqn": nqn, "namespaces": [], "listen_addresses": []}
        return True

    def subsystem_delete(self, nqn):
        self._rec("subsystem_delete", nqn=nqn)
        self.subsystems.pop(nqn, None)
        return True

    def nvmf_subsystem_add_ns(self, nqn, dev_name, uuid=None, nguid=None, nsid=None,
                              eui64=None, idempotent=True):
        self._rec("nvmf_subsystem_add_ns", nqn=nqn, dev_name=dev_name, nsid=nsid)
        self.subsystems[nqn]["namespaces"].append({"bdev_name": dev_name, "nsid": nsid})
        return True

    def listeners_create(self, nqn, trtype, traddr, trsvcid, ana_state=None):
        self._rec("listeners_create", nqn=nqn, traddr=traddr, trsvcid=trsvcid)
        self.subsystems[nqn]["listen_addresses"].append(
            {"trtype": trtype, "traddr": traddr, "trsvcid": str(trsvcid)})
        return True

    # -- lvstore / lvols
    def create_lvstore(self, name, bdev_name, cluster_sz, clear_method,
                       num_md_pages_per_cluster_ratio=1):
        self._rec("create_lvstore", name=name, bdev_name=bdev_name)
        self.lvstores[name] = bdev_name
        return True

    def create_lvol(self, name, size_in_mib, lvs_name, lvol_priority_class=0,
                    ndcs=0, npcs=0, uuid=None):
        self._rec("create_lvol", name=name, size_in_mib=size_in_mib, lvs_name=lvs_name)
        self.bdevs.add(f"{lvs_name}/{name}")
        return f"{lvs_name}/{name}"

    def delete_lvol(self, name, sync=False, special_delete=False):
        self._rec("delete_lvol", name=name)
        self.bdevs.discard(name)
        return True, None

    def bdev_lvol_resize(self, name, size_in_mib):
        self._rec("bdev_lvol_resize", name=name, size_in_mib=size_in_mib)
        return True

    def nvmf_subsystem_remove_ns(self, nqn, nsid):
        self._rec("nvmf_subsystem_remove_ns", nqn=nqn, nsid=nsid)
        subsystem = self.subsystems.get(nqn)
        if subsystem is None:
            raise RPCException("subsystem not found")
        subsystem["namespaces"] = [ns for ns in subsystem["namespaces"]
                                   if ns.get("nsid") != nsid]
        return True

    def bdev_raid_delete(self, name):
        self._rec("bdev_raid_delete", name=name)
        if name not in self.raids:
            raise RPCException("raid not found")
        self.raids.pop(name)
        self.bdevs.discard(name)
        return True

    # -- crypto
    def lvol_crypto_key_create(self, name, key, key2):
        self._rec("lvol_crypto_key_create", name=name)
        self.crypto_keys = getattr(self, "crypto_keys", set())
        if name in self.crypto_keys:
            raise RPCException("key already exists")
        self.crypto_keys.add(name)
        return True

    def lvol_crypto_create(self, name, base_name, key_name):
        self._rec("lvol_crypto_create", name=name, base_name=base_name,
                  key_name=key_name)
        self.bdevs.add(name)
        return name

    def lvol_crypto_delete(self, name):
        self._rec("lvol_crypto_delete", name=name)
        self.bdevs.discard(name)
        return True


class SpdkRegistry:
    """node mgmt_ip -> FakeSpdk; drop-in for simplyblock_edge.rpc.node_rpc_client."""

    def __init__(self):
        self.nodes = {}

    def for_ip(self, ip):
        return self.nodes.setdefault(ip, FakeSpdk())

    def __call__(self, node, timeout=None, retry=None):
        return self.for_ip(node.mgmt_ip)


class FakeEdgeK8s:
    """Drop-in for the simplyblock_edge.k8s entry points ops/monitor use."""

    def __init__(self):
        self.deployed = []
        self.deleted = []
        self.ready = {}       # hostname -> bool (default True)
        self.running = {}     # hostname -> bool (default True)
        self.unreachable = False

    def _check(self):
        if self.unreachable:
            from simplyblock_edge.k8s import EdgeK8sError
            raise EdgeK8sError("kube-apiserver unreachable")

    def deploy_spdk_pod(self, cluster, node, spdk_image, proxy_image):
        self._check()
        self.deployed.append(node.hostname)
        self.running[node.hostname] = True

    def delete_spdk_pod(self, cluster, node):
        self._check()
        self.deleted.append(node.hostname)
        self.running[node.hostname] = False

    def node_ready(self, cluster, node, timeout=None):
        self._check()
        return self.ready.get(node.hostname, True)

    def pod_running(self, cluster, node, timeout=None):
        self._check()
        return self.running.get(node.hostname, True)
