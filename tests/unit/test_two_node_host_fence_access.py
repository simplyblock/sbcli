"""The host fence against the database's host access, unfaked.

Review on #1527 (mxsrc): every fence test faked the re-add, so the release's
key handling had no coverage, and the fence trusted a subsystem's live state,
which can allow any host and list hosts at once. These tests run the real
``default_host_fence_revoke`` / ``default_host_fence_release`` through
``controllers.subsystem_access`` and ``host_auth``'s key registration, against
a fake SPDK that keeps SPDK's rules: adding a listed host is EINVAL, removing an
unlisted one is an error, and a remove disconnects the host.
"""

import errno

import pytest

from simplyblock_core.arbitration import arbiter as arb
from simplyblock_core.arbitration import host_fence as hf
from simplyblock_core.controllers import subsystem_access as sa
from simplyblock_core.rpc_client import RPCRemoteError

CL = "cl-1"
CLUSTER_NQN = "nqn.2023-02.io.simplyblock:cl-1"
VOL1 = CLUSTER_NQN + ":lvol:v1"
VOL2 = CLUSTER_NQN + ":lvol:v2"
HUB = CLUSTER_NQN + ":hublvol:LVS_1"
WIN = "nqn.2014-08.org.nvmexpress:uuid:winner"
LOSE = "nqn.2014-08.org.nvmexpress:uuid:loser"
LOSE_UID = "nqn.2014-08.io.simplyblock:uuid:loser-uid"
EXT = "nqn.2014-08.org.nvmexpress:uuid:external"


class SPDK:
    """nvmf subsystems and keyring of one node, with SPDK's answers."""

    def __init__(self, subs, connected=None):
        self.subs = {n: {"any": a, "hosts": {x: {} for x in h}} for n, (a, h) in subs.items()}
        self.connected = {n: set(v) for n, v in (connected or {}).items()}
        self.keys: dict = {}
        self.log: list = []

    def subsystem_list(self):
        return [{"nqn": n, "allow_any_host": s["any"], "hosts": [{"nqn": h} for h in s["hosts"]]}
                for n, s in self.subs.items()]

    def nvmf_subsystem_get_controllers(self, nqn):
        return [{"hostnqn": h} for h in sorted(self.connected.get(nqn, ()))]

    def subsystem_add_host(self, nqn, host, **kw):
        if host in self.subs[nqn]["hosts"]:
            raise RPCRemoteError("Invalid parameters", -errno.EINVAL)
        self.subs[nqn]["hosts"][host] = {k: v for k, v in kw.items() if v}
        self.log.append(("add", nqn, host))
        return True

    def subsystem_remove_host(self, nqn, host):
        if host not in self.subs[nqn]["hosts"]:
            raise RPCRemoteError("Internal error", -errno.ENOENT)
        del self.subs[nqn]["hosts"][host]
        self.connected.get(nqn, set()).discard(host)
        self.log.append(("remove", nqn, host))
        return True

    def subsystem_allow_any_host(self, nqn, allow):
        self.subs[nqn]["any"] = allow
        return True

    def keyring_file_add_key(self, name, path, allow_existing=False):
        self.keys[name] = path


class NodeAPI:
    def __init__(self, fail=False):
        self.fail, self.written = fail, {}

    def write_key_file(self, name, value):
        if self.fail:
            return None, "disk full"
        self.written[name] = value
        return "/etc/simplyblock/keys/" + name, None


class Node:
    def __init__(self, spdk, hosts, api=None):
        self.uuid, self.cluster_id, self.client_host_nqns = "winner", CL, hosts
        self.spdk, self.api = spdk, api or NodeAPI()

    def get_id(self):
        return self.uuid

    def rpc_client(self, **_):
        return self.spdk

    def client(self):
        return self.api


class LVol:
    def __init__(self, uid, nqn, hosts=(), pool=""):
        self.uuid, self.nqn, self.allowed_hosts, self.pool_uuid = uid, nqn, list(hosts), pool

    def get_id(self):
        return self.uuid


class Pool:
    def __init__(self, uid, dhchap=False):
        self.uuid, self.dhchap = uid, dhchap
        self.dhchap_key = "DHHC-1:00:pool-key:" if dhchap else ""
        self.dhchap_ctrlr_key = "DHHC-1:00:pool-ctrl:" if dhchap else ""

    def get_id(self):
        return self.uuid


class ClusterRec:
    nqn = CLUSTER_NQN
    tls = False
    tls_config = None


class DB:
    def __init__(self, lvols, pools=()):
        self.lvols, self.pools = lvols, {p.uuid: p for p in pools}

    def get_lvols(self, _cid):
        return list(self.lvols)

    def get_pool_by_id(self, pid):
        if pid not in self.pools:
            raise KeyError(pid)
        return self.pools[pid]

    def get_cluster_by_id(self, _cid):
        return ClusterRec()


@pytest.fixture(autouse=True)
def _cluster_lookup(monkeypatch):
    # host_auth's pool-key path reads the cluster through its own DBController.
    from simplyblock_core.controllers import host_auth
    monkeypatch.setattr(host_auth, "DBController", lambda: DB([]))


def keyed(host, key="DHHC-1:00:host-key:"):
    return {"nqn": host, "dhchap_key": key}


# ---- review point 1: a loser already listed on an allow-any subsystem --------

def test_listed_loser_on_an_allow_any_subsystem_is_revoked_not_stuck():
    """add_host_to_lvol lists a host without switching allow_any_host off. The
    old revoke re-added that host, SPDK refused (EINVAL), and the fence stayed
    in revoking forever: StorageFenced never set."""
    spdk = SPDK({VOL1: (True, [LOSE])}, connected={VOL1: {LOSE, WIN}})
    db = DB([LVol("v1", VOL1, [keyed(LOSE)])])
    changed = arb.default_host_fence_revoke(db, Node(spdk, [WIN]), [LOSE])
    assert changed == {VOL1: {"mode": hf.MODE_ANY, "added": [WIN], "removed": [LOSE]}}
    assert spdk.subs[VOL1]["any"] is False and LOSE not in spdk.subs[VOL1]["hosts"]
    assert LOSE not in spdk.connected[VOL1]


# ---- review point 2: the listed loser comes back with its keys ------------------

def test_release_gives_a_listed_loser_back_with_its_keys():
    spdk = SPDK({VOL1: (True, [LOSE])}, connected={VOL1: {LOSE}})
    db = DB([LVol("v1", VOL1, [keyed(LOSE)])])
    winner = Node(spdk, [WIN])
    changed = arb.default_host_fence_revoke(db, winner, [LOSE])
    assert arb.default_host_fence_release(db, winner, changed) == []
    entry = spdk.subs[VOL1]["hosts"][LOSE]
    assert entry.get("dhchap_key"), "the loser came back without its DH-HMAC-CHAP key"
    assert spdk.subs[VOL1]["any"] is True


# ---- review point 3: removed from the pool while fenced: stays out -----------------

def test_a_host_no_longer_authorized_is_not_given_back():
    spdk = SPDK({VOL1: (False, [WIN, LOSE])})
    lvol = LVol("v1", VOL1, [keyed(WIN), keyed(LOSE)])
    db = DB([lvol])
    winner = Node(spdk, [WIN])
    changed = arb.default_host_fence_revoke(db, winner, [LOSE])
    lvol.allowed_hosts = [keyed(WIN)]                    # remove_host_from_pool meanwhile
    left_out = arb.default_host_fence_release(db, winner, changed)
    assert LOSE not in spdk.subs[VOL1]["hosts"], "re-granted, without auth, to a revoked host"
    assert left_out == ["%s %s" % (VOL1, LOSE)]


# ---- review point 4: a failed key registration fails the release loudly -----------

def test_a_key_that_cannot_be_registered_fails_the_release():
    spdk = SPDK({VOL1: (False, [WIN, LOSE])})
    db = DB([LVol("v1", VOL1, [keyed(WIN), keyed(LOSE)])])
    changed = arb.default_host_fence_revoke(db, Node(spdk, [WIN]), [LOSE])
    with pytest.raises(sa.KeyRegistrationError):
        arb.default_host_fence_release(db, Node(spdk, [WIN], NodeAPI(fail=True)), changed)
    assert LOSE not in spdk.subs[VOL1]["hosts"], "added without its keys"


def test_pool_level_dhchap_uses_the_pools_keys():
    spdk = SPDK({VOL1: (False, [WIN, LOSE])})
    db = DB([LVol("v1", VOL1, [{"nqn": WIN}, {"nqn": LOSE}], pool="p1")], [Pool("p1", dhchap=True)])
    winner = Node(spdk, [WIN])
    changed = arb.default_host_fence_revoke(db, winner, [LOSE])
    arb.default_host_fence_release(db, winner, changed)
    entry = spdk.subs[VOL1]["hosts"][LOSE]
    assert entry["dhchap_key"].startswith("pool_p1_") and entry["dhchap_group"]


# ---- review point 5: key names per subsystem, not per host ----------------------

def test_key_names_are_scoped_per_subsystem():
    """The key material is generated per volume; host-scoped names were
    rewritten by every volume and the last writer won."""
    spdk = SPDK({VOL1: (False, [WIN, LOSE]), VOL2: (False, [WIN, LOSE])})
    db = DB([LVol("v1", VOL1, [keyed(WIN), keyed(LOSE, "DHHC-1:00:k1:")]),
             LVol("v2", VOL2, [keyed(WIN), keyed(LOSE, "DHHC-1:00:k2:")])])
    winner = Node(spdk, [WIN])
    changed = arb.default_host_fence_revoke(db, winner, [LOSE])
    arb.default_host_fence_release(db, winner, changed)
    k1 = spdk.subs[VOL1]["hosts"][LOSE]["dhchap_key"]
    k2 = spdk.subs[VOL2]["hosts"][LOSE]["dhchap_key"]
    assert k1 != k2
    written = {winner.api.written[k].get_secret_value() for k in (k1, k2)}
    assert written == {"DHHC-1:00:k1:", "DHHC-1:00:k2:"}


# ---- review point 7: hosts the database authorizes keep access -----------------

def test_an_authorized_host_missing_on_the_node_keeps_access():
    """add_host_to_lvol skips offline nodes, so the winner may lack a host the
    database authorizes; converting the subsystem to an allow-list must not
    lock it out for the duration of the fence."""
    spdk = SPDK({VOL1: (True, [])}, connected={VOL1: {LOSE}})
    db = DB([LVol("v1", VOL1, [{"nqn": EXT}, {"nqn": LOSE}])])
    arb.default_host_fence_revoke(db, Node(spdk, [WIN]), [LOSE])
    assert {WIN, EXT} <= set(spdk.subs[VOL1]["hosts"]) and LOSE not in spdk.subs[VOL1]["hosts"]


def test_both_identities_and_never_the_hublvol():
    spdk = SPDK({VOL1: (True, []), HUB: (True, [])}, connected={VOL1: {LOSE, LOSE_UID}})
    db = DB([LVol("v1", VOL1)])
    arb.default_host_fence_revoke(db, Node(spdk, [WIN]), [LOSE, LOSE_UID])
    assert not {LOSE, LOSE_UID} & spdk.connected[VOL1]
    assert spdk.subs[HUB]["any"] is True and not spdk.subs[HUB]["hosts"]


def test_revoke_is_idempotent_on_the_real_path():
    spdk = SPDK({VOL1: (True, [LOSE])}, connected={VOL1: {LOSE}})
    db = DB([LVol("v1", VOL1, [keyed(LOSE)])])
    winner = Node(spdk, [WIN])
    first = arb.default_host_fence_revoke(db, winner, [LOSE])
    spdk.log.clear()
    assert arb.default_host_fence_revoke(db, winner, [LOSE]) == {} and spdk.log == []
    assert hf.merge(first, {}) == first


def test_intended_access_follows_volume_creation():
    db = DB([LVol("a", VOL1), LVol("b", VOL2, [keyed(LOSE)]), LVol("c", VOL2)])
    it = sa.intended_access(db, CL)
    assert it[VOL1].any_host is True and not it[VOL1].hosts
    assert it[VOL2].any_host is False and set(it[VOL2].hosts) == {LOSE}
    assert it[VOL2].hosts[LOSE].lvol_id == "b"
