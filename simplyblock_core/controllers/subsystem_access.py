"""Who may connect to a volume subsystem, as the database intends it, and how
one host is given that access on a node.

A subsystem's live access in SPDK is two independent fields, ``allow_any_host``
and the host list, and nothing keeps them consistent: ``add_host_to_lvol``
lists a host without switching ``allow_any_host`` off, so a volume created
before its pool had hosts is "any host" *and* lists hosts. Code that changes
access from the live state alone (the two-node host fence) then restores
something nobody authorized, or fails on a host that is already listed.

This module is the database's side of that question, for one cluster:

- ``intended_access`` derives, per subsystem NQN, whether any host may connect
  (no volume of the subsystem names allowed hosts, which is what volume
  creation decides) and which hosts are authorized, with their keys.
- ``grant`` lists one authorized host on one node with the keys the database
  gives it, and fails loudly when a key cannot be registered: a host added
  without its keys is a host admitted unauthenticated.

Keys a grant registers are named per subsystem, not per host: the key
material is generated per volume, so a host-scoped key name would be rewritten
by every volume's grant and the last one would win.
"""

import hashlib
from dataclasses import dataclass, field

from simplyblock_core import constants, utils
from simplyblock_core.controllers import host_auth

logger = utils.get_logger(__name__)

KEY_TYPES = ("dhchap_key", "dhchap_ctrlr_key", "psk")


class KeyRegistrationError(RuntimeError):
    """A key the database gives a host could not be registered on the node."""


@dataclass(frozen=True)
class HostGrant:
    """One authorized host of a subsystem, with what its access needs."""
    host_nqn: str
    lvol_id: str
    #: Per-host key material from the volume's allowed_hosts entry.
    keys: dict = field(default_factory=dict, hash=False, compare=False)
    #: The volume's pool uses pool-level DH-HMAC-CHAP (one shared key pair).
    pool_id: str = ""
    pool_dhchap: bool = False


@dataclass
class Intended:
    """The access the database intends for one subsystem."""
    nqn: str
    any_host: bool
    hosts: dict = field(default_factory=dict)   # host nqn -> HostGrant


def intended_access(db, cluster_id: str) -> dict:
    """{subsystem nqn: Intended} for every volume subsystem of the cluster,
    each derived by :func:`intended_for` from the volumes sharing it."""
    by_nqn: dict = {}
    for lv in db.get_lvols(cluster_id):
        if getattr(lv, "nqn", ""):
            by_nqn.setdefault(lv.nqn, []).append(lv)
    pools: dict = {}
    return {nqn: intended_for(db, nqn, lvols, pools) for nqn, lvols in by_nqn.items()}


def intended_for(db, nqn: str, lvols, pools: dict | None = None) -> Intended:
    """The access ``lvols`` intend for the subsystem ``nqn`` they share, or
    are about to share (a consistency-group move builds its target from this
    before the namespaces arrive). It allows any host only if none of them
    names allowed hosts, and a host listed by more than one takes the entry of
    the volume that sorts first, so every call gives the same answer.
    ``pools`` caches pool lookups across calls."""
    pools = {} if pools is None else pools
    it = Intended(nqn, True)
    for lv in sorted(lvols, key=lambda v: v.get_id()):
        if not lv.allowed_hosts:
            continue
        it.any_host = False
        pool = None
        if lv.pool_uuid:
            if lv.pool_uuid not in pools:
                try:
                    pools[lv.pool_uuid] = db.get_pool_by_id(lv.pool_uuid)
                except KeyError:
                    pools[lv.pool_uuid] = None
            pool = pools[lv.pool_uuid]
        for entry in lv.allowed_hosts:
            host_nqn = entry.get("nqn", "")
            if not host_nqn or host_nqn in it.hosts:
                continue
            it.hosts[host_nqn] = HostGrant(
                host_nqn=host_nqn, lvol_id=lv.get_id(),
                keys={k: entry[k] for k in KEY_TYPES if entry.get(k)},
                pool_id=lv.pool_uuid or "", pool_dhchap=bool(pool is not None and pool.dhchap))
    return it


def key_scope(subsystem_nqn: str) -> str:
    return hashlib.sha1(subsystem_nqn.encode()).hexdigest()[:10]


def grant(rpc, snode, subsystem_nqn: str, host: HostGrant, db) -> None:
    """List ``host`` on ``subsystem_nqn`` on ``snode`` with the keys the
    database gives it. Raises KeyRegistrationError when a required key could
    not be registered, and whatever the RPC raises; adds nothing then."""
    if host.pool_dhchap:
        pool = db.get_pool_by_id(host.pool_id)
        names = host_auth._register_pool_dhchap_keys_on_node(pool, snode, rpc)
        missing = [k for k, v in (("dhchap_key", pool.dhchap_key),
                                  ("dhchap_ctrlr_key", pool.dhchap_ctrlr_key))
                   if v and not names.get(k)]
        if missing:
            raise KeyRegistrationError("pool keys %s of %s not registered on %s"
                                       % (missing, host.pool_id, snode.get_id()))
        rpc.subsystem_add_host(subsystem_nqn, host.host_nqn,
                               dhchap_key=names.get("dhchap_key"),
                               dhchap_ctrlr_key=names.get("dhchap_ctrlr_key"),
                               dhchap_group=constants.DHCHAP_DHGROUP)
        return
    if host.keys:
        names = host_auth._register_dhchap_keys_on_node(snode, host.host_nqn, host.keys, rpc,
                                                        scope=key_scope(subsystem_nqn))
        missing = [k for k in host.keys if not names.get(k)]
        if missing:
            raise KeyRegistrationError("keys %s of host %s not registered on %s"
                                       % (missing, host.host_nqn, snode.get_id()))
        cluster = db.get_cluster_by_id(snode.cluster_id)
        rpc.subsystem_add_host(subsystem_nqn, host.host_nqn, psk=names.get("psk"),
                               dhchap_key=names.get("dhchap_key"),
                               dhchap_ctrlr_key=names.get("dhchap_ctrlr_key"),
                               dhchap_group=host_auth._get_dhchap_group(cluster))
        return
    rpc.subsystem_add_host(subsystem_nqn, host.host_nqn)
