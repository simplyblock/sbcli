# coding=utf-8
from simplyblock_core import constants, utils
from simplyblock_core.db_controller import DBController
from simplyblock_core.rpc_client import RPCException

logger = utils.get_logger(__name__)


def _get_dhchap_group(cluster, pool=None):
    """Return the DH group to set on the target subsystem for DH-HMAC-CHAP.

    For pool-level DHCHAP the fixed DHCHAP_DHGROUP constant is used.
    Falls back to cluster.tls_config for legacy cluster-level config,
    otherwise returns 'null' (HMAC-CHAP only, no DH key exchange).
    """
    if pool and getattr(pool, 'dhchap', False):
        return constants.DHCHAP_DHGROUP
    if cluster and cluster.tls and cluster.tls_config:
        params = cluster.tls_config.get("params", cluster.tls_config)
        groups = params.get("dhchap_dhgroups") or []
        if groups:
            return groups[0]
    return "null"


def _register_pool_dhchap_keys_on_node(pool, snode, rpc_client):
    """Write pool-level DHCHAP key files to a storage node and register in SPDK keyring.

    All LVols in a DHCHAP pool share one key pair stored on the pool.
    Key names are pool-scoped so a single registration serves all LVols.

    Returns a dict with 'dhchap_key' and 'dhchap_ctrlr_key' keyring names,
    or an empty dict on failure.
    """
    snode_api = snode.client()
    safe_pool = pool.get_id().replace("-", "_")
    key_names = {}

    for key_type, key_value in (
        ("dhchap_key", pool.dhchap_key.get_secret_value()),
        ("dhchap_ctrlr_key", pool.dhchap_ctrlr_key.get_secret_value()),
    ):
        if not key_value:
            continue
        key_name = f"pool_{safe_pool}_{key_type}"
        result, error = snode_api.write_key_file(key_name, key_value)
        if error:
            logger.error("Failed to write pool key %s on node %s: %s",
                         key_name, snode.get_id(), error)
            continue
        key_path = result
        try:
            rpc_client.keyring_file_add_key(key_name, key_path, allow_existing=True)
        except RPCException as e:
            logger.error("Failed to register pool key %s in SPDK keyring on node %s: %s",
                         key_name, snode.get_id(), e)
            continue
        key_names[key_type] = key_name

    return key_names


def _register_dhchap_keys_on_node(snode, host_nqn, host_entry, rpc_client):
    """Write DHCHAP key files to a storage node and register them in SPDK's keyring.

    Returns a dict mapping key type ('dhchap_key', 'dhchap_ctrlr_key', 'psk')
    to the SPDK keyring name for use in subsystem_add_host.
    """
    snode_api = snode.client()
    # Sanitize host NQN for use as filename
    safe_host = host_nqn.replace(":", "_").replace(".", "_")
    key_names = {}

    for key_type in ("dhchap_key", "dhchap_ctrlr_key", "psk"):
        key_value = host_entry.get(key_type)
        if not key_value:
            continue
        key_name = f"{key_type}_{safe_host}"
        result, error = snode_api.write_key_file(key_name, key_value)
        if error:
            logger.error("Failed to write key file %s on node %s: %s", key_name, snode.get_id(), error)
            continue
        key_path = result
        try:
            rpc_client.keyring_file_add_key(key_name, key_path, allow_existing=True)
        except RPCException as e:
            logger.error("Failed to register key %s in SPDK keyring on node %s: %s",
                         key_name, snode.get_id(), e)
            continue
        key_names[key_type] = key_name

    return key_names


def add_host_to_subsystem(rpc_client, snode, nqn, host_entry, pool, dhchap_group,
                          pool_key_names=None):
    """Register *host_entry*'s keys on *snode* and add it to subsystem *nqn*.

    A DHCHAP pool shares one key pair held on the pool; other volumes carry any
    keys per-host on the allowed-hosts entry. ``pool_key_names`` lets a caller
    that already registered the pool keys reuse them across hosts on the node.
    """
    if pool and pool.dhchap:
        if pool_key_names is None:
            pool_key_names = _register_pool_dhchap_keys_on_node(pool, snode, rpc_client)
        return rpc_client.subsystem_add_host(
            nqn, host_entry["nqn"],
            dhchap_key=pool_key_names.get("dhchap_key"),
            dhchap_ctrlr_key=pool_key_names.get("dhchap_ctrlr_key"),
            dhchap_group=dhchap_group,
        )
    if any(host_entry.get(k) for k in ("dhchap_key", "dhchap_ctrlr_key", "psk")):
        key_names = _register_dhchap_keys_on_node(snode, host_entry["nqn"], host_entry, rpc_client)
        return rpc_client.subsystem_add_host(
            nqn, host_entry["nqn"],
            psk=key_names.get("psk"),
            dhchap_key=key_names.get("dhchap_key"),
            dhchap_ctrlr_key=key_names.get("dhchap_ctrlr_key"),
            dhchap_group=dhchap_group,
        )
    return rpc_client.subsystem_add_host(nqn, host_entry["nqn"])


def apply_allowed_hosts_on_node(lvol, snode, *, timeout=None, retry=None):
    """Register keys and add every allowed host of *lvol* to its subsystem on *snode*.

    Run after a subsystem is (re)created — on initial placement, restart/rejoin,
    and migration. ``timeout``/``retry`` tighten the derived RPC client for the
    failure-recovery callers that need bounded RPCs.
    """
    db_ctrl = DBController()
    cluster = db_ctrl.get_cluster_by_id(snode.cluster_id)
    pool = None
    if lvol.pool_uuid:
        try:
            pool = db_ctrl.get_pool_by_id(lvol.pool_uuid)
        except KeyError:
            pass
    rpc_kwargs = {}
    if timeout is not None:
        rpc_kwargs["timeout"] = timeout
    if retry is not None:
        rpc_kwargs["retry"] = retry
    rpc_client = snode.rpc_client(**rpc_kwargs)
    dhchap_group = _get_dhchap_group(cluster, pool)
    pool_key_names = (_register_pool_dhchap_keys_on_node(pool, snode, rpc_client)
                      if pool and pool.dhchap else {})
    for host_entry in lvol.allowed_hosts:
        logger.info("adding allowed host %s to subsystem %s", host_entry["nqn"], lvol.nqn)
        add_host_to_subsystem(rpc_client, snode, lvol.nqn, host_entry, pool, dhchap_group,
                              pool_key_names)
