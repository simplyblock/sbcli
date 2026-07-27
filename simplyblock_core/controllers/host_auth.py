# coding=utf-8
from pydantic import SecretStr

from simplyblock_core import constants, utils
from simplyblock_core.db_controller import DBController
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.snode_client import SNodeClientException

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


def _register_key_on_node(snode, rpc_client, key_name, key_value):
    """Register a single key in SPDK's keyring on a storage node.

    Prefers the spdk-proxy's keyring_add_key interceptor, which delivers the
    key material inline and keeps it on a memory-backed volume. A proxy that
    predates interception support forwards keyring_add_key to SPDK, which
    rejects the unknown method (-32601); we then fall back to the deprecated
    SNodeAPI write_key_file + keyring_file_add_key path (key file on persistent
    host storage) so nodes not yet restarted after an upgrade keep working.

    "File exists" (code -17) means the key is already registered, which is
    fine (e.g. same host on another volume) and handled via allow_existing.

    Returns True if the key is registered (or already was), False on failure.
    """
    try:
        rpc_client.keyring_add_key(key_name, key_value, allow_existing=True)
        return True
    except RPCException as e:
        if e.code != -32601:  # anything but "Method not found" is a real failure
            logger.error("Failed to register key %s in SPDK keyring on node %s: %s",
                         key_name, snode.get_id(), e.message)
            return False
        logger.warning("Node %s uses a spdk-proxy without keyring_add_key support; "
                       "falling back to the deprecated on-disk key path — restart "
                       "the storage node to enable in-memory key delivery",
                       snode.get_id())

    try:
        key_path, error = snode.client().write_key_file(key_name, key_value)
    except SNodeClientException as e:
        logger.error("Failed to write key file %s on node %s: %s",
                     key_name, snode.get_id(), e.message)
        return False
    if error:
        logger.error("Failed to write key file %s on node %s: %s",
                     key_name, snode.get_id(), error)
        return False
    try:
        rpc_client.keyring_file_add_key(key_name, key_path, allow_existing=True)
        return True
    except RPCException as e:
        logger.error("Failed to register key %s in SPDK keyring on node %s: %s",
                     key_name, snode.get_id(), e.message)
        return False


def _register_pool_dhchap_keys_on_node(pool, snode, rpc_client):
    """Register pool-level DHCHAP keys on a storage node's SPDK keyring.

    All LVols in a DHCHAP pool share one key pair stored on the pool.
    Key names are pool-scoped so a single registration serves all LVols.

    Returns a dict with 'dhchap_key' and 'dhchap_ctrlr_key' keyring names,
    or an empty dict on failure.
    """
    safe_pool = pool.get_id().replace("-", "_")
    key_names = {}

    for key_type, key_value in (
        ("dhchap_key", pool.dhchap_key),
        ("dhchap_ctrlr_key", pool.dhchap_ctrlr_key),
    ):
        if not key_value or not key_value.get_secret_value():
            continue
        key_name = f"pool_{safe_pool}_{key_type}"
        if _register_key_on_node(snode, rpc_client, key_name, key_value):
            key_names[key_type] = key_name

    return key_names


def _register_dhchap_keys_on_node(snode, host_nqn, host_entry, rpc_client):
    """Register per-host DHCHAP/PSK keys on a storage node's SPDK keyring.

    Returns a dict mapping key type ('dhchap_key', 'dhchap_ctrlr_key', 'psk')
    to the SPDK keyring name for use in subsystem_add_host.
    """
    # Sanitize host NQN for use as key name
    safe_host = host_nqn.replace(":", "_").replace(".", "_")
    key_names = {}

    for key_type in ("dhchap_key", "dhchap_ctrlr_key", "psk"):
        key_value = host_entry.get(key_type)
        if not key_value:
            continue
        key_name = f"{key_type}_{safe_host}"
        if _register_key_on_node(snode, rpc_client, key_name, SecretStr(key_value)):
            key_names[key_type] = key_name

    return key_names


def _reapply_allowed_hosts(lvol, snode, rpc_client):
    """Re-register allowed hosts (with DHCHAP keys) on a subsystem after recreation."""
    db_ctrl = DBController()
    cluster = db_ctrl.get_cluster_by_id(snode.cluster_id)
    dhchap_group = _get_dhchap_group(cluster)
    for host_entry in lvol.allowed_hosts:
        logger.info("adding allowed host %s to subsystem %s", host_entry["nqn"], lvol.nqn)
        has_keys = any(host_entry.get(k) for k in ("dhchap_key", "dhchap_ctrlr_key", "psk"))
        if has_keys:
            key_names = _register_dhchap_keys_on_node(snode, host_entry["nqn"], host_entry, rpc_client)
            rpc_client.subsystem_add_host(
                lvol.nqn, host_entry["nqn"],
                psk=key_names.get("psk"),
                dhchap_key=key_names.get("dhchap_key"),
                dhchap_ctrlr_key=key_names.get("dhchap_ctrlr_key"),
                dhchap_group=dhchap_group,
            )
        else:
            rpc_client.subsystem_add_host(lvol.nqn, host_entry["nqn"])
