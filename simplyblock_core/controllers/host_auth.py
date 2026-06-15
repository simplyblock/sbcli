# coding=utf-8
from simplyblock_core import constants, utils
from simplyblock_core.db_controller import DBController

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
        ret, err = rpc_client._request2("keyring_file_add_key",
                                        {"name": key_name, "path": key_path})
        if not ret and err:
            if err.get("code") == -17:
                logger.info("Pool key %s already in SPDK keyring on node %s, reusing",
                            key_name, snode.get_id())
            else:
                logger.error("Failed to register pool key %s in SPDK keyring on node %s: %s",
                             key_name, snode.get_id(), err.get("message", err))
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
        # Register in SPDK keyring — "File exists" (code -17) means the key
        # is already registered, which is fine (e.g. same host on another volume).
        ret, err = rpc_client._request2("keyring_file_add_key",
                                        {"name": key_name, "path": key_path})
        if not ret and err:
            if err.get("code") == -17:
                logger.info("Key %s already in SPDK keyring on node %s, reusing",
                            key_name, snode.get_id())
            else:
                logger.error("Failed to register key %s in SPDK keyring on node %s: %s",
                             key_name, snode.get_id(), err.get("message", err))
                continue
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
