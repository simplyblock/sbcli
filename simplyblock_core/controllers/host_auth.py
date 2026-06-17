# coding=utf-8
from simplyblock_core.db_controller import DBController
from simplyblock_core import utils

logger = utils.get_logger(__name__)


def _reapply_allowed_hosts(lvol, snode, rpc_client):
    """Re-register allowed hosts (with DHCHAP keys) on a subsystem after recreation."""
    from simplyblock_core.controllers.lvol_controller import _register_dhchap_keys_on_node, _get_dhchap_group
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
