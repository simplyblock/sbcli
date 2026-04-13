import logging

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster

from ._base import KMS
from ._exceptions import KMSException
from ._hcp import HCPClient
from ._fdb import LocalKMS

logger = logging.getLogger()

logger.setLevel(logging.DEBUG)


def create_kms_connection(cluster: Cluster) -> KMS:
    if not cluster.deploy_kms:
        return LocalKMS(cluster)

    db_controller = DBController()
    if cluster.mode == "docker":
        mnode = db_controller.get_mgmt_nodes(cluster.get_id())[0]
        if not mnode:
            raise KMSException("Cluster has no mgmt nodes")
        address = f"{mnode.mgmt_ip}:8200"
    else:
        address = "simplyblock-kms:8200"

    return HCPClient(address, cluster.kms_root_token, cluster.get_id())
