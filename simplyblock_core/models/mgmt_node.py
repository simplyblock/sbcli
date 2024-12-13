# coding=utf-8

from simplyblock_core.models.base_model import BaseNodeObject


class MgmtNode(BaseNodeObject):

    baseboard_sn: str = ""
    cluster_id: str = ""
    docker_ip_port: str = ""
    hostname: str = ""
    mgmt_ip: str = ""
