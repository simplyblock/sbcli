import os
import sys
import logging


from simplyblock_core.storage_node_ops import generate_automated_deployment_config, upgrade_automated_deployment_config
from simplyblock_core import constants, utils
from simplyblock_cli.clibase import range_type
from simplyblock_web import node_utils_k8s

import argparse

from kubernetes.client import ApiException


logger = logging.getLogger(__name__)
logger.setLevel(constants.LOG_LEVEL)

POD_PREFIX = "snode-spdk-pod"

def _is_pod_present_for_node():
    k8s_core_v1 = utils.get_k8s_core_client()
    namespace = node_utils_k8s.get_namespace()
    node_name = os.environ.get("HOSTNAME", "")

    if not node_name:
        raise RuntimeError(f"{node_name} env variable not set")

    try:
        resp = k8s_core_v1.list_namespaced_pod(namespace)
        for pod in resp.items:
            if (pod.metadata.name.startswith(POD_PREFIX) and
                pod.spec.node_name == node_name):
                return True
    except ApiException as e:
        raise RuntimeError(f"API error: {e}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error: {e}")
    return False


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Automated Deployment Configuration Script")
    parser.add_argument('--max-lvol', help='Max logical volume per storage node', type=str,
                                       dest='max_lvol', required=False)
    parser.add_argument('--max-size', help='Maximum amount of GB to be utilized on this storage node',
                                       type=str, dest='max_prov', required=False)
    parser.add_argument('--nodes-per-socket', help='number of each node to be added per each socket.',
                                       type=str, dest='nodes_per_socket', required=False)
    parser.add_argument('--sockets-to-use',
                                       help='The system socket to use when adding the storage nodes', type=str, dest='sockets_to_use', required=False)
    parser.add_argument('--pci-allowed',
                                       help='Comma separated list of PCI addresses of Nvme devices to use for storage devices.',
                                       type=str, default='', dest='pci_allowed', required=False)
    parser.add_argument('--pci-blocked',
                                       help='Comma separated list of PCI addresses of Nvme devices to not use for storage devices',
                                       type=str, default='', dest='pci_blocked', required=False)
    parser.add_argument('--upgrade',
                                       help='Upgrade', action='store_true', dest='upgrade', required=False)
    parser.add_argument('--cores-percentage', help='The percentage of cores to be used for spdk',
                                       type=range_type(0, 100), dest='cores_percentage', required=False, default=0)
    args = parser.parse_args()


    if args.upgrade:
        upgrade_automated_deployment_config()
    else:
        if _is_pod_present_for_node():
            logger.info("Skipped generating automated deployment configuration — pod already present.")
            sys.exit(0)

        if not args.max_lvol:
            parser.error('--max-lvol required.')
        if not args.max_prov:
            parser.error('--max-size required.')

        try:
            max_lvol = int(args.max_lvol)
        except ValueError:
            parser.error(
                f"Invalid value for max_lvol {args.max_lvol}. It must be number.")
        sockets_to_use = [0]
        if args.sockets_to_use:
            try:
                sockets_to_use = [int(x) for x in args.sockets_to_use.split(',')]
            except ValueError:
                parser.error(
                    f"Invalid value for sockets_to_use {args.sockets_to_use}. It must be a comma-separated list of integers.")

        nodes_per_socket = 1
        if args.nodes_per_socket:
            if int(args.nodes_per_socket) not in [1, 2]:
                parser.error(f"nodes_per_socket {args.nodes_per_socket}must be either 1 or 2")
        if args.pci_allowed and args.pci_blocked:
            parser.error("pci-allowed and pci-blocked cannot be both specified")
        max_prov = utils.parse_size(args.max_prov, assume_unit='G')
        if max_prov == -1:
            parser.error('--max-prov is not correct.')

        pci_allowed = []
        pci_blocked = []
        if args.pci_allowed:
            pci_allowed = [str(x) for x in args.pci_allowed.split(',')]
        if args.pci_blocked:
            pci_blocked = [str(x) for x in args.pci_blocked.split(',')]

        generate_automated_deployment_config(
            max_lvol=max_lvol,
            max_prov=max_prov,
            sockets_to_use=sockets_to_use,
            nodes_per_socket=nodes_per_socket,
            pci_allowed=pci_allowed,
            pci_blocked=pci_blocked,
            cores_percentage=args.cores_percentage
        )