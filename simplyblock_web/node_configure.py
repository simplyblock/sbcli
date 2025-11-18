#!/usr/bin/env python
# encoding: utf-8

import argparse
import logging
import os
import sys
from typing import List, Optional, cast

from kubernetes.client import ApiException, CoreV1Api

from simplyblock_core import constants, utils
from simplyblock_core.storage_node_ops import (
    generate_automated_deployment_config,
    upgrade_automated_deployment_config,
)
from simplyblock_cli.clibase import range_type
from simplyblock_web import node_utils_k8s
import os
import subprocess


logger = logging.getLogger(__name__)
logger.setLevel(constants.LOG_LEVEL)

POD_PREFIX: str = "snode-spdk-pod"

def _is_pod_present_for_node() -> bool:
    """
    Check if a pod with the specified prefix is already running on the current node.
    
    Returns:
        bool: True if a matching pod is found, False otherwise
        
    Raises:
        RuntimeError: If there's an error communicating with the Kubernetes API
    """
    k8s_core_v1: CoreV1Api = cast(CoreV1Api, utils.get_k8s_core_client())
    namespace: str = node_utils_k8s.get_namespace()
    node_name: Optional[str] = os.environ.get("HOSTNAME")

    if not node_name:
        raise RuntimeError("HOSTNAME environment variable not set")

    try:
        resp = k8s_core_v1.list_namespaced_pod(namespace)
        for pod in resp.items:
            if (
                pod.metadata and 
                pod.metadata.name and 
                pod.spec and 
                pod.spec.node_name == node_name and
                pod.metadata.name.startswith(POD_PREFIX)
            ):
                return True
    except ApiException as e:
        raise RuntimeError(f"Kubernetes API error: {e}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error while checking for existing pods: {e}")
    return False


def parse_arguments() -> argparse.Namespace:
    """
    Parse and validate command line arguments.
    
    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description="Automated Deployment Configuration Script")
    
    # Define command line arguments
    parser.add_argument(
        '--max-lvol',
        help='Max logical volume per storage node',
        type=str,
        dest='max_lvol',
        required=False
    )
    parser.add_argument(
        '--max-size',
        help='Maximum amount of GB to be utilized on this storage node',
        type=str,
        dest='max_prov',
        required=False
    )
    parser.add_argument(
        '--nodes-per-socket',
        help='Number of each node to be added per each socket',
        type=str,
        dest='nodes_per_socket',
        required=False
    )
    parser.add_argument(
        '--sockets-to-use',
        help='The system socket to use when adding the storage nodes',
        type=str,
        dest='sockets_to_use',
        required=False
    )
    parser.add_argument(
        '--pci-allowed',
        help='Comma separated list of PCI addresses of Nvme devices to use for storage devices',
        type=str,
        default='',
        dest='pci_allowed',
        required=False
    )
    parser.add_argument(
        '--pci-blocked',
        help='Comma separated list of PCI addresses of Nvme devices to not use for storage devices',
        type=str,
        default='',
        dest='pci_blocked',
        required=False
    )
    parser.add_argument(
        '--upgrade',
        help='Upgrade the deployment configuration',
        action='store_true',
        dest='upgrade',
        required=False
    )
    parser.add_argument(
        '--cores-percentage',
        help='The percentage of cores to be used for spdk (0-100)',
        type=range_type(0, 100),
        dest='cores_percentage',
        required=False,
        default=0
    )
    
    return parser.parse_args()

def validate_arguments(args: argparse.Namespace) -> None:
    """
    Validate the provided command line arguments.
    
    Args:
        args: Parsed command line arguments
        
    Raises:
        argparse.ArgumentError: If any argument validation fails
    """
    if not args.upgrade:
        if not args.max_lvol:
            raise argparse.ArgumentError(None, '--max-lvol is required')
        if not args.max_prov:
            args.max_prov=0

        try:
            max_lvol = int(args.max_lvol)
            if max_lvol <= 0:
                raise ValueError("max-lvol must be a positive integer")
        except ValueError as e:
            raise argparse.ArgumentError(
                None,
                f"Invalid value for max-lvol '{args.max_lvol}': {str(e)}"
            )
            
        if args.pci_allowed and args.pci_blocked:
            raise argparse.ArgumentError(
                None,
                "pci-allowed and pci-blocked cannot be both specified"
            )
            
        max_prov = utils.parse_size(args.max_prov, assume_unit='G')
        if max_prov < 0:
            raise argparse.ArgumentError(
                None,
                f"Invalid storage size: {args.max_prov}. Must be a positive value with optional unit (e.g., 100G, 1T)"
            )


def main() -> None:
    """Main entry point for the node configuration script."""
    try:
        args = parse_arguments()
        
        if args.upgrade:
            upgrade_automated_deployment_config()
            return

        if not args.max_prov:
            args.max_prov=0
        validate_arguments(args)
        
        if _is_pod_present_for_node():
            logger.info("Skipped generating automated deployment configuration — pod already present.")
            sys.exit(0)
            
        # Process socket configuration
        sockets_to_use: List[int] = [0]
        if args.sockets_to_use:
            try:
                sockets_to_use = [int(x) for x in args.sockets_to_use.split(',')]
            except ValueError as e:
                raise argparse.ArgumentError(
                    None,
                    f"Invalid value for sockets-to-use '{args.sockets_to_use}': {str(e)}"
                )
        
        nodes_per_socket: int = 1
        if args.nodes_per_socket:
            try:
                nodes_per_socket = int(args.nodes_per_socket)
                if nodes_per_socket not in [1, 2]:
                    raise ValueError("must be either 1 or 2")
            except ValueError as e:
                raise argparse.ArgumentError(
                    None,
                    f"Invalid value for nodes-per-socket '{args.nodes_per_socket}': {str(e)}"
                )
        
        # Process PCI device filters
        pci_allowed: List[str] = []
        pci_blocked: List[str] = []
        
        if args.pci_allowed:
            pci_allowed = [pci.strip() for pci in args.pci_allowed.split(',') if pci.strip()]
        if args.pci_blocked:
            pci_blocked = [pci.strip() for pci in args.pci_blocked.split(',') if pci.strip()]
        
        # Generate the deployment configuration
        generate_automated_deployment_config(
            max_lvol=int(args.max_lvol),
            max_prov=utils.parse_size(args.max_prov, assume_unit='G'),
            nodes_per_socket=nodes_per_socket,
            sockets_to_use=sockets_to_use,
            pci_allowed=pci_allowed,
            pci_blocked=pci_blocked,
            cores_percentage=args.cores_percentage
        )

        logger.info(f"create RPC socket mount")
        mount_point = "/mnt/ramdisk"
        size = "1G"
        fstab_entry = f"tmpfs {mount_point} tmpfs size={size},mode=1777,noatime 0 0\n"

        # 1️⃣ Create the mount point if it doesn't exist
        os.makedirs(mount_point, exist_ok=True)

        # 2️⃣ Add to /etc/fstab if not already present
        with open("/etc/fstab", "r+") as fstab:
            lines = fstab.readlines()
            if not any(mount_point in line for line in lines):
                fstab.write(fstab_entry)
                print(f"Added fstab entry for {mount_point}")
            else:
                print(f"fstab entry for {mount_point} already exists")

        # 3️⃣ Mount the RAM disk immediately
        subprocess.run(["mount", mount_point], check=True)

        # 4️⃣ Verify
        subprocess.run(["df", "-h", mount_point])
        
    except argparse.ArgumentError as e:
        logger.error(f"Argument error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
