#!/usr/bin/env python

import argparse
import logging
import os
import sys
from typing import cast

from kubernetes.client import ApiException, CoreV1Api

from simplyblock_core import constants, utils
from simplyblock_core.storage_node_ops import (
    generate_automated_deployment_config,
    upgrade_automated_deployment_config,
)
from simplyblock_web import node_utils_k8s

logger = logging.getLogger(__name__)
logger.setLevel(constants.LOG_LEVEL)

POD_PREFIX: str = "snode-spdk-pod"


def _discard_previous_config() -> None:
    """Remove the node configuration this run is about to replace.

    Generation writes the whole document, so a run that succeeds replaces it
    anyway. A run that fails writes nothing, and what it leaves behind is the
    file some earlier deployment wrote: a different cluster, a different device
    mode, a different set of disks. Nothing downstream can tell that apart from
    a configuration this deployment produced — `node_add` reads it and refuses
    the node for whatever the old file says, naming the device class rather
    than the configure that never ran.

    So the file is discarded at the point the decision to regenerate has been
    made: after the pod-present check, which skips generation entirely and
    leaves a running pod's configuration alone, and before the generation whose
    failure is the case this exists for.

    TODO: this is host-wide, and so is the file. Two storage nodes of different
    clusters on one worker share /etc/simplyblock/sn_config_file, and today the
    second one's generation replaces the first one's document wholesale, with
    or without this. Whether that is supported at all is open; if it is, the
    path has to carry the cluster and this has to discard only its own.
    """
    for path in (constants.NODES_CONFIG_FILE, f"{constants.NODES_CONFIG_FILE}_read_only"):
        try:
            os.remove(path)
        except FileNotFoundError:
            continue  # A first install has none, which is the ordinary case.
        except OSError as e:
            logger.warning(f"The previous node configuration {path} could not be removed: {e}")
            continue
        logger.info(f"Discarded the previous node configuration {path}")


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
    node_name: str | None = os.environ.get("HOSTNAME")

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
        '--force',
        help='Force format detected or passed nvme pci address to 4K and clean partitions',
        action='store_true',
        dest='force',
        required=False
    )
    parser.add_argument(
        '--device-model',
        help='NVMe SSD model string, example: --model PM1628. Can be used alone to filter by model, or combined with --size-range to further filter by size.',
        type=str,
        default='',
        dest='device_model',
        required=False
    )
    parser.add_argument(
        '--size-range',
        help='NVMe SSD device size range separated by -, can be X(m,g,t) or bytes as integer, example: --size-range 50G-1T or --size-range 1232345-67823987. Can be used alone to filter by size, or combined with --device-model to further filter by model.',
        type=str,
        default='',
        dest='size_range',
        required=False
    )
    parser.add_argument(
        '--nvme-devices',
        help='Comma separated list of nvme namespace names like nvme0n1,nvme1n1...',
        type=str,
        default='',
        dest='nvme_names',
        required=False
    )
    parser.add_argument(
        '--lblk',
        help='Configure the node with Linux block devices (lblk cluster mode) instead of '
             'NVMe PCIe devices: eligible unmounted, unheld, unpartitioned whole disks are '
             'wrapped in SPDK AIO bdevs',
        action='store_true',
        dest='lblk',
        required=False
    )
    parser.add_argument(
        '--blk-names',
        help='Comma separated list of block device names to use, like sdb,sdc (requires --lblk)',
        type=str,
        default='',
        dest='blk_names',
        required=False
    )
    parser.add_argument(
        '--blk-names-exclude',
        help='Comma separated list of block device names to exclude, like sda (requires --lblk)',
        type=str,
        default='',
        dest='blk_names_exclude',
        required=False
    )
    parser.add_argument(
        '--blk-serials',
        help='Comma separated list of block device serial numbers (or WWNs) to use (requires --lblk)',
        type=str,
        default='',
        dest='blk_serials',
        required=False
    )
    parser.add_argument(
        '--jm-percent',
        help='Journal size in percent of the node\'s total selected capacity when the '
             'journal is carved by splitting a selected partition (requires --lblk with partitions)',
        type=int,
        default=3,
        dest='jm_percent',
        required=False
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
            args.max_prov = 0

        try:
            max_lvol = int(args.max_lvol)
            if max_lvol <= 0:
                raise ValueError("max-lvol must be a positive integer")
            if max_lvol > constants.MAX_SUBSYSTEMS_PER_NODE:
                raise ValueError(
                    f"max-lvol must not exceed {constants.MAX_SUBSYSTEMS_PER_NODE}, "
                    f"the maximum number of subsystems per storage node")
        except ValueError as e:
            raise argparse.ArgumentError(
                None,
                f"Invalid value for max-lvol '{args.max_lvol}': {e!s}"
            )

        if args.pci_allowed and args.pci_blocked:
            raise argparse.ArgumentError(
                None,
                "pci-allowed and pci-blocked cannot be both specified"
            )

        # getattr defaults: validate_arguments is also driven with minimal
        # namespaces (tests, callers predating the lblk selectors).
        lblk = getattr(args, 'lblk', False)
        blk_names = getattr(args, 'blk_names', '')
        blk_names_exclude = getattr(args, 'blk_names_exclude', '')
        blk_serials = getattr(args, 'blk_serials', '')
        use_lblk = bool(lblk or blk_names or blk_names_exclude or blk_serials)
        if use_lblk and not lblk:
            raise argparse.ArgumentError(
                None, "--blk-names/--blk-names-exclude/--blk-serials require --lblk")
        if use_lblk and (args.pci_allowed or args.pci_blocked
                         or getattr(args, 'device_model', '')
                         or getattr(args, 'size_range', '')
                         or getattr(args, 'nvme_names', '')):
            raise argparse.ArgumentError(
                None, "--lblk cannot be combined with NVMe device selection options")
        if sum([bool(blk_names), bool(blk_names_exclude), bool(blk_serials)]) > 1:
            raise argparse.ArgumentError(
                None, "Choose only one of --blk-names, --blk-names-exclude, --blk-serials")

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
            args.max_prov = 0
        validate_arguments(args)

        if _is_pod_present_for_node():
            logger.info("Skipped generating automated deployment configuration — pod already present.")
            sys.exit(0)

        # Process socket configuration
        sockets_to_use: list[int] = [0]
        if args.sockets_to_use:
            try:
                sockets_to_use = [int(x) for x in args.sockets_to_use.split(',')]
            except ValueError as e:
                raise argparse.ArgumentError(
                    None,
                    f"Invalid value for sockets-to-use '{args.sockets_to_use}': {e!s}"
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
                    f"Invalid value for nodes-per-socket '{args.nodes_per_socket}': {e!s}"
                )

        # Process PCI device filters
        pci_allowed: list[str] = []
        pci_blocked: list[str] = []
        nvme_names: list[str] = []

        if args.pci_allowed:
            pci_allowed = [pci.strip() for pci in args.pci_allowed.split(',') if pci.strip()]
        if args.pci_blocked:
            pci_blocked = [pci.strip() for pci in args.pci_blocked.split(',') if pci.strip()]
        if args.nvme_names:
            nvme_names = [nvme_name.strip() for nvme_name in args.nvme_names.split(',') if nvme_name.strip()]

        lblk_selection = None
        if args.lblk:
            lblk_selection = {
                "names": [x.strip() for x in args.blk_names.split(',') if x.strip()] or None,
                "names_exclude": [x.strip() for x in args.blk_names_exclude.split(',') if x.strip()] or None,
                "serials": [x.strip() for x in args.blk_serials.split(',') if x.strip()] or None,
            }

        _discard_previous_config()

        # Generate the deployment configuration.
        #
        # The result is checked because failure is how this reports "no device
        # matched", "the sockets did not validate" and "the memory does not add
        # up", and the caller is an init container: exiting 0 on any of them
        # tells Kubernetes the node was configured, and the pod then starts on
        # whatever /etc/simplyblock/sn_config_file the host already had. A
        # previous deployment's file is the case this was written for, and
        # nothing said so until the node_add that read it was refused.
        configured = generate_automated_deployment_config(
            max_lvol=int(args.max_lvol),
            max_prov=utils.parse_size(args.max_prov, assume_unit='G'),
            nodes_per_socket=nodes_per_socket,
            sockets_to_use=sockets_to_use,
            pci_allowed=pci_allowed,
            pci_blocked=pci_blocked,
            force=args.force,
            device_model=args.device_model,
            size_range=args.size_range,
            nvme_names=nvme_names,
            k8s=True,
            lblk_selection=lblk_selection,
            jm_percent=int(args.jm_percent or 3)
        )
        # The generation reports its own outcome and nothing else does: every
        # failure path -- no device matched the filters, the sockets did not
        # validate, the memory did not add up -- answers False and writes
        # nothing.
        if not configured:
            logger.error(
                "The node configuration could not be generated, so nothing was written; "
                "the node is not configured and any configuration already on this host is "
                "a previous deployment's")
            sys.exit(1)

    except argparse.ArgumentError as e:
        logger.error(f"Argument error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
