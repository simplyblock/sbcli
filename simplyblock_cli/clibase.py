#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

import argparse
import json
import math
import re
import sys
import time
import argcomplete

from simplyblock_core import cluster_ops, utils, db_controller
from simplyblock_core import storage_node_ops as storage_ops
from simplyblock_core import mgmt_node_ops as mgmt_ops
from simplyblock_core import constants
from simplyblock_core.controllers import pool_controller, lvol_controller, snapshot_controller, device_controller, \
    tasks_controller
from simplyblock_core.controllers import caching_node_controller, health_controller
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.cluster import Cluster


def range_type(min, max):
    def f(arg):
        arg = int(arg)

        if not (min <= arg < max):
            raise argparse.ArgumentTypeError(f"Value '{arg}' must be in the interval [{min} {max})")

        return arg

    return f


def size_type(min=None, max=None):
    def f(arg):
        size = utils.parse_size(arg)

        if size == -1:
            raise argparse.ArgumentTypeError(f"Invalid size '{arg}' passed")
        elif min is not None and size < min:
            raise argparse.ArgumentTypeError(f"Size must be larger than {utils.humanbytes(min)}")
        elif max is not None and size > max:
            raise argparse.ArgumentTypeError(f"Size must be smaller than {utils.humanbytes(max)}")

        return size

    return f


def regex_type(regex):
    def f(arg):
        if (match := re.match(regex, arg)) is not None:
            return match
        else:
            raise argparse.ArgumentTypeError(f"Argument '{arg}' invalid: does not match regex ({regex})")

    return f


class CLIWrapperBase:

    def __init__(self):
        self.parser.add_argument("--cmd", help='cmd', nargs='+')
        argcomplete.autocomplete(self.parser)

    def init_parser(self):
        self.parser = argparse.ArgumentParser(description='Simplyblock management CLI')
        self.parser.add_argument("-d", '--debug', help='Print debug messages', required=False, action='store_true')
        self.parser.add_argument('--dev', help='Enable developer options', required=False, action='store_true')
        self.subparser = self.parser.add_subparsers(dest='command')

    def add_command(self, command, help, aliases=None):
        aliases = aliases or []
        storagenode = self.subparser.add_parser(command, description=help, help=help, aliases=aliases)
        storagenode_subparser = storagenode.add_subparsers(dest=command)
        return storagenode_subparser

    def add_sub_command(self, parent_parser, command, help, usage=None):
        return parent_parser.add_parser(command, description=help, help=help, usage=usage)

    def storage_node__deploy(self, sub_command, args):
        isolate_cores = args.isolate_cores
        return storage_ops.deploy(args.ifname, isolate_cores)

    def storage_node__configure_upgrade(self, sub_command, args):
        storage_ops.upgrade_automated_deployment_config()

    def storage_node__configure(self, sub_command, args):
        if not args.max_lvol:
            self.parser.error(f"Mandatory argument '--max-lvol' not provided for {sub_command}")
        if not args.max_prov:
            self.parser.error(f"Mandatory argument '--max-size' not provided for {sub_command}")
        sockets_to_use = [0]
        if args.sockets_to_use:
            try:
                sockets_to_use = [int(x) for x in args.sockets_to_use.split(',')]
            except ValueError:
                self.parser.error(
                        f"Invalid value for sockets_to_use {args.sockets_to_use}. It must be a comma-separated list of integers.")

        if args.nodes_per_socket not in [1, 2]:
            self.parser.error(f"nodes_per_socket {args.nodes_per_socket}must be either 1 or 2")
        if args.pci_allowed and args.pci_blocked:
            self.parser.error("pci-allowed and pci-blocked cannot be both specified")
        max_prov = utils.parse_size(args.max_prov, assume_unit='G')
        pci_allowed = []
        pci_blocked = []
        if args.pci_allowed:
            pci_allowed = [str(x) for x in args.pci_allowed.split(',')]
        if args.pci_blocked:
            pci_blocked = [str(x) for x in args.pci_blocked.split(',')]

        return storage_ops.generate_automated_deployment_config(args.max_lvol, max_prov, sockets_to_use,
                                                                args.nodes_per_socket, pci_allowed, pci_blocked)

    def storage_node__deploy_cleaner(self, sub_command, args):
        storage_ops.deploy_cleaner()
        return True  # remove once CLI changed to exceptions

    def storage_node__add_node(self, sub_command, args):
        cluster_id = args.cluster_id
        node_addr = args.node_addr
        ifname = args.ifname
        data_nics = args.data_nics
        spdk_image = args.spdk_image
        spdk_debug = args.spdk_debug

        small_bufsize = args.small_bufsize
        large_bufsize = args.large_bufsize
        num_partitions_per_dev = args.partitions
        jm_percent = args.jm_percent

        max_snap = args.max_snap
        enable_test_device = args.enable_test_device
        enable_ha_jm = args.enable_ha_jm
        namespace = args.namespace
        ha_jm_count = args.ha_jm_count

        out = storage_ops.add_node(
            cluster_id=cluster_id,
            node_addr=node_addr,
            iface_name=ifname,
            data_nics_list=data_nics,
            max_snap=max_snap,
            spdk_image=spdk_image,
            spdk_debug=spdk_debug,
            small_bufsize=small_bufsize,
            large_bufsize=large_bufsize,
            num_partitions_per_dev=num_partitions_per_dev,
            jm_percent=jm_percent,
            enable_test_device=enable_test_device,
            namespace=namespace,
            enable_ha_jm=enable_ha_jm,
            id_device_by_nqn=args.id_device_by_nqn,
            partition_size=args.partition_size,
            ha_jm_count=ha_jm_count,
            full_page_unmap=args.full_page_unmap
        )

        return out

    def storage_node__delete(self, sub_command, args):
        return storage_ops.delete_storage_node(args.node_id, args.force_remove)

    def storage_node__remove(self, sub_command, args):
        return storage_ops.remove_storage_node(args.node_id, args.force_remove)

    def storage_node__list(self, sub_command, args):
        return storage_ops.list_storage_nodes(args.json, args.cluster_id)

    def storage_node__get(self, sub_command, args):
        return storage_ops.get(args.node_id)

    def storage_node__restart(self, sub_command, args):
        node_id = args.node_id

        spdk_image = args.spdk_image
        spdk_debug = args.spdk_debug
        reattach_volume = args.reattach_volume

        max_lvol = args.max_lvol
        max_snap = args.max_snap
        max_prov = utils.parse_size(args.max_prov)

        small_bufsize = args.small_bufsize
        large_bufsize = args.large_bufsize
        ssd_pcie = args.ssd_pcie

        return storage_ops.restart_storage_node(
            node_id, max_lvol, max_snap, max_prov,
            spdk_image, spdk_debug,
            small_bufsize, large_bufsize, node_ip=args.node_ip, reattach_volume=reattach_volume, force=args.force,
            new_ssd_pcie=ssd_pcie)

    def storage_node__shutdown(self, sub_command, args):
        return storage_ops.shutdown_storage_node(args.node_id, args.force)

    def storage_node__suspend(self, sub_command, args):
        return storage_ops.suspend_storage_node(args.node_id, args.force)

    def storage_node__resume(self, sub_command, args):
        return storage_ops.resume_storage_node(args.node_id)

    def storage_node__get_io_stats(self, sub_command, args):
        node_id = args.node_id
        history = args.history
        records = args.records
        data = storage_ops.get_node_iostats_history(node_id, history, records_count=records)

        if data:
            return utils.print_table(data)
        else:
            return False

    def storage_node__get_capacity(self, sub_command, args):
        node_id = args.node_id
        history = args.history
        data = storage_ops.get_node_capacity(node_id, history)
        if data:
            return utils.print_table(data)
        else:
            return False

    def storage_node__list_devices(self, sub_command, args):
        return self.storage_node_list_devices(args)

    def storage_node__device_testing_mode(self, sub_command, args):
        return device_controller.set_device_testing_mode(args.device_id, args.mode)

    def storage_node__get_device(self, sub_command, args):
        device_id = args.device_id
        return device_controller.get_device(device_id)

    def storage_node__reset_device(self, sub_command, args):
        return device_controller.reset_storage_device(args.device_id)

    def storage_node__restart_device(self, sub_command, args):
        return device_controller.restart_device(args.device_id)

    def storage_node__add_device(self, sub_command, args):
        return device_controller.add_device(args.device_id)

    def storage_node__remove_device(self, sub_command, args):
        return device_controller.device_remove(args.device_id, args.force)

    def storage_node__set_failed_device(self, sub_command, args):
        return device_controller.device_set_failed(args.device_id)

    def storage_node__get_capacity_device(self, sub_command, args):
        device_id = args.device_id
        history = args.history
        data = device_controller.get_device_capacity(device_id, history)
        if data:
            return utils.print_table(data)
        else:
            return False

    def storage_node__get_io_stats_device(self, sub_command, args):
        device_id = args.device_id
        history = args.history
        records = args.records
        data = device_controller.get_device_iostats(device_id, history, records_count=records)
        if data:
            return utils.print_table(data)
        else:
            return False

    def storage_node__port_list(self, sub_command, args):
        node_id = args.node_id
        return storage_ops.get_node_ports(node_id)

    def storage_node__port_io_stats(self, sub_command, args):
        port_id = args.port_id
        history = args.history
        return storage_ops.get_node_port_iostats(port_id, history)

    def storage_node__check(self, sub_command, args):
        node_id = args.node_id
        return health_controller.check_node(node_id)

    def storage_node__check_device(self, sub_command, args):
        device_id = args.device_id
        return health_controller.check_device(device_id)

    def storage_node__info(self, sub_command, args):
        node_id = args.node_id
        return storage_ops.get_info(node_id)

    def storage_node__info_spdk(self, sub_command, args):
        node_id = args.node_id
        return storage_ops.get_spdk_info(node_id)

    def storage_node__remove_jm_device(self, sub_command, args):
        return device_controller.remove_jm_device(args.jm_device_id, args.force)

    def storage_node__restart_jm_device(self, sub_command, args):
        return device_controller.restart_jm_device(args.jm_device_id, args.force)

    def storage_node__send_cluster_map(self, sub_command, args):
        node_id = args.node_id
        return storage_ops.send_cluster_map(node_id)

    def storage_node__get_cluster_map(self, sub_command, args):
        node_id = args.node_id
        return storage_ops.get_cluster_map(node_id)

    def storage_node__make_primary(self, sub_command, args):
        node_id = args.node_id
        return storage_ops.make_sec_new_primary(node_id)

    def storage_node__dump_lvstore(self, sub_command, args):
        node_id = args.node_id
        return storage_ops.dump_lvstore(node_id)

    def storage_node__set(self, sub_command, args):
        return storage_ops.set_value(args.node_id, args.attr_name, args.attr_value)

    def cluster__deploy(self, sub_command, args):
        return self.cluster_deploy(args)

    def cluster__create(self, sub_command, args):
        return self.cluster_create(args)

    def cluster__add(self, sub_command, args):
        return self.cluster_add(args)

    def cluster__activate(self, sub_command, args):
        cluster_ops.cluster_activate(args.cluster_id, args.force, args.force_lvstore_create)
        return True

    def cluster__list(self, sub_command, args):
        data = cluster_ops.list()

        if args.json:
            return json.dumps(data, indent=2)
        else:
            return utils.print_table(data)

    def cluster__status(self, sub_command, args):
        return utils.print_table(cluster_ops.get_cluster_status(args.cluster_id))

    def cluster__show(self, sub_command, args):
        return cluster_ops.list_all_info(args.cluster_id)

    def cluster__get(self, sub_command, args):
        return json.dumps(cluster_ops.get_cluster(args.cluster_id), indent=2, sort_keys=True)

    def cluster__get_capacity(self, sub_command, args):
        is_json = args.json
        data = cluster_ops.get_capacity(args.cluster_id, args.history)

        if is_json:
            return json.dumps(data, indent=2)
        else:
            return utils.print_table([
                {
                    "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
                    "Absolut": utils.humanbytes(record['size_total']),
                    "Provisioned": utils.humanbytes(record['size_prov']),
                    "Used": utils.humanbytes(record['size_used']),
                    "Free": utils.humanbytes(record['size_free']),
                    "Util %": f"{record['size_util']}%",
                    "Prov Util %": f"{record['size_prov_util']}%",
                }
                for record in data
            ])

    def cluster__get_io_stats(self, sub_command, args):
        return utils.print_table([
            {
                "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
                "Read speed": utils.humanbytes(record['read_bytes_ps']),
                "Read IOPS": record["read_io_ps"],
                "Read lat": record["read_latency_ps"],
                "Write speed": utils.humanbytes(record["write_bytes_ps"]),
                "Write IOPS": record["write_io_ps"],
                "Write lat": record["write_latency_ps"],
            }
            for record in cluster_ops.get_iostats_history(args.cluster_id, args.history, args.records)
        ])

    def cluster__get_logs(self, sub_command, args):
        cluster_logs = cluster_ops.get_logs(**args.__dict__)

        if args.is_json:
            return json.dumps(cluster_logs, indent=2)
        else:
            return utils.print_table(cluster_logs)

    def cluster__get_secret(self, sub_command, args):
        cluster_id = args.cluster_id
        return cluster_ops.get_secret(cluster_id)

    def cluster__update_secret(self, sub_command, args):
        cluster_ops.set_secret(args.cluster_id, args.secret)
        return True

    def cluster__check(self, sub_command, args):
        cluster_id = args.cluster_id
        return health_controller.check_cluster(cluster_id)

    def cluster__update(self, sub_command, args):
        cluster_ops.update_cluster(**args.__dict__)
        return True

    def cluster__graceful_shutdown(self, sub_command, args):
        cluster_ops.cluster_grace_shutdown(args.cluster_id)
        return True

    def cluster__graceful_startup(self, sub_command, args):
        cluster_ops.cluster_grace_startup(args.cluster_id, args.clear_data, args.spdk_image)
        return True

    def cluster__list_tasks(self, sub_command, args):
        return tasks_controller.list_tasks(**args.__dict__)

    def cluster__cancel_task(self, sub_command, args):
        return tasks_controller.cancel_task(args.task_id)

    def cluster__delete(self, sub_command, args):
        cluster_ops.delete_cluster(args.cluster_id)
        return True

    def cluster__suspend(self, sub_command, args):
        return cluster_ops.set_cluster_status(args.cluster_id, Cluster.STATUS_SUSPENDED)

    def cluster_unsuspend(self, sub_command, args):
        return cluster_ops.set_cluster_status(args.cluster_id, Cluster.STATUS_ACTIVE)

    def cluster_get_cli_ssh_pass(self, sub_command, args):
        cluster_id = args.cluster_id
        return cluster_ops.get_ssh_pass(cluster_id)

    def cluster__set(self, sub_command, args):
        cluster_ops.set(args.cluster_id, args.attr_name, args.attr_value)
        return True

    def cluster__complete_expand(self, sub_command, args):
        cluster_ops.cluster_expand(args.cluster_id)
        return True

    def volume__add(self, sub_command, args):
        name = args.name
        size = args.size
        max_size = args.max_size
        host_id = args.host_id
        ha_type = args.ha_type
        pool = args.pool
        comp = None
        crypto = args.encrypt
        distr_vuid = args.distr_vuid
        with_snapshot = args.snapshot
        lvol_priority_class = args.lvol_priority_class
        results, error = lvol_controller.add_lvol_ha(
            name, size, host_id, ha_type, pool, comp, crypto,
            distr_vuid,
            args.max_rw_iops,
            args.max_rw_mbytes,
            args.max_r_mbytes,
            args.max_w_mbytes,
            with_snapshot=with_snapshot,
            max_size=max_size,
            crypto_key1=args.crypto_key1,
            crypto_key2=args.crypto_key2,
            lvol_priority_class=lvol_priority_class,
            uid=args.uid, pvc_name=args.pvc_name, namespace=args.namespace)
        if results:
            return results
        else:
            return error

    def volume__qos_set(self, sub_command, args):
        return lvol_controller.set_lvol(
            args.volume_id, args.max_rw_iops, args.max_rw_mbytes,
            args.max_r_mbytes, args.max_w_mbytes)

    def volume__list(self, sub_command, args):
        return lvol_controller.list_lvols(args.json, args.cluster_id, args.pool, args.all)

    def volume__list_mem(self, sub_command, args):
        return lvol_controller.list_lvols_mem(args.json, args.csv)

    def volume__get(self, sub_command, args):
        return lvol_controller.get_lvol(args.volume_id, args.json)

    def volume__delete(self, sub_command, args):
        for id in args.volume_id:
            force = args.force
            return lvol_controller.delete_lvol(id, force)

    def volume__connect(self, sub_command, args):
        kwargs = {}
        if (ctrl_loss_tmo := args.ctrl_loss_tmo) is not None:
            kwargs['ctrl_loss_tmo'] = ctrl_loss_tmo

        data = lvol_controller.connect_lvol(args.volume_id, **kwargs)
        if data:
            return "\n".join(con['connect'] for con in data)

    def volume__resize(self, sub_command, args):
        volume_id = args.volume_id
        size = args.size
        ret, err = lvol_controller.resize_lvol(volume_id, size)
        return ret

    def volume__create_snapshot(self, sub_command, args):
        volume_id = args.volume_id
        name = args.name
        snapshot_id, error = lvol_controller.create_snapshot(volume_id, name)
        return snapshot_id if not error else error

    def volume__clone(self, sub_command, args):
        new_size = args.resize

        clone_id, error = snapshot_controller.clone(args.snapshot_id, args.clone_name, new_size)
        return clone_id if not error else error

    def volume__move(self, sub_command, args):
        return lvol_controller.move(args.volume_id, args.node_id, args.force)

    def volume__get_capacity(self, sub_command, args):
        volume_id = args.volume_id
        history = args.history
        ret = lvol_controller.get_capacity(volume_id, history)
        if ret:
            return utils.print_table(ret)
        else:
            return False

    def volume__get_io_stats(self, sub_command, args):
        volume_id = args.volume_id
        history = args.history
        records = args.records
        data = lvol_controller.get_io_stats(volume_id, history, records_count=records)
        if data:
            return utils.print_table(data)
        else:
            return False

    def volume__check(self, sub_command, args):
        volume_id = args.volume_id
        return health_controller.check_lvol(volume_id)

    def volume__inflate(self, sub_command, args):
        return lvol_controller.inflate_lvol(args.volume_id)

    def control_plane__add(self, sub_command, args):
        cluster_id = args.cluster_id
        cluster_ip = args.cluster_ip
        cluster_secret = args.cluster_secret
        ifname = args.ifname
        return mgmt_ops.deploy_mgmt_node(cluster_ip, cluster_id, ifname, cluster_secret)

    def control_plane__list(self, sub_command, args):
        return mgmt_ops.list_mgmt_nodes(args.json)

    def control_plane__remove(self, sub_command, args):
        return mgmt_ops.remove_mgmt_node(args.node_id)

    def storage_pool__add(self, sub_command, args):
        has_secret = args.has_secret
        if has_secret is None:
            has_secret = False
        return pool_controller.add_pool(
            args.name,
            args.pool_max,
            args.lvol_max,
            args.max_rw_iops,
            args.max_rw_mbytes,
            args.max_r_mbytes,
            args.max_w_mbytes,
            has_secret,
            args.cluster_id
        )

    def storage_pool__set(self, sub_command, args):
        pool_max = args.pool_max
        lvol_max = args.lvol_max

        ret, err = pool_controller.set_pool(
            args.pool_id,
            pool_max,
            lvol_max,
            args.max_rw_iops,
            args.max_rw_mbytes,
            args.max_r_mbytes,
            args.max_w_mbytes)
        return ret

    def storage_pool__list(self, sub_command, args):
        return pool_controller.list_pools(args.json, args.cluster_id)

    def storage_pool__get(self, sub_command, args):
        return pool_controller.get_pool(args.pool_id, args.json)

    def storage_pool__delete(self, sub_command, args):
        return pool_controller.delete_pool(args.pool_id)

    def storage_pool__enable(self, sub_command, args):
        return pool_controller.set_status(args.pool_id, Pool.STATUS_ACTIVE)

    def storage_pool__disable(self, sub_command, args):
        return pool_controller.set_status(args.pool_id, Pool.STATUS_INACTIVE)

    def storage_pool__get_secret(self, sub_command, args):
        return pool_controller.get_secret(args.pool_id)

    def storage_pool__update_secret(self, sub_command, args):
        return pool_controller.set_secret(args.pool_id, args.secret)

    def storage_pool__get_capacity(self, sub_command, args):
        return pool_controller.get_capacity(args.pool_id)

    def storage_pool__get_io_stats(self, sub_command, args):
        return pool_controller.get_io_stats(args.pool_id, args.history, args.records)

    def snapshot__add(self, sub_command, args):
        snapshot_id, error = snapshot_controller.add(args.volume_id, args.name)
        return snapshot_id if not error else error

    def snapshot__list(self, sub_command, args):
        return snapshot_controller.list(args.all)

    def snapshot__delete(self, sub_command, args):
        return snapshot_controller.delete(args.snapshot_id, args.force)

    def snapshot__clone(self, sub_command, args):
        new_size = args.resize

        success, details = snapshot_controller.clone(args.snapshot_id, args.lvol_name, new_size)
        return details

    def caching_node__deploy(self, sub_command, args):
        return caching_node_controller.deploy(args.ifname)

    def caching_node__add_node(self, sub_command, args):
        cluster_id = args.cluster_id
        node_ip = args.node_ip
        ifname = args.ifname
        data_nics = []
        spdk_image = args.spdk_image
        namespace = args.namespace
        multipathing = args.multipathing == "on"
        spdk_cpu_mask = args.spdk_cpu_mask
        spdk_mem = args.spdk_mem

        return caching_node_controller.add_node(
            cluster_id, node_ip, ifname, data_nics, spdk_cpu_mask, spdk_mem, spdk_image, namespace, multipathing)

    def caching_node__list(self, sub_command, args):
        return caching_node_controller.list_nodes()

    def caching_node__list_lvols(self, sub_command, args):
        return caching_node_controller.list_lvols(args.node_id)

    def caching_node__remove(self, sub_command, args):
        return caching_node_controller.remove_node(args.node_id, args.force)

    def caching_node__connect(self, sub_command, args):
        return caching_node_controller.connect(args.node_id, args.lvol_id)

    def caching_node__disconnect(self, sub_command, args):
        return caching_node_controller.disconnect(args.node_id, args.lvol_id)

    def caching_node__recreate(self, sub_command, args):
        return caching_node_controller.recreate(args.node_id)

    def caching_node__get_lvol_stats(self, sub_command, args):
        data = caching_node_controller.get_io_stats(args.lvol_id, args.history)
        if data:
            return utils.print_table(data)
        else:
            return False

    def storage_node_list_devices(self, args):
        node_id = args.node_id
        is_json = args.json
        out = storage_ops.list_storage_devices(node_id, is_json)
        return out

    def cluster_add(self, args):
        page_size_in_blocks = args.page_size
        blk_size = 4096
        cap_warn = args.cap_warn
        cap_crit = args.cap_crit
        prov_cap_warn = args.prov_cap_warn
        prov_cap_crit = args.prov_cap_crit
        distr_ndcs = args.distr_ndcs
        distr_npcs = args.distr_npcs
        distr_bs = args.distr_bs
        distr_chunk_bs = args.distr_chunk_bs
        ha_type = args.ha_type

        enable_node_affinity = args.enable_node_affinity
        qpair_count = args.qpair_count
        max_queue_size = args.max_queue_size
        inflight_io_threshold = args.inflight_io_threshold
        enable_qos = args.enable_qos
        strict_node_anti_affinity = args.strict_node_anti_affinity

        return cluster_ops.add_cluster(
            blk_size, page_size_in_blocks, cap_warn, cap_crit, prov_cap_warn, prov_cap_crit,
            distr_ndcs, distr_npcs, distr_bs, distr_chunk_bs, ha_type, enable_node_affinity,
            qpair_count, max_queue_size, inflight_io_threshold, enable_qos, strict_node_anti_affinity)

    def cluster_deploy(self, args):
        grafana_endpoint = ""
        secondary_nodes = False
        namespace = None
        lvol_name = "lvol01"
        lvol_size = utils.parse_size("10G")
        pool_max = utils.parse_size("25G")
        max_size = utils.parse_size("1000G")
        pool_name = "pool01"
        with_snapshot = False
        host_id = None
        crypto = False
        crypto_key1 = None
        crypto_key2 = None
        max_rw_iops = None
        max_rw_mbytes = None
        max_r_mbytes = None
        max_w_mbytes = None
        lvol_priority_class = 0
        id_device_by_nqn = False
        fstype = "xfs"

        storage_nodes = args.storage_nodes
        test = args.test
        ha_type = args.ha_type
        ha_jm_count = args.ha_jm_count
        distr_ndcs = args.distr_ndcs
        distr_npcs = args.distr_npcs
        enable_qos = args.enable_qos
        ifname = args.ifname
        page_size_in_blocks = args.page_size
        blk_size = args.blk_size
        CLI_PASS = args.CLI_PASS
        cap_warn = args.cap_warn
        cap_crit = args.cap_crit
        prov_cap_warn = args.prov_cap_warn
        prov_cap_crit = args.prov_cap_crit
        distr_bs = args.distr_bs
        distr_chunk_bs = args.distr_chunk_bs
        log_del_interval = args.log_del_interval
        metrics_retention_period = args.metrics_retention_period
        contact_point = args.contact_point
        enable_node_affinity = args.enable_node_affinity
        qpair_count = args.qpair_count
        max_queue_size = args.max_queue_size
        inflight_io_threshold = args.inflight_io_threshold
        strict_node_anti_affinity = args.strict_node_anti_affinity

        data_nics = args.data_nics
        spdk_image = args.spdk_image
        spdk_debug = args.spdk_debug

        small_bufsize = args.small_bufsize
        large_bufsize = args.large_bufsize
        num_partitions_per_dev = args.partitions
        partition_size = args.partition_size
        jm_percent = args.jm_percent
        spdk_cpu_mask = args.spdk_cpu_mask
        max_lvol = args.max_lvol
        max_snap = args.max_snap
        max_prov = utils.parse_size(args.max_prov, assume_unit='G')
        number_of_devices = args.number_of_devices
        enable_test_device = args.enable_test_device
        enable_ha_jm = args.enable_ha_jm
        number_of_distribs = args.number_of_distribs
        namespace = args.namespace
        secondary_nodes = args.secondary_nodes

        lvol_name = args.lvol_name
        lvol_size = args.lvol_size
        max_size = args.max_size
        lvol_ha_type = args.lvol_ha_type
        pool_name = args.pool_name
        pool_max = args.pool_max
        host_id = args.host_id
        comp = None
        distr_vuid = args.distr_vuid

        cluster_ops.deploy_cluster(
            storage_nodes, test, ha_type, distr_ndcs, distr_npcs, enable_qos, ifname,
            blk_size, page_size_in_blocks, CLI_PASS, cap_warn, cap_crit, prov_cap_warn,
            prov_cap_crit, log_del_interval, metrics_retention_period, contact_point, grafana_endpoint,
            distr_bs, distr_chunk_bs, enable_node_affinity,
            qpair_count, max_queue_size, inflight_io_threshold, strict_node_anti_affinity, data_nics,
            spdk_image, spdk_debug, small_bufsize, large_bufsize, num_partitions_per_dev, jm_percent, spdk_cpu_mask,
            max_lvol,
            max_snap, max_prov, number_of_devices, enable_test_device, enable_ha_jm, ha_jm_count, number_of_distribs,
            namespace, secondary_nodes, partition_size,
            lvol_name, lvol_size, lvol_ha_type, pool_name, pool_max, host_id, comp, crypto, distr_vuid, max_rw_iops,
            max_rw_mbytes, max_r_mbytes, max_w_mbytes,
            with_snapshot, max_size, crypto_key1, crypto_key2, lvol_priority_class, id_device_by_nqn, fstype)
        return True

    def cluster_create(self, args):
        page_size_in_blocks = args.page_size
        blk_size = 4096
        CLI_PASS = args.CLI_PASS
        cap_warn = args.cap_warn
        cap_crit = args.cap_crit
        prov_cap_warn = args.prov_cap_warn
        prov_cap_crit = args.prov_cap_crit
        ifname = args.ifname
        distr_ndcs = args.distr_ndcs
        distr_npcs = args.distr_npcs
        distr_bs = args.distr_bs
        distr_chunk_bs = args.distr_chunk_bs
        ha_type = args.ha_type
        log_del_interval = args.log_del_interval
        metrics_retention_period = args.metrics_retention_period
        contact_point = args.contact_point
        grafana_endpoint = args.grafana_endpoint
        enable_node_affinity = args.enable_node_affinity
        qpair_count = args.qpair_count
        max_queue_size = args.max_queue_size
        inflight_io_threshold = args.inflight_io_threshold
        enable_qos = args.enable_qos
        strict_node_anti_affinity = args.strict_node_anti_affinity

        return cluster_ops.create_cluster(
            blk_size, page_size_in_blocks,
            CLI_PASS, cap_warn, cap_crit, prov_cap_warn, prov_cap_crit,
            ifname, log_del_interval, metrics_retention_period, contact_point, grafana_endpoint,
            distr_ndcs, distr_npcs, distr_bs, distr_chunk_bs, ha_type, enable_node_affinity,
            qpair_count, max_queue_size, inflight_io_threshold, enable_qos, strict_node_anti_affinity)

    def query_yes_no(self, question, default="yes"):
        """Ask a yes/no question via raw_input() and return their answer.

        "question" is a string that is presented to the user.
        "default" is the presumed answer if the user just hits <Enter>.
                It must be "yes" (the default), "no" or None (meaning
                an answer is required of the user).

        The "answer" return value is True for "yes" or False for "no".
        """
        valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
        if default is None:
            prompt = " [y/n] "
        elif default == "yes":
            prompt = " [Y/n] "
        elif default == "no":
            prompt = " [y/N] "
        else:
            raise ValueError("invalid default answer: '%s'" % default)

        while True:
            sys.stdout.write(question + prompt)
            choice = str(input()).lower()
            if default is not None and choice == "":
                return valid[default]
            elif choice in valid:
                return valid[choice]
            else:
                sys.stdout.write("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")

    def _completer_get_cluster_list(self, prefix, parsed_args, **kwargs):
        db = db_controller.DBController()
        return (cluster.get_id() for cluster in db.get_clusters() if cluster.get_id().startswith(prefix))

    def _completer_get_sn_list(self, prefix, parsed_args, **kwargs):
        db = db_controller.DBController()
        return (cluster.get_id() for cluster in db.get_storage_nodes() if cluster.get_id().startswith(prefix))
