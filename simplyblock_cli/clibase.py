#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

import argparse
import math
import re
import sys
import argcomplete
from simplyblock_core import cluster_ops, utils, db_controller
from simplyblock_core import storage_node_ops as storage_ops
from simplyblock_core import mgmt_node_ops as mgmt_ops
from simplyblock_core import constants
from simplyblock_core.controllers import pool_controller, lvol_controller, snapshot_controller, device_controller, \
    tasks_controller
from simplyblock_core.controllers import caching_node_controller, health_controller
from simplyblock_core.models.pool import Pool


class CLIWrapperBase:

    def __init__(self):
        self.parser.add_argument("--cmd", help='cmd', nargs = '+')
        argcomplete.autocomplete(self.parser)

    def init_parser(self):
        self.parser = argparse.ArgumentParser(prog=constants.SIMPLY_BLOCK_CLI_NAME, description='SimplyBlock management CLI')
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
        spdk_cpu_mask = None
        if args.spdk_cpu_mask:
            if self.validate_cpu_mask(args.spdk_cpu_mask):
                spdk_cpu_mask = args.spdk_cpu_mask
            else:
                return f"Invalid cpu mask value: {args.spdk_cpu_mask}"
        isolate_cores = args.isolate_cores
        return storage_ops.deploy(args.ifname, spdk_cpu_mask, isolate_cores)

    def storage_node__deploy_cleaner(self, sub_command, args):
        return storage_ops.deploy_cleaner()

    def storage_node__add_node(self, sub_command, args):
        if not args.max_lvol:
            self.parser.error(f"Mandatory argument '--max-lvol' not provided for {sub_command}")
        if not args.max_prov:
            self.parser.error(f"Mandatory argument '--max-prov' not provided for {sub_command}")
        # if not args.spdk_cpu_mask:
        #     self.parser.error(f"Mandatory argument '--cpu-mask' not provided for {sub_command}")
        cluster_id = args.cluster_id
        node_ip = args.node_ip
        ifname = args.ifname
        data_nics = args.data_nics
        spdk_image = args.spdk_image
        spdk_debug = args.spdk_debug

        small_bufsize = args.small_bufsize
        large_bufsize = args.large_bufsize
        num_partitions_per_dev = args.partitions
        jm_percent = args.jm_percent
        spdk_cpu_mask = None
        if args.spdk_cpu_mask:
            if self.validate_cpu_mask(args.spdk_cpu_mask):
                spdk_cpu_mask = args.spdk_cpu_mask
            else:
                return f"Invalid cpu mask value: {args.spdk_cpu_mask}"

        max_lvol = args.max_lvol
        max_snap = args.max_snap
        max_prov = args.max_prov
        number_of_devices = args.number_of_devices
        enable_test_device = args.enable_test_device
        enable_ha_jm = args.enable_ha_jm
        number_of_distribs = args.number_of_distribs
        namespace = args.namespace
        ha_jm_count = args.ha_jm_count
        spdk_mem = args.spdk_mem
        ssd_pcie = args.ssd_pcie
        spdk_cpu_count = args.vcpu_count

        out = storage_ops.add_node(
            cluster_id=cluster_id,
            node_ip=node_ip,
            iface_name=ifname,
            data_nics_list=data_nics,
            max_lvol=max_lvol,
            max_snap=max_snap,
            max_prov=max_prov,
            spdk_image=spdk_image,
            spdk_debug=spdk_debug,
            small_bufsize=small_bufsize,
            large_bufsize=large_bufsize,
            spdk_cpu_mask=spdk_cpu_mask,
            num_partitions_per_dev=num_partitions_per_dev,
            jm_percent=jm_percent,
            number_of_devices=number_of_devices,
            enable_test_device=enable_test_device,
            namespace=namespace,
            number_of_distribs=number_of_distribs,
            enable_ha_jm=enable_ha_jm,
            is_secondary_node=args.is_secondary_node,   # pass
            id_device_by_nqn=args.id_device_by_nqn,
            partition_size=args.partition_size,
            ha_jm_count=ha_jm_count,
            spdk_hp_mem=spdk_mem,
            ssd_pcie=ssd_pcie,
            spdk_cpu_count=spdk_cpu_count,
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

        max_lvol = args.max_lvol
        max_snap = args.max_snap
        max_prov = args.max_prov if args.max_prov else 0
        number_of_devices = args.number_of_devices

        small_bufsize = args.small_bufsize
        large_bufsize = args.large_bufsize

        return storage_ops.restart_storage_node(
            node_id, max_lvol, max_snap, max_prov,
            spdk_image, spdk_debug,
            small_bufsize, large_bufsize, number_of_devices, node_ip=args.node_ip, force=args.force)

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
        return storage_ops.set(args.node_id, args.attr_name, args.attr_value)

    def cluster__deploy(self, sub_command, args):
        return self.cluster_deploy(args)

    def cluster__create(self, sub_command, args):
        return self.cluster_create(args)

    def cluster__add(self, sub_command, args):
        return self.cluster_add(args)

    def cluster__activate(self, sub_command, args):
        cluster_id = args.cluster_id
        return cluster_ops.cluster_activate(cluster_id, args.force, args.force_lvstore_create)

    def cluster__list(self, sub_command, args):
        return cluster_ops.list()

    def cluster__status(self, sub_command, args):
        cluster_id = args.cluster_id
        return cluster_ops.get_cluster_status(cluster_id)

    def cluster__show(self, sub_command, args):
        cluster_id = args.cluster_id
        return cluster_ops.list_all_info(cluster_id)

    def cluster__get(self, sub_command, args):
        return cluster_ops.get_cluster(args.cluster_id)

    def cluster__get_capacity(self, sub_command, args):
        cluster_id = args.cluster_id
        history = args.history
        is_json = args.json
        data = cluster_ops.get_capacity(cluster_id, history, is_json=is_json)
        if is_json:
            return data
        else:
            return utils.print_table(data)

    def cluster__get_io_stats(self, sub_command, args):
        data = cluster_ops.get_iostats_history(args.cluster_id, args.history, args.records)
        if data:
            return utils.print_table(data)
        else:
            return False

    def cluster__get_logs(self, sub_command, args):
        cluster_id = args.cluster_id
        return cluster_ops.get_logs(cluster_id)

    def cluster__get_secret(self, sub_command, args):
        cluster_id = args.cluster_id
        return cluster_ops.get_secret(cluster_id)

    def cluster__update_secret(self, sub_command, args):
        cluster_id = args.cluster_id
        secret = args.secret
        return cluster_ops.set_secret(cluster_id, secret)

    def cluster__check(self, sub_command, args):
        cluster_id = args.cluster_id
        return health_controller.check_cluster(cluster_id)

    def cluster__update(self, sub_command, args):
        return cluster_ops.update_cluster(args.cluster_id, mgmt_only=args.mgmt_only, restart_cluster=args.restart)
    
    def cluster__graceful_shutdown(self, sub_command, args):
        return cluster_ops.cluster_grace_shutdown(args.cluster_id)

    def cluster__graceful_startup(self, sub_command, args):
        return cluster_ops.cluster_grace_startup(args.cluster_id, args.clear_data, args.spdk_image)

    def cluster__list_tasks(self, sub_command, args):
        return tasks_controller.list_tasks(args.cluster_id)

    def cluster__cancel_task(self, sub_command, args):
        return tasks_controller.cancel_task(args.task_id)

    def cluster__delete(self, sub_command, args):
        return cluster_ops.delete_cluster(args.cluster_id)
    
    def cluster_suspend(self, sub_command, args):
        cluster_id = args.cluster_id
        return cluster_ops.suspend_cluster(cluster_id) 
    
    def cluster_unsuspend(self, sub_command, args):
        cluster_id = args.cluster_id
        return cluster_ops.unsuspend_cluster(cluster_id) 
    
    def cluster_get_cli_ssh_pass(self, sub_command, args):
        cluster_id = args.cluster_id
        return cluster_ops.get_ssh_pass(cluster_id)

    def volume__add(self, sub_command, args):
        name = args.name
        size = self.parse_size(args.size)
        max_size = self.parse_size(args.max_size)
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
        volume_id = args.volume_id
        data = lvol_controller.connect_lvol(volume_id)
        if data:
            return "\n".join(con['connect'] for con in data)

    def volume__resize(self, sub_command, args):
        volume_id = args.volume_id
        size = self.parse_size(args.size)
        return lvol_controller.resize_lvol(volume_id, size)

    def volume__create_snapshot(self, sub_command, args):
        volume_id = args.volume_id
        name = args.name
        snapshot_id, error = lvol_controller.create_snapshot(volume_id, name)
        return snapshot_id if not error else error

    def volume__clone(self, sub_command, args):
        new_size = 0
        if args.resize:
            new_size = self.parse_size(args.resize)

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
            self.parse_size(args.pool_max),
            self.parse_size(args.lvol_max),
            args.max_rw_iops,
            args.max_rw_mbytes,
            args.max_r_mbytes,
            args.max_w_mbytes,
            has_secret,
            args.cluster_id
        )

    def storage_pool__set(self, sub_command, args):
        pool_max = None
        lvol_max = None
        if args.pool_max:
            pool_max = self.parse_size(args.pool_max)
        if args.lvol_max:
            lvol_max = self.parse_size(args.lvol_max)
        return pool_controller.set_pool(
            args.pool_id,
            pool_max,
            lvol_max,
            args.max_rw_iops,
            args.max_rw_mbytes,
            args.max_r_mbytes,
            args.max_w_mbytes)

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
        new_size = 0
        if args.resize:
            new_size = self.parse_size(args.resize)

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

        spdk_cpu_mask = None
        if args.spdk_cpu_mask:
            if self.validate_cpu_mask(args.spdk_cpu_mask):
                spdk_cpu_mask = args.spdk_cpu_mask
            else:
                return f"Invalid cpu mask value: {args.spdk_cpu_mask}"

        spdk_mem = None
        if args.spdk_mem:
            spdk_mem = self.parse_size(args.spdk_mem)
            if spdk_mem < 1 * 1024 * 1024:
                return f"SPDK memory:{args.spdk_mem} must be larger than 1G"

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


    def cluster_deploy(self,args):
        
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
        grafana_endpoint = args.grafana_endpoint
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
        spdk_cpu_mask = None
        if args.spdk_cpu_mask:
            if self.validate_cpu_mask(args.spdk_cpu_mask):
                spdk_cpu_mask = args.spdk_cpu_mask
            else:
                return f"Invalid cpu mask value: {args.spdk_cpu_mask}"

        max_lvol = args.max_lvol
        max_snap = args.max_snap
        max_prov = args.max_prov
        number_of_devices = args.number_of_devices
        enable_test_device = args.enable_test_device
        enable_ha_jm = args.enable_ha_jm
        number_of_distribs = args.number_of_distribs
        namespace = args.namespace
        secondary_nodes = args.secondary_nodes
        
        lvol_name = args.lvol_name
        lvol_size = self.parse_size(args.lvol_size)
        max_size = self.parse_size(args.max_size)
        lvol_ha_type = args.lvol_ha_type
        pool_name = args.pool_name
        pool_max = self.parse_size(args.pool_max)
        host_id = args.host_id
        comp = None
        crypto = args.encrypt
        distr_vuid = args.distr_vuid
        with_snapshot = args.snapshot
        lvol_priority_class = args.lvol_priority_class
        max_rw_iops = args.max_rw_iops
        max_rw_mbytes = args.max_rw_mbytes
        max_r_mbytes = args.max_r_mbytes
        max_w_mbytes = args.max_w_mbytes
        crypto_key1 = args.crypto_key1
        crypto_key2 = args.crypto_key2
        fstype = args.fstype

        return cluster_ops.deploy_cluster(
            storage_nodes,test,ha_type,distr_ndcs,distr_npcs,enable_qos,ifname,
            blk_size, page_size_in_blocks,CLI_PASS, cap_warn, cap_crit, prov_cap_warn, 
            prov_cap_crit,log_del_interval, metrics_retention_period, contact_point, grafana_endpoint,
            distr_bs, distr_chunk_bs, enable_node_affinity,
            qpair_count, max_queue_size, inflight_io_threshold, strict_node_anti_affinity,data_nics,
            spdk_image,spdk_debug,small_bufsize,large_bufsize,num_partitions_per_dev,jm_percent,spdk_cpu_mask,max_lvol,
            max_snap,max_prov,number_of_devices,enable_test_device,enable_ha_jm,ha_jm_count,number_of_distribs,namespace,secondary_nodes,partition_size,
            lvol_name, lvol_size, lvol_ha_type, pool_name, pool_max, host_id, comp, crypto, distr_vuid, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes, 
            with_snapshot, max_size, crypto_key1, crypto_key2, lvol_priority_class, fstype)

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

    def parse_size(self, size_string: str):
        try:
            x = int(size_string)
            return x
        except Exception:
            pass
        try:
            if size_string:
                size_string = size_string.lower()
                size_string = size_string.replace(" ", "")
                size_string = size_string.replace("b", "")
                size_number = int(size_string[:-1])
                size_v = size_string[-1]
                one_k = constants.ONE_KB
                multi = 0
                if size_v == "k":
                    multi = 1
                elif size_v == "m":
                    multi = 2
                elif size_v == "g":
                    multi = 3
                elif size_v == "t":
                    multi = 4
                else:
                    print(f"Error parsing size: {size_string}")
                    return -1
                return size_number * math.pow(one_k, multi)
            else:
                return -1
        except:
            print(f"Error parsing size: {size_string}")
            return -1

    def validate_cpu_mask(self, spdk_cpu_mask):
        return re.match("^(0x|0X)?[a-fA-F0-9]+$", spdk_cpu_mask)

    def _completer_get_cluster_list(self, prefix, parsed_args, **kwargs):
        db = db_controller.DBController()
        return (cluster.get_id() for cluster in db.get_clusters() if cluster.get_id().startswith(prefix))


    def _completer_get_sn_list(self, prefix, parsed_args, **kwargs):
        db = db_controller.DBController()
        return (cluster.get_id() for cluster in db.get_storage_nodes() if cluster.get_id().startswith(prefix))
