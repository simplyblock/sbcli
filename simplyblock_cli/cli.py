#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

from simplyblock_cli.clibase import CLIWrapperBase
from simplyblock_core import utils
import logging
import sys

class CLIWrapper(CLIWrapperBase):

    def __init__(self):
        self.developer_mode = True if "--dev" in sys.argv else False
        if self.developer_mode:
            idx = sys.argv.index("--dev")
            args = sys.argv[0:idx]
            for i in range(idx + 1, len(sys.argv)):
                args.append(sys.argv[i])
            sys.argv = args

        self.logger = utils.get_logger()
        self.init_parser()
        self.init_storage_node()
        self.init_cluster()
        self.init_volume()
        self.init_control_plane()
        self.init_storage_pool()
        self.init_snapshot()
        self.init_caching_node()
        super().__init__()

    def init_storage_node(self):
        subparser = self.add_command('storage-node', 'Storage node commands', aliases=['sn',])
        self.init_storage_node__deploy(subparser)
        self.init_storage_node__deploy_cleaner(subparser)
        self.init_storage_node__add_node(subparser)
        self.init_storage_node__delete(subparser)
        self.init_storage_node__remove(subparser)
        self.init_storage_node__list(subparser)
        self.init_storage_node__get(subparser)
        self.init_storage_node__restart(subparser)
        self.init_storage_node__shutdown(subparser)
        self.init_storage_node__suspend(subparser)
        self.init_storage_node__resume(subparser)
        self.init_storage_node__get_io_stats(subparser)
        self.init_storage_node__get_capacity(subparser)
        self.init_storage_node__list_devices(subparser)
        if self.developer_mode:
            self.init_storage_node__device_testing_mode(subparser)
        self.init_storage_node__get_device(subparser)
        self.init_storage_node__reset_device(subparser)
        self.init_storage_node__restart_device(subparser)
        self.init_storage_node__add_device(subparser)
        self.init_storage_node__remove_device(subparser)
        self.init_storage_node__set_failed_device(subparser)
        self.init_storage_node__get_capacity_device(subparser)
        self.init_storage_node__get_io_stats_device(subparser)
        self.init_storage_node__port_list(subparser)
        self.init_storage_node__port_io_stats(subparser)
        self.init_storage_node__check(subparser)
        self.init_storage_node__check_device(subparser)
        self.init_storage_node__info(subparser)
        if self.developer_mode:
            self.init_storage_node__info_spdk(subparser)
        if self.developer_mode:
            self.init_storage_node__remove_jm_device(subparser)
        self.init_storage_node__restart_jm_device(subparser)
        if self.developer_mode:
            self.init_storage_node__send_cluster_map(subparser)
        if self.developer_mode:
            self.init_storage_node__get_cluster_map(subparser)
        self.init_storage_node__make_primary(subparser)
        if self.developer_mode:
            self.init_storage_node__dump_lvstore(subparser)
        if self.developer_mode:
            self.init_storage_node__set(subparser)


    def init_storage_node__deploy(self, subparser):
        subcommand = self.add_sub_command(subparser, 'deploy', 'Prepares a host to be used as a storage node')
        argument = subcommand.add_argument('--ifname', help='Management interface name, e.g. eth0', type=str, dest='ifname', required=False)
        argument = subcommand.add_argument('--cpu-mask', help='SPDK app CPU mask, default is all cores found', type=str, dest='spdk_cpu_mask', required=False)
        argument = subcommand.add_argument('--isolate-cores', help='Isolate cores in kernel args for provided cpu mask', default=False, dest='isolate_cores', required=False, action='store_true')

    def init_storage_node__deploy_cleaner(self, subparser):
        subcommand = self.add_sub_command(subparser, 'deploy-cleaner', 'Cleans a previous simplyblock deploy (local run)')

    def init_storage_node__add_node(self, subparser):
        subcommand = self.add_sub_command(subparser, 'add-node', 'Adds a storage node by its IP address')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str)
        subcommand.add_argument('node_ip', help='IP of storage node to add', type=str)
        subcommand.add_argument('ifname', help='Management interface name', type=str)
        argument = subcommand.add_argument('--journal-partition', help='1: auto-create small partitions for journal on nvme devices. 0: use a separate (the smallest) nvme device of the node for journal. The journal needs a maximum of 3 percent of total available raw disk space.', type=int, default=1, dest='partitions', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--jm-percent', help='Number in percent to use for JM from each device', type=int, default=3, dest='jm_percent', required=False)
        argument = subcommand.add_argument('--data-nics', help='Storage network interface name(s). Can be more than one.', type=str, dest='data_nics', required=False, nargs='+')
        argument = subcommand.add_argument('--max-lvol', help='Max logical volume per storage node', type=int, dest='max_lvol', required=False)
        argument = subcommand.add_argument('--max-size', help='Maximum amount of GB to be utilized on this storage node', type=str, dest='max_prov', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--number-of-distribs', help='The number of distirbs to be created on the node', type=int, default=4, dest='number_of_distribs', required=False)
        argument = subcommand.add_argument('--number-of-devices', help='Number of devices per storage node if it\'s not supported EC2 instance', type=int, dest='number_of_devices', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--size-of-device', help='Size of device per storage node', type=str, dest='partition_size', required=False)
        argument = subcommand.add_argument('--vcpu-count', help='Number of vCPUs used for SPDK. Remaining CPUs will be used for Linux system, TCP/IP processing, and other workloads. The default on non-Kubernetes hosts is 80%%.', type=int, dest='vcpu_count', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--cpu-mask', help='SPDK app CPU mask, default is all cores found', type=str, dest='spdk_cpu_mask', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--spdk-image', help='SPDK image uri', type=str, dest='spdk_image', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--spdk-debug', help='Enable spdk debug logs', dest='spdk_debug', required=False, action='store_true')
        if self.developer_mode:
            argument = subcommand.add_argument('--iobuf_small_bufsize', help='bdev_set_options param', type=int, default=0, dest='small_bufsize', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--iobuf_large_bufsize', help='bdev_set_options param', type=int, default=0, dest='large_bufsize', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--enable-test-device', help='Enable creation of test device', dest='enable_test_device', required=False, action='store_true')
        if self.developer_mode:
            argument = subcommand.add_argument('--disable-ha-jm', help='Disable HA JM for distrib creation', dest='enable_ha_jm', required=False, action='store_false')
        argument = subcommand.add_argument('--ha-jm-count', help='HA JM count', type=int, default=3, dest='ha_jm_count', required=False)
        argument = subcommand.add_argument('--is-secondary-node', help='Adds as secondary node. A secondary node does not have any disks attached. It is only used for I/O processing in case a primary goes down.', default=False, dest='is_secondary_node', required=False, action='store_true')
        argument = subcommand.add_argument('--namespace', help='Kubernetes namespace to deploy on', type=str, dest='namespace', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--id-device-by-nqn', help='Use device nqn to identify it instead of serial number', dest='id_device_by_nqn', required=False, action='store_true')
        if self.developer_mode:
            argument = subcommand.add_argument('--max-snap', help='Max snapshot per storage node', type=int, default=5000, dest='max_snap', required=False)
        argument = subcommand.add_argument('--spdk-mem', help='Set spdk hugepage size limitation', type=str, default='', dest='spdk_mem', required=False)
        argument = subcommand.add_argument('--ssd-pcie', help='Nvme PCIe address to use for storage device. Can be more than one.', type=str, default='', dest='ssd_pcie', required=False, nargs='+')

    def init_storage_node__delete(self, subparser):
        subcommand = self.add_sub_command(subparser, 'delete', 'Deletes a storage node object from the state database.')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list
        argument = subcommand.add_argument('--force', help='Force delete storage node from DB...Hopefully you know what you do', dest='force_remove', required=False, action='store_true')
        sub_command.add_argument("--iobuf_small_bufsize", help='bdev_set_options param', dest='small_bufsize',  type=int, default=0)
        sub_command.add_argument("--iobuf_large_bufsize", help='bdev_set_options param', dest='large_bufsize',  type=int, default=0)
        sub_command.add_argument("--enable-test-device", help='Enable creation of test device', action='store_true')
        sub_command.add_argument("--disable-ha-jm", help='Disable HA JM for distrib creation', action='store_false', dest='enable_ha_jm', default=True)
        sub_command.add_argument("--ha-jm-count", help='HA JM count', dest='ha_jm_count', type=int, default=constants.HA_JM_COUNT)
        sub_command.add_argument("--secondary-stg-name", help="secondary storage name", type=str, default=None)
        sub_command.add_argument("--secondary-io-timeout-us", "secondary storage I/O timeout in us", type=int, default=0)
        sub_command.add_argument("--ghost-capacity", "ghost queue capacity", type=int, default=0)
        sub_command.add_argument("--fifo-main-capacity", "main fifo queue capacity", type=int, default=0)
        sub_command.add_argument("--fifo-small-capacity", "small fifo queue capacity", type=int, default=0)
        sub_command.add_argument("--is-secondary-node", help='add as secondary node', action='store_true', dest='is_secondary_node', default=False)
        sub_command.add_argument("--namespace", help='k8s namespace to deploy on',)
        sub_command.add_argument("--id-device-by-nqn", help='Use device nqn to identify it instead of serial number', action='store_true', dest='id_device_by_nqn', default=False)

    def init_storage_node__remove(self, subparser):
        subcommand = self.add_sub_command(subparser, 'remove', 'Removes a storage node from the cluster')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list
        argument = subcommand.add_argument('--force-remove', help='Force remove all logical volumes and snapshots', dest='force_remove', required=False, action='store_true')

    def init_storage_node__list(self, subparser):
        subcommand = self.add_sub_command(subparser, 'list', 'Lists all storage nodes')
        argument = subcommand.add_argument('--cluster-id', help='Cluster id', type=str, dest='cluster_id', required=False)
        argument = subcommand.add_argument('--json', help='Print outputs in json format', dest='json', required=False, action='store_true')

    def init_storage_node__get(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get', 'Gets a storage node\'s information')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list

    def init_storage_node__restart(self, subparser):
        subcommand = self.add_sub_command(subparser, 'restart', 'Restarts a storage node')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list
        argument = subcommand.add_argument('--max-lvol', help='Max logical volume per storage node', type=int, default=0, dest='max_lvol', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--max-snap', help='Max snapshot per storage node', type=int, default=5000, dest='max_snap', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--max-size', help='Maximum amount of GB to be utilized on this storage node', type=str, default='0', dest='max_prov', required=False)
        argument = subcommand.add_argument('--node-addr', '--node-ip', help='Restart Node on new node', type=str, dest='node_ip', required=False)
        argument = subcommand.add_argument('--number-of-devices', help='Number of devices per storage node if it\'s not supported EC2 instance', type=int, dest='number_of_devices', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--spdk-image', help='SPDK image uri', type=str, dest='spdk_image', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--reattach-volume', help='Reattach volume to new instance', dest='reattach_volume', required=False, action='store_true')
        if self.developer_mode:
            argument = subcommand.add_argument('--spdk-debug', help='Enable spdk debug logs', dest='spdk_debug', required=False, action='store_true')
        if self.developer_mode:
            argument = subcommand.add_argument('--iobuf_small_bufsize', help='bdev_set_options param', type=int, default=0, dest='small_bufsize', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--iobuf_large_bufsize', help='bdev_set_options param', type=int, default=0, dest='large_bufsize', required=False)
        argument = subcommand.add_argument('--force', help='Force restart', dest='force', required=False, action='store_true')

    def init_storage_node__shutdown(self, subparser):
        subcommand = self.add_sub_command(subparser, 'shutdown', 'Initiates a storage node shutdown')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list
        argument = subcommand.add_argument('--force', help='Force node shutdown', dest='force', required=False, action='store_true')

    def init_storage_node__suspend(self, subparser):
        subcommand = self.add_sub_command(subparser, 'suspend', 'Suspends a storage node')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list
        argument = subcommand.add_argument('--force', help='Force node suspend', dest='force', required=False, action='store_true')

    def init_storage_node__resume(self, subparser):
        subcommand = self.add_sub_command(subparser, 'resume', 'Resumes a storage node')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list

    def init_storage_node__get_io_stats(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-io-stats', 'Gets storage node IO statistics')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list
        argument = subcommand.add_argument('--history', help='list history records -one for every 15 minutes- for XX days and YY hours -up to 10 days in total-, format: XXdYYh', type=str, dest='history', required=False)
        argument = subcommand.add_argument('--records', help='Number of records, default: 20', type=int, default=20, dest='records', required=False)

    def init_storage_node__get_capacity(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-capacity', 'Gets a storage node\'s capacity statistics')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list
        argument = subcommand.add_argument('--history', help='list history records -one for every 15 minutes- for XX days and YY hours -up to 10 days in total-, format: XXdYYh', type=str, dest='history', required=False)

    def init_storage_node__list_devices(self, subparser):
        subcommand = self.add_sub_command(subparser, 'list-devices', 'Lists storage devices')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list
        argument = subcommand.add_argument('--json', help='Print outputs in json format', dest='json', required=False, action='store_true')

    def init_storage_node__device_testing_mode(self, subparser):
        subcommand = self.add_sub_command(subparser, 'device-testing-mode', 'Sets a device to testing mode')
        subcommand.add_argument('device_id', help='Device id', type=str)
        subcommand.add_argument('mode', help='Testing mode', type=str, default='full_pass_through')

    def init_storage_node__get_device(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-device', 'Gets storage device by its id')
        subcommand.add_argument('device_id', help='Device id', type=str)

    def init_storage_node__reset_device(self, subparser):
        subcommand = self.add_sub_command(subparser, 'reset-device', 'Resets a storage device')
        subcommand.add_argument('device_id', help='Device id', type=str)

    def init_storage_node__restart_device(self, subparser):
        subcommand = self.add_sub_command(subparser, 'restart-device', 'Restarts a storage device')
        subcommand.add_argument('device_id', help='Device id', type=str)

    def init_storage_node__add_device(self, subparser):
        subcommand = self.add_sub_command(subparser, 'add-device', 'Adds a new storage device')
        subcommand.add_argument('device_id', help='Device id', type=str)

    def init_storage_node__remove_device(self, subparser):
        subcommand = self.add_sub_command(subparser, 'remove-device', 'Logically removes a storage device')
        subcommand.add_argument('device_id', help='Device id', type=str)
        argument = subcommand.add_argument('--force', help='Force device remove', dest='force', required=False, action='store_true')

    def init_storage_node__set_failed_device(self, subparser):
        subcommand = self.add_sub_command(subparser, 'set-failed-device', 'Sets storage device to failed state')
        subcommand.add_argument('device_id', help='Device ID', type=str)

    def init_storage_node__get_capacity_device(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-capacity-device', 'Gets a device\'s capacity')
        subcommand.add_argument('device_id', help='Device id', type=str)
        argument = subcommand.add_argument('--history', help='list history records -one for every 15 minutes- for XX days and YY hours -up to 10 days in total-, format: XXdYYh', type=str, dest='history', required=False)

    def init_storage_node__get_io_stats_device(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-io-stats-device', 'Gets a device\'s IO statistics')
        subcommand.add_argument('device_id', help='Device id', type=str)
        argument = subcommand.add_argument('--history', help='list history records -one for every 15 minutes- for XX days and YY hours -up to 10 days in total-, format: XXdYYh', type=str, dest='history', required=False)
        argument = subcommand.add_argument('--records', help='Number of records, default: 20', type=int, default=20, dest='records', required=False)

    def init_storage_node__port_list(self, subparser):
        subcommand = self.add_sub_command(subparser, 'port-list', 'Gets the data interfaces list of a storage node')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list

    def init_storage_node__port_io_stats(self, subparser):
        subcommand = self.add_sub_command(subparser, 'port-io-stats', 'Gets the data interfaces\' IO stats')
        subcommand.add_argument('port_id', help='Data port id', type=str)
        argument = subcommand.add_argument('--history', help='list history records -one for every 15 minutes- for XX days and YY hours -up to 10 days in total, format: XXdYYh', type=str, dest='history', required=False)

    def init_storage_node__check(self, subparser):
        subcommand = self.add_sub_command(subparser, 'check', 'Checks the health status of a storage node')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list

    def init_storage_node__check_device(self, subparser):
        subcommand = self.add_sub_command(subparser, 'check-device', 'Checks the health status of a device')
        subcommand.add_argument('device_id', help='Device id', type=str)

    def init_storage_node__info(self, subparser):
        subcommand = self.add_sub_command(subparser, 'info', 'Gets the node\'s information')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list

    def init_storage_node__info_spdk(self, subparser):
        subcommand = self.add_sub_command(subparser, 'info-spdk', 'Gets the SPDK memory information')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list

    def init_storage_node__remove_jm_device(self, subparser):
        subcommand = self.add_sub_command(subparser, 'remove-jm-device', 'Removes a journaling device')
        subcommand.add_argument('jm_device_id', help='Journaling device id', type=str)
        argument = subcommand.add_argument('--force', help='Force device remove', dest='force', required=False, action='store_true')

    def init_storage_node__restart_jm_device(self, subparser):
        subcommand = self.add_sub_command(subparser, 'restart-jm-device', 'Restarts a journaling device')
        subcommand.add_argument('jm_device_id', help='Journaling device id', type=str)
        argument = subcommand.add_argument('--force', help='Force device remove', dest='force', required=False, action='store_true')

    def init_storage_node__send_cluster_map(self, subparser):
        subcommand = self.add_sub_command(subparser, 'send-cluster-map', 'Sends a new cluster map')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list

    def init_storage_node__get_cluster_map(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-cluster-map', 'Get the current cluster map')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list

    def init_storage_node__make_primary(self, subparser):
        subcommand = self.add_sub_command(subparser, 'make-primary', 'Forces to make the provided node id primary')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list

    def init_storage_node__dump_lvstore(self, subparser):
        subcommand = self.add_sub_command(subparser, 'dump-lvstore', 'Dump lvstore data')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list

    def init_storage_node__set(self, subparser):
        subcommand = self.add_sub_command(subparser, 'set', 'set storage node db value')
        subcommand.add_argument('node_id', help='Storage node id', type=str)
        subcommand.add_argument('attr_name', help='attr_name', type=str)
        subcommand.add_argument('attr_value', help='attr_value', type=str)

        sub_command = self.add_sub_command(subparser, 'send-cluster-map', 'send cluster map')
        sub_command.add_argument("id", help='UUID of storage node').completer = self._completer_get_sn_list

        sub_command = self.add_sub_command(subparser, 'get-cluster-map', 'get cluster map')
        sub_command.add_argument("id", help='UUID of storage node').completer = self._completer_get_sn_list

        sub_command = self.add_sub_command(subparser, 'make-primary',
                                           'In case of HA SNode, make the current node as primary')
        sub_command.add_argument("id", help='UUID of storage node').completer = self._completer_get_sn_list

        sub_command = self.add_sub_command(subparser, 'dump-lvstore','Dump lvstore data')
        sub_command.add_argument("id", help='UUID of storage node').completer = self._completer_get_sn_list


        # S3 bdev for tiering

        sub_command = self.add_sub_command(subparser, 's3-bdev-create', 'Create a local S3 bdev for tiering (otherwise can connect to an existing remote one over fabric)')
        sub_command.add_argument("id", help='UUID of storage node').completer = self._completer_get_sn_list
        sub_command.add_argument("--name", 'S3 bdev name', type=str)
        sub_command.add_argument("--bdb-lcpu-mask", 'S3 bdev SPDK thread mask', type=int, default=0)
        sub_command.add_argument("--s3-lcpu-mask", 'S3 bdev worker pthread mask', type=int, default=0)
        sub_command.add_argument("--s3-thread-pool-size", 'S3 bdev worker pthread pool size', type=int, default=32)

        sub_command = self.add_sub_command(subparser, 's3-bdev-delete', 'Delete a local S3 bdev')
        sub_command.add_argument("id", help='UUID of storage node').completer = self._completer_get_sn_list
        sub_command.add_argument("--name", 'S3 bdev name', type=str)

        sub_command = self.add_sub_command(subparser, 's3-bdev-add-bucket-name', 'Register a bucket name in the local S3 bdev if a bucket was newly added to the S3 endpoint')
        sub_command.add_argument("id", help='UUID of storage node').completer = self._completer_get_sn_list
        sub_command.add_argument("--name", 'S3 bdev name', type=str)
        sub_command.add_argument("--bucket-name", 'S3 bucket name', type=str)

        # check lvol
        #
        # ----------------- cluster -----------------
        #

    def init_cluster(self):
        subparser = self.add_command('cluster', 'Cluster commands')
        self.init_cluster__deploy(subparser)
        self.init_cluster__create(subparser)
        self.init_cluster__add(subparser)
        self.init_cluster__activate(subparser)
        self.init_cluster__list(subparser)
        self.init_cluster__status(subparser)
        self.init_cluster__show(subparser)
        self.init_cluster__get(subparser)
        self.init_cluster__get_capacity(subparser)
        self.init_cluster__get_io_stats(subparser)
        self.init_cluster__get_logs(subparser)
        self.init_cluster__get_secret(subparser)
        self.init_cluster__update_secret(subparser)
        self.init_cluster__check(subparser)
        self.init_cluster__update(subparser)
        if self.developer_mode:
            self.init_cluster__graceful_shutdown(subparser)
        if self.developer_mode:
            self.init_cluster__graceful_startup(subparser)
        self.init_cluster__list_tasks(subparser)
        self.init_cluster__cancel_task(subparser)
        self.init_cluster__delete(subparser)
        if self.developer_mode:
            self.init_cluster__set(subparser)


    def init_cluster__deploy(self, subparser):
        subcommand = self.add_sub_command(subparser, 'deploy', 'Deploys a storage nodes')
        argument = subcommand.add_argument('--storage-nodes', help='comma separated ip addresses', type=str, dest='storage_nodes', required=False)
        argument = subcommand.add_argument('--test', help='Test Cluster', dest='test', required=False, action='store_true')
        argument = subcommand.add_argument('--secondary-nodes', help='comma separated ip addresses', type=str, dest='secondary_nodes', required=False)
        argument = subcommand.add_argument('--ha-type', help='Logical volume HA type (single, ha), default is cluster HA type', type=str, default='ha', dest='ha_type', required=False, choices=['single','ha',])
        if self.developer_mode:
            argument = subcommand.add_argument('--ha-jm-count', help='HA JM count', type=int, default=3, dest='ha_jm_count', required=False)
        argument = subcommand.add_argument('--data-chunks-per-stripe', help='Erasure coding schema parameter k (distributed raid), default: 1', type=int, default=1, dest='distr_ndcs', required=False)
        argument = subcommand.add_argument('--parity-chunks-per-stripe', help='Erasure coding schema parameter n (distributed raid), default: 1', type=int, default=1, dest='distr_npcs', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--enable-qos', help='Enable qos bdev for storage nodes', type=bool, default=True, dest='enable_qos', required=False)
        argument = subcommand.add_argument('--ifname', help='Management interface name, e.g. eth0', type=str, dest='ifname', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--blk_size', help='The block size in bytes', type=int, default=512, dest='blk_size', required=False, choices=['512','4096',])
        if self.developer_mode:
            argument = subcommand.add_argument('--page_size', help='The size of a data page in bytes', type=int, default=2097152, dest='page_size', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--CLI_PASS', help='Password for CLI SSH connection', type=str, dest='CLI_PASS', required=False)
        argument = subcommand.add_argument('--cap-warn', help='Capacity warning level in percent, default: 89', type=int, default=89, dest='cap_warn', required=False)
        argument = subcommand.add_argument('--cap-crit', help='Capacity critical level in percent, default: 99', type=int, default=99, dest='cap_crit', required=False)
        argument = subcommand.add_argument('--prov-cap-warn', help='Capacity warning level in percent, default: 250', type=int, default=250, dest='prov_cap_warn', required=False)
        argument = subcommand.add_argument('--prov-cap-crit', help='Capacity critical level in percent, default: 500', type=int, default=500, dest='prov_cap_crit', required=False)
        argument = subcommand.add_argument('--log-del-interval', help='Logging retention period, default: 3d', type=str, default='3d', dest='log_del_interval', required=False)
        argument = subcommand.add_argument('--metrics-retention-period', help='Retention period for I/O statistics (Prometheus), default: 7d', type=str, default='7d', dest='metrics_retention_period', required=False)
        argument = subcommand.add_argument('--contact-point', help='Email or slack webhook url to be used for alerting', type=str, default='', dest='contact_point', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--distr-bs', help='(Dev) distrb bdev block size, default: 4096', type=int, default=4096, dest='distr_bs', required=False)
        argument = subcommand.add_argument('--chunk-size-in-bytes', help='(Dev) distrb bdev chunk block size, default: 4096', type=int, default=4096, dest='distr_chunk_bs', required=False)
        argument = subcommand.add_argument('--enable-node-affinity', help='Enable node affinity for storage nodes', dest='enable_node_affinity', required=False, action='store_true')
        argument = subcommand.add_argument('--qpair-count', help='NVMe/TCP transport qpair count per logical volume', type=int, default=3, dest='qpair_count', required=False, choices=range(1, 128))
        if self.developer_mode:
            argument = subcommand.add_argument('--max-queue-size', help='The max size the queue will grow', type=int, default=128, dest='max_queue_size', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--inflight-io-threshold', help='The number of inflight IOs allowed before the IO queuing starts', type=int, default=4, dest='inflight_io_threshold', required=False)
        argument = subcommand.add_argument('--strict-node-anti-affinity', help='Enable strict node anti affinity for storage nodes. Never more than one chunk is placed on a node. This requires a minimum of _data-chunks-in-stripe + parity-chunks-in-stripe + 1_ nodes in the cluster."', dest='strict_node_anti_affinity', required=False, action='store_true')
        argument = subcommand.add_argument('--journal-partition', help='1: auto-partition nvme devices for journal. 0: use a separate nvme device for journal. The smallest NVMe device available on the host will be chosen as a journal. It should provide about 3%% of the entire node’s NVMe capacity. If set to false, partitions on other devices will be auto-created to store the journal.', type=str, default='True', dest='partitions', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--jm-percent', help='Number in percent to use for JM from each device', type=int, default=3, dest='jm_percent', required=False)
        argument = subcommand.add_argument('--data-nics', help='Storage network interface name(s). Can be more than one.', type=str, dest='data_nics', required=False, nargs='+')
        argument = subcommand.add_argument('--max-lvol', help='Max logical volume per storage node', type=int, dest='max_lvol', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--max-snap', help='Max snapshot per storage node', type=int, default=5000, dest='max_snap', required=False)
        argument = subcommand.add_argument('--max-size', help='Maximum amount of GB to be provisioned via all storage nodes', type=str, default='', dest='max_prov', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--number-of-distribs', help='The number of distirbs to be created on the node', type=int, default=4, dest='number_of_distribs', required=False)
        argument = subcommand.add_argument('--number-of-devices', help='Number of devices per storage node if it\'s not supported EC2 instance', type=int, default=0, dest='number_of_devices', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--size-of-device', help='Size of device per storage node', type=str, dest='partition_size', required=False)
        argument = subcommand.add_argument('--vcpu-count', help='Number of vCPUs used for SPDK. Remaining CPUs will be used for Linux system, TCP/IP processing, and other workloads. The default on non-Kubernetes hosts is 80%%.', type=int, dest='vcpu_count', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--cpu-mask', help='SPDK app CPU mask, default is all cores found', type=str, dest='spdk_cpu_mask', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--spdk-image', help='SPDK image uri', type=str, dest='spdk_image', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--spdk-debug', help='Enable spdk debug logs', dest='spdk_debug', required=False, action='store_true')
        if self.developer_mode:
            argument = subcommand.add_argument('--iobuf_small_bufsize', help='bdev_set_options param', type=int, default=0, dest='small_bufsize', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--iobuf_large_bufsize', help='bdev_set_options param', type=int, default=0, dest='large_bufsize', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--enable-test-device', help='Enable creation of test device', dest='enable_test_device', required=False, action='store_true')
        if self.developer_mode:
            argument = subcommand.add_argument('--disable-ha-jm', help='Disable HA JM for distrib creation', dest='enable_ha_jm', required=False, action='store_true')
        argument = subcommand.add_argument('--is-secondary-node', help='Adds as secondary node. A secondary node does not have any disks attached. It is only used for I/O processing in case a primary goes down.', default=False, dest='is_secondary_node', required=False, action='store_true')
        argument = subcommand.add_argument('--namespace', help='k8s namespace to deploy on', type=str, dest='namespace', required=False)
        argument = subcommand.add_argument('--id-device-by-nqn', help='Use device nqn to identify it instead of serial number', default=False, dest='id_device_by_nqn', required=False, action='store_true')
        if self.developer_mode:
            argument = subcommand.add_argument('--lvol-name', help='Logical volume name or id', type=str, default='lvol01', dest='lvol_name', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--lvol-size', help='Logical volume size: 10M, 10G, 10(bytes)', type=str, default='10G', dest='lvol_size', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--pool-name', help='Pool id or name', type=str, default='pool01', dest='pool_name', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--pool-max', help='Pool maximum size: 20M, 20G, 0(default)', type=str, default='25G', dest='pool_max', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--snapshot', '-s', help='Make logical volume with snapshot capability, default: false', dest='snapshot', required=False, action='store_true')
        if self.developer_mode:
            argument = subcommand.add_argument('--max-volume-size', help='Logical volume max size', type=str, default='1000G', dest='max_size', required=False)
        argument = subcommand.add_argument('--host-id', help='Primary storage node id or hostname', type=str, dest='host_id', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--encrypt', help='Use inline data encryption and decryption on the logical volume', dest='encrypt', required=False, action='store_true')
        if self.developer_mode:
            argument = subcommand.add_argument('--distr-vuid', help='(Dev) set vuid manually, default: random (1-99999)', type=int, dest='distr_vuid', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--lvol-ha-type', help='Logical volume HA type (single, ha), default is cluster HA type', type=str, default='ha', dest='lvol_ha_type', required=False, choices=['single','default','ha',])

    def init_cluster__create(self, subparser):
        subcommand = self.add_sub_command(subparser, 'create', 'Creates a new cluster')
        if self.developer_mode:
            argument = subcommand.add_argument('--page_size', help='The size of a data page in bytes', type=int, default=2097152, dest='page_size', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--CLI_PASS', help='Password for CLI SSH connection', type=str, dest='CLI_PASS', required=False)
        argument = subcommand.add_argument('--cap-warn', help='Capacity warning level in percent, default: 89', type=int, default=89, dest='cap_warn', required=False)
        argument = subcommand.add_argument('--cap-crit', help='Capacity critical level in percent, default: 99', type=int, default=99, dest='cap_crit', required=False)
        argument = subcommand.add_argument('--prov-cap-warn', help='Capacity warning level in percent, default: 250', type=int, default=250, dest='prov_cap_warn', required=False)
        argument = subcommand.add_argument('--prov-cap-crit', help='Capacity critical level in percent, default: 500', type=int, default=500, dest='prov_cap_crit', required=False)
        argument = subcommand.add_argument('--ifname', help='Management interface name, e.g. eth0', type=str, dest='ifname', required=False)
        argument = subcommand.add_argument('--log-del-interval', help='Logging retention policy, default: 3d', type=str, default='3d', dest='log_del_interval', required=False)
        argument = subcommand.add_argument('--metrics-retention-period', help='Retention period for I/O statistics (Prometheus), default: 7d', type=str, default='7d', dest='metrics_retention_period', required=False)
        argument = subcommand.add_argument('--contact-point', help='Email or slack webhook url to be used for alerting', type=str, default='', dest='contact_point', required=False)
        argument = subcommand.add_argument('--grafana-endpoint', help='Endpoint url for Grafana', type=str, default='', dest='grafana_endpoint', required=False)
        argument = subcommand.add_argument('--data-chunks-per-stripe', help='Erasure coding schema parameter k (distributed raid), default: 1', type=int, default=1, dest='distr_ndcs', required=False)
        argument = subcommand.add_argument('--parity-chunks-per-stripe', help='Erasure coding schema parameter n (distributed raid), default: 1', type=int, default=1, dest='distr_npcs', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--distr-bs', help='(Dev) distrb bdev block size, default: 4096', type=int, default=4096, dest='distr_bs', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--distr-chunk-bs', help='(Dev) distrb bdev chunk block size, default: 4096', type=int, default=4096, dest='distr_chunk_bs', required=False)
        argument = subcommand.add_argument('--ha-type', help='Logical volume HA type (single, ha), default is cluster ha type', type=str, default='ha', dest='ha_type', required=False, choices=['single','ha',])
        argument = subcommand.add_argument('--enable-node-affinity', help='Enable node affinity for storage nodes', dest='enable_node_affinity', required=False, action='store_true')
        argument = subcommand.add_argument('--qpair-count', help='NVMe/TCP transport qpair count per logical volume', type=int, default=0, dest='qpair_count', required=False, choices=range(0, 128))
        if self.developer_mode:
            argument = subcommand.add_argument('--max-queue-size', help='The max size the queue will grow', type=int, default=128, dest='max_queue_size', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--inflight-io-threshold', help='The number of inflight IOs allowed before the IO queuing starts', type=int, default=4, dest='inflight_io_threshold', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--enable-qos', help='Enable qos bdev for storage nodes, true by default', type=bool, default=True, dest='enable_qos', required=False)
        argument = subcommand.add_argument('--strict-node-anti-affinity', help='Enable strict node anti affinity for storage nodes. Never more than one chunk is placed on a node. This requires a minimum of _data-chunks-in-stripe + parity-chunks-in-stripe + 1_ nodes in the cluster.', dest='strict_node_anti_affinity', required=False, action='store_true')
        # add cluster
        sub_command = self.add_sub_command(subparser, 'add', 'Add new cluster')
        sub_command.add_argument("--blk_size", help='The block size in bytes', type=int, choices=[512, 4096], default=512)
        sub_command.add_argument("--page_size", help='The size of a data page in bytes', type=int, default=2097152)
        sub_command.add_argument("--cap-warn", help='Capacity warning level in percent, default=80',
                                 type=int, required=False, dest="cap_warn")
        sub_command.add_argument("--cap-crit", help='Capacity critical level in percent, default=90',
                                 type=int, required=False, dest="cap_crit")
        sub_command.add_argument("--prov-cap-warn", help='Capacity warning level in percent, default=180',
                                 type=int, required=False, dest="prov_cap_warn")
        sub_command.add_argument("--prov-cap-crit", help='Capacity critical level in percent, default=190',
                                 type=int, required=False, dest="prov_cap_crit")
        sub_command.add_argument("--distr-ndcs", help='(Dev) set ndcs manually, default: 4', type=int, default=0)
        sub_command.add_argument("--distr-npcs", help='(Dev) set npcs manually, default: 1', type=int, default=0)
        sub_command.add_argument("--distr-bs", help='(Dev) distrb bdev block size, default: 4096', type=int,
                                 default=4096)
        sub_command.add_argument("--distr-chunk-bs", help='(Dev) distrb bdev chunk block size, default: 4096', type=int,
                                 default=4096)
        sub_command.add_argument("--ha-type", help='LVol HA type (single, ha), default is cluster single type',
                                 dest='ha_type', choices=["single", "ha"], default='single')
        sub_command.add_argument("--enable-node-affinity", help='Enable node affinity for storage nodes', action='store_true')
        sub_command.add_argument("--qpair-count", help='tcp transport qpair count', type=int, dest='qpair_count',
                                 default=0, choices=range(128))
        sub_command.add_argument("--max-queue-size", help='The max size the queue will grow', type=int, default=128)
        sub_command.add_argument("--inflight-io-threshold", help='The number of inflight IOs allowed before the IO queuing starts', type=int, default=4)
        sub_command.add_argument("--enable-qos", help='Enable qos bdev for storage nodes', action='store_true', dest='enable_qos')
        sub_command.add_argument("--strict-node-anti-affinity", help='Enable strict node anti affinity for storage nodes', action='store_true')
        sub_command.add_argument("--support-storage-tiering", help="Whether to support storage tiering", type=bool, default=False)
        sub_command.add_argument("--disaster-recovery", help="AZ disaster recovery mode", type=bool, default=False)

    def init_cluster__add(self, subparser):
        subcommand = self.add_sub_command(subparser, 'add', 'Adds a new cluster')
        if self.developer_mode:
            argument = subcommand.add_argument('--page_size', help='The size of a data page in bytes', type=int, default=2097152, dest='page_size', required=False)
        argument = subcommand.add_argument('--cap-warn', help='Capacity warning level in percent, default: 89', type=int, default=89, dest='cap_warn', required=False)
        argument = subcommand.add_argument('--cap-crit', help='Capacity critical level in percent, default: 99', type=int, default=99, dest='cap_crit', required=False)
        argument = subcommand.add_argument('--prov-cap-warn', help='Capacity warning level in percent, default: 250', type=int, default=250, dest='prov_cap_warn', required=False)
        argument = subcommand.add_argument('--prov-cap-crit', help='Capacity critical level in percent, default: 500', type=int, default=500, dest='prov_cap_crit', required=False)
        argument = subcommand.add_argument('--data-chunks-per-stripe', help='Erasure coding schema parameter k (distributed raid), default: 1', type=int, default=1, dest='distr_ndcs', required=False)
        argument = subcommand.add_argument('--parity-chunks-per-stripe', help='Erasure coding schema parameter n (distributed raid), default: 1', type=int, default=1, dest='distr_npcs', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--distr-bs', help='(Dev) distrb bdev block size, default: 4096', type=int, default=4096, dest='distr_bs', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--distr-chunk-bs', help='(Dev) distrb bdev chunk block size, default: 4096', type=int, default=4096, dest='distr_chunk_bs', required=False)
        argument = subcommand.add_argument('--ha-type', help='Logical volume HA type (single, ha), default is cluster single type', type=str, default='ha', dest='ha_type', required=False, choices=['single','ha',])
        argument = subcommand.add_argument('--enable-node-affinity', help='Enables node affinity for storage nodes', dest='enable_node_affinity', required=False, action='store_true')
        argument = subcommand.add_argument('--qpair-count', help='NVMe/TCP transport qpair count per logical volume', type=int, default=0, dest='qpair_count', required=False, choices=range(0, 128))
        if self.developer_mode:
            argument = subcommand.add_argument('--max-queue-size', help='The max size the queue will grow', type=int, default=128, dest='max_queue_size', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--inflight-io-threshold', help='The number of inflight IOs allowed before the IO queuing starts', type=int, default=4, dest='inflight_io_threshold', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--enable-qos', help='Enable qos bdev for storage nodes, default: true', type=bool, default=True, dest='enable_qos', required=False)
        argument = subcommand.add_argument('--strict-node-anti-affinity', help='Enable strict node anti affinity for storage nodes. Never more than one chunk is placed on a node. This requires a minimum of _data-chunks-in-stripe + parity-chunks-in-stripe + 1_ nodes in the cluster."', dest='strict_node_anti_affinity', required=False, action='store_true')
        # toggle disaster recovery status
        sub_command = self.add_sub_command(subparser, 'toggle-disaster-recovery-status', 'Toggle AZ disaster recovery status')
        sub_command.add_argument("cluster_id", help='the cluster UUID').completer = self._completer_get_cluster_list
        sub_command.add_argument("--disaster-recovery", help="AZ disaster recovery status", type=bool)

        # Activate cluster
        sub_command = self.add_sub_command(subparser, 'activate', 'Create distribs and raid0 bdevs on all the storage node and move the cluster to active state')
        sub_command.add_argument("cluster_id", help='the cluster UUID').completer = self._completer_get_cluster_list
        sub_command.add_argument("--force", help='Force recreate distr and lv stores', required=False, action='store_true')
        sub_command.add_argument("--force-lvstore-create", help='Force recreate lv stores', required=False, action='store_true', dest='force_lvstore_create')

    def init_cluster__activate(self, subparser):
        subcommand = self.add_sub_command(subparser, 'activate', 'Activates a cluster.')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list
        argument = subcommand.add_argument('--force', help='Force recreate distr and lv stores', dest='force', required=False, action='store_true')
        argument = subcommand.add_argument('--force-lvstore-create', help='Force recreate lv stores', dest='force_lvstore_create', required=False, action='store_true').completer = self._completer_get_cluster_list

    def init_cluster__list(self, subparser):
        subcommand = self.add_sub_command(subparser, 'list', 'Shows the cluster list')

    def init_cluster__status(self, subparser):
        subcommand = self.add_sub_command(subparser, 'status', 'Shows a cluster\'s status')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list

    def init_cluster__show(self, subparser):
        subcommand = self.add_sub_command(subparser, 'show', 'Shows a cluster\'s statistics')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list

    def init_cluster__get(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get', 'Gets a cluster\'s information')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list

    def init_cluster__get_capacity(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-capacity', 'Gets a cluster\'s capacity')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list
        argument = subcommand.add_argument('--json', help='Print json output', dest='json', required=False, action='store_true')
        argument = subcommand.add_argument('--history', help='(XXdYYh), list history records (one for every 15 minutes) for XX days and YY hours (up to 10 days in total).', type=str, dest='history', required=False)

    def init_cluster__get_io_stats(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-io-stats', 'Gets a cluster\'s I/O statistics')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list
        argument = subcommand.add_argument('--records', help='Number of records, default: 20', type=int, default=20, dest='records', required=False)
        argument = subcommand.add_argument('--history', help='(XXdYYh), list history records (one for every 15 minutes) for XX days and YY hours (up to 10 days in total).', type=str, dest='history', required=False)

    def init_cluster__get_logs(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-logs', 'Returns a cluster\'s status logs')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list
        argument = subcommand.add_argument('--limit', help='show last number of logs, default 50', type=int, default=50, dest='limit', required=False)

    def init_cluster__get_secret(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-secret', 'Gets a cluster\'s secret')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list

    def init_cluster__update_secret(self, subparser):
        subcommand = self.add_sub_command(subparser, 'update-secret', 'Updates a cluster\'s secret')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list
        subcommand.add_argument('secret', help='new 20 characters password', type=str)

    def init_cluster__check(self, subparser):
        subcommand = self.add_sub_command(subparser, 'check', 'Checks a cluster\'s health')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list

    def init_cluster__update(self, subparser):
        subcommand = self.add_sub_command(subparser, 'update', 'Updates a cluster to new version')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list
        argument = subcommand.add_argument('--cp-only', help='Update the control plane only', type=bool, default=False, dest='mgmt_only', required=False)
        argument = subcommand.add_argument('--restart', help='Restart the management services', type=bool, default=False, dest='restart', required=False)
        argument = subcommand.add_argument('--spdk-image', help='Restart the storage nodes using the provided image', type=str, dest='spdk_image', required=False)
        argument = subcommand.add_argument('--mgmt-image', help='Restart the management services using the provided image', type=str, dest='mgmt_image', required=False)

    def init_cluster__graceful_shutdown(self, subparser):
        subcommand = self.add_sub_command(subparser, 'graceful-shutdown', 'Initiates a graceful shutdown of a cluster\'s storage nodes')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list

    def init_cluster__graceful_startup(self, subparser):
        subcommand = self.add_sub_command(subparser, 'graceful-startup', 'Initiates a graceful startup of a cluster\'s storage nodes')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list
        argument = subcommand.add_argument('--clear-data', help='clear Alceml data', dest='clear_data', required=False, action='store_true')
        argument = subcommand.add_argument('--spdk-image', help='SPDK image uri', type=str, dest='spdk_image', required=False)

    def init_cluster__list_tasks(self, subparser):
        subcommand = self.add_sub_command(subparser, 'list-tasks', 'Lists tasks of a cluster')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list
        argument = subcommand.add_argument('--limit', help='show last number of tasks, default 50', type=int, default=50, dest='limit', required=False)

    def init_cluster__cancel_task(self, subparser):
        subcommand = self.add_sub_command(subparser, 'cancel-task', 'Cancels task by task id')
        subcommand.add_argument('task_id', help='Task id', type=str)

    def init_cluster__delete(self, subparser):
        subcommand = self.add_sub_command(subparser, 'delete', 'Deletes a cluster')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list

    def init_cluster__set(self, subparser):
        subcommand = self.add_sub_command(subparser, 'set', 'set cluster db value')
        subcommand.add_argument('cluster_id', help='cluster id', type=str)
        subcommand.add_argument('attr_name', help='attr_name', type=str)
        subcommand.add_argument('attr_value', help='attr_value', type=str)


    def init_volume(self):
        subparser = self.add_command('volume', 'Logical volume commands', aliases=['lvol',])
        self.init_volume__add(subparser)
        self.init_volume__qos_set(subparser)
        self.init_volume__list(subparser)
        if self.developer_mode:
            self.init_volume__list_mem(subparser)
        self.init_volume__get(subparser)
        self.init_volume__delete(subparser)
        self.init_volume__connect(subparser)
        self.init_volume__resize(subparser)
        self.init_volume__create_snapshot(subparser)
        self.init_volume__clone(subparser)
        if self.developer_mode:
            self.init_volume__move(subparser)
        self.init_volume__get_capacity(subparser)
        self.init_volume__get_io_stats(subparser)
        self.init_volume__check(subparser)
        self.init_volume__inflate(subparser)
        #
        # ----------------- lvol -----------------
        #

        subparser = self.add_command('lvol', 'LVol commands')
        # add lvol
        sub_command = self.add_sub_command(subparser, 'add', 'Add a new logical volume')
        sub_command.add_argument("name", help='LVol name or id')
        sub_command.add_argument("size", help='LVol size: 10M, 10G, 10(bytes)')
        sub_command.add_argument("pool", help='Pool UUID or name')
        sub_command.add_argument("--snapshot", "-s", help='Make LVol with snapshot capability, default is False',
                                 required=False, action='store_true')
        sub_command.add_argument("--max-size", help='LVol max size', dest='max_size', default="0")
        sub_command.add_argument("--host-id", help='Primary storage node UUID or Hostname', dest='host_id')

        #
        # sub_command.add_argument("--compress",
        #                          help='Use inline data compression and de-compression on the logical volume',
        #                          required=False, action='store_true')
        sub_command.add_argument("--encrypt", help='Use inline data encryption and de-cryption on the logical volume',
                                 required=False, action='store_true')
        sub_command.add_argument("--crypto-key1", help='the hex value of key1 to be used for lvol encryption',
                                 dest='crypto_key1', default=None)
        sub_command.add_argument("--crypto-key2", help='the hex value of key2 to be used for lvol encryption',
                                 dest='crypto_key2', default=None)
        sub_command.add_argument("--max-rw-iops", help='Maximum Read Write IO Per Second', type=int)
        sub_command.add_argument("--max-rw-mbytes", help='Maximum Read Write Mega Bytes Per Second', type=int)
        sub_command.add_argument("--max-r-mbytes", help='Maximum Read Mega Bytes Per Second', type=int)
        sub_command.add_argument("--max-w-mbytes", help='Maximum Write Mega Bytes Per Second', type=int)
        sub_command.add_argument("--distr-vuid", help='(Dev) set vuid manually, default: random (1-99999)', type=int,
                                 default=0)
        sub_command.add_argument("--ha-type", help='LVol HA type (single, ha), default is cluster HA type',
                                 dest='ha_type', choices=["single", "ha", "default"], default='default')
        sub_command.add_argument("--lvol-priority-class", help='Lvol priority class', type=int, default=0)
        sub_command.add_argument("--is-tiered", help="sends tiered I/O", type=bool, default=False)
        sub_command.add_argument("--force-fetch", help="fetches are forced", type=bool, default=False)
        sub_command.add_argument("--sync-fetch", help="reads require synchronous fetches", type=bool, default=True)
        sub_command.add_argument("--pure-flush-or-evict", help="pure flush or evict", type=bool, default=False)
        sub_command.add_argument("--not-evict-blob-md", help="what to do with blob md", type=int, default=0)
        sub_command.add_argument("--namespace", help='Set LVol namespace for k8s clients')
        sub_command.add_argument("--uid", help='Set LVol UUID')
        sub_command.add_argument("--pvc_name", help='Set LVol PVC name for k8s clients')

        # get blobid
        sub_command = self.add_sub_command(subparser, 'get-lvol-blobid', 'Get the blob ID of an lvol (used for snapshots to back up)')
        sub_command.add_argument("--lvol-name", help="lvol uuid", type=str)

        # snapshot backup
        sub_command = self.add_sub_command(subparser, 'backup-snapshot', 'Asynchronously back up a snapshot')
        sub_command.add_argument("--lvol-name", help="lvol uuid", type=str)
        sub_command.add_argument("--timeout-us", help="secondary stg I/O timeout in us", type=int)
        sub_command.add_argument("--dev-page-size", help="distrib page size in bytes", type=int)
        sub_command.add_argument("--nmax-retries", help="max total retries across all flush jobs", type=int, default=4)
        sub_command.add_argument("--nmax-flush-jobs", help="max parallel flush jobs", type=int, default=4)

        # snapshot backup status
        sub_command = self.add_sub_command(subparser, 'get-snapshot-backup-status', 'Get async snapshot backup status')
        sub_command.add_argument("--lvol-name", help="lvol uuid", type=str)

        # snapshot recovery
        sub_command = self.add_sub_command(subparser, 'recover-snapshot', 'Restore snapshot into the new lvstore after AZ disaster')
        sub_command.add_argument("--lvs-name", help="new lvstore name", type=str)
        sub_command.add_argument("--orig-name", help="user-provided name of the original snapshot", type=str)
        sub_command.add_argument("--orig-uuid", help="uuid of the original snapshot lvol", type=str)
        sub_command.add_argument("--clear-method", help="any valid clear method", type=int)
        sub_command.add_argument("--id-of-blob-to-recover", help="id of the original snapshot blob", type=int)

        # set lvol tiering modes
        sub_command = self.add_sub_command(subparser, 'set-tiering-modes', 'Set tiering modes of an lvol')
        sub_command.add_argument("--is-tiered", help="sends tiered I/O", type=bool)
        sub_command.add_argument("--force-fetch", help="fetches are forced", type=bool)
        sub_command.add_argument("--sync-fetch", help="reads require synchronous fetches", type=bool)
        sub_command.add_argument("--pure-flush-or-evict", help="pure flush or evict", type=bool)
        #sub_command.add_argument("--not-evict-blob-md", help="what to do with blob md", type=int)

        # update page list capacities
        sub_command = self.add_sub_command(subparser, 'set-distr-cache-capacities', 'Set storage tiering page list capacities of a distrib')
        sub_command.add_argument("--lvol-uuid", help="lvol uuid", type=str)
        sub_command.add_argument("--distr-name", help="distrib bdev name", type=str)
        sub_command.add_argument("--ghost-capacity", help="ghost queue capacity", type=str)
        sub_command.add_argument("--fifo-main-capacity", help="main fifo queue capacity", type=str)
        sub_command.add_argument("--fifo-small-capacity", help="small fifo queue capacity", type=str)

        # update secondary I/O timeout value
        sub_command = self.add_sub_command(subparser, 'set-distr-secondary-io-timeout', 'Set secondary stg I/O timeout in us for a distrib')
        sub_command.add_arugment("--lvol-uuid", help="lvol uuid", type=str)
        sub_command.add_argument("--distr-name", help="distrib bdev name", type=str)
        sub_command.add_argument("--secondary-io-timeout-us", help="secondary stg I/O timeout in us", type=int)

    def init_volume__add(self, subparser):
        subcommand = self.add_sub_command(subparser, 'add', 'Adds a new logical volume')
        subcommand.add_argument('name', help='New logical volume name', type=str)
        subcommand.add_argument('size', help='Logical volume size: 10M, 10G, 10(bytes)', type=str)
        subcommand.add_argument('pool', help='Pool id or name', type=str)
        argument = subcommand.add_argument('--snapshot', '-s', help='Make logical volume with snapshot capability, default: false', default=False, dest='snapshot', required=False, action='store_true')
        argument = subcommand.add_argument('--max-size', help='Logical volume max size', type=str, default='1000T', dest='max_size', required=False)
        argument = subcommand.add_argument('--host-id', help='Primary storage node id or Hostname', type=str, dest='host_id', required=False)
        argument = subcommand.add_argument('--encrypt', help='Use inline data encryption and decryption on the logical volume', dest='encrypt', required=False, action='store_true')
        argument = subcommand.add_argument('--crypto-key1', help='Hex value of key1 to be used for logical volume encryption', type=str, dest='crypto_key1', required=False)
        argument = subcommand.add_argument('--crypto-key2', help='Hex value of key2 to be used for logical volume encryption', type=str, dest='crypto_key2', required=False)
        argument = subcommand.add_argument('--max-rw-iops', help='Maximum Read Write IO Per Second', type=int, dest='max_rw_iops', required=False)
        argument = subcommand.add_argument('--max-rw-mbytes', help='Maximum Read Write Megabytes Per Second', type=int, dest='max_rw_mbytes', required=False)
        argument = subcommand.add_argument('--max-r-mbytes', help='Maximum Read Megabytes Per Second', type=int, dest='max_r_mbytes', required=False)
        argument = subcommand.add_argument('--max-w-mbytes', help='Maximum Write Megabytes Per Second', type=int, dest='max_w_mbytes', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--distr-vuid', help='(Dev) set vuid manually, default: random (1-99999)', type=int, dest='distr_vuid', required=False)
        argument = subcommand.add_argument('--ha-type', help='Logical volume HA type (single, ha), default is cluster HA type', type=str, default='default', dest='ha_type', required=False, choices=['single','default','ha',])
        argument = subcommand.add_argument('--lvol-priority-class', help='Logical volume priority class', type=int, default=0, dest='lvol_priority_class', required=False)
        argument = subcommand.add_argument('--namespace', help='Set logical volume namespace for k8s clients', type=str, dest='namespace', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--uid', help='Set logical volume id', type=str, dest='uid', required=False)
        argument = subcommand.add_argument('--pvc-name', '--pvc_name', help='Set logical volume PVC name for k8s clients', type=str, dest='pvc_name', required=False)

    def init_volume__qos_set(self, subparser):
        subcommand = self.add_sub_command(subparser, 'qos-set', 'Changes QoS settings for an active logical volume')
        subcommand.add_argument('volume_id', help='Logical volume id', type=str)
        argument = subcommand.add_argument('--max-rw-iops', help='Maximum Read Write IO Per Second', type=int, dest='max_rw_iops', required=False)
        argument = subcommand.add_argument('--max-rw-mbytes', help='Maximum Read Write Megabytes Per Second', type=int, dest='max_rw_mbytes', required=False)
        argument = subcommand.add_argument('--max-r-mbytes', help='Maximum Read Megabytes Per Second', type=int, dest='max_r_mbytes', required=False)
        argument = subcommand.add_argument('--max-w-mbytes', help='Maximum Write Megabytes Per Second', type=int, dest='max_w_mbytes', required=False)

    def init_volume__list(self, subparser):
        subcommand = self.add_sub_command(subparser, 'list', 'Lists logical volumes')
        argument = subcommand.add_argument('--cluster-id', help='List logical volumes in particular cluster', type=str, dest='cluster_id', required=False)
        argument = subcommand.add_argument('--pool', help='List logical volumes in particular pool id or name', type=str, dest='pool', required=False)
        argument = subcommand.add_argument('--json', help='Print outputs in json format', dest='json', required=False, action='store_true')
        argument = subcommand.add_argument('--all', help='List soft deleted logical volumes', dest='all', required=False, action='store_true')

    def init_volume__list_mem(self, subparser):
        subcommand = self.add_sub_command(subparser, 'list-mem', 'Gets the size and max_size of a logical volume')
        argument = subcommand.add_argument('--json', help='Print outputs in json format', dest='json', required=False, action='store_true')
        argument = subcommand.add_argument('--csv', help='Print outputs in csv format', dest='csv', required=False, action='store_true')

    def init_volume__get(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get', 'Gets the logical volume details')
        subcommand.add_argument('volume_id', help='Logical volume id or name', type=str)
        argument = subcommand.add_argument('--json', help='Print outputs in json format', dest='json', required=False, action='store_true')

    def init_volume__delete(self, subparser):
        subcommand = self.add_sub_command(subparser, 'delete', 'Deletes a logical volume')
        subcommand.add_argument('volume_id', help='Logical volumes id or ids', type=str, nargs='+')
        argument = subcommand.add_argument('--force', help='Force delete logical volume from the cluster', dest='force', required=False, action='store_true')

    def init_volume__connect(self, subparser):
        subcommand = self.add_sub_command(subparser, 'connect', 'Gets the logical volume\'s NVMe/TCP connection string(s)')
        subcommand.add_argument('volume_id', help='Logical volume id', type=str)
        argument = subcommand.add_argument('--ctrl-loss-tmo', help='Control loss timeout for this volume', type=int, dest='ctrl_loss_tmo', required=False)

    def init_volume__resize(self, subparser):
        subcommand = self.add_sub_command(subparser, 'resize', 'Resizes a logical volume')
        subcommand.add_argument('volume_id', help='Logical volume id', type=str)
        subcommand.add_argument('size', help='New logical volume size size: 10M, 10G, 10(bytes)', type=str)

    def init_volume__create_snapshot(self, subparser):
        subcommand = self.add_sub_command(subparser, 'create-snapshot', 'Creates a snapshot from a logical volume')
        subcommand.add_argument('volume_id', help='Logical volume id', type=str)
        subcommand.add_argument('name', help='Snapshot name', type=str)

    def init_volume__clone(self, subparser):
        subcommand = self.add_sub_command(subparser, 'clone', 'Provisions a logical volumes from an existing snapshot')
        subcommand.add_argument('snapshot_id', help='Snapshot id', type=str)
        subcommand.add_argument('clone_name', help='Clone name', type=str)
        argument = subcommand.add_argument('--resize', help='New logical volume size: 10M, 10G, 10(bytes). Can only increase.', type=str, dest='resize', required=False)

    def init_volume__move(self, subparser):
        subcommand = self.add_sub_command(subparser, 'move', 'Moves a full copy of the logical volume between nodes')
        subcommand.add_argument('volume_id', help='Logical volume id', type=str)
        subcommand.add_argument('node_id', help='Destination node id', type=str)
        argument = subcommand.add_argument('--force', help='Force logical volume delete from source node', dest='force', required=False, action='store_true')

    def init_volume__get_capacity(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-capacity', 'Gets a logical volume\'s capacity')
        subcommand.add_argument('volume_id', help='Logical volume id', type=str)
        argument = subcommand.add_argument('--history', help='(XXdYYh), list history records (one for every 15 minutes) for XX days and YY hours (up to 10 days in total).', type=str, dest='history', required=False)

    def init_volume__get_io_stats(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-io-stats', 'Gets a logical volume\'s I/O statistics')
        subcommand.add_argument('volume_id', help='Logical volume id', type=str)
        argument = subcommand.add_argument('--history', help='(XXdYYh), list history records (one for every 15 minutes) for XX days and YY hours (up to 10 days in total).', type=str, dest='history', required=False)
        argument = subcommand.add_argument('--records', help='Number of records, default: 20', type=int, default=20, dest='records', required=False)

    def init_volume__check(self, subparser):
        subcommand = self.add_sub_command(subparser, 'check', 'Checks a logical volume\'s health')
        subcommand.add_argument('volume_id', help='Logical volume id', type=str)

    def init_volume__inflate(self, subparser):
        subcommand = self.add_sub_command(subparser, 'inflate', 'Inflate a logical volume')
        subcommand.add_argument('volume_id', help='Cloned logical volume id', type=str)
        # lvol inflate
        sub_command = self.add_sub_command(subparser, 'inflate', 'Inflate a logical volume',
                                           usage='All unallocated clusters are allocated and copied from the parent or zero filled if not allocated in the parent. '
                                                 'Then all dependencies on the parent are removed.')
        sub_command.add_argument("lvol_id", help='cloned lvol id')   
        # mgmt-node ops
        subparser = self.add_command('mgmt', 'Management node commands')


    def init_control_plane(self):
        subparser = self.add_command('control-plane', 'Control plane commands', aliases=['cp','mgmt',])
        self.init_control_plane__add(subparser)
        self.init_control_plane__list(subparser)
        self.init_control_plane__remove(subparser)


    def init_control_plane__add(self, subparser):
        subcommand = self.add_sub_command(subparser, 'add', 'Adds a control plane to the cluster (local run)')
        subcommand.add_argument('cluster_ip', help='Cluster IP address', type=str)
        subcommand.add_argument('cluster_id', help='Cluster id', type=str)
        subcommand.add_argument('cluster_secret', help='Cluster secret', type=str)
        subcommand.add_argument('ifname', help='Management interface name', type=str)

    def init_control_plane__list(self, subparser):
        subcommand = self.add_sub_command(subparser, 'list', 'Lists all control plane nodes')
        argument = subcommand.add_argument('--json', help='Print outputs in json format', dest='json', required=False, action='store_true')

    def init_control_plane__remove(self, subparser):
        subcommand = self.add_sub_command(subparser, 'remove', 'Removes a control plane node')
        subcommand.add_argument('node_id', help='Control plane node id', type=str)


    def init_storage_pool(self):
        subparser = self.add_command('storage-pool', 'Storage pool commands', aliases=['pool',])
        self.init_storage_pool__add(subparser)
        self.init_storage_pool__set(subparser)
        self.init_storage_pool__list(subparser)
        self.init_storage_pool__get(subparser)
        self.init_storage_pool__delete(subparser)
        self.init_storage_pool__enable(subparser)
        self.init_storage_pool__disable(subparser)
        self.init_storage_pool__get_capacity(subparser)
        self.init_storage_pool__get_io_stats(subparser)


    def init_storage_pool__add(self, subparser):
        subcommand = self.add_sub_command(subparser, 'add', 'Adds a new storage pool')
        subcommand.add_argument('name', help='New pool name', type=str)
        subcommand.add_argument('cluster_id', help='Cluster id', type=str)
        argument = subcommand.add_argument('--pool-max', help='Pool maximum size: 20M, 20G, 0. Default: 0', type=str, default='0', dest='pool_max', required=False)
        argument = subcommand.add_argument('--lvol-max', help='Logical volume maximum size: 20M, 20G, 0. Default: 0', type=str, default='0', dest='lvol_max', required=False)
        argument = subcommand.add_argument('--max-rw-iops', help='Maximum Read Write IO Per Second', type=int, dest='max_rw_iops', required=False)
        argument = subcommand.add_argument('--max-rw-mbytes', help='Maximum Read Write Megabytes Per Second', type=int, dest='max_rw_mbytes', required=False)
        argument = subcommand.add_argument('--max-r-mbytes', help='Maximum Read Megabytes Per Second', type=int, dest='max_r_mbytes', required=False)
        argument = subcommand.add_argument('--max-w-mbytes', help='Maximum Write Megabytes Per Second', type=int, dest='max_w_mbytes', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--has-secret', help='Pool is created with a secret (all further API interactions with the pool and logical volumes in the pool require this secret)', dest='has_secret', required=False, action='store_true')

    def init_storage_pool__set(self, subparser):
        subcommand = self.add_sub_command(subparser, 'set', 'Sets a storage pool\'s attributes')
        subcommand.add_argument('pool_id', help='Pool id', type=str)
        argument = subcommand.add_argument('--pool-max', help='Pool maximum size: 20M, 20G', type=str, dest='pool_max', required=False)
        argument = subcommand.add_argument('--lvol-max', help='Logical volume maximum size: 20M, 20G', type=str, dest='lvol_max', required=False)
        argument = subcommand.add_argument('--max-rw-iops', help='Maximum Read Write IO Per Second', type=int, dest='max_rw_iops', required=False)
        argument = subcommand.add_argument('--max-rw-mbytes', help='Maximum Read Write Megabytes Per Second', type=int, dest='max_rw_mbytes', required=False)
        argument = subcommand.add_argument('--max-r-mbytes', help='Maximum Read Megabytes Per Second', type=int, dest='max_r_mbytes', required=False)
        argument = subcommand.add_argument('--max-w-mbytes', help='Maximum Write Megabytes Per Second', type=int, dest='max_w_mbytes', required=False)

    def init_storage_pool__list(self, subparser):
        subcommand = self.add_sub_command(subparser, 'list', 'Lists all storage pools')
        argument = subcommand.add_argument('--json', help='Print outputs in json format', dest='json', required=False, action='store_true')
        argument = subcommand.add_argument('--cluster-id', help='Cluster id', type=str, dest='cluster_id', required=False)

    def init_storage_pool__get(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get', 'Gets a storage pool\'s details')
        subcommand.add_argument('pool_id', help='Pool id', type=str)
        argument = subcommand.add_argument('--json', help='Print outputs in json format', dest='json', required=False, action='store_true')

    def init_storage_pool__delete(self, subparser):
        subcommand = self.add_sub_command(subparser, 'delete', 'Deletes a storage pool')
        subcommand.add_argument('pool_id', help='Pool id', type=str)

    def init_storage_pool__enable(self, subparser):
        subcommand = self.add_sub_command(subparser, 'enable', 'Set a storage pool\'s status to Active')
        subcommand.add_argument('pool_id', help='Pool id', type=str)

    def init_storage_pool__disable(self, subparser):
        subcommand = self.add_sub_command(subparser, 'disable', 'Sets a storage pool\'s status to Inactive.')
        subcommand.add_argument('pool_id', help='Pool id', type=str)

    def init_storage_pool__get_capacity(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-capacity', 'Gets a storage pool\'s capacity')
        subcommand.add_argument('pool_id', help='Pool id', type=str)

    def init_storage_pool__get_io_stats(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-io-stats', 'Gets a storage pool\'s I/O statistics')
        subcommand.add_argument('pool_id', help='Pool id', type=str)
        argument = subcommand.add_argument('--history', help='(XXdYYh), list history records (one for every 15 minutes) for XX days and YY hours (up to 10 days in total).', type=str, dest='history', required=False)
        argument = subcommand.add_argument('--records', help='Number of records, default: 20', type=int, default=20, dest='records', required=False)


    def init_snapshot(self):
        subparser = self.add_command('snapshot', 'Snapshot commands')
        self.init_snapshot__add(subparser)
        self.init_snapshot__list(subparser)
        self.init_snapshot__delete(subparser)
        self.init_snapshot__clone(subparser)


    def init_snapshot__add(self, subparser):
        subcommand = self.add_sub_command(subparser, 'add', 'Creates a new snapshot')
        subcommand.add_argument('volume_id', help='Logical volume id', type=str)
        subcommand.add_argument('name', help='New snapshot name', type=str)

    def init_snapshot__list(self, subparser):
        subcommand = self.add_sub_command(subparser, 'list', 'Lists all snapshots')
        argument = subcommand.add_argument('--all', help='List soft deleted snapshots', dest='all', required=False, action='store_true')

    def init_snapshot__delete(self, subparser):
        subcommand = self.add_sub_command(subparser, 'delete', 'Deletes a snapshot')
        subcommand.add_argument('snapshot_id', help='Snapshot id', type=str)
        argument = subcommand.add_argument('--force', help='Force remove', dest='force', required=False, action='store_true')

    def init_snapshot__clone(self, subparser):
        subcommand = self.add_sub_command(subparser, 'clone', 'Provisions a new logical volume from an existing snapshot')
        subcommand.add_argument('snapshot_id', help='Snapshot id', type=str)
        subcommand.add_argument('lvol_name', help='Logical volume name', type=str)
        argument = subcommand.add_argument('--resize', help='New logical volume size: 10M, 10G, 10(bytes)', type=str, dest='resize', required=False)


    def init_caching_node(self):
        subparser = self.add_command('caching-node', 'Caching node commands', aliases=['cn',])
        self.init_caching_node__deploy(subparser)
        self.init_caching_node__add_node(subparser)
        self.init_caching_node__list(subparser)
        self.init_caching_node__list_lvols(subparser)
        self.init_caching_node__remove(subparser)
        self.init_caching_node__connect(subparser)
        self.init_caching_node__disconnect(subparser)
        self.init_caching_node__recreate(subparser)
        self.init_caching_node__get_lvol_stats(subparser)


    def init_caching_node__deploy(self, subparser):
        subcommand = self.add_sub_command(subparser, 'deploy', 'Deploys a caching node on this machine (local run)')
        argument = subcommand.add_argument('--ifname', help='Management interface name, e.g. eth0', type=str, dest='ifname', required=False)

    def init_caching_node__add_node(self, subparser):
        subcommand = self.add_sub_command(subparser, 'add-node', 'Adds a new caching node to the cluster')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str)
        subcommand.add_argument('node_ip', help='Node IP address', type=str)
        subcommand.add_argument('ifname', help='Management interface name', type=str)
        argument = subcommand.add_argument('--vcpu-count', help='Number of vCPUs used for SPDK. Remaining CPUs will be used for Linux system, TCP/IP processing, and other workloads. The default on non-Kubernetes hosts is 80%%.', type=int, dest='vcpu_count', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--cpu-mask', help='SPDK app CPU mask, default is all cores found', type=str, dest='spdk_cpu_mask', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--memory', help='SPDK huge memory allocation. By default it will acquire all available huge pages.', type=int, dest='spdk_mem', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--spdk-image', help='SPDK image uri', type=str, dest='spdk_image', required=False)
        argument = subcommand.add_argument('--namespace', help='k8s namespace to deploy on', type=str, dest='namespace', required=False)
        argument = subcommand.add_argument('--multipathing', help='Enable multipathing for logical volume connection, default: on', type=str, default='True', dest='multipathing', required=False, choices=['on','off',])

    def init_caching_node__list(self, subparser):
        subcommand = self.add_sub_command(subparser, 'list', 'Lists all caching nodes')

    def init_caching_node__list_lvols(self, subparser):
        subcommand = self.add_sub_command(subparser, 'list-lvols', 'Lists all connected logical volumes')
        subcommand.add_argument('node_id', help='Caching node id', type=str)

    def init_caching_node__remove(self, subparser):
        subcommand = self.add_sub_command(subparser, 'remove', 'Removes a caching node from the cluster')
        subcommand.add_argument('node_id', help='Caching node id', type=str)
        argument = subcommand.add_argument('--force', help='Force remove', dest='force', required=False, action='store_true')

    def init_caching_node__connect(self, subparser):
        subcommand = self.add_sub_command(subparser, 'connect', 'Connects a logical volume to the caching node')
        subcommand.add_argument('node_id', help='Caching node id', type=str)
        subcommand.add_argument('lvol_id', help='Logical volume id', type=str)

    def init_caching_node__disconnect(self, subparser):
        subcommand = self.add_sub_command(subparser, 'disconnect', 'Disconnects a logical volume from the caching node')
        subcommand.add_argument('node_id', help='Caching node id', type=str)
        subcommand.add_argument('lvol_id', help='Logical volume id', type=str)

    def init_caching_node__recreate(self, subparser):
        subcommand = self.add_sub_command(subparser, 'recreate', 'Recreate a caching node\'s bdevs')
        subcommand.add_argument('node_id', help='Caching node id', type=str)

    def init_caching_node__get_lvol_stats(self, subparser):
        subcommand = self.add_sub_command(subparser, 'get-lvol-stats', 'Gets a logical volume\'s statistics')
        subcommand.add_argument('lvol_id', help='Logical volume id', type=str)
        argument = subcommand.add_argument('--history', help='(XXdYYh), list history records (one for every 15 minutes) for XX days and YY hours (up to 10 days in total).', type=str, dest='history', required=False)


    def run(self):
        args = self.parser.parse_args()
        if args.debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

        ret = ""
        args_dict = args.__dict__
        if args.command in ['storage-node', 'sn']:
            sub_command = args_dict['storage-node']
            if sub_command in ['deploy']:
                ret = self.storage_node__deploy(sub_command, args)
            elif sub_command in ['deploy-cleaner']:
                ret = self.storage_node__deploy_cleaner(sub_command, args)
            elif sub_command in ['add-node']:
                if not self.developer_mode:
                    args.jm_percent = 3
                    args.number_of_distribs = 4
                    args.partition_size = None
                    args.spdk_cpu_mask = None
                    args.spdk_image = None
                    args.spdk_debug = None
                    args.small_bufsize = 0
                    args.large_bufsize = 0
                    args.enable_test_device = None
                    args.enable_ha_jm = True
                    args.id_device_by_nqn = False
                    args.max_snap = 5000
                ret = self.storage_node__add_node(sub_command, args)
            elif sub_command in ['delete']:
                ret = self.storage_node__delete(sub_command, args)
            elif sub_command in ['remove']:
                ret = self.storage_node__remove(sub_command, args)
            elif sub_command in ['list']:
                ret = self.storage_node__list(sub_command, args)
            elif sub_command in ['get']:
                ret = self.storage_node__get(sub_command, args)
            elif sub_command in ['restart']:
                if not self.developer_mode:
                    args.max_snap = 5000
                    args.max_prov = '0'
                    args.spdk_image = None
                    args.reattach_volume = None
                    args.spdk_debug = None
                    args.small_bufsize = 0
                    args.large_bufsize = 0
                ret = self.storage_node__restart(sub_command, args)
            elif sub_command in ['shutdown']:
                ret = self.storage_node__shutdown(sub_command, args)
            elif sub_command in ['suspend']:
                ret = self.storage_node__suspend(sub_command, args)
            elif sub_command in ['resume']:
                ret = self.storage_node__resume(sub_command, args)
            elif sub_command in ['get-io-stats']:
                ret = self.storage_node__get_io_stats(sub_command, args)
            elif sub_command in ['get-capacity']:
                ret = self.storage_node__get_capacity(sub_command, args)
            elif sub_command in ['list-devices']:
                ret = self.storage_node__list_devices(sub_command, args)
            elif sub_command in ['device-testing-mode']:
                if not self.developer_mode:
                    print("This command is private.")
                    ret = False

            if sub_command == "deploy":
                ret = storage_ops.deploy(args.ifname)

            elif sub_command == "deploy-cleaner":
                ret = storage_ops.deploy_cleaner()

            elif sub_command == "add-node":
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

                secondary_stg_name = args.secondary_stg_name
                secondary_io_timeout_us = args.secondary_io_timeout_us
                ghost_capacity = args.ghost_capacity
                fifo_small_capacity = args.fifo_small_capacity
                fifo_main_capacity = args.fifo_main_capacity

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
                    is_secondary_node=args.is_secondary_node,
                    id_device_by_nqn=args.id_device_by_nqn,
                    partition_size=args.partition_size,
                    ha_jm_count=ha_jm_count,
                    secondary_stg_name=secondary_stg_name,
                    secondary_io_timeout_us=secondary_io_timeout_us,
                    ghost_capacity=ghost_capacity,
                    fifo_main_capacity=fifo_small_capacity,
                    fifo_main_capacity=fifo_main_capacity
                )

                return out

            elif sub_command == "list":
                ret = storage_ops.list_storage_nodes(args.json, args.cluster_id)

            elif sub_command == "remove":
                ret = storage_ops.remove_storage_node(args.node_id, args.force_remove)

            elif sub_command == "delete":
                ret = storage_ops.delete_storage_node(args.node_id)

            elif sub_command == "restart":
                node_id = args.node_id

                spdk_image = args.spdk_image
                spdk_debug = args.spdk_debug

                max_lvol = args.max_lvol
                max_snap = args.max_snap
                max_prov = args.max_prov if args.max_prov else 0
                number_of_devices = args.number_of_devices

                small_bufsize = args.small_bufsize
                large_bufsize = args.large_bufsize

                ret = storage_ops.restart_storage_node(
                    node_id, max_lvol, max_snap, max_prov,
                    spdk_image, spdk_debug,
                    small_bufsize, large_bufsize, number_of_devices, node_ip=args.node_ip, force=args.force)

            elif sub_command == "list-devices":
                ret = self.storage_node_list_devices(args)

            elif sub_command == "device-testing-mode":
                ret = device_controller.set_device_testing_mode(args.device_id, args.mode)
            # elif sub_command == "jm-device-testing-mode":
            #     ret = device_controller.set_jm_device_testing_mode(args.device_id, args.mode)

            elif sub_command == "remove-device":
                ret = device_controller.device_remove(args.device_id, args.force)

            elif sub_command == "shutdown":
                ret = storage_ops.shutdown_storage_node(args.node_id, args.force)

            elif sub_command == "suspend":
                ret = storage_ops.suspend_storage_node(args.node_id, args.force)

            elif sub_command == "resume":
                ret = storage_ops.resume_storage_node(args.node_id)

            elif sub_command == "reset-device":
                ret = device_controller.reset_storage_device(args.device_id)

            elif sub_command == "restart-device":
                ret = device_controller.restart_device(args.id)

            elif sub_command == "add-device":
                ret = device_controller.add_device(args.id)

            elif sub_command == "set-failed-device":
                ret = device_controller.device_set_failed(args.id)

            elif sub_command == "get-capacity-device":
                device_id = args.device_id
                history = args.history
                data = device_controller.get_device_capacity(device_id, history)
                if data:
                    ret = utils.print_table(data)
                else:
                    ret = self.storage_node__device_testing_mode(sub_command, args)
            elif sub_command in ['get-device']:
                ret = self.storage_node__get_device(sub_command, args)
            elif sub_command in ['reset-device']:
                ret = self.storage_node__reset_device(sub_command, args)
            elif sub_command in ['restart-device']:
                ret = self.storage_node__restart_device(sub_command, args)
            elif sub_command in ['add-device']:
                ret = self.storage_node__add_device(sub_command, args)
            elif sub_command in ['remove-device']:
                ret = self.storage_node__remove_device(sub_command, args)
            elif sub_command in ['set-failed-device']:
                ret = self.storage_node__set_failed_device(sub_command, args)
            elif sub_command in ['get-capacity-device']:
                ret = self.storage_node__get_capacity_device(sub_command, args)
            elif sub_command in ['get-io-stats-device']:
                ret = self.storage_node__get_io_stats_device(sub_command, args)
            elif sub_command in ['port-list']:
                ret = self.storage_node__port_list(sub_command, args)
            elif sub_command in ['port-io-stats']:
                ret = self.storage_node__port_io_stats(sub_command, args)
            elif sub_command in ['check']:
                ret = self.storage_node__check(sub_command, args)
            elif sub_command in ['check-device']:
                ret = self.storage_node__check_device(sub_command, args)
            elif sub_command in ['info']:
                ret = self.storage_node__info(sub_command, args)
            elif sub_command in ['info-spdk']:
                if not self.developer_mode:
                    print("This command is private.")
                    ret = False
                else:
                    ret = self.storage_node__info_spdk(sub_command, args)
            elif sub_command in ['remove-jm-device']:
                if not self.developer_mode:
                    print("This command is private.")
                    ret = False
                else:
                    ret = self.storage_node__remove_jm_device(sub_command, args)
            elif sub_command in ['restart-jm-device']:
                ret = self.storage_node__restart_jm_device(sub_command, args)
            elif sub_command in ['send-cluster-map']:
                if not self.developer_mode:
                    print("This command is private.")
                    ret = False
                else:
                    ret = self.storage_node__send_cluster_map(sub_command, args)
            elif sub_command in ['get-cluster-map']:
                if not self.developer_mode:
                    print("This command is private.")
                    ret = False
                else:
                    ret = self.storage_node__get_cluster_map(sub_command, args)
            elif sub_command in ['make-primary']:
                ret = self.storage_node__make_primary(sub_command, args)
            elif sub_command in ['dump-lvstore']:
                if not self.developer_mode:
                    print("This command is private.")
                    ret = False
                else:
                    ret = self.storage_node__dump_lvstore(sub_command, args)
            elif sub_command in ['set']:
                if not self.developer_mode:
                    print("This command is private.")
                    ret = False
                else:
                    ret = self.storage_node__set(sub_command, args)
            else:
                self.parser.print_help()

        elif args.command in ['cluster']:
            sub_command = args_dict['cluster']
            if sub_command in ['deploy']:
                if not self.developer_mode:
                    args.ha_jm_count = 3
                    args.enable_qos = True
                    args.blk_size = 512
                    args.page_size = 2097152
                    args.CLI_PASS = None
                    args.distr_bs = 4096
                    args.max_queue_size = 128
                    args.inflight_io_threshold = 4
                    args.jm_percent = 3
                    args.max_snap = 5000
                    args.number_of_distribs = 4
                    args.partition_size = None
                    args.spdk_cpu_mask = None
                    args.spdk_image = None
                    args.spdk_debug = None
                    args.small_bufsize = 0
                    args.large_bufsize = 0
                    args.enable_test_device = None
                    args.enable_ha_jm = False
                    args.lvol_name = 'lvol01'
                    args.lvol_size = '10G'
                    args.pool_name = 'pool01'
                    args.pool_max = '25G'
                    args.snapshot = False
                    args.max_size = '1000G'
                    args.encrypt = None
                    args.distr_vuid = None
                    args.lvol_ha_type = 'ha'
                ret = self.cluster__deploy(sub_command, args)
            elif sub_command in ['create']:
                if not self.developer_mode:
                    args.page_size = 2097152
                    args.CLI_PASS = None
                    args.distr_bs = 4096
                    args.distr_chunk_bs = 4096
                    args.max_queue_size = 128
                    args.inflight_io_threshold = 4
                    args.enable_qos = True
                ret = self.cluster__create(sub_command, args)
            elif sub_command in ['add']:
                if not self.developer_mode:
                    args.page_size = 2097152
                    args.distr_bs = 4096
                    args.distr_chunk_bs = 4096
                    args.max_queue_size = 128
                    args.inflight_io_threshold = 4
                    args.enable_qos = True
                ret = self.cluster__add(sub_command, args)
            elif sub_command in ['activate']:
                ret = self.cluster__activate(sub_command, args)
            elif sub_command in ['list']:
                ret = self.cluster__list(sub_command, args)
            elif sub_command in ['status']:
                ret = self.cluster__status(sub_command, args)
            elif sub_command in ['show']:
                ret = self.cluster__show(sub_command, args)
            elif sub_command in ['get']:
                ret = self.cluster__get(sub_command, args)
            elif sub_command in ['get-capacity']:
                ret = self.cluster__get_capacity(sub_command, args)
            elif sub_command in ['get-io-stats']:
                ret = self.cluster__get_io_stats(sub_command, args)
            elif sub_command in ['get-logs']:
                ret = self.cluster__get_logs(sub_command, args)
            elif sub_command in ['get-secret']:
                ret = self.cluster__get_secret(sub_command, args)
            elif sub_command in ['update-secret']:
                ret = self.cluster__update_secret(sub_command, args)
            elif sub_command in ['check']:
                ret = self.cluster__check(sub_command, args)
            elif sub_command in ['update']:
                ret = self.cluster__update(sub_command, args)
            elif sub_command in ['graceful-shutdown']:
                if not self.developer_mode:
                    print("This command is private.")
                    ret = False
                    return False

            elif sub_command == "port-list":
                node_id = args.node_id
                ret = storage_ops.get_node_ports(node_id)

            elif sub_command == "port-io-stats":
                port_id = args.port_id
                history = args.history
                ret = storage_ops.get_node_port_iostats(port_id, history)

            elif sub_command == "check":
                node_id = args.id
                ret = health_controller.check_node(node_id)

            elif sub_command == "check-device":
                device_id = args.id
                ret = health_controller.check_device(device_id)

            elif sub_command == "info":
                node_id = args.id
                ret = storage_ops.get_info(node_id)

            elif sub_command == "info-spdk":
                node_id = args.id
                ret = storage_ops.get_spdk_info(node_id)
            
            elif sub_command == "s3-bdev-create":
                node_id = args.id
                ret = storage_ops.s3_bdev_create(node_id, args.name, args.bdb_lcpu_mask, args.s3_lcpu_mask, args.s3_thread_pool_size)

            elif sub_command == "s3-bdev-delete":
                node_id = args.id
                ret = storage_ops.s3_bdev_delete(node_id, args.name)
            
            elif sub_command == "s3-bdev-add-bucket-name":
                node_id = args.id
                ret = storage_ops.s3_bdev_add_bucket_name(node_id, args.name, args.bucket_name)

            elif sub_command == "get":
                ret = storage_ops.get(args.id)

            elif sub_command == "remove-jm-device":
                ret = device_controller.remove_jm_device(args.jm_device_id, args.force)

            elif sub_command == "restart-jm-device":
                ret = device_controller.restart_jm_device(args.jm_device_id, args.force)

            elif sub_command == "send-cluster-map":
                id = args.id
                ret = storage_ops.send_cluster_map(id)
            elif sub_command == "get-cluster-map":
                id = args.id
                ret = storage_ops.get_cluster_map(id)
            elif sub_command == "make-primary":
                id = args.id
                ret = storage_ops.make_sec_new_primary(id)
            elif sub_command == "dump-lvstore":
                node_id = args.id
                ret = storage_ops.dump_lvstore(node_id)
            else:
                self.parser.print_help()

        elif args.command == 'cluster':
            sub_command = args_dict[args.command]
            if sub_command == 'create':
                ret = self.cluster_create(args)
            elif sub_command == 'add':
                ret = self.cluster_add(args)
            elif sub_command == 'toggle-disaster-recovery-status':
                ret = self.cluster_toggle_disaster_recovery_status(args)
            elif sub_command == 'activate':
                cluster_id = args.cluster_id
                ret = cluster_ops.cluster_activate(cluster_id, args.force, args.force_lvstore_create)
            elif sub_command == 'status':
                cluster_id = args.cluster_id
                ret = cluster_ops.get_cluster_status(cluster_id)
            elif sub_command == 'show':
                cluster_id = args.cluster_id
                ret = cluster_ops.list_all_info(cluster_id)
            elif sub_command == 'list':
                ret = cluster_ops.list()
            elif sub_command == 'suspend':
                cluster_id = args.cluster_id
                ret = cluster_ops.suspend_cluster(cluster_id)
            elif sub_command == 'unsuspend':
                cluster_id = args.cluster_id
                ret = cluster_ops.unsuspend_cluster(cluster_id)
            elif sub_command == "get-capacity":
                cluster_id = args.cluster_id
                history = args.history
                is_json = args.json
                data = cluster_ops.get_capacity(cluster_id, history, is_json=is_json)
                if is_json:
                    ret = data
                else:
                    ret = self.cluster__graceful_shutdown(sub_command, args)
            elif sub_command in ['graceful-startup']:
                if not self.developer_mode:
                    print("This command is private.")
                    ret = False
                else:
                    ret = self.cluster__graceful_startup(sub_command, args)
            elif sub_command in ['list-tasks']:
                ret = self.cluster__list_tasks(sub_command, args)
            elif sub_command in ['cancel-task']:
                ret = self.cluster__cancel_task(sub_command, args)
            elif sub_command in ['delete']:
                ret = self.cluster__delete(sub_command, args)
            elif sub_command in ['set']:
                if not self.developer_mode:
                    print("This command is private.")
                    ret = False
                else:
                    ret = self.cluster__set(sub_command, args)
            else:
                self.parser.print_help()

        elif args.command in ['volume', 'lvol']:
            sub_command = args_dict['volume']
            if sub_command in ['add']:
                if not self.developer_mode:
                    args.distr_vuid = None
                    args.uid = None
                ret = self.volume__add(sub_command, args)
            elif sub_command in ['qos-set']:
                ret = self.volume__qos_set(sub_command, args)
            elif sub_command in ['list']:
                ret = self.volume__list(sub_command, args)
            elif sub_command in ['list-mem']:
                if not self.developer_mode:
                    print("This command is private.")
                    ret = False
                else:
                    ret = self.volume__list_mem(sub_command, args)
            elif sub_command in ['get']:
                ret = self.volume__get(sub_command, args)
            elif sub_command in ['delete']:
                ret = self.volume__delete(sub_command, args)
            elif sub_command in ['connect']:
                ret = self.volume__connect(sub_command, args)
            elif sub_command in ['resize']:
                ret = self.volume__resize(sub_command, args)
            elif sub_command in ['create-snapshot']:
                ret = self.volume__create_snapshot(sub_command, args)
            elif sub_command in ['clone']:
                ret = self.volume__clone(sub_command, args)
            elif sub_command in ['move']:
                if not self.developer_mode:
                    print("This command is private.")
                    ret = False
        elif args.command == 'lvol':
            sub_command = args_dict[args.command]
            if sub_command == "add":
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
                    uid=args.uid, pvc_name=args.pvc_name, namespace=args.namespace,
                    is_tiered=args.is_tiered, force_fetch=args.force_fetch, sync_fetch=args.sync_fetch, 
                    pure_flush_or_evict=args.pure_flush_or_evict, not_evict_blob_md=args.not_evict_blob_md)
                if results:
                    ret = results
                else:
                    ret = error
            elif sub_command == "add-distr":
                pass
            elif sub_command == 'get-lvol-blobid':
                ret = lvol_controller.get_blobid(args.lvol_name)
            elif sub_command == "backup-snapshot":
                ret = lvol_controller.backup_snapshot(args.lvol_name, args.timeout_us, args.dev_page_size,
                                                      args.nmax_retries, args.nmax_flush_jobs)
            elif sub_command == 'get-snapshot-backup-status':
                ret = lvol_controller.get_snapshot_backup_status(args.lvol_name)
            elif sub_command == 'recover-snapshot':
                ret = lvol_controller.restore_snapshot(args.lvs_name, args.orig_name, args.orig_uuid, 
                                                       args.clear_method, args.id_of_blob_to_recover)
            elif sub_command == 'set-tiering-modes':
                ret = lvol_controller.set_tiering_modes(args.lvol_uuid, args.is_tiered, args.force_fetch, args.sync_fetch, 
                                                        args.pure_flush_or_evict, True)
            elif sub_command == 'set-distr-cache-capacities':
                ret = lvol_controller.set_distr_cache_capacities(args.lvol_uuid, args.distr_name, 
                                                                 args.ghost_capacity, args.fifo_main_capacity, args.fifo_small_capacity)
            elif sub_command == 'set-distr-secondary-io-timeout':
                ret = lvol_controller.set_distr_timeout_us(args.lvol_uuid, args.distr_name, args.secondary_io_timeout_us)
            elif sub_command == "qos-set":
                ret = lvol_controller.set_lvol(
                    args.id, args.max_rw_iops, args.max_rw_mbytes,
                    args.max_r_mbytes, args.max_w_mbytes)
            elif sub_command == "list":
                ret = lvol_controller.list_lvols(args.json, args.cluster_id, args.pool, args.all)
            elif sub_command == "list-mem":
                ret = lvol_controller.list_lvols_mem(args.json, args.csv)
            elif sub_command == "get":
                ret = lvol_controller.get_lvol(args.id, args.json)
            elif sub_command == "delete":
                for id in args.id:
                    force = args.force
                    ret = lvol_controller.delete_lvol(id, force)
            elif sub_command == "connect":
                id = args.id
                data = lvol_controller.connect_lvol(id)
                if data:
                    ret = "\n".join(con['connect'] for con in data)
            elif sub_command == "resize":
                id = args.id
                size = self.parse_size(args.size)
                ret = lvol_controller.resize_lvol(id, size)
            elif sub_command == "create-snapshot":
                id = args.id
                name = args.name
                ret = lvol_controller.create_snapshot(id, name)
            elif sub_command == "clone":
                new_size = 0
                if args.resize:
                    new_size = self.parse_size(args.resize)
                ret = snapshot_controller.clone(args.snapshot_id, args.clone_name, new_size)
            elif sub_command == "get-io-stats":
                id = args.id
                history = args.history
                records = args.records
                data = lvol_controller.get_io_stats(id, history, records_count=records)
                if data:
                    ret = utils.print_table(data)
                else:
                    ret = self.volume__move(sub_command, args)
            elif sub_command in ['get-capacity']:
                ret = self.volume__get_capacity(sub_command, args)
            elif sub_command in ['get-io-stats']:
                ret = self.volume__get_io_stats(sub_command, args)
            elif sub_command in ['check']:
                ret = self.volume__check(sub_command, args)
            elif sub_command in ['inflate']:
                ret = self.volume__inflate(sub_command, args)
            else:
                self.parser.print_help()

        elif args.command in ['control-plane', 'cp', 'mgmt']:
            sub_command = args_dict['control-plane']
            if sub_command in ['add']:
                ret = self.control_plane__add(sub_command, args)
            elif sub_command in ['list']:
                ret = self.control_plane__list(sub_command, args)
            elif sub_command in ['remove']:
                ret = self.control_plane__remove(sub_command, args)
            else:
                self.parser.print_help()

        elif args.command in ['storage-pool', 'pool']:
            sub_command = args_dict['storage-pool']
            if sub_command in ['add']:
                if not self.developer_mode:
                    args.has_secret = None
                ret = self.storage_pool__add(sub_command, args)
            elif sub_command in ['set']:
                ret = self.storage_pool__set(sub_command, args)
            elif sub_command in ['list']:
                ret = self.storage_pool__list(sub_command, args)
            elif sub_command in ['get']:
                ret = self.storage_pool__get(sub_command, args)
            elif sub_command in ['delete']:
                ret = self.storage_pool__delete(sub_command, args)
            elif sub_command in ['enable']:
                ret = self.storage_pool__enable(sub_command, args)
            elif sub_command in ['disable']:
                ret = self.storage_pool__disable(sub_command, args)
            elif sub_command in ['get-capacity']:
                ret = self.storage_pool__get_capacity(sub_command, args)
            elif sub_command in ['get-io-stats']:
                ret = self.storage_pool__get_io_stats(sub_command, args)
            else:
                self.parser.print_help()

        elif args.command in ['snapshot']:
            sub_command = args_dict['snapshot']
            if sub_command in ['add']:
                ret = self.snapshot__add(sub_command, args)
            elif sub_command in ['list']:
                ret = self.snapshot__list(sub_command, args)
            elif sub_command in ['delete']:
                ret = self.snapshot__delete(sub_command, args)
            elif sub_command in ['clone']:
                ret = self.snapshot__clone(sub_command, args)
            else:
                self.parser.print_help()

        elif args.command in ['caching-node', 'cn']:
            sub_command = args_dict['caching-node']
            if sub_command in ['deploy']:
                ret = self.caching_node__deploy(sub_command, args)
            elif sub_command in ['add-node']:
                if not self.developer_mode:
                    args.spdk_cpu_mask = None
                    args.spdk_mem = None
                    args.spdk_image = None
                ret = self.caching_node__add_node(sub_command, args)
            elif sub_command in ['list']:
                ret = self.caching_node__list(sub_command, args)
            elif sub_command in ['list-lvols']:
                ret = self.caching_node__list_lvols(sub_command, args)
            elif sub_command in ['remove']:
                ret = self.caching_node__remove(sub_command, args)
            elif sub_command in ['connect']:
                ret = self.caching_node__connect(sub_command, args)
            elif sub_command in ['disconnect']:
                ret = self.caching_node__disconnect(sub_command, args)
            elif sub_command in ['recreate']:
                ret = self.caching_node__recreate(sub_command, args)
            elif sub_command in ['get-lvol-stats']:
                ret = self.caching_node__get_lvol_stats(sub_command, args)
            else:
                self.parser.print_help()

        else:
            self.parser.print_help()

        if not ret:
            exit(1)

        print(ret)

    def storage_node_list_devices(self, args):
        node_id = args.node_id
        sort = args.sort
        if sort:
            sort = sort[0]
        is_json = args.json
        out = storage_ops.list_storage_devices(node_id, sort, is_json)
        return out

    def cluster_add(self, args):
        page_size_in_blocks = args.page_size
        blk_size = args.blk_size
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

        support_storage_tiering = args.support_storage_tiering
        disaster_recovery = args.disaster_recovery

        return cluster_ops.add_cluster(
            blk_size, page_size_in_blocks, cap_warn, cap_crit, prov_cap_warn, prov_cap_crit,
            distr_ndcs, distr_npcs, distr_bs, distr_chunk_bs, ha_type, enable_node_affinity,
            qpair_count, max_queue_size, inflight_io_threshold, enable_qos, strict_node_anti_affinity,
            support_storage_tiering, disaster_recovery)

    def cluster_toggle_disaster_recovery_status(self, args):
        cluster_id = args.cluster_id
        disaster_recovery = args.disaster_recovery

    def cluster_create(self, args):
        page_size_in_blocks = args.page_size
        blk_size = args.blk_size
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
                one_k = 1000
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


def main():
    utils.init_sentry_sdk()
    cli = CLIWrapper()
    cli.run()
