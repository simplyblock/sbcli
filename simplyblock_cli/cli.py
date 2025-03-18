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
        if self.developer_mode:
            self.init_storage_node__make_primary(subparser)
        if self.developer_mode:
            self.init_storage_node__dump_lvstore(subparser)


    def init_storage_node__deploy(self, subparser):
        subcommand = self.add_sub_command(subparser, 'deploy', 'Prepares a host to be used as a storage node')
        argument = subcommand.add_argument('--ifname', help='Management interface name, e.g. eth0', type=str, dest='ifname', required=False)

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
        argument = subcommand.add_argument('--is-secondary-node', help='Adds as secondary node. A secondary node does not have any disks attached. It is only used for I/O processing in case a primary goes down.', dest='is_secondary_node', required=False, action='store_true')
        argument = subcommand.add_argument('--namespace', help='Kubernetes namespace to deploy on', type=str, dest='namespace', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--id-device-by-nqn', help='Use device nqn to identify it instead of serial number', dest='id_device_by_nqn', required=False, action='store_true')
        if self.developer_mode:
            argument = subcommand.add_argument('--max-snap', help='Max snapshot per storage node', type=int, default=5000, dest='max_snap', required=False)

    def init_storage_node__delete(self, subparser):
        subcommand = self.add_sub_command(subparser, 'delete', 'Deletes a storage node object from the state database.')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list

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
            argument = subcommand.add_argument('--max-size', help='Maximum amount of GB to be utilized on this storage node', type=str, default='', dest='max_prov', required=False)
        argument = subcommand.add_argument('--node-ip', help='Restart Node on new node', type=str, dest='node_ip', required=False)
        argument = subcommand.add_argument('--number-of-devices', help='Number of devices per storage node if it\'s not supported EC2 instance', type=int, dest='number_of_devices', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--spdk-image', help='SPDK image uri', type=str, dest='spdk_image', required=False)
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
        subcommand.add_argument('-s', help='Sort the outputs', type=str)
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
        subcommand = self.add_sub_command(subparser, 'make-primary', 'In case of HA SNode, makes the current node as primary')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list

    def init_storage_node__dump_lvstore(self, subparser):
        subcommand = self.add_sub_command(subparser, 'dump-lvstore', 'Dump lvstore data')
        subcommand.add_argument('node_id', help='Storage node id', type=str).completer = self._completer_get_sn_list


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
            argument = subcommand.add_argument('--enable-qos', help='Enable qos bdev for storage nodes', dest='enable_qos', required=False, action='store_true')
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
            argument = subcommand.add_argument('--grafana-endpoint', help='Endpoint url for Grafana', type=str, default='', dest='grafana_endpoint', required=False)
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
            argument = subcommand.add_argument('--disable-ha-jm', help='Disable HA JM for distrib creation', dest='enable_ha_jm', required=False, action='store_false')
        argument = subcommand.add_argument('--is-secondary-node', help='Adds as secondary node. A secondary node does not have any disks attached. It is only used for I/O processing in case a primary goes down.', dest='is_secondary_node', required=False, action='store_true')
        argument = subcommand.add_argument('--namespace', help='k8s namespace to deploy on', type=str, dest='namespace', required=False)
        argument = subcommand.add_argument('--id-device-by-nqn', help='Use device nqn to identify it instead of serial number', dest='id_device_by_nqn', required=False, action='store_true')
        if self.developer_mode:
            argument = subcommand.add_argument('--lvol-name', help='Logical volume name or id', type=str, default='lvol01', dest='lvol_name', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--lvol-size', help='Logical volume size: 10M, 10G, 10(bytes)', type=str, default='10G', dest='lvol_size', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--pool-name', help='Pool id or name', type=str, default='pool01', dest='pool_name', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--pool-max', help='Pool maximum size: 20M, 20G, 0(default)', type=str, default='25G', dest='pool_max', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--snapshot', help='Make logical volume with snapshot capability, default: false', dest='snapshot', required=False, action='store_true')
        if self.developer_mode:
            argument = subcommand.add_argument('--max-volume-size', help='Logical volume max size', type=str, default='1000G', dest='max_size', required=False)
        argument = subcommand.add_argument('--host-id', help='Primary storage node id or hostname', type=str, dest='host_id', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--encrypt', help='Use inline data encryption and decryption on the logical volume', dest='encrypt', required=False, action='store_true')
        if self.developer_mode:
            argument = subcommand.add_argument('--crypto-key1', help='Hex value of key1 to be used for logical volume encryption', type=str, dest='crypto_key1', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--crypto-key2', help='Hex value of key2 to be used for logical volume encryption', type=str, dest='crypto_key2', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--max-rw-iops', help='Maximum Read Write IO Per Second', type=int, dest='max_rw_iops', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--max-rw-mbytes', help='Maximum Read Write Megabytes Per Second', type=int, dest='max_rw_mbytes', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--max-r-mbytes', help='Maximum Read Megabytes Per Second', type=int, dest='max_r_mbytes', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--max-w-mbytes', help='Maximum Write Megabytes Per Second', type=int, dest='max_w_mbytes', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--distr-vuid', help='(Dev) set vuid manually, default: random (1-99999)', type=int, dest='distr_vuid', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--lvol-ha-type', help='Logical volume HA type (single, ha), default is cluster HA type', type=str, default='default', dest='lvol_ha_type', required=False, choices=['single','default','ha',])
        argument = subcommand.add_argument('--lvol-priority-class', help='Logical volume priority class', type=int, default=0, dest='lvol_priority_class', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--fstype', help='Filesystem type for testing (ext4, xfs)', type=str, default='xfs', dest='fstype', required=False, choices=['ext4','xfs',])

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
            argument = subcommand.add_argument('--enable-qos', help='Enable qos bdev for storage nodes, true by default', type=bool, default=False, dest='enable_qos', required=False)
        argument = subcommand.add_argument('--strict-node-anti-affinity', help='Enable strict node anti affinity for storage nodes. Never more than one chunk is placed on a node. This requires a minimum of _data-chunks-in-stripe + parity-chunks-in-stripe + 1_ nodes in the cluster.', dest='strict_node_anti_affinity', required=False, action='store_true')

    def init_cluster__add(self, subparser):
        subcommand = self.add_sub_command(subparser, 'add', 'Adds a new cluster')
        if self.developer_mode:
            argument = subcommand.add_argument('--page_size', help='The size of a data page in bytes', type=int, default=2097152, dest='page_size', required=False)
        argument = subcommand.add_argument('--cap-warn', help='Capacity warning level in percent, default: 89', type=int, default=89, dest='cap_warn', required=False)
        argument = subcommand.add_argument('--cap-crit', help='Capacity critical level in percent, default: 99', type=int, default=99, dest='cap_crit', required=False)
        argument = subcommand.add_argument('--prov-cap-warn', help='Capacity warning level in percent, default: 250', type=int, default=250, dest='prov_cap_warn', required=False)
        argument = subcommand.add_argument('--prov-cap-crit', help='Capacity critical level in percent, default: 500', type=int, default=500, dest='prov_cap_crit', required=False)
        argument = subcommand.add_argument('--data-chunks-per-stripe', help='Erasure coding schema parameter k (distributed raid), default: 1', type=int, default=0, dest='distr_ndcs', required=False)
        argument = subcommand.add_argument('--parity-chunks-per-stripe', help='Erasure coding schema parameter n (distributed raid), default: 1', type=int, default=0, dest='distr_npcs', required=False)
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
            argument = subcommand.add_argument('--enable-qos', help='Enable qos bdev for storage nodes, default: true', type=bool, default=False, dest='enable_qos', required=False)
        argument = subcommand.add_argument('--strict-node-anti-affinity', help='Enable strict node anti affinity for storage nodes. Never more than one chunk is placed on a node. This requires a minimum of _data-chunks-in-stripe + parity-chunks-in-stripe + 1_ nodes in the cluster."', dest='strict_node_anti_affinity', required=False, action='store_true')

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

    def init_cluster__cancel_task(self, subparser):
        subcommand = self.add_sub_command(subparser, 'cancel-task', 'Cancels task by task id')
        subcommand.add_argument('task_id', help='Task id', type=str)

    def init_cluster__delete(self, subparser):
        subcommand = self.add_sub_command(subparser, 'delete', 'Deletes a cluster')
        subcommand.add_argument('cluster_id', help='Cluster id', type=str).completer = self._completer_get_cluster_list


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


    def init_volume__add(self, subparser):
        subcommand = self.add_sub_command(subparser, 'add', 'Adds a new logical volume')
        subcommand.add_argument('name', help='New logical volume name', type=str)
        subcommand.add_argument('size', help='Logical volume size: 10M, 10G, 10(bytes)', type=str)
        subcommand.add_argument('pool', help='Pool id or name', type=str)
        argument = subcommand.add_argument('--snapshot', help='Make logical volume with snapshot capability, default: false', dest='snapshot', required=False, action='store_true')
        argument = subcommand.add_argument('--max-size', help='Logical volume max size', type=int, dest='max_size', required=False)
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
        argument = subcommand.add_argument('--ha-type', help='Logical volume HA type (single, ha), default is cluster HA type', type=str, default='ha', dest='ha_type', required=False, choices=['single','default','ha',])
        argument = subcommand.add_argument('--lvol-priority-class', help='Logical volume priority class', type=int, default=0, dest='lvol_priority_class', required=False)
        argument = subcommand.add_argument('--namespace', help='Set logical volume namespace for k8s clients', type=str, dest='namespace', required=False)
        if self.developer_mode:
            argument = subcommand.add_argument('--uid', help='Set logical volume id', type=str, dest='uid', required=False)
        argument = subcommand.add_argument('--pvc_name', help='Set logical volume PVC name for k8s clients', type=str, dest='pvc_name', required=False)

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
        subcommand.add_argument('volume_id', help='Logical volumes id or ids', type=str)
        argument = subcommand.add_argument('--force', help='Force delete logical volume from the cluster', dest='force', required=False, action='store_true')

    def init_volume__connect(self, subparser):
        subcommand = self.add_sub_command(subparser, 'connect', 'Gets the logical volume\'s NVMe/TCP connection string(s)')
        subcommand.add_argument('volume_id', help='Logical volume id', type=str)

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
        subcommand.add_argument('snapshot_id', help='Logical volume id', type=str)
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
                    args.max_prov = ''
                    args.spdk_image = None
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
                if not self.developer_mode:
                    print("This command is private.")
                    ret = False
                else:
                    ret = self.storage_node__make_primary(sub_command, args)
            elif sub_command in ['dump-lvstore']:
                if not self.developer_mode:
                    print("This command is private.")
                    ret = False
                else:
                    ret = self.storage_node__dump_lvstore(sub_command, args)
            else:
                self.parser.print_help()

        elif args.command in ['cluster']:
            sub_command = args_dict['cluster']
            if sub_command in ['deploy']:
                if not self.developer_mode:
                    args.ha_jm_count = 3
                    args.enable_qos = None
                    args.blk_size = 512
                    args.page_size = 2097152
                    args.CLI_PASS = None
                    args.grafana_endpoint = ''
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
                    args.encrypt = False
                    args.crypto_key1 = None
                    args.crypto_key2 = None
                    args.max_rw_iops = None
                    args.max_rw_mbytes = None
                    args.max_r_mbytes = None
                    args.max_w_mbytes = None
                    args.distr_vuid = None
                    args.lvol_ha_type = 'default'
                    args.fstype = 'xfs'
                ret = self.cluster__deploy(sub_command, args)
            elif sub_command in ['create']:
                if not self.developer_mode:
                    args.page_size = 2097152
                    args.CLI_PASS = None
                    args.distr_bs = 4096
                    args.distr_chunk_bs = 4096
                    args.max_queue_size = 128
                    args.inflight_io_threshold = 4
                    args.enable_qos = False
                ret = self.cluster__create(sub_command, args)
            elif sub_command in ['add']:
                if not self.developer_mode:
                    args.page_size = 2097152
                    args.distr_bs = 4096
                    args.distr_chunk_bs = 4096
                    args.max_queue_size = 128
                    args.inflight_io_threshold = 4
                    args.enable_qos = False
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
            return False

        print(ret)


def main():
    cli = CLIWrapper()
    cli.run()
