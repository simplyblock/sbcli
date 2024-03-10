import argparse
import logging
import math
import re
import sys

from simplyblock_core import cluster_ops, utils
from simplyblock_core import kv_store
from simplyblock_core import compute_node_ops as compute_ops
from simplyblock_core import storage_node_ops as storage_ops
from simplyblock_core import mgmt_node_ops as mgmt_ops
from simplyblock_core import constants
from simplyblock_core.controllers import pool_controller, lvol_controller, snapshot_controller
from simplyblock_core.controllers import caching_node_controller, health_controller
from simplyblock_core.models.pool import Pool


class CLIWrapper:

    def __init__(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(constants.LOG_LEVEL)
        self.db_store = kv_store.KVStore()
        self.init_parser()

        # storage-node command
        subparser = self.add_command('storage-node', 'Storage node commands', aliases=['sn'])
        # Add storage node
        sub_command = self.add_sub_command(subparser, "deploy", 'Deploy local services for remote ops (local run)')
        sub_command.add_argument("--ifname", help='Management interface name, default: eth0')

        sub_command = self.add_sub_command(subparser, "deploy-cleaner", 'clean local deploy (local run)')

        # sub_command = self.add_sub_command(subparser, "add", 'Add storage node')
        # sub_command.add_argument("cluster_id", help='UUID of the cluster to which the node will belong')
        # sub_command.add_argument("ifname", help='Management interface name')
        # sub_command.add_argument("--data-nics", help='Data interface names', nargs='+', dest='data_nics')

        sub_command = self.add_sub_command(subparser, "add-node", 'Add storage node by ip')
        sub_command.add_argument("cluster_id", help='UUID of the cluster to which the node will belong')
        sub_command.add_argument("node_ip", help='IP of storage node to add')
        sub_command.add_argument("ifname", help='Management interface name')
        sub_command.add_argument("--data-nics", help='Data interface names', nargs='+', dest='data_nics')
        sub_command.add_argument("--cpu-mask", help='SPDK app CPU mask, default is all cores found',
                                 dest='spdk_cpu_mask')
        sub_command.add_argument("--memory", help='SPDK huge memory allocation, default is 4G', dest='spdk_mem')
        sub_command.add_argument("--dev-split", help='Split nvme devices by this factor, can be 2 or more',
                                 dest='dev_split', type=int, default=1)
        sub_command.add_argument("--spdk-image", help='SPDK image uri', dest='spdk_image')
        sub_command.add_argument("--spdk-cmd-params", help='Extra params for SPDK app command', nargs='+', dest='cmd_params')

        sub_command.add_argument("--bdev_io_pool_size", help='bdev_set_options param', dest='bdev_io_pool_size',  type=int, default=0)
        sub_command.add_argument("--bdev_io_cache_size", help='bdev_set_options param', dest='bdev_io_cache_size',  type=int, default=0)
        sub_command.add_argument("--iobuf_small_cache_size", help='bdev_set_options param', dest='iobuf_small_cache_size',  type=int, default=0)
        sub_command.add_argument("--iobuf_large_cache_size", help='bdev_set_options param', dest='iobuf_large_cache_size',  type=int, default=0)

        # remove storage node
        sub_command = self.add_sub_command(subparser, "remove", 'Remove storage node')
        sub_command.add_argument("node_id", help='UUID of storage node')
        sub_command.add_argument("--force-remove", help='Force remove all LVols and snapshots',
                                 dest='force_remove', required=False, action='store_true')
        sub_command.add_argument("--force-migrate", help='Force migrate All LVols to other nodes',
                                 dest='force_migrate', required=False, action='store_true')
        # List all storage nodes
        sub_command = self.add_sub_command(subparser, "list", 'List storage nodes')
        sub_command.add_argument("--cluster-id", help='id of the cluster for which nodes are listed')
        sub_command.add_argument("--json", help='Print outputs in json format', action='store_true')

        # Restart storage node
        sub_command = self.add_sub_command(
            subparser, "restart", 'Restart a storage node. All functions and device drivers will be reset. '
                                  'During restart, the node does not accept IO. In a high-availability setup, '
                                  'this will not impact operations.')
        sub_command.add_argument("node_id", help='UUID of storage node')
        sub_command.add_argument("--cpu-mask", help='SPDK app CPU mask, default is all cores found', dest='spdk_cpu_mask')
        sub_command.add_argument("--memory", help='SPDK huge memory allocation, default is 4G', dest='spdk_mem')
        sub_command.add_argument("--spdk-image", help='SPDK image uri', dest='spdk_image')
        sub_command.add_argument("--spdk-cmd-params", help='Extra params for SPDK app command', nargs='+', dest='cmd_params')

        sub_command.add_argument("--bdev_io_pool_size", help='bdev_set_options param', dest='bdev_io_pool_size',  type=int, default=0)
        sub_command.add_argument("--bdev_io_cache_size", help='bdev_set_options param', dest='bdev_io_cache_size',  type=int, default=0)
        sub_command.add_argument("--iobuf_small_cache_size", help='bdev_set_options param', dest='iobuf_small_cache_size',  type=int, default=0)
        sub_command.add_argument("--iobuf_large_cache_size", help='bdev_set_options param', dest='iobuf_large_cache_size',  type=int, default=0)

        # sub_command.add_argument("-t", '--test', help='Run smart test on the NVMe devices', action='store_true')

        # Shutdown storage node
        sub_command = self.add_sub_command(
            subparser, "shutdown", 'Shutdown a storage node. Once the command is issued, the node will stop accepting '
                                   'IO,but IO, which was previously received, will still be processed. '
                                   'In a high-availability setup, this will not impact operations.')
        sub_command.add_argument("node_id", help='UUID of storage node')
        sub_command.add_argument("--force", help='Force node shutdown', required=False, action='store_true')

        # Suspend storage node
        sub_command = self.add_sub_command(
            subparser, "suspend", 'Suspend a storage node. The node will stop accepting new IO, but will finish '
                                  'processing any IO, which has been received already.')
        sub_command.add_argument("node_id", help='UUID of storage node')
        sub_command.add_argument("--force", help='Force node suspend', required=False, action='store_true')

        # Resume storage node
        sub_command = self.add_sub_command(subparser, "resume", 'Resume a storage node')
        sub_command.add_argument("node_id", help='UUID of storage node')

        sub_command = self.add_sub_command(subparser, "get-io-stats", 'Returns the current io statistics of a node')
        sub_command.add_argument("node_id", help='Node ID')
        sub_command.add_argument("--history", help='list history records -one for every 15 minutes- '
                                                   'for XX days and YY hours -up to 10 days in total-, format: XXdYYh')

        sub_command = self.add_sub_command(
            subparser, 'get-capacity', 'Returns the size, absolute and relative utilization of the node in bytes')
        sub_command.add_argument("node_id", help='Node ID')
        sub_command.add_argument("--history", help='list history records -one for every 15 minutes- '
                                                   'for XX days and YY hours -up to 10 days in total-, format: XXdYYh')

        # List storage devices of the storage node
        sub_command = self.add_sub_command(subparser, "list-devices", 'List storage devices')
        sub_command.add_argument("node_id", help='the node\'s UUID')
        sub_command.add_argument(
            "-s", '--sort', help='Sort the outputs', required=False, nargs=1, choices=['node-seq', 'dev-seq', 'serial'])
        sub_command.add_argument(
            "--json", help='Print outputs in json format', required=False, action='store_true')

        sub_command = self.add_sub_command(subparser, "device-testing-mode", 'set pt testing bdev mode')
        sub_command.add_argument("device_id", help='Device UUID')
        sub_command.add_argument("mode", help='Testing mode', choices=[
            'full_pass_trhough', 'io_error_on_read', 'io_error_on_write',
            'io_error_on_unmap', 'io_error_on_all', 'discard_io_all',
            'hotplug_removal'], default='full_pass_trhough')

        sub_command = self.add_sub_command(subparser, "get-device", 'Get storage device by id')
        sub_command.add_argument("device_id", help='the devices\'s UUID')

        # Reset storage device
        sub_command = self.add_sub_command(subparser, "reset-device", 'Reset storage device')
        sub_command.add_argument("device_id", help='the devices\'s UUID')

        # Reset storage device
        sub_command = self.add_sub_command(subparser, "restart-device", 'Re add "removed" storage device')
        sub_command.add_argument("id", help='the devices\'s UUID')

        # run tests against storage device
        sub_command = self.add_sub_command(subparser, 'run-smart', 'Run tests against storage device')
        sub_command.add_argument("device_id", help='the devices\'s UUID')

        # Add a new storage device
        sub_command = self.add_sub_command(subparser, 'add-device', 'Add a new storage device')
        sub_command.add_argument("name", help='Storage device name (as listed in the operating system). '
                                              'The device will be de-attached from the operating system and '
                                              'attached to the storage node')
        sub_command.add_argument("node_id", help='UUID of the node')
        sub_command.add_argument("cluster_id", help='UUID of the cluster')

        # Replace storage node
        # sub_command = self.add_sub_command(
        #     subparser, 'replace', 'Replace a storage node. This command is run on the new physical server, which is '
        #                           'expected to replace the old server. Attention!!! All the nvme devices, '
        #                           'which are part of the cluster to which the node belongs, must be inserted into '
        #                           'the new server before this command is run. The old node will be de-commissioned '
        #                           'and cannot be used any more.')
        # sub_command.add_argument("node_id", help='UUID of the node to be replaced')
        # sub_command.add_argument("ifname", help='Management interface name')
        # sub_command.add_argument("--data-nics", help='Data interface names', nargs='+', dest='data_nics')

        sub_command = self.add_sub_command(
            subparser, 'remove-device', 'Remove a storage device. The device will become unavailable, independently '
                                        'if it was physically removed from the server. This function can be used if '
                                        'auto-detection of removal did not work or if the device must be maintained '
                                        'otherwise while remaining inserted into the server. ')
        sub_command.add_argument("device_id", help='Storage device ID')
        sub_command.add_argument("--force", help='Force device remove', required=False, action='store_true')

        sub_command = self.add_sub_command(subparser, 'set-ro-device', 'Set storage device read only')
        sub_command.add_argument("device_id", help='Storage device ID')

        sub_command = self.add_sub_command(
            subparser, 'set-failed-device', 'Set storage device to failed state. This command can be used, '
                                            'if an administrator believes that the device must be changed, '
                                            'but its status and health state do not lead to an automatic detection '
                                            'of the failure state. Attention!!! The failed state is final, all data '
                                            'on the device will be automatically recovered to other devices '
                                            'in the cluster. ')
        sub_command.add_argument("device_id", help='Storage device ID')

        sub_command = self.add_sub_command(subparser, 'set-online-device', 'Set storage device to online state')
        sub_command.add_argument("device_id", help='Storage device ID')

        sub_command = self.add_sub_command(
            subparser, 'get-capacity-device', 'Returns the size, absolute and relative utilization of '
                                              'the device in bytes')
        sub_command.add_argument("device_id", help='Storage device ID')
        sub_command.add_argument("--history", help='list history records -one for every 15 minutes- '
                                                   'for XX days and YY hours -up to 10 days in total-, format: XXdYYh')

        sub_command = self.add_sub_command(
            subparser, 'get-io-stats-device', 'Returns the io statistics. If --history is not selected, this is '
                                              'a monitor, which updates current statistics records every two seconds '
                                              '(similar to ping):read-iops write-iops total-iops read-mbs '
                                              'write-mbs total-mbs')
        sub_command.add_argument("device_id", help='Storage device ID')
        sub_command.add_argument("--history", help='list history records -one for every 15 minutes- '
                                                   'for XX days and YY hours -up to 10 days in total-, format: XXdYYh')

        sub_command = self.add_sub_command(
            subparser, 'get-event-log', 'Returns storage node event log in syslog format. This includes events from '
                                        'the storage node itself, the network interfaces and all devices on the node, '
                                        'including health status information and updates.')
        sub_command.add_argument("node_id", help='Storage node ID')
        sub_command.add_argument("--from", help='from time, format: dd-mm-yy hh:mm')
        sub_command.add_argument("--to", help='to time, format: dd-mm-yy hh:mm')

        sub_command = self.add_sub_command(
            subparser, 'get-log-page-device', 'Get nvme log-page information from the device. Attention! The '
                                              'availability of particular log pages depends on the device model. '
                                              'For more information, see nvme specification. ')
        sub_command.add_argument("device-id", help='Storage device ID')
        sub_command.add_argument(
            "log-page", help='Can be [error , smart , telemetry , dev-self-test , endurance , persistent-event]',
            choices=["error", "smart", "telemetry", "dev-self-test", "endurance", "persistent-event"])

        sub_command = self.add_sub_command(subparser, 'port-list', 'Get Data interfaces list for a node')
        sub_command.add_argument("node_id", help='Storage node ID')

        sub_command = self.add_sub_command(subparser, 'port-io-stats', 'Get Data interfaces IO stats')
        sub_command.add_argument("port_id", help='Data port ID')
        sub_command.add_argument("--history", help='list history records -one for every 15 minutes- '
                                                   'for XX days and YY hours -up to 10 days in total, format: XXdYYh',
                                 type=int, default=20)

        #  get-host-secret
        sub_command = self.add_sub_command(
            subparser, 'get-host-secret', 'Returns the auto-generated host secret required for the nvmeof '
                                          'connection between host and cluster')
        sub_command.add_argument("id", help='Storage node ID')

        #  get-ctrl-secret
        sub_command = self.add_sub_command(
            subparser, 'get-ctrl-secret', 'Returns the auto-generated controller secret required for the nvmeof '
                                          'connection between host and cluster')
        sub_command.add_argument("id", help='Storage node ID')

        # #  run health ckecks
        # sub_command = self.add_sub_command(subparser, 'health-check', 'Run health checks')
        # sub_command.add_argument("id", help='Storage node ID')

        # check storage node
        sub_command = self.add_sub_command(subparser, "check", 'Health check storage node')
        sub_command.add_argument("id", help='UUID of storage node')

        # check device
        sub_command = self.add_sub_command(subparser, "check-device", 'Health check device')
        sub_command.add_argument("id", help='device UUID')

        # node info
        sub_command = self.add_sub_command(subparser, "info", 'Get node information')
        sub_command.add_argument("id", help='Node UUID')

        # node info-spdk
        sub_command = self.add_sub_command(subparser, "info-spdk", 'Get SPDK memory information')
        sub_command.add_argument("id", help='Node UUID')

        # Initialize cluster parser
        subparser = self.add_command('cluster', 'Cluster commands')

        sub_command = self.add_sub_command(subparser, 'create',
                                           'Create an new cluster with this node as mgmt (local run)')
        sub_command.add_argument(
            "--blk_size", help='The block size in bytes', type=int, choices=[512, 4096], default=512)

        sub_command.add_argument(
            "--page_size", help='The size of a data page in bytes', type=int, default=2097152)

        sub_command.add_argument(
            "--ha_type", help='Can be "single" for single node clusters or  "HA", which requires at least 3 nodes',
            choices=["single", "ha"], default='single')
        sub_command.add_argument(
            "--tls", help='TCP/IP transport security can be turned on and off. '
                          'If turned on, both hosts and storage nodes must '
                          'authenticate the connection via TLS certificates',
            choices=["on", "off"], default='off')
        sub_command.add_argument(
            "--auth-hosts-only", help='if turned on, hosts must be explicitely added to the '
                                      'cluster to be able to connect to any NVMEoF subsystem in the cluster',
            choices=["on", "off"], default='off')
        sub_command.add_argument(
            "--model_ids", help='a list of supported NVMe device model-ids', nargs='+',
            default=['Amazon EC2 NVMe Instance Storage'])
        sub_command.add_argument("--CLI_PASS", help='Password for CLI SSH connection', required=False)
        # cap-warn ( % ), cap-crit ( % ), prov-cap-warn ( % ), prov-cap-crit. ( % )
        sub_command.add_argument("--cap-warn", help='Capacity warning level in percent, default=80',
                                 type=int, required=False, dest="cap_warn")
        sub_command.add_argument("--cap-crit", help='Capacity critical level in percent, default=90',
                                 type=int, required=False, dest="cap_crit")
        sub_command.add_argument("--prov-cap-warn", help='Capacity warning level in percent, default=180',
                                 type=int, required=False, dest="prov_cap_warn")
        sub_command.add_argument("--prov-cap-crit", help='Capacity critical level in percent, default=190',
                                 type=int, required=False, dest="prov_cap_crit")
        sub_command.add_argument("--ifname", help='Management interface name, default: eth0')

        # show cluster list
        sub_command = self.add_sub_command(subparser, 'list', 'Show clusters list')

        # # join cluster
        # sub_command = self.add_sub_command(subparser, 'join', 'join cluster')
        # sub_command.add_argument("cluster_ip", help='the cluster IP address')
        # sub_command.add_argument("cluster_id", help='the cluster UUID')
        # sub_command.add_argument("role", help='Choose the node role in the cluster', choices=["management", "storage", "storage-alloc"])
        # sub_command.add_argument("ifname", help='Management interface name')
        # sub_command.add_argument("--data-nics", help='Data interface names', nargs='+', dest='data_nics')
        # sub_command.add_argument("--cpu-mask", help='SPDK app CPU mask, default is all cores found',  dest='spdk_cpu_mask')
        # sub_command.add_argument("--memory", help='SPDK huge memory allocation, default is 4G',  dest='spdk_mem')

        # show cluster info
        sub_command = self.add_sub_command(
            subparser, 'status', 'Show cluster status')
        sub_command.add_argument("cluster_id", help='the cluster UUID')

        # show cluster info
        sub_command = self.add_sub_command(subparser, 'get', 'Show cluster info')
        sub_command.add_argument("id", help='the cluster UUID')

        sub_command = self.add_sub_command(
            subparser, 'suspend', 'Suspend cluster. The cluster will stop processing all IO. '
                                  'Attention! This will cause an "all paths down" event for nvmeof/iscsi volumes '
                                  'on all hosts connected to the cluster.')
        sub_command.add_argument("cluster_id", help='the cluster UUID')

        sub_command = self.add_sub_command(
            subparser, 'unsuspend', 'Unsuspend cluster. The cluster will start processing IO again.')
        sub_command.add_argument("cluster_id", help='the cluster UUID')

        sub_command = self.add_sub_command(
            subparser, 'add-dev-model', 'Add a device to the white list by the device model id. '
                                        'When adding nodes to the cluster later on, all devices of the specified '
                                        'model-ids, which are present on the node to be added to the cluster, '
                                        'are added to the storage node for the cluster. This does not apply for '
                                        'already added devices, but only affect devices on additional nodes, '
                                        'which will be added to the cluster. It is always possible to also add '
                                        'devices present on a server to a node manually.')
        sub_command.add_argument("cluster_id", help='the cluster UUID')
        sub_command.add_argument("model_ids", help='a list of supported NVMe device model-ids', nargs='+')

        sub_command = self.add_sub_command(
            subparser, 'rm-dev-model', 'Remove device from the white list by the device model id. This does not apply '
                                       'for already added devices, but only affect devices on additional nodes, '
                                       'which will be added to the cluster. ')
        sub_command.add_argument("cluster_id", help='the cluster UUID')
        sub_command.add_argument("model-ids", help='a list of NVMe device model-ids', nargs='+')

        sub_command = self.add_sub_command(
            subparser, 'add-host-auth', 'If the "authorized hosts only" security feature is turned on, '
                                        'hosts must be explicitly added to the cluster via their nqn before '
                                        'they can discover the subsystem initiate a connection.')
        sub_command.add_argument("cluster_id", help='the cluster UUID')
        sub_command.add_argument("host-nqn", help='NQN of the host to allow to discover and connect to teh cluster')

        sub_command = self.add_sub_command(
            subparser, 'rm-host-auth', 'If the "authorized hosts only" security feature is turned on, '
                                       'this function removes hosts, which have been added to the cluster '
                                       'via their nqn, from the list of authorized hosts. After a host has '
                                       'been removed, it cannot connect any longer to the subsystem and cluster.')
        sub_command.add_argument("cluster_id", help='the cluster UUID')
        sub_command.add_argument("host-nqn", help='NQN of the host to remove from the allowed hosts list')

        sub_command = self.add_sub_command(
            subparser, 'get-capacity', 'Returns the current total available capacity, utilized capacity '
                                       '(in percent and absolute) and provisioned capacity (in percent and absolute) '
                                       'in GB in the cluster.')
        sub_command.add_argument("cluster_id", help='the cluster UUID')
        sub_command.add_argument("--history", help='(XXdYYh), list history records (one for every 15 minutes) '
                                                   'for XX days and YY hours (up to 10 days in total).')

        sub_command = self.add_sub_command(
            subparser, 'get-io-stats', 'Returns the io statistics. If --history is not selected, this is a monitor, '
                                       'which updates current statistics records every two seconds '
                                       '(similar to ping):read-iops write-iops total-iops read-mbs write-mbs total-mbs')
        sub_command.add_argument("cluster_id", help='the cluster UUID')
        sub_command.add_argument("--records", help='Number of records, default: 20', type=int, default=20)
        sub_command.add_argument("--history", help='(XXdYYh), list history records (one for every 15 minutes) '
                                                   'for XX days and YY hours (up to 10 days in total).')
        sub_command.add_argument("--random", help='Generate random data', action='store_true')

        sub_command = self.add_sub_command(
            subparser, 'set-log-level', 'Defines the detail of the log information collected and stored')
        sub_command.add_argument("cluster_id", help='the cluster UUID')
        sub_command.add_argument("level", help='Log level', choices=["debug", "test", "prod"])

        # sub_command = self.add_sub_command(
        #     subparser, 'get-event-log', 'returns cluster event log in syslog format')
        # sub_command.add_argument("cluster-id", help='the cluster UUID')
        # sub_command.add_argument("--from", help='from time, format: dd-mm-yy hh:mm')
        # sub_command.add_argument("--to", help='to time, format: dd-mm-yy hh:mm')

        sub_command = self.add_sub_command(
            subparser, 'get-cli-ssh-pass', 'returns the ssh password for the CLI ssh connection')
        sub_command.add_argument("cluster_id", help='the cluster UUID')

        # get-logs
        sub_command = self.add_sub_command(subparser, 'get-logs', 'Returns distr logs')
        sub_command.add_argument("cluster_id", help='cluster uuid')

        # get-secret
        sub_command = self.add_sub_command(subparser, 'get-secret', 'Returns auto generated, 20 characters secret.')
        sub_command.add_argument("cluster_id", help='cluster uuid')

        # set-secret
        sub_command = self.add_sub_command(subparser, 'set-secret', 'Updates the secret (replaces the existing '
                                                                    'one with a new one) and returns the new one.')
        sub_command.add_argument("cluster_id", help='cluster uuid')
        sub_command.add_argument("secret", help='new 20 characters password')

        # check cluster
        sub_command = self.add_sub_command(subparser, "check", 'Health check cluster')
        sub_command.add_argument("id", help='cluster UUID')

        # lvol ops
        subparser = self.add_command('lvol', 'LVol commands')
        # add lvol
        sub_command = self.add_sub_command(subparser, 'add', 'Add a new logical volume')
        sub_command.add_argument("name", help='LVol name or id')
        sub_command.add_argument("size", help='LVol size: 10M, 10G, 10(bytes)')
        sub_command.add_argument("pool", help='Pool UUID or name')
        sub_command.add_argument("--host_id", help='Primary storage node UUID or Hostname')
        sub_command.add_argument("--ha-type", help='LVol HA type (single, ha), default is cluster HA type',
                                 dest='ha_type', choices=["single", "ha", "default"], default='default')

        sub_command.add_argument("--compress",
                                 help='Use inline data compression and de-compression on the logical volume',
                                 required=False, action='store_true')
        sub_command.add_argument("--encrypt", help='Use inline data encryption and de-cryption on the logical volume',
                                 required=False, action='store_true')
        sub_command.add_argument("--thick", help='Deactivate thin provisioning', required=False, action='store_true')
        sub_command.add_argument("--node-ha",
                                 help='The maximum amount of concurrent node failures accepted without interruption of operations',
                                 required=False, default=1, type=int, choices=[0, 1, 2])
        sub_command.add_argument("--dev-redundancy",
                                 help='{1,2} supported minimal concurrent device failures without data loss',
                                 required=False, action='store_true')
        sub_command.add_argument("--max-rw-iops", help='Maximum Read Write IO Per Second', type=int)
        sub_command.add_argument("--max-rw-mbytes", help='Maximum Read Write Mega Bytes Per Second', type=int)
        sub_command.add_argument("--max-r-mbytes", help='Maximum Read Mega Bytes Per Second', type=int)
        sub_command.add_argument("--max-w-mbytes", help='Maximum Write Mega Bytes Per Second', type=int)
        sub_command.add_argument("--distr-vuid", help='(Dev) set vuid manually, default: random (1-99999)', type=int,
                                 default=0)
        sub_command.add_argument("--distr-ndcs", help='(Dev) set ndcs manually, default: 4', type=int, default=0)
        sub_command.add_argument("--distr-npcs", help='(Dev) set npcs manually, default: 1', type=int, default=0)
        sub_command.add_argument("--distr-bs", help='(Dev) distrb bdev block size, default: 4096', type=int,
                                 default=4096)
        sub_command.add_argument("--distr-chunk-bs", help='(Dev) distrb bdev chunk block size, default: 4096', type=int,
                                 default=4096)


        # set lvol params
        sub_command = self.add_sub_command(subparser, 'qos-set', 'Change qos settings for an active logical volume')
        sub_command.add_argument("id", help='LVol id')
        sub_command.add_argument("--max-rw-iops", help='Maximum Read Write IO Per Second', type=int)
        sub_command.add_argument("--max-rw-mbytes", help='Maximum Read Write Mega Bytes Per Second', type=int)
        sub_command.add_argument("--max-r-mbytes", help='Maximum Read Mega Bytes Per Second', type=int)
        sub_command.add_argument("--max-w-mbytes", help='Maximum Write Mega Bytes Per Second', type=int)

        # list lvols
        sub_command = self.add_sub_command(subparser, 'list', 'List all LVols')
        sub_command.add_argument("--cluster-id", help='List LVols in particular cluster')
        sub_command.add_argument("--json", help='Print outputs in json format', required=False, action='store_true')
        # get lvol
        sub_command = self.add_sub_command(subparser, 'get', 'Get LVol details')
        sub_command.add_argument("id", help='LVol id')
        sub_command.add_argument("--json", help='Print outputs in json format', required=False, action='store_true')
        # delete lvol
        sub_command = self.add_sub_command(
            subparser, 'delete', 'Delete LVol. This is only possible, if no more snapshots and non-inflated clones '
                                 'of the volume exist. The volume must be suspended before it can be deleted. ')
        sub_command.add_argument("id", help='LVol id or ids', nargs='+')
        sub_command.add_argument("--force", help='Force delete LVol from the cluster', required=False,
                                 action='store_true')

        # show connection string
        sub_command = self.add_sub_command(
            subparser, 'connect', 'show connection strings to cluster. Multiple connections to the cluster are '
                                  'always available for multi-pathing and high-availability.')
        sub_command.add_argument("id", help='LVol id')

        # lvol resize
        sub_command = self.add_sub_command(
            subparser, 'resize', 'Resize LVol. The lvol cannot be exceed the maximum size for lvols. It cannot '
                                 'exceed total remaining provisioned space in pool. It cannot drop below the '
                                 'current utilization.')
        sub_command.add_argument("id", help='LVol id')
        sub_command.add_argument("size", help='New LVol size size: 10M, 10G, 10(bytes)')

        # lvol set read-only
        sub_command = self.add_sub_command(
            subparser, 'set-read-only', 'Set LVol Read-only. Current write IO in flight will still be processed, '
                                        'but for new IO, only read and unmap IO are possible.')
        sub_command.add_argument("id", help='LVol id')

        # lvol set read-write
        sub_command = self.add_sub_command(subparser, 'set-read-write', 'Set LVol Read-Write.')
        sub_command.add_argument("id", help='LVol id')

        # lvol suspend
        sub_command = self.add_sub_command(
            subparser, 'suspend', 'Suspend LVol. IO in flight will still be processed, but new IO is not accepted. '
                                  'Make sure that the volume is detached from all hosts before '
                                  'suspending it to avoid IO errors.')
        sub_command.add_argument("id", help='LVol id')

        # lvol unsuspend
        sub_command = self.add_sub_command(subparser, 'unsuspend', 'Unsuspend LVol. IO may be resumed.')
        sub_command.add_argument("id", help='LVol id')

        # lvol create-snapshot
        sub_command = self.add_sub_command(subparser, 'create-snapshot', 'Create snapshot from LVol')
        sub_command.add_argument("id", help='LVol id')
        sub_command.add_argument("name", help='snapshot name')

        # lvol clone
        sub_command = self.add_sub_command(subparser, 'clone', 'create LVol based on a snapshot')
        sub_command.add_argument("snapshot_id", help='snapshot UUID')
        sub_command.add_argument("clone_name", help='clone name')

        # lvol move
        sub_command = self.add_sub_command(
            subparser, 'move', 'Moves a full copy of the logical volume between clusters')
        sub_command.add_argument("id", help='LVol id')
        sub_command.add_argument("cluster-id", help='Destination Cluster ID')
        sub_command.add_argument("node-id", help='Destination Node ID')

        # lvol replicate
        sub_command = self.add_sub_command(
            subparser, 'replicate', 'Create a replication path between two volumes in two clusters')
        sub_command.add_argument("id", help='LVol id')
        sub_command.add_argument("cluster-a", help='A Cluster ID')
        sub_command.add_argument("cluster-b", help='B Cluster ID')
        sub_command.add_argument("--asynchronous",
                                 help='Replication may be performed synchronously(default) and asynchronously',
                                 action='store_true')

        # lvol inflate
        sub_command = self.add_sub_command(
            subparser, 'inflate', 'Inflate a clone to "full" logical volume and disconnect it '
                                  'from its parent snapshot.')
        sub_command.add_argument("id", help='LVol id')

        # lvol get-capacity
        sub_command = self.add_sub_command(
            subparser, 'get-capacity', 'Returns the current (or historic) provisioned and utilized '
                                       '(in percent and absolute) capacity.')
        sub_command.add_argument("id", help='LVol id')
        sub_command.add_argument("--history", help='(XXdYYh), list history records (one for every 15 minutes) '
                                                   'for XX days and YY hours (up to 10 days in total).')

        # lvol get-io-stats
        sub_command = self.add_sub_command(
            subparser, 'get-io-stats', 'Returns either the current or historic io statistics '
                                       '(read-IO, write-IO, total-IO, read mbs, write mbs, total mbs).')
        sub_command.add_argument("id", help='LVol id')
        sub_command.add_argument("--history", help='(XXdYYh), list history records (one for every 15 minutes) '
                                                   'for XX days and YY hours (up to 10 days in total).')

        sub_command = self.add_sub_command(subparser, 'send-cluster-map', 'send distr cluster map')
        sub_command.add_argument("id", help='LVol id')

        sub_command = self.add_sub_command(subparser, 'get-cluster-map', 'get distr cluster map')
        sub_command.add_argument("id", help='LVol id')

        # check lvol
        sub_command = self.add_sub_command(subparser, "check", 'Health check LVol')
        sub_command.add_argument("id", help='UUID of LVol')


        # mgmt-node ops
        subparser = self.add_command('mgmt', 'Management node commands')

        sub_command = self.add_sub_command(subparser, 'add', 'Add Management node to the cluster')
        sub_command.add_argument("cluster_ip", help='the cluster IP address')
        sub_command.add_argument("cluster_id", help='the cluster UUID')
        sub_command.add_argument("ifname", help='Management interface name')

        sub_command = self.add_sub_command(subparser, "list", 'List Management nodes')
        sub_command.add_argument("--json", help='Print outputs in json format', action='store_true')

        sub_command = self.add_sub_command(subparser, "remove", 'Remove Management node')
        sub_command.add_argument("hostname", help='hostname')

        sub_command = self.add_sub_command(subparser, 'show', 'List management nodes')
        sub_command = self.add_sub_command(subparser, 'status', 'Show management cluster status')

        # pool ops
        subparser = self.add_command('pool', 'Pool commands')
        # add pool
        sub_command = self.add_sub_command(subparser, 'add', 'Add a new Pool')
        sub_command.add_argument("name", help='Pool name')
        sub_command.add_argument("--pool-max", help='Pool maximum size: 20M, 20G, 0(default)', default="0")
        sub_command.add_argument("--lvol-max", help='LVol maximum size: 20M, 20G, 0(default)', default="0")
        sub_command.add_argument("--max-rw-iops", help='Maximum Read Write IO Per Second', type=int)
        sub_command.add_argument("--max-rw-mbytes", help='Maximum Read Write Mega Bytes Per Second', type=int)
        sub_command.add_argument("--max-r-mbytes", help='Maximum Read Mega Bytes Per Second', type=int)
        sub_command.add_argument("--max-w-mbytes", help='Maximum Write Mega Bytes Per Second', type=int)
        sub_command.add_argument("--has-secret", help='Pool is created with a secret (all further API interactions '
                                                      'with the pool and logical volumes in the '
                                                      'pool require this secret)', required=False, action='store_true')

        # set pool params
        sub_command = self.add_sub_command(subparser, 'set', 'Set pool attributes')
        sub_command.add_argument("id", help='Pool UUID')
        sub_command.add_argument("--pool-max", help='Pool maximum size: 20M, 20G')
        sub_command.add_argument("--lvol-max", help='LVol maximum size: 20M, 20G')
        sub_command.add_argument("--max-rw-iops", help='Maximum Read Write IO Per Second', type=int)
        sub_command.add_argument("--max-rw-mbytes", help='Maximum Read Write Mega Bytes Per Second', type=int)
        sub_command.add_argument("--max-r-mbytes", help='Maximum Read Mega Bytes Per Second', type=int)
        sub_command.add_argument("--max-w-mbytes", help='Maximum Write Mega Bytes Per Second', type=int)

        # list pools
        sub_command = self.add_sub_command(subparser, 'list', 'List pools')
        sub_command.add_argument("--json", help='Print outputs in json format', required=False, action='store_true')
        sub_command.add_argument("--cluster-id", help='ID of the cluster', required=False, action='store_true')
        # get pool
        sub_command = self.add_sub_command(subparser, 'get', 'get pool details')
        sub_command.add_argument("id", help='pool uuid')
        sub_command.add_argument("--json", help='Print outputs in json format', required=False, action='store_true')

        # delete pool
        sub_command = self.add_sub_command(
            subparser, 'delete', 'delete pool. It is only possible to delete a pool if it is empty '
                                 '(no provisioned logical volumes contained).')
        sub_command.add_argument("id", help='pool uuid')

        # enable
        sub_command = self.add_sub_command(subparser, 'enable', 'Set pool status to Active')
        sub_command.add_argument("pool_id", help='pool uuid')
        # disable
        sub_command = self.add_sub_command(
            subparser, 'disable', 'Set pool status to Inactive. Attention! This will suspend all new IO to '
                                  'the pool! IO in flight processing will be completed.')
        sub_command.add_argument("pool_id", help='pool uuid')

        # get-secret
        sub_command = self.add_sub_command(subparser, 'get-secret', 'Returns auto generated, 20 characters secret.')
        sub_command.add_argument("pool_id", help='pool uuid')

        # get-secret
        sub_command = self.add_sub_command(subparser, 'upd-secret', 'Updates the secret (replaces the existing '
                                                                    'one with a new one) and returns the new one.')
        sub_command.add_argument("pool_id", help='pool uuid')
        sub_command.add_argument("secret", help='new 20 characters password')

        # get-capacity
        sub_command = self.add_sub_command(subparser, 'get-capacity', 'Return provisioned, utilized (absolute) '
                                                                      'and utilized (percent) storage on the Pool.')
        sub_command.add_argument("pool_id", help='pool uuid')

        # get-io-stats
        sub_command = self.add_sub_command(
            subparser, 'get-io-stats', 'Returns either the current or historic io statistics '
                                       '(read-IO, write-IO, total-IO, read mbs, write mbs, total mbs).')
        sub_command.add_argument("id", help='Pool id')
        sub_command.add_argument("--history", help='(XXdYYh), list history records (one for every 15 minutes) '
                                                   'for XX days and YY hours (up to 10 days in total).')

        subparser = self.add_command('snapshot', 'Snapshot commands')

        sub_command = self.add_sub_command(subparser, 'add', 'Create new snapshot')
        sub_command.add_argument("id", help='LVol UUID')
        sub_command.add_argument("name", help='snapshot name')

        sub_command = self.add_sub_command(subparser, 'list', 'List snapshots')

        sub_command = self.add_sub_command(subparser, 'delete', 'Delete a snapshot')
        sub_command.add_argument("id", help='snapshot UUID')

        sub_command = self.add_sub_command(subparser, 'clone', 'Create LVol from snapshot')
        sub_command.add_argument("id", help='snapshot UUID')
        sub_command.add_argument("lvol_name", help='LVol name')

        # Caching node cli
        subparser = self.add_command('caching-node', 'Caching client node commands', aliases=['cn'])

        sub_command = self.add_sub_command(subparser, 'deploy', 'Deploy caching node on this machine (local exec)')
        sub_command.add_argument("--ifname", help='Management interface name, default: eth0')

        sub_command = self.add_sub_command(subparser, 'add-node', 'Add new Caching node to the cluster')
        sub_command.add_argument("cluster_id", help='UUID of the cluster to which the node will belong')
        sub_command.add_argument("node_ip", help='IP of caching node to add')
        sub_command.add_argument("ifname", help='Management interface name')
        sub_command.add_argument("--cpu-mask", help='SPDK app CPU mask, default is all cores found',
                                 dest='spdk_cpu_mask')
        sub_command.add_argument("--memory", help='SPDK huge memory allocation, default is Max hugepages available', dest='spdk_mem')
        sub_command.add_argument("--spdk-image", help='SPDK image uri', dest='spdk_image')

        sub_command = self.add_sub_command(subparser, 'list', 'List Caching nodes')

        sub_command = self.add_sub_command(subparser, 'list-lvols', 'List connected lvols')
        sub_command.add_argument("id", help='Caching Node UUID')

        sub_command = self.add_sub_command(subparser, 'remove', 'Remove Caching node from the cluster')
        sub_command.add_argument("id", help='Caching Node UUID')
        sub_command.add_argument("--force", help='Force remove', required=False, action='store_true')

        # sub_command = self.add_sub_command(subparser, 'restart', 'Restart Caching node')
        # sub_command.add_argument("id", help='Caching Node UUID')

        sub_command = self.add_sub_command(subparser, 'connect', 'Connect to LVol')
        sub_command.add_argument("node_id", help='Caching node UUID')
        sub_command.add_argument("lvol_id", help='LVol UUID')

        sub_command = self.add_sub_command(subparser, 'disconnect', 'Disconnect LVol from Caching node')
        sub_command.add_argument("node_id", help='Caching node UUID')
        sub_command.add_argument("lvol_id", help='LVol UUID')

        sub_command = self.add_sub_command(subparser, 'recreate', 'recreate Caching node bdevs ')
        sub_command.add_argument("node_id", help='Caching node UUID')



    def init_parser(self):
        self.parser = argparse.ArgumentParser(description='Ultra management CLI')
        self.parser.add_argument("-d", '--debug', help='Print debug messages', required=False, action='store_true')
        self.subparser = self.parser.add_subparsers(dest='command')

    def add_command(self, command, help, aliases=None):
        aliases = aliases or []
        storagenode = self.subparser.add_parser(command, description=help, help=help, aliases=aliases)
        storagenode_subparser = storagenode.add_subparsers(dest=command)
        return storagenode_subparser

    def add_sub_command(self, parent_parser, command, help):
        return parent_parser.add_parser(command, description=help, help=help)

    def run(self):
        args = self.parser.parse_args()
        if args.debug:
            self.logger.setLevel(logging.DEBUG)
        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

        args_dict = args.__dict__
        ret = ""
        if args.command in ['storage-node', 'sn']:
            sub_command = args_dict['storage-node']

            if sub_command in ['get-log-page-device', 'get-event-log',
                               "get-ctrl-secret", "get-host-secret"]:
                ret = "Not Implemented!"

            elif sub_command == "deploy":
                ret = storage_ops.deploy(args.ifname)

            elif sub_command == "deploy-cleaner":
                ret = storage_ops.deploy_cleaner()

            elif sub_command == "add":
                ret = self.storage_node_add(args)

            elif sub_command == "add-node":
                cluster_id = args.cluster_id
                node_ip = args.node_ip
                ifname = args.ifname
                data_nics = args.data_nics
                dev_split = args.dev_split
                spdk_image = args.spdk_image
                cmd_params = args.cmd_params

                bdev_io_pool_size = args.bdev_io_pool_size
                bdev_io_cache_size = args.bdev_io_cache_size
                iobuf_small_cache_size = args.iobuf_small_cache_size
                iobuf_large_cache_size = args.iobuf_large_cache_size

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

                out = storage_ops.add_node(
                    cluster_id, node_ip, ifname, data_nics, spdk_cpu_mask, spdk_mem, dev_split, spdk_image, cmd_params,
                bdev_io_pool_size, bdev_io_cache_size, iobuf_small_cache_size, iobuf_large_cache_size)
                return out

            elif sub_command == "list":
                ret = storage_ops.list_storage_nodes(self.db_store, args.json)

            elif sub_command == "remove":
                ret = storage_ops.remove_storage_node(args.node_id, args.force_remove, args.force_migrate)

            elif sub_command == "restart":
                node_id = args.node_id

                spdk_image = args.spdk_image
                cmd_params = args.cmd_params

                cpu_mask = None
                if args.spdk_cpu_mask:
                    if self.validate_cpu_mask(args.spdk_cpu_mask):
                        cpu_mask = args.spdk_cpu_mask
                    else:
                        return f"Invalid cpu mask value: {args.spdk_cpu_mask}"

                spdk_mem = None
                if args.spdk_mem:
                    spdk_mem = self.parse_size(args.spdk_mem)
                    if spdk_mem < 1 * 1024 * 1024:
                        return f"SPDK memory:{args.spdk_mem} must be larger than 1G"


                bdev_io_pool_size = args.bdev_io_pool_size
                bdev_io_cache_size = args.bdev_io_cache_size
                iobuf_small_cache_size = args.iobuf_small_cache_size
                iobuf_large_cache_size = args.iobuf_large_cache_size

                ret = storage_ops.restart_storage_node(
                    node_id, cpu_mask, spdk_mem,
                    spdk_image, cmd_params,
                    bdev_io_pool_size, bdev_io_cache_size,
                    iobuf_small_cache_size, iobuf_large_cache_size)

            elif sub_command == "list-devices":
                ret = self.storage_node_list_devices(args)

            elif sub_command == "device-testing-mode":
                ret = storage_ops.set_device_testing_mode(args.device_id, args.mode)

            elif sub_command == "remove-device":
                ret = storage_ops.device_remove(args.device_id, args.force)

            elif sub_command == "shutdown":
                # answer = self.query_yes_no("Are you sure?", default=None)
                # if answer is True:
                ret = storage_ops.shutdown_storage_node(args.node_id, args.force)

            elif sub_command == "suspend":
                ret = storage_ops.suspend_storage_node(args.node_id, args.force)

            elif sub_command == "resume":
                ret = storage_ops.resume_storage_node(args.node_id)

            elif sub_command == "reset-device":
                ret = storage_ops.reset_storage_device(args.device_id)

            elif sub_command == "restart-device":
                ret = storage_ops.restart_device(args.id)

            elif sub_command == "run-smart":
                dev_name = args.name
                ret = storage_ops.run_test_storage_device(self.db_store, dev_name)

            elif sub_command == "add-device":
                dev_name = args.name
                node_id = args.node_id
                cluster_id = args.cluster_id
                ret = storage_ops.add_storage_device(dev_name, node_id, cluster_id)

            elif sub_command == "replace":
                old_node_name = args.name
                ifname = args.ifname
                ret = storage_ops.replace_node(self.db_store, old_node_name, ifname)

            elif sub_command == "get-capacity-device":
                device_id = args.device_id
                history = args.history
                data = storage_ops.get_device_capacity(device_id, history)
                if data:
                    ret = utils.print_table(data)
                else:
                    return False
            elif sub_command == "get-device":
                device_id = args.device_id
                ret = storage_ops.get_device(device_id)

            elif sub_command == "get-io-stats-device":
                device_id = args.device_id
                history = args.history
                data = storage_ops.get_device_iostats(device_id, history)
                if data:
                    ret = utils.print_table(data)
                else:
                    return False
            elif sub_command == "get-capacity":
                node_id = args.node_id
                history = args.history
                data = storage_ops.get_node_capacity(node_id, history)
                if data:
                    ret = utils.print_table(data)
                else:
                    return False

            elif sub_command == "get-io-stats":
                node_id = args.node_id
                history = args.history
                data = storage_ops.get_node_iostats_history(node_id, history)

                if data:
                    ret = utils.print_table(data)
                else:
                    return False

            elif sub_command == "port-list":
                node_id = args.node_id
                ret = storage_ops.get_node_ports(node_id)

            elif sub_command == "port-io-stats":
                port_id = args.port_id
                history = args.history
                ret = storage_ops.get_node_port_iostats(port_id, history)

            elif sub_command == "get-host-secret":
                node_id = args.id
                ret = storage_ops.get_host_secret(node_id)

            elif sub_command == "get-ctrl-secret":
                node_id = args.id
                ret = storage_ops.get_ctrl_secret(node_id)

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

            else:
                self.parser.print_help()

        elif args.command == 'cluster':
            sub_command = args_dict[args.command]
            if sub_command in ["add-dev-model", "rm-dev-model", "add-host-auth",
                               "rm-host-auth", "set-log-level"]:
                ret = "Not Implemented!"
            elif sub_command == 'init':
                ret = self.cluster_init(args)
            elif sub_command == 'create':
                ret = self.cluster_create(args)
            elif sub_command == 'join':
                ret = self.cluster_join(args)
            elif sub_command == 'status':
                cluster_id = args.cluster_id
                ret = cluster_ops.show_cluster(cluster_id)
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
                data = cluster_ops.get_capacity(cluster_id, history)
                if data:
                    ret = utils.print_table(data)
                else:
                    return False

            elif sub_command == "get-io-stats":
                data = cluster_ops.get_iostats_history(args.cluster_id, args.history, args.records)
                if data:
                    ret = utils.print_table(data)
                else:
                    return False
            elif sub_command == "get-cli-ssh-pass":
                cluster_id = args.cluster_id
                ret = cluster_ops.get_ssh_pass(cluster_id)
            elif sub_command == "get-secret":
                cluster_id = args.cluster_id
                ret = cluster_ops.get_secret(cluster_id)
            elif sub_command == "upd-secret":
                cluster_id = args.cluster_id
                secret = args.secret
                ret = cluster_ops.set_secret(cluster_id, secret)
            elif sub_command == "get-logs":
                cluster_id = args.cluster_id
                ret = cluster_ops.get_logs(cluster_id)
            elif sub_command == "check":
                cluster_id = args.id
                ret = health_controller.check_cluster(cluster_id)
            elif sub_command == "get":
                ret = cluster_ops.get_cluster(args.id)
            else:
                self.parser.print_help()

        elif args.command == 'compute-node':
            sub_command = args_dict[args.command]
            if sub_command == "add":
                ret = compute_ops.add_compute_node(self.db_store)
            elif sub_command == "reset":
                ret = compute_ops.reset_compute_node(self.db_store)
            elif sub_command == "remove":
                ret = compute_ops.remove_compute_node(self.db_store)
            elif sub_command == "suspend":
                ret = compute_ops.suspend_compute_node(self.db_store)
            elif sub_command == "resume":
                ret = compute_ops.resume_compute_node(self.db_store)
            elif sub_command == "shutdown":
                ret = compute_ops.shutdown_compute_node(self.db_store)
            elif sub_command == "list":
                ret = compute_ops.list_compute_nodes(self.db_store, args.json)
            else:
                self.parser.print_help()

        elif args.command == 'lvol':
            sub_command = args_dict[args.command]
            if sub_command == "add":
                name = args.name
                size = self.parse_size(args.size)
                host_id = args.host_id
                ha_type = args.ha_type
                pool = args.pool
                comp = args.compress
                crypto = args.encrypt
                distr_vuid = args.distr_vuid
                distr_ndcs = args.distr_ndcs
                distr_npcs = args.distr_npcs
                distr_bs = args.distr_bs
                distr_chunk_bs = args.distr_chunk_bs
                results, error = lvol_controller.add_lvol_ha(
                    name, size, host_id, ha_type, pool, comp, crypto,
                    distr_vuid, distr_ndcs, distr_npcs,
                    args.max_rw_iops,
                    args.max_rw_mbytes,
                    args.max_r_mbytes,
                    args.max_w_mbytes,
                    distr_bs,
                    distr_chunk_bs)
                if results:
                    ret = results
                else:
                    ret = error
            elif sub_command == "add-distr":
                pass
            elif sub_command == "qos-set":
                ret = lvol_controller.set_lvol(
                    args.id, args.max_rw_iops, args.max_rw_mbytes,
                    args.max_r_mbytes, args.max_w_mbytes)
            elif sub_command == "list":
                ret = lvol_controller.list_lvols(args.json)
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
            elif sub_command == "set-read-only":
                id = args.id
                ret = lvol_controller.set_read_only(id)
            elif sub_command == "create-snapshot":
                id = args.id
                name = args.name
                ret = lvol_controller.create_snapshot(id, name)
            elif sub_command == "clone":
                snapshot_id = args.snapshot_id
                clone_name = args.clone_name
                ret = lvol_controller.clone(snapshot_id, clone_name)

            elif sub_command == "get-io-stats":
                id = args.id
                history = args.history
                data = lvol_controller.get_io_stats(id, history)
                if data:
                    ret = utils.print_table(data)
                else:
                    return False
            elif sub_command == "get-capacity":
                id = args.id
                history = args.history
                ret = lvol_controller.get_capacity(id, history)
            elif sub_command == "send-cluster-map":
                id = args.id
                ret = lvol_controller.send_cluster_map(id)
            elif sub_command == "get-cluster-map":
                id = args.id
                ret = lvol_controller.get_cluster_map(id)
            elif sub_command == "check":
                id = args.id
                ret = health_controller.check_lvol(id)
            else:
                self.parser.print_help()

        elif args.command == 'mgmt':
            sub_command = args_dict[args.command]
            if sub_command == "add":
                cluster_id = args.cluster_id
                cluster_ip = args.cluster_ip
                ifname = args.ifname
                ret = cluster_ops.join_cluster(cluster_ip, cluster_id, "management", ifname, [], None, None)
            elif sub_command == "list":
                ret = mgmt_ops.list_mgmt_nodes(args.json)
            elif sub_command == "remove":
                ret = mgmt_ops.remove_mgmt_node(args.hostname)
            elif sub_command == "show":
                ret = mgmt_ops.show_cluster()
            elif sub_command == "status":
                ret = mgmt_ops.cluster_status()
            else:
                self.parser.print_help()

        elif args.command == 'pool':
            sub_command = args_dict[args.command]
            if sub_command == "add":
                ret = pool_controller.add_pool(
                    args.name,
                    self.parse_size(args.pool_max),
                    self.parse_size(args.lvol_max),
                    args.max_rw_iops,
                    args.max_rw_mbytes,
                    args.max_r_mbytes,
                    args.max_w_mbytes,
                    args.has_secret)

            elif sub_command == "set":
                pool_max = None
                lvol_max = None
                if args.pool_max:
                    pool_max = self.parse_size(args.pool_max)
                if args.lvol_max:
                    lvol_max = self.parse_size(args.lvol_max)
                ret = pool_controller.set_pool(
                    args.id,
                    pool_max,
                    lvol_max,
                    args.max_rw_iops,
                    args.max_rw_mbytes,
                    args.max_r_mbytes,
                    args.max_w_mbytes)

            elif sub_command == "get":
                ret = pool_controller.get_pool(args.id, args.json)

            elif sub_command == "list":
                ret = pool_controller.list_pools(args.json)

            elif sub_command == "delete":
                ret = pool_controller.delete_pool(args.id)

            elif sub_command == "enable":
                ret = pool_controller.set_status(args.pool_id, Pool.STATUS_ACTIVE)

            elif sub_command == "disable":
                ret = pool_controller.set_status(args.pool_id, Pool.STATUS_INACTIVE)

            elif sub_command == "get-secret":
                ret = pool_controller.get_secret(args.pool_id)

            elif sub_command == "upd-secret":
                ret = pool_controller.set_secret(args.pool_id, args.secret)

            elif sub_command == "get-capacity":
                ret = pool_controller.get_capacity(args.pool_id)

            elif sub_command == "get-io-stats":
                ret = pool_controller.get_io_stats(args.id, args.history)

            else:
                self.parser.print_help()

        elif args.command == 'snapshot':
            sub_command = args_dict[args.command]
            if sub_command == "add":
                ret = snapshot_controller.add(args.id, args.name)
            if sub_command == "list":
                ret = snapshot_controller.list()
            if sub_command == "delete":
                ret = snapshot_controller.delete(args.id)
            if sub_command == "clone":
                ret = snapshot_controller.clone(args.id, args.lvol_name)

        elif args.command in ['caching-node', 'cn']:
            sub_command = args_dict['caching-node']
            if sub_command == "deploy":
                ret = caching_node_controller.deploy(args.ifname)

            if sub_command == "add-node":
                cluster_id = args.cluster_id
                node_ip = args.node_ip
                ifname = args.ifname
                data_nics = []
                spdk_image = args.spdk_image

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

                ret = caching_node_controller.add_node(
                    cluster_id, node_ip, ifname, data_nics, spdk_cpu_mask, spdk_mem, spdk_image)

            if sub_command == "list":
                #cluster_id
                ret = caching_node_controller.list_nodes()
            if sub_command == "list-lvols":
                ret = caching_node_controller.list_lvols(args.id)
            if sub_command == "remove":
                ret = caching_node_controller.remove_node(args.id, args.force)

            if sub_command == "connect":
                ret = caching_node_controller.connect(args.node_id, args.lvol_id)

            if sub_command == "disconnect":
                ret = caching_node_controller.disconnect(args.node_id, args.lvol_id)

            if sub_command == "recreate":
                ret = caching_node_controller.recreate(args.node_id)


        else:
            self.parser.print_help()

        print(ret)

    def storage_node_list(self, args):
        out = storage_ops.list_storage_nodes(self.db_store, args.json)
        return out

    def storage_node_add(self, args):
        cluster_id = args.cluster_id
        ifname = args.ifname
        data_nics = args.data_nics
        # TODO: Validate the inputs
        out = storage_ops.add_storage_node(cluster_id, ifname, data_nics)
        return out

    def storage_node_list_devices(self, args):
        node_id = args.node_id
        sort = args.sort
        if sort:
            sort = sort[0]
        is_json = args.json
        out = storage_ops.list_storage_devices(self.db_store, node_id, sort, is_json)
        return out

    def cluster_init(self, args):
        page_size_in_blocks = args.page_size_in_blocks
        blk_size = args.blk_size
        model_ids = args.model_ids
        ha_type = args.ha_type
        tls = args.tls == 'on'
        auth_hosts_only = args.auth_hosts_only == 'on'
        dhchap = args.dhchap
        NQN = args.NQN
        iSCSI = args.iSCSI
        CLI_PASS = args.CLI_PASS

        # TODO: Validate the inputs
        return cluster_ops.add_cluster(
            blk_size, page_size_in_blocks, model_ids, ha_type, tls,
            auth_hosts_only, dhchap, NQN, iSCSI, CLI_PASS)

    def cluster_create(self, args):
        page_size_in_blocks = args.page_size
        blk_size = args.blk_size
        ha_type = args.ha_type
        tls = args.tls == 'on'
        auth_hosts_only = args.auth_hosts_only == 'on'
        CLI_PASS = args.CLI_PASS
        model_ids = args.model_ids
        cap_warn = args.cap_warn
        cap_crit = args.cap_crit
        prov_cap_warn = args.prov_cap_warn
        prov_cap_crit = args.prov_cap_crit
        ifname = args.ifname

        # TODO: Validate the inputs
        return cluster_ops.create_cluster(
            blk_size, page_size_in_blocks, ha_type, tls,
            auth_hosts_only, CLI_PASS, model_ids, cap_warn, cap_crit, prov_cap_warn, prov_cap_crit,
            ifname)

    def cluster_join(self, args):
        cluster_id = args.cluster_id
        cluster_ip = args.cluster_ip
        role = args.role
        ifname = args.ifname
        data_nics = args.data_nics
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

        return cluster_ops.join_cluster(cluster_ip, cluster_id, role, ifname, data_nics, spdk_cpu_mask, spdk_mem)

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


def main():
    logger_handler = logging.StreamHandler()
    logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(filename)s:%(lineno)d: %(message)s'))
    logger = logging.getLogger()
    logger.addHandler(logger_handler)

    cli = CLIWrapper()
    cli.run()
