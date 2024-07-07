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
from simplyblock_core.controllers import pool_controller, lvol_controller, snapshot_controller, device_controller, \
    tasks_controller
from simplyblock_core.controllers import caching_node_controller, health_controller
from simplyblock_core.models.pool import Pool


class CLIWrapper:

    def __init__(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(constants.LOG_LEVEL)
        self.db_store = kv_store.KVStore()
        self.init_parser()

        #
        #----------------- storage-node -----------------
        #

        subparser = self.add_command('storage-node', 'Storage node commands', aliases=['sn'])
        # Add storage node
        sub_command = self.add_sub_command(subparser, "deploy", 'Deploy local services for remote ops (local run)')
        sub_command.add_argument("--ifname", help='Management interface name, default: eth0')

        self.add_sub_command(subparser, "deploy-cleaner", 'clean local deploy (local run)')

        sub_command = self.add_sub_command(subparser, "add-node", 'Add storage node by ip')
        sub_command.add_argument("cluster_id", help='UUID of the cluster to which the node will belong')
        sub_command.add_argument("node_ip", help='IP of storage node to add')
        sub_command.add_argument("ifname", help='Management interface name')
        sub_command.add_argument("--partitions", help='Number of partitions to create per device', type=int, default=0)
        sub_command.add_argument("--jm-percent", help='Number in percent to use for JM from each device',
                                 type=int, default=3, dest='jm_percent')
        sub_command.add_argument("--data-nics", help='Data interface names', nargs='+', dest='data_nics')
        sub_command.add_argument("--max-lvol", help='Max lvol per storage node', dest='max_lvol', type=int)
        sub_command.add_argument("--max-snap", help='Max snapshot per storage node', dest='max_snap', type=int)
        sub_command.add_argument("--max-prov", help='Max provisioning size of all storage nodes', dest='max_prov')
        sub_command.add_argument("--number-of-devices", help='Number of devices per storage node if it\'s not supported EC2 instance', dest='number_of_devices', type=int)

        sub_command.add_argument("--spdk-image", help='SPDK image uri', dest='spdk_image')
        sub_command.add_argument("--spdk-debug", help='Enable spdk debug logs', dest='spdk_debug', required=False, action='store_true')

        sub_command.add_argument("--iobuf_small_bufsize", help='bdev_set_options param', dest='small_bufsize',  type=int, default=0)
        sub_command.add_argument("--iobuf_large_bufsize", help='bdev_set_options param', dest='large_bufsize',  type=int, default=0)

        # delete storage node
        sub_command = self.add_sub_command(subparser, "delete", 'Delete storage node obj')
        sub_command.add_argument("node_id", help='UUID of storage node')

        # remove storage node
        sub_command = self.add_sub_command(subparser, "remove", 'Remove storage node')
        sub_command.add_argument("node_id", help='UUID of storage node')
        sub_command.add_argument("--force-remove", help='Force remove all LVols and snapshots',
                                 dest='force_remove', required=False, action='store_true')
        sub_command.add_argument("--force-migrate", help='Force migrate All LVols to other nodes',
                                 dest='force_migrate', required=False, action='store_true')
        # List all storage nodes
        sub_command = self.add_sub_command(subparser, "list", 'List storage nodes')
        sub_command.add_argument("--cluster-id", help='id of the cluster for which nodes are listed', dest='cluster_id')
        sub_command.add_argument("--json", help='Print outputs in json format', action='store_true')

        sub_command = self.add_sub_command(subparser, "get", 'Get storage node info')
        sub_command.add_argument("id", help='UUID of storage node')

        # Restart storage node
        sub_command = self.add_sub_command(
            subparser, "restart", 'Restart a storage node', usage='All functions and device drivers will be reset. '
                                  'During restart, the node does not accept IO. In a high-availability setup, '
                                  'this will not impact operations')
        sub_command.add_argument("node_id", help='UUID of storage node')
        sub_command.add_argument("--max-lvol", help='Max lvol per storage node', dest='max_lvol', type=int, default=0)
        sub_command.add_argument("--max-snap", help='Max snapshot per storage node', dest='max_snap', type=int, default=0)
        sub_command.add_argument("--max-prov", help='Max provisioning size of all storage nodes', dest='max_prov', default="")
        sub_command.add_argument("--number-of-devices", help='Number of devices per storage node if it\'s not supported EC2 instance', dest='number_of_devices', type=int, default=0)

        sub_command.add_argument("--spdk-image", help='SPDK image uri', dest='spdk_image')
        sub_command.add_argument("--spdk-debug", help='Enable spdk debug logs', dest='spdk_debug', required=False, action='store_true')

        sub_command.add_argument("--iobuf_small_bufsize", help='bdev_set_options param', dest='small_bufsize',  type=int, default=0)
        sub_command.add_argument("--iobuf_large_bufsize", help='bdev_set_options param', dest='large_bufsize',  type=int, default=0)

        # sub_command.add_argument("-t", '--test', help='Run smart test on the NVMe devices', action='store_true')

        # Shutdown storage node
        sub_command = self.add_sub_command(
            subparser, "shutdown", 'Shutdown a storage node', usage='Once the command is issued, the node will stop accepting '
                                   'IO,but IO, which was previously received, will still be processed. '
                                   'In a high-availability setup, this will not impact operations.')
        sub_command.add_argument("node_id", help='UUID of storage node')
        sub_command.add_argument("--force", help='Force node shutdown', required=False, action='store_true')

        # Suspend storage node
        sub_command = self.add_sub_command(
            subparser, "suspend", 'Suspend a storage node', usage='The node will stop accepting new IO, but will finish '
                                  'processing any IO, which has been received already.')
        sub_command.add_argument("node_id", help='UUID of storage node')
        sub_command.add_argument("--force", help='Force node suspend', required=False, action='store_true')

        # Resume storage node
        sub_command = self.add_sub_command(subparser, "resume", 'Resume a storage node')
        sub_command.add_argument("node_id", help='UUID of storage node')

        sub_command = self.add_sub_command(subparser, "get-io-stats", 'Get node IO statistics')
        sub_command.add_argument("node_id", help='Node ID')
        sub_command.add_argument("--history", help='list history records -one for every 15 minutes- '
                                                   'for XX days and YY hours -up to 10 days in total-, format: XXdYYh')

        sub_command = self.add_sub_command(
            subparser, 'get-capacity', 'Get node capacity statistics')
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

        sub_command = self.add_sub_command(subparser, "device-testing-mode", 'Set device testing mode')
        sub_command.add_argument("device_id", help='Device UUID')
        sub_command.add_argument("mode", help='Testing mode', choices=[
            'full_pass_through', 'io_error_on_read', 'io_error_on_write',
            'io_error_on_unmap', 'io_error_on_all', 'discard_io_all',
            'hotplug_removal'], default='full_pass_through')

        sub_command = self.add_sub_command(subparser, "get-device", 'Get storage device by id')
        sub_command.add_argument("device_id", help='the devices\'s UUID')

        # Reset storage device
        sub_command = self.add_sub_command(
            subparser, "reset-device", 'Reset storage device',
            usage="Hardware device reset. Resetting the device can return the device from an "
                  "unavailable into online state, if successful")
        sub_command.add_argument("device_id", help='the devices\'s UUID')

        # Reset storage device
        sub_command = self.add_sub_command(subparser, "restart-device", 'Restart storage device',
                                           usage="a previously removed or unavailable device may be returned into "
                                                 "online state. If the device is not physically present, accessible "
                                                 "or healthy, it will flip back into unavailable state again.")
        sub_command.add_argument("id", help='the devices\'s UUID')

        # Add a new storage device
        sub_command = self.add_sub_command(subparser, 'add-device', 'Add a new storage device',
                                           usage="Adding a device will include a previously detected device "
                                                 "(currently in \"new\" state) into cluster and will launch and "
                                                 "auto-rebalancing background process in which some cluster "
                                                 "capacity is re-distributed to this newly added device.")
        sub_command = self.add_sub_command(
            subparser, 'remove-device', 'Remove a storage device', usage='The device will become unavailable, independently '
                                        'if it was physically removed from the server. This function can be used if '
                                        'auto-detection of removal did not work or if the device must be maintained '
                                        'otherwise while remaining inserted into the server. ')
        sub_command.add_argument("device_id", help='Storage device ID')
        sub_command.add_argument("--force", help='Force device remove', required=False, action='store_true')

        # sub_command = self.add_sub_command(
        #     subparser, 'set-failed-device', 'Set storage device to failed state. ', usage='This command can be used, '
        #                                     'if an administrator believes that the device must be changed, '
        #                                     'but its status and health state do not lead to an automatic detection '
        #                                     'of the failure state. Attention!!! The failed state is final, all data '
        #                                     'on the device will be automatically recovered to other devices '
        #                                     'in the cluster. ')

        sub_command = self.add_sub_command(
            subparser, 'get-capacity-device', 'Get device capacity')
        sub_command.add_argument("device_id", help='Storage device ID')
        sub_command.add_argument("--history", help='list history records -one for every 15 minutes- '
                                                   'for XX days and YY hours -up to 10 days in total-, format: XXdYYh')

        sub_command = self.add_sub_command(
            subparser, 'get-io-stats-device', 'Get device IO statistics')
        sub_command.add_argument("device_id", help='Storage device ID')
        sub_command.add_argument("--history", help='list history records -one for every 15 minutes- '
                                                   'for XX days and YY hours -up to 10 days in total-, format: XXdYYh')

        sub_command = self.add_sub_command(subparser, 'port-list', 'Get Data interfaces list for a node')
        sub_command.add_argument("node_id", help='Storage node ID')

        sub_command = self.add_sub_command(subparser, 'port-io-stats', 'Get Data interfaces IO stats')
        sub_command.add_argument("port_id", help='Data port ID')
        sub_command.add_argument("--history", help='list history records -one for every 15 minutes- '
                                                   'for XX days and YY hours -up to 10 days in total, format: XXdYYh')

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

        #
        # ----------------- cluster -----------------
        #

        subparser = self.add_command('cluster', 'Cluster commands')

        sub_command = self.add_sub_command(subparser, 'create',
                                           'Create an new cluster with this node as mgmt (local run)')
        sub_command.add_argument(
            "--blk_size", help='The block size in bytes', type=int, choices=[512, 4096], default=512)

        sub_command.add_argument(
            "--page_size", help='The size of a data page in bytes', type=int, default=2097152)

        sub_command.add_argument("--CLI_PASS", help='Password for CLI SSH connection', required=False)
        sub_command.add_argument("--cap-warn", help='Capacity warning level in percent, default=80',
                                 type=int, required=False, dest="cap_warn")
        sub_command.add_argument("--cap-crit", help='Capacity critical level in percent, default=90',
                                 type=int, required=False, dest="cap_crit")
        sub_command.add_argument("--prov-cap-warn", help='Capacity warning level in percent, default=180',
                                 type=int, required=False, dest="prov_cap_warn")
        sub_command.add_argument("--prov-cap-crit", help='Capacity critical level in percent, default=190',
                                 type=int, required=False, dest="prov_cap_crit")
        sub_command.add_argument("--ifname", help='Management interface name, default: eth0')
        sub_command.add_argument("--log-del-interval", help='graylog deletion interval, default: 7d',
                                 dest='log_del_interval', default='7d')
        sub_command.add_argument("--metrics-retention-period", help='retention period for prometheus metrics, default: 7d',
                                 dest='metrics_retention_period', default='7d')
        sub_command.add_argument("--contact-point", help='the email or slack webhook url to be used for alerting',
                                 dest='contact_point', default='')
        sub_command.add_argument("--grafana-endpoint", help='the endpoint url for grafana',
                                 dest='grafana_endpoint', default='')

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

        # show cluster list
        self.add_sub_command(subparser, 'list', 'Show clusters list')

        # show cluster info
        sub_command = self.add_sub_command(
            subparser, 'status', 'Show cluster status')
        sub_command.add_argument("cluster_id", help='the cluster UUID')

        # show cluster info
        sub_command = self.add_sub_command(subparser, 'get', 'Show cluster info')
        sub_command.add_argument("id", help='the cluster UUID')

        sub_command = self.add_sub_command(
            subparser, 'suspend', 'Suspend cluster')
        sub_command.add_argument("cluster_id", help='the cluster UUID')

        sub_command = self.add_sub_command(
            subparser, 'unsuspend', 'Unsuspend cluster')
        sub_command.add_argument("cluster_id", help='the cluster UUID')

        sub_command = self.add_sub_command(
            subparser, 'get-capacity', 'Get cluster capacity')
        sub_command.add_argument("cluster_id", help='the cluster UUID')
        sub_command.add_argument("--json", help='Print json output', required=False, action='store_true')
        sub_command.add_argument("--history", help='(XXdYYh), list history records (one for every 15 minutes) '
                                                   'for XX days and YY hours (up to 10 days in total).')

        sub_command = self.add_sub_command(
            subparser, 'get-io-stats', 'Get cluster IO statistics')
        sub_command.add_argument("cluster_id", help='the cluster UUID')
        sub_command.add_argument("--records", help='Number of records, default: 20', type=int, default=20)
        sub_command.add_argument("--history", help='(XXdYYh), list history records (one for every 15 minutes) '
                                                   'for XX days and YY hours (up to 10 days in total).')

        # sub_command = self.add_sub_command(
        #     subparser, 'get-cli-ssh-pass', 'returns the ssh password for the CLI ssh connection')
        # sub_command.add_argument("cluster_id", help='the cluster UUID')

        # get-logs
        sub_command = self.add_sub_command(subparser, 'get-logs', 'Returns cluster status logs')
        sub_command.add_argument("cluster_id", help='cluster uuid')

        # get-secret
        sub_command = self.add_sub_command(subparser, 'get-secret', 'Get cluster secret')
        sub_command.add_argument("cluster_id", help='cluster uuid')

        # set-secret
        sub_command = self.add_sub_command(subparser, 'upd-secret', 'Updates the cluster secret')
        sub_command.add_argument("cluster_id", help='cluster uuid')
        sub_command.add_argument("secret", help='new 20 characters password')

        # check cluster
        sub_command = self.add_sub_command(subparser, "check", 'Health check cluster')
        sub_command.add_argument("id", help='cluster UUID')

        # update cluster
        sub_command = self.add_sub_command(subparser, "update", 'Update cluster mgmt services')
        sub_command.add_argument("id", help='cluster UUID')

        # graceful-shutdown storage nodes
        sub_command = self.add_sub_command(subparser, "graceful-shutdown", 'Graceful shutdown of storage nodes')
        sub_command.add_argument("id", help='cluster UUID')

        # graceful-startup storage nodes
        sub_command = self.add_sub_command(subparser, "graceful-startup", 'Graceful startup of storage nodes')
        sub_command.add_argument("id", help='cluster UUID')

        # get tasks list
        sub_command = self.add_sub_command(subparser, "list-tasks", 'List tasks by cluster ID')
        sub_command.add_argument("cluster_id", help='UUID of the cluster')

        # cancel task
        sub_command = self.add_sub_command(subparser, "cancel-task", 'Cancel task by ID')
        sub_command.add_argument("id", help='UUID of the Task')

        # delete cluster
        sub_command = self.add_sub_command(
            subparser, 'delete', 'Delete Cluster',
            usage="This is only possible, if no storage nodes and pools are attached to the cluster")
        sub_command.add_argument("id", help='cluster UUID')


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
        sub_command.add_argument("--ha-type", help='LVol HA type (single, ha), default is cluster HA type',
                                 dest='ha_type', choices=["single", "ha", "default"], default='default')
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
        sub_command = self.add_sub_command(subparser, 'list', 'List LVols')
        sub_command.add_argument("--cluster-id", help='List LVols in particular cluster', dest="cluster_id")
        sub_command.add_argument("--pool", help='List LVols in particular Pool ID or name', dest="pool")
        sub_command.add_argument("--json", help='Print outputs in json format', required=False, action='store_true')
        sub_command.add_argument("--all", help='List soft deleted LVols', required=False, action='store_true')

        # Get the size and max_size of the lvol
        sub_command = self.add_sub_command(subparser, 'list-mem', 'Get the size and max_size of the lvol')
        sub_command.add_argument("--json", help='Print outputs in json format', required=False, action='store_true')
        sub_command.add_argument("--csv", help='Print outputs in csv format', required=False, action='store_true')

        # get lvol
        sub_command = self.add_sub_command(subparser, 'get', 'Get LVol details')
        sub_command.add_argument("id", help='LVol id or name')
        sub_command.add_argument("--json", help='Print outputs in json format', required=False, action='store_true')

        # delete lvol
        sub_command = self.add_sub_command(
            subparser, 'delete', 'Delete LVol', usage='This is only possible, if no more snapshots and non-inflated clones '
                                 'of the volume exist. The volume must be suspended before it can be deleted. ')
        sub_command.add_argument("id", help='LVol id or ids', nargs='+')
        sub_command.add_argument("--force", help='Force delete LVol from the cluster', required=False,
                                 action='store_true')

        # show connection string
        sub_command = self.add_sub_command(
            subparser, 'connect', 'Get lvol connection strings', usage='Multiple connections to the cluster are '
                                  'always available for multi-pathing and high-availability.')
        sub_command.add_argument("id", help='LVol id')

        # lvol resize
        sub_command = self.add_sub_command(
            subparser, 'resize', 'Resize LVol', usage='The lvol cannot be exceed the maximum size for lvols. It cannot '
                                 'exceed total remaining provisioned space in pool. It cannot drop below the '
                                 'current utilization.')
        sub_command.add_argument("id", help='LVol id')
        sub_command.add_argument("size", help='New LVol size size: 10M, 10G, 10(bytes)')

        # lvol create-snapshot
        sub_command = self.add_sub_command(subparser, 'create-snapshot', 'Create snapshot from LVol')
        sub_command.add_argument("id", help='LVol id')
        sub_command.add_argument("name", help='snapshot name')

        # lvol clone
        sub_command = self.add_sub_command(subparser, 'clone', 'create LVol based on a snapshot')
        sub_command.add_argument("snapshot_id", help='snapshot UUID')
        sub_command.add_argument("clone_name", help='clone name')
        sub_command.add_argument("--resize", help='New LVol size: 10M, 10G, 10(bytes)')

        # lvol move
        sub_command = self.add_sub_command(
            subparser, 'move', 'Moves a full copy of the logical volume between nodes')
        sub_command.add_argument("id", help='LVol UUID')
        # sub_command.add_argument("cluster-id", help='Destination Cluster ID')
        sub_command.add_argument("node_id", help='Destination Node UUID')
        sub_command.add_argument("--force", help='Force LVol delete from source node', required=False, action='store_true')

        # lvol get-capacity
        sub_command = self.add_sub_command(
            subparser, 'get-capacity',"Get LVol capacity")
        sub_command.add_argument("id", help='LVol id')
        sub_command.add_argument("--history", help='(XXdYYh), list history records (one for every 15 minutes) '
                                                   'for XX days and YY hours (up to 10 days in total).')

        # lvol get-io-stats
        sub_command = self.add_sub_command(
            subparser, 'get-io-stats', help="Get LVol IO statistics")
        sub_command.add_argument("id", help='LVol id')
        sub_command.add_argument("--history", help='(XXdYYh), list history records (one for every 15 minutes) '
                                                   'for XX days and YY hours (up to 10 days in total).')

        sub_command = self.add_sub_command(subparser, 'send-cluster-map', 'send cluster map')
        sub_command.add_argument("id", help='LVol id')

        sub_command = self.add_sub_command(subparser, 'get-cluster-map', 'get cluster map')
        sub_command.add_argument("id", help='LVol id')

        # check lvol
        sub_command = self.add_sub_command(subparser, "check", 'Health check LVol')
        sub_command.add_argument("id", help='UUID of LVol')


        # mgmt-node ops
        subparser = self.add_command('mgmt', 'Management node commands')

        sub_command = self.add_sub_command(subparser, 'add', 'Add Management node to the cluster (local run)')
        sub_command.add_argument("cluster_ip", help='the cluster IP address')
        sub_command.add_argument("cluster_id", help='the cluster UUID')
        sub_command.add_argument("ifname", help='Management interface name')

        sub_command = self.add_sub_command(subparser, "list", 'List Management nodes')
        sub_command.add_argument("--json", help='Print outputs in json format', action='store_true')

        sub_command = self.add_sub_command(subparser, "remove", 'Remove Management node')
        sub_command.add_argument("id", help='Mgmt node uuid')

        # pool ops
        subparser = self.add_command('pool', 'Pool commands')
        # add pool
        sub_command = self.add_sub_command(subparser, 'add', 'Add a new Pool')
        sub_command.add_argument("name", help='Pool name')
        sub_command.add_argument("cluster_id", help='Cluster UUID')
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
        sub_command.add_argument("--cluster-id", help='ID of the cluster', dest="cluster_id")

        # get pool
        sub_command = self.add_sub_command(subparser, 'get', 'get pool details')
        sub_command.add_argument("id", help='pool uuid')
        sub_command.add_argument("--json", help='Print outputs in json format', required=False, action='store_true')

        # delete pool
        sub_command = self.add_sub_command(
            subparser, 'delete', 'Delete Pool', usage=
            "It is only possible to delete a pool if it is empty (no provisioned logical volumes contained).")
        sub_command.add_argument("id", help='pool uuid')

        # enable
        sub_command = self.add_sub_command(subparser, 'enable', 'Set pool status to Active')
        sub_command.add_argument("pool_id", help='pool uuid')
        # disable
        sub_command = self.add_sub_command(
            subparser, 'disable', 'Set pool status to Inactive.')
        sub_command.add_argument("pool_id", help='pool uuid')

        # get-secret
        sub_command = self.add_sub_command(subparser, 'get-secret', 'Get pool secret')
        sub_command.add_argument("pool_id", help='pool uuid')

        # get-secret
        sub_command = self.add_sub_command(subparser, 'upd-secret', 'Updates pool secret')
        sub_command.add_argument("pool_id", help='pool uuid')
        sub_command.add_argument("secret", help='new 20 characters password')

        # get-capacity
        sub_command = self.add_sub_command(subparser, 'get-capacity', 'Get pool capacity')
        sub_command.add_argument("pool_id", help='pool uuid')

        # get-io-stats
        sub_command = self.add_sub_command(
            subparser, 'get-io-stats', 'Get pool IO statistics')
        sub_command.add_argument("id", help='Pool id')
        sub_command.add_argument("--history", help='(XXdYYh), list history records (one for every 15 minutes) '
                                                   'for XX days and YY hours (up to 10 days in total).')

        subparser = self.add_command('snapshot', 'Snapshot commands')

        sub_command = self.add_sub_command(subparser, 'add', 'Create new snapshot')
        sub_command.add_argument("id", help='LVol UUID')
        sub_command.add_argument("name", help='snapshot name')

        self.add_sub_command(subparser, 'list', 'List snapshots')
        sub_command.add_argument("--all", help='List soft deleted snapshots', required=False, action='store_true')

        sub_command = self.add_sub_command(subparser, 'delete', 'Delete a snapshot')
        sub_command.add_argument("id", help='snapshot UUID')
        sub_command.add_argument("--force", help='Force remove', required=False, action='store_true')

        sub_command = self.add_sub_command(subparser, 'clone', 'Create LVol from snapshot')
        sub_command.add_argument("id", help='snapshot UUID')
        sub_command.add_argument("lvol_name", help='LVol name')
        sub_command.add_argument("--resize", help='New LVol size: 10M, 10G, 10(bytes)')

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
        sub_command.add_argument("--namespace", help='k8s namespace to deploy on',)

        self.add_sub_command(subparser, 'list', 'List Caching nodes')

        sub_command = self.add_sub_command(subparser, 'list-lvols', 'List connected lvols')
        sub_command.add_argument("id", help='Caching Node UUID')

        sub_command = self.add_sub_command(subparser, 'remove', 'Remove Caching node from the cluster')
        sub_command.add_argument("id", help='Caching Node UUID')
        sub_command.add_argument("--force", help='Force remove', required=False, action='store_true')

        sub_command = self.add_sub_command(subparser, 'connect', 'Connect to LVol')
        sub_command.add_argument("node_id", help='Caching node UUID')
        sub_command.add_argument("lvol_id", help='LVol UUID')

        sub_command = self.add_sub_command(subparser, 'disconnect', 'Disconnect LVol from Caching node')
        sub_command.add_argument("node_id", help='Caching node UUID')
        sub_command.add_argument("lvol_id", help='LVol UUID')

        sub_command = self.add_sub_command(subparser, 'recreate', 'recreate Caching node bdevs')
        sub_command.add_argument("node_id", help='Caching node UUID')

    def init_parser(self):
        self.parser = argparse.ArgumentParser(prog=constants.SIMPLY_BLOCK_CLI_NAME, description='SimplyBlock management CLI')
        self.parser.add_argument("-d", '--debug', help='Print debug messages', required=False, action='store_true')
        self.subparser = self.parser.add_subparsers(dest='command')

    def add_command(self, command, help, aliases=None):
        aliases = aliases or []
        storagenode = self.subparser.add_parser(command, description=help, help=help, aliases=aliases)
        storagenode_subparser = storagenode.add_subparsers(dest=command)
        return storagenode_subparser

    def add_sub_command(self, parent_parser, command, help, usage=None):
        return parent_parser.add_parser(command, description=help, help=help, usage=usage)

    def run(self):
        args = self.parser.parse_args()
        if args.debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(constants.LOG_LEVEL)
        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

        args_dict = args.__dict__
        ret = ""
        if args.command in ['storage-node', 'sn']:
            sub_command = args_dict['storage-node']

            if sub_command == "deploy":
                ret = storage_ops.deploy(args.ifname)

            elif sub_command == "deploy-cleaner":
                ret = storage_ops.deploy_cleaner()

            elif sub_command == "add-node":
                if not args.max_lvol:
                    self.parser.error(f"Mandatory argument '--max-lvol' not provided for {sub_command}")
                if not args.max_snap:
                    self.parser.error(f"Mandatory argument '--max-snap' not provided for {sub_command}")
                if not args.max_prov:
                    self.parser.error(f"Mandatory argument '--max-prov' not provided for {sub_command}")
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

                max_lvol = args.max_lvol
                max_snap = args.max_snap
                max_prov = self.parse_size(args.max_prov)
                number_of_devices = args.number_of_devices
                if max_prov < 1 * 1024 * 1024 * 1024:
                    return f"Max provisioning memory:{args.max_prov} must be larger than 1G"

                out = storage_ops.add_node(
                    cluster_id, node_ip, ifname, data_nics, max_lvol, max_snap, max_prov, spdk_image, spdk_debug,
                    small_bufsize, large_bufsize, num_partitions_per_dev, jm_percent, number_of_devices)
                return out

            elif sub_command == "list":
                ret = storage_ops.list_storage_nodes(args.json, args.cluster_id)

            elif sub_command == "remove":
                ret = storage_ops.remove_storage_node(args.node_id, args.force_remove, args.force_migrate)

            elif sub_command == "delete":
                ret = storage_ops.delete_storage_node(args.node_id)

            elif sub_command == "restart":
                node_id = args.node_id

                spdk_image = args.spdk_image
                spdk_debug = args.spdk_debug

                max_lvol = args.max_lvol
                max_snap = args.max_snap
                max_prov = self.parse_size(args.max_prov) if args.max_prov else 0
                number_of_devices = args.number_of_devices

                small_bufsize = args.small_bufsize
                large_bufsize = args.large_bufsize

                ret = storage_ops.restart_storage_node(
                    node_id, max_lvol, max_snap, max_prov,
                    spdk_image, spdk_debug,
                    small_bufsize, large_bufsize, number_of_devices)

            elif sub_command == "list-devices":
                ret = self.storage_node_list_devices(args)

            elif sub_command == "device-testing-mode":
                ret = device_controller.set_device_testing_mode(args.device_id, args.mode)

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
                ret = "Not implemented!"

            elif sub_command == "set-failed-device":
                ret = "Not implemented!"

            elif sub_command == "get-capacity-device":
                device_id = args.device_id
                history = args.history
                data = device_controller.get_device_capacity(device_id, history)
                if data:
                    ret = utils.print_table(data)
                else:
                    return False

            elif sub_command == "get-device":
                device_id = args.device_id
                ret = device_controller.get_device(device_id)

            elif sub_command == "get-io-stats-device":
                device_id = args.device_id
                history = args.history
                data = device_controller.get_device_iostats(device_id, history)
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

            elif sub_command == "get":
                ret = storage_ops.get(args.id)

            else:
                self.parser.print_help()

        elif args.command == 'cluster':
            sub_command = args_dict[args.command]
            if sub_command == 'create':
                ret = self.cluster_create(args)
            elif sub_command == 'add':
                ret = self.cluster_add(args)
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
                is_json = args.json
                data = cluster_ops.get_capacity(cluster_id, history, is_json=is_json)
                if is_json:
                    ret = data
                else:
                    ret = utils.print_table(data)

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
            elif sub_command == "update":
                ret = cluster_ops.update_cluster(args.id)

            elif sub_command == "list-tasks":
                ret = tasks_controller.list_tasks(args.cluster_id)

            elif sub_command == "cancel-task":
                ret = tasks_controller.cancel_task(args.id)

            elif sub_command == "graceful-shutdown":
                ret = cluster_ops.cluster_grace_shutdown(args.id)

            elif sub_command == "graceful-startup":
                ret = cluster_ops.cluster_grace_startup(args.id)

            elif sub_command == "delete":
                ret = cluster_ops.delete_cluster(args.id)

            else:
                self.parser.print_help()

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
                distr_ndcs = args.distr_ndcs
                distr_npcs = args.distr_npcs
                distr_bs = args.distr_bs
                distr_chunk_bs = args.distr_chunk_bs
                with_snapshot = args.snapshot
                results, error = lvol_controller.add_lvol_ha(
                    name, size, host_id, ha_type, pool, comp, crypto,
                    distr_vuid, distr_ndcs, distr_npcs,
                    args.max_rw_iops,
                    args.max_rw_mbytes,
                    args.max_r_mbytes,
                    args.max_w_mbytes,
                    distr_bs,
                    distr_chunk_bs,
                    with_snapshot=with_snapshot,
                    max_size=max_size,
                    crypto_key1=args.crypto_key1,
                    crypto_key2=args.crypto_key2)
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
            elif sub_command == 'move':
                ret = lvol_controller.move(args.id, args.node_id, args.force)
            else:
                self.parser.print_help()

        elif args.command == 'mgmt':
            sub_command = args_dict[args.command]
            if sub_command == "add":
                cluster_id = args.cluster_id
                cluster_ip = args.cluster_ip
                ifname = args.ifname
                ret = mgmt_ops.deploy_mgmt_node(cluster_ip, cluster_id, ifname)
            elif sub_command == "list":
                ret = mgmt_ops.list_mgmt_nodes(args.json)
            elif sub_command == "remove":
                ret = mgmt_ops.remove_mgmt_node(args.id)
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
                    args.has_secret,
                    args.cluster_id
                )

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
                ret = pool_controller.list_pools(args.json, args.cluster_id)

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
                ret = snapshot_controller.list(args.all)
            if sub_command == "delete":
                ret = snapshot_controller.delete(args.id, args.force)
            if sub_command == "clone":
                new_size = 0
                if args.resize:
                    new_size = self.parse_size(args.resize)
                ret = snapshot_controller.clone(args.id, args.lvol_name, new_size)

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
                namespace = args.namespace

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
                    cluster_id, node_ip, ifname, data_nics, spdk_cpu_mask, spdk_mem, spdk_image, namespace)

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

    def storage_node_list_devices(self, args):
        node_id = args.node_id
        sort = args.sort
        if sort:
            sort = sort[0]
        is_json = args.json
        out = storage_ops.list_storage_devices(self.db_store, node_id, sort, is_json)
        return out

    def cluster_add(self, args):
        page_size_in_blocks = args.page_size
        blk_size = args.blk_size
        cap_warn = args.cap_warn
        cap_crit = args.cap_crit
        prov_cap_warn = args.prov_cap_warn
        prov_cap_crit = args.prov_cap_crit

        return cluster_ops.add_cluster(
            blk_size, page_size_in_blocks, cap_warn, cap_crit, prov_cap_warn, prov_cap_crit)

    def cluster_create(self, args):
        page_size_in_blocks = args.page_size
        blk_size = args.blk_size
        CLI_PASS = args.CLI_PASS
        cap_warn = args.cap_warn
        cap_crit = args.cap_crit
        prov_cap_warn = args.prov_cap_warn
        prov_cap_crit = args.prov_cap_crit
        ifname = args.ifname
        log_del_interval = args.log_del_interval
        metrics_retention_period = args.metrics_retention_period
        contact_point = args.contact_point
        grafana_endpoint = args.grafana_endpoint

        return cluster_ops.create_cluster(
            blk_size, page_size_in_blocks,
            CLI_PASS, cap_warn, cap_crit, prov_cap_warn, prov_cap_crit,
            ifname, log_del_interval, metrics_retention_period, contact_point, grafana_endpoint)

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
