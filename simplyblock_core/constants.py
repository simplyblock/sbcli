import logging
import os

KVD_DB_VERSION = 730
KVD_DB_FILE_PATH = '/etc/foundationdb/fdb.cluster'
KVD_DB_TIMEOUT_MS = 10000
SPK_DIR = '/home/ec2-user/spdk'
RPC_HTTP_PROXY_PORT = 8080
LOG_LEVEL = logging.INFO
LOG_WEB_DEBUG = True

INSTALL_DIR = os.path.dirname(os.path.realpath(__file__))

NODE_MONITOR_INTERVAL_SEC = 3
DEVICE_MONITOR_INTERVAL_SEC = 5
STAT_COLLECTOR_INTERVAL_SEC = 60*5  # 5 minutes
LVOL_STAT_COLLECTOR_INTERVAL_SEC = 2
LVOL_MONITOR_INTERVAL_SEC = 60
DEV_MONITOR_INTERVAL_SEC = 10
DEV_STAT_COLLECTOR_INTERVAL_SEC = 2
PROT_STAT_COLLECTOR_INTERVAL_SEC = 2
DISTR_EVENT_COLLECTOR_INTERVAL_SEC = 2
DISTR_EVENT_COLLECTOR_NUM_OF_EVENTS = 10
CAP_MONITOR_INTERVAL_SEC = 30
SSD_VENDOR_WHITE_LIST = ["1d0f:cd01", "1d0f:cd00"]

PMEM_DIR = '/tmp/pmem'

NVME_PROGRAM_FAIL_COUNT = 50
NVME_ERASE_FAIL_COUNT = 50
NVME_CRC_ERROR_COUNT = 50
DEVICE_OVERLOAD_STDEV_VALUE = 50
DEVICE_OVERLOAD_CAPACITY_THRESHOLD = 50

CLUSTER_NQN = "nqn.2023-02.io.simplyblock"

weights = {
    "lvol": 50,
    "cpu": 10,
    "r_io": 10,
    "w_io": 10,
    "r_b": 10,
    "w_b": 10
}

# To use 75% of hugepages to calculate ssd size to use for the ocf bdev
CACHING_NODE_MEMORY_FACTOR = 0.75

HEALTH_CHECK_INTERVAL_SEC = 60

GRAYLOG_CHECK_INTERVAL_SEC = 60

FDB_CHECK_INTERVAL_SEC = 60

SIMPLY_BLOCK_DOCKER_IMAGE = "simplyblock/simplyblock:lvol"
SIMPLY_BLOCK_CLI_NAME = "sbcli-lvol"
TASK_EXEC_INTERVAL_SEC = 30
TASK_EXEC_RETRY_COUNT = 8

SIMPLY_BLOCK_SPDK_CORE_IMAGE = "simplyblock/spdk-core:latest"
SIMPLY_BLOCK_SPDK_CORE_IMAGE_ARM64 = "simplyblock/spdk-core:latest-arm64"
SIMPLY_BLOCK_SPDK_ULTRA_IMAGE = "simplyblock/spdk:prerelease-latest"

GELF_PORT = 12201

MIN_HUGE_PAGE_MEMORY_FOR_LVOL = 209715200
MIN_SYS_MEMORY_FOR_LVOL = 524288000
EXTRA_SMALL_POOL_COUNT = 1024
EXTRA_LARGE_POOL_COUNT = 128
EXTRA_HUGE_PAGE_MEMORY = 2147483648
EXTRA_SYS_MEMORY = 2147483648

INSTANCE_STORAGE_DATA = {
        'i4i.large': {'number_of_devices': 1, 'size_per_device_gb': 468},
        'i4i.xlarge': {'number_of_devices': 1, 'size_per_device_gb': 937},
        'i4i.2xlarge': {'number_of_devices': 1, 'size_per_device_gb': 1875},
        'i4i.4xlarge': {'number_of_devices': 1, 'size_per_device_gb': 3750},
        'i4i.8xlarge': {'number_of_devices': 2, 'size_per_device_gb': 3750},
        'i4i.12xlarge': {'number_of_devices': 3, 'size_per_device_gb': 3750},
        'i4i.16xlarge': {'number_of_devices': 4, 'size_per_device_gb': 3750},
        'i4i.24xlarge': {'number_of_devices': 6, 'size_per_device_gb': 3750},
        'i4i.32xlarge': {'number_of_devices': 8, 'size_per_device_gb': 3750},
        'i4i.metal': {'number_of_devices': 8, 'size_per_device_gb': 3750},
        'i3en.large': {'number_of_devices': 1, 'size_per_device_gb': 1250},
        'i3en.xlarge': {'number_of_devices': 1, 'size_per_device_gb': 2500},
        'i3en.2xlarge': {'number_of_devices': 2, 'size_per_device_gb': 2500},
        'i3en.3xlarge': {'number_of_devices': 1, 'size_per_device_gb': 7500},
        'i3en.6xlarge': {'number_of_devices': 2, 'size_per_device_gb': 7500},
        'i3en.12xlarge': {'number_of_devices': 4, 'size_per_device_gb': 7500},
        'i3en.24xlarge': {'number_of_devices': 8, 'size_per_device_gb': 7500},
        'i3en.metal': {'number_of_devices': 8, 'size_per_device_gb': 7500},
    }
