import logging
import os
SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))


def get_from_env_var_file(name, default=None):
    if not name:
        return False
    with open(f"{SCRIPT_PATH}/env_var", "r", encoding="utf-8") as fh:
        for line in fh.readlines():
            if line.startswith(name):
                return line.split("=", 1)[1].strip()
    return default


KVD_DB_VERSION = 730
KVD_DB_FILE_PATH = '/etc/foundationdb/fdb.cluster'
KVD_DB_TIMEOUT_MS = 10000
SPK_DIR = '/home/ec2-user/spdk'
RPC_HTTP_PROXY_PORT = 8080
LOG_LEVEL = logging.INFO
LOG_WEB_LEVEL = logging.DEBUG
LOG_WEB_DEBUG = True if LOG_WEB_LEVEL == logging.DEBUG else False

INSTALL_DIR = os.path.dirname(os.path.realpath(__file__))

NODE_MONITOR_INTERVAL_SEC = 10
DEVICE_MONITOR_INTERVAL_SEC = 5
STAT_COLLECTOR_INTERVAL_SEC = 60*5  # 5 minutes
LVOL_STAT_COLLECTOR_INTERVAL_SEC = 5
LVOL_MONITOR_INTERVAL_SEC = 60
DEV_MONITOR_INTERVAL_SEC = 10
DEV_STAT_COLLECTOR_INTERVAL_SEC = 5
PROT_STAT_COLLECTOR_INTERVAL_SEC = 2
SPDK_STAT_COLLECTOR_INTERVAL_SEC = 5
DISTR_EVENT_COLLECTOR_INTERVAL_SEC = 2
DISTR_EVENT_COLLECTOR_NUM_OF_EVENTS = 10
CAP_MONITOR_INTERVAL_SEC = 30
SSD_VENDOR_WHITE_LIST = ["1d0f:cd01", "1d0f:cd00"]
CACHED_LVOL_STAT_COLLECTOR_INTERVAL_SEC = 5
DEV_DISCOVERY_INTERVAL_SEC = 60

PMEM_DIR = '/tmp/pmem'

NVME_PROGRAM_FAIL_COUNT = 50
NVME_ERASE_FAIL_COUNT = 50
NVME_CRC_ERROR_COUNT = 50
DEVICE_OVERLOAD_STDEV_VALUE = 50
DEVICE_OVERLOAD_CAPACITY_THRESHOLD = 50

CLUSTER_NQN = "nqn.2023-02.io.simplyblock"

weights = {
    "lvol": 100,
    # "cpu": 10,
    # "r_io": 10,
    # "w_io": 10,
    # "r_b": 10,
    # "w_b": 10
}


HEALTH_CHECK_INTERVAL_SEC = 30

GRAYLOG_CHECK_INTERVAL_SEC = 60

FDB_CHECK_INTERVAL_SEC = 60

TASK_EXEC_INTERVAL_SEC = 10
TASK_EXEC_RETRY_COUNT = 8

SIMPLY_BLOCK_SPDK_CORE_IMAGE = "simplyblock/spdk-core:v24.05-tag-latest"
SIMPLY_BLOCK_DOCKER_IMAGE = get_from_env_var_file(
        "SIMPLY_BLOCK_DOCKER_IMAGE","simplyblock/simplyblock:main")
SIMPLY_BLOCK_CLI_NAME = get_from_env_var_file(
        "SIMPLY_BLOCK_COMMAND_NAME", "sbcli")
SIMPLY_BLOCK_SPDK_ULTRA_IMAGE = get_from_env_var_file(
        "SIMPLY_BLOCK_SPDK_ULTRA_IMAGE", "public.ecr.aws/simply-block/ultra:main-latest")

GELF_PORT = 12202

MIN_HUGE_PAGE_MEMORY_FOR_LVOL = 209715200
MIN_SYS_MEMORY_FOR_LVOL = 524288000
EXTRA_SMALL_POOL_COUNT = 4096
EXTRA_LARGE_POOL_COUNT = 10240
EXTRA_HUGE_PAGE_MEMORY = 1147483648
EXTRA_SYS_MEMORY = 0.10

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

        'm6id.large': {'number_of_devices': 1, 'size_per_device_gb': 116},
        'm6id.xlarge': {'number_of_devices': 1, 'size_per_device_gb': 237},
        'm6id.2xlarge': {'number_of_devices': 1, 'size_per_device_gb': 474},
        'm6id.4xlarge': {'number_of_devices': 1, 'size_per_device_gb': 950},
        'm6id.8xlarge': {'number_of_devices': 1, 'size_per_device_gb': 1900},
    }

MAX_SNAP_COUNT = 15

SPDK_PROXY_MULTI_THREADING_ENABLED=True
SPDK_PROXY_TIMEOUT=60*5
LVOL_NVME_CONNECT_RECONNECT_DELAY=2
LVOL_NVME_CONNECT_CTRL_LOSS_TMO=-1
LVOL_NVME_CONNECT_NR_IO_QUEUES=6
LVOL_NVME_CONNECT_TIMEOUT_S=10
LVOL_NVME_KEEP_ALIVE_TO=3
LVOL_NVMF_PORT_START=9110
QPAIR_COUNT=32
NVME_TIMEOUT_US=20000000

NVMF_MAX_SUBSYSTEMS=50000
HA_JM_COUNT=2

TEMP_CORES_FILE = "/etc/simplyblock/tmp_cores_config"
