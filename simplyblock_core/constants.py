import logging
import os
SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))


def get_config_var(name, default=None):
    """
    OS environment variable is checked first, if not found, check the env_var file.
    """
    if not name:
        return False
    if os.getenv(name):
        return os.getenv(name)
    else:
        with open(f"{SCRIPT_PATH}/env_var", "r", encoding="utf-8") as fh:
            for line in fh.readlines():
                if line.startswith(name):
                    return line.split("=", 1)[1].strip()
    return default


KiB=1024
MiB=1024*1024
GiB=1024*1024*1024

KVD_DB_VERSION = 730
KVD_DB_FILE_PATH = os.getenv('FDB_CLUSTER_FILE', '/etc/foundationdb/fdb.cluster')
KVD_DB_TIMEOUT_MS = 10000
SPK_DIR = '/home/ec2-user/spdk'
LOG_LEVEL = logging.INFO
LOG_WEB_LEVEL = logging.DEBUG
LOG_WEB_DEBUG = True if LOG_WEB_LEVEL == logging.DEBUG else False

INSTALL_DIR = os.path.dirname(os.path.realpath(__file__))

NODE_MONITOR_INTERVAL_SEC = 10
DEVICE_MONITOR_INTERVAL_SEC = 5
STAT_COLLECTOR_INTERVAL_SEC = 60*5  # 5 minutes
LVOL_STAT_COLLECTOR_INTERVAL_SEC = 5
LVOL_MONITOR_INTERVAL_SEC = 30
DEV_MONITOR_INTERVAL_SEC = 10
DEV_STAT_COLLECTOR_INTERVAL_SEC = 5
PROT_STAT_COLLECTOR_INTERVAL_SEC = 2
SPDK_STAT_COLLECTOR_INTERVAL_SEC = 30
DISTR_EVENT_COLLECTOR_INTERVAL_SEC = 2
DISTR_EVENT_COLLECTOR_NUM_OF_EVENTS = 10
CAP_MONITOR_INTERVAL_SEC = 10
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
SIMPLY_BLOCK_DOCKER_IMAGE = get_config_var(
        "SIMPLY_BLOCK_DOCKER_IMAGE","simplyblock/simplyblock:main")
SIMPLY_BLOCK_CLI_NAME = get_config_var(
        "SIMPLY_BLOCK_COMMAND_NAME", "sbcli")
SIMPLY_BLOCK_SPDK_ULTRA_IMAGE = get_config_var(
        "SIMPLY_BLOCK_SPDK_ULTRA_IMAGE", "public.ecr.aws/simply-block/ultra:main-latest")

GELF_PORT = 12202

MIN_HUGE_PAGE_MEMORY_FOR_LVOL = 209715200
MIN_SYS_MEMORY_FOR_LVOL = 524288000
EXTRA_SMALL_POOL_COUNT = 4096
EXTRA_LARGE_POOL_COUNT = 10240
EXTRA_HUGE_PAGE_MEMORY = 3221225472
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

MAX_SNAP_COUNT = 100

SPDK_PROXY_MULTI_THREADING_ENABLED=True
SPDK_PROXY_TIMEOUT=60*5
LVOL_NVME_CONNECT_RECONNECT_DELAY=2
LVOL_NVME_CONNECT_CTRL_LOSS_TMO=60*60
LVOL_NVME_CONNECT_NR_IO_QUEUES=3
LVOL_NVME_KEEP_ALIVE_TO=10
LVOL_NVME_KEEP_ALIVE_TO_TCP=7
QPAIR_COUNT=32
CLIENT_QPAIR_COUNT=3
NVME_TIMEOUT_US=8000000
NVMF_MAX_SUBSYSTEMS=50000
KATO=10000
ACK_TO=11
BDEV_RETRY=0
TRANSPORT_RETRY=3
CTRL_LOSS_TO=1
FAST_FAIL_TO=0
RECONNECT_DELAY_CLUSTER=1
LVOL_CLUSTER_RATIO=1


SENTRY_SDK_DNS = "https://745047b017ac424b4173550e19910fb7@o4508953941311488.ingest.de.sentry.io/4508996361584720"
ONE_KB = 1024
TEMP_CORES_FILE = "/etc/simplyblock/tmp_cores_config"
PROMETHEUS_MULTIPROC_DIR = "/etc/simplyblock/metrics"

LINUX_DRV_MASS_STORAGE_ID = 1
LINUX_DRV_MASS_STORAGE_NVME_TYPE_ID = 8



NODES_CONFIG_FILE = "/etc/simplyblock/sn_config_file"
SYSTEM_INFO_FILE = "/etc/simplyblock/system_info"

LVO_MAX_NAMESPACES_PER_SUBSYS=32

CR_GROUP = "simplyblock.simplyblock.io"
CR_VERSION  = "v1alpha1"

GRAFANA_K8S_ENDPOINT = "http://simplyblock-grafana:3000"
GRAYLOG_K8S_ENDPOINT = "http://simplyblock-graylog:9000"
OS_K8S_ENDPOINT = "http://opensearch-cluster-master:9200"

WEBAPI_K8S_ENDPOINT = "http://simplyblock-webappapi:5000/api/v2"

K8S_NAMESPACE = os.getenv('K8S_NAMESPACE', 'simplyblock')
OS_STATEFULSET_NAME = "simplyblock-opensearch"
MONGODB_STATEFULSET_NAME = "simplyblock-mongo"
GRAYLOG_STATEFULSET_NAME = "simplyblock-graylog"
PROMETHEUS_STATEFULSET_NAME = "simplyblock-prometheus"
FDB_SERVICE_NAME = "simplyblock-fdb-cluster"
FDB_CONFIG_NAME = "simplyblock-fdb-cluster-config"
ADMIN_DEPLOY_NAME = "simplyblock-admin-control"

os_env_patch = [
    {"name": "OPENSEARCH_JAVA_OPTS", "value": "-Xms1g -Xmx1g"},
    {"name": "bootstrap.memory_lock", "value": "false"},
    {"name": "action.auto_create_index", "value": "false"},
    {"name": "plugins.security.ssl.http.enabled", "value": "false"},
    {"name": "plugins.security.disabled", "value": "true"},
    {"name": "discovery.type", "value": ""},
    {"name": "discovery.seed_hosts", "value": ",".join([
        "simplyblock-opensearch-0.opensearch-cluster-master-headless",
        "simplyblock-opensearch-1.opensearch-cluster-master-headless",
        "simplyblock-opensearch-2.opensearch-cluster-master-headless"
    ])},
    {"name": "cluster.initial_master_nodes", "value": ",".join([
        "simplyblock-opensearch-0",
        "simplyblock-opensearch-1",
        "simplyblock-opensearch-2"
    ])}
]

os_patch = {
    "spec": {
        "replicas": 3,
        "template": {
            "spec": {
                "containers": [
                    {
                        "name": "opensearch",
                        "env": os_env_patch
                    }
                ]
            }
        }
    }
}

mongodb_patch = {
    "spec": {
        "members": 3,
    }
}

prometheus_patch = {
    "spec": {
        "replicas": 3,
    }
}

qos_class_meta_and_migration_weight_percent = 25

MIG_PARALLEL_JOBS = 64
MIG_JOB_SIZE = 64

# ports ranges
RPC_PORT_RANGE_START = 8080
NODE_NVMF_PORT_START=9060
NODE_HUBLVOL_PORT_START=9030
FW_PORT_START = 50001
# todo(hamdy): make it configurable: sfam-2586
LVOL_NVMF_PORT_ENV = os.getenv("LVOL_NVMF_PORT_START", "")
LVOL_NVMF_PORT_START = int(LVOL_NVMF_PORT_ENV) if LVOL_NVMF_PORT_ENV else 9100