### simplyblock Load test framework
import argparse
import traceback
import os
import subprocess
import time
from __init__ import get_load_tests
from logger_config import setup_logger
from exceptions.custom_exception import TestNotFoundException, MultipleExceptions
from e2e_tests.cluster_test_base import TestClusterBase
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils
from utils.common_utils import CommonUtils

logger = setup_logger(__name__)

def check_for_dumps():
    logger.info("Checking for core dumps!!")
    cluster_base = TestClusterBase()
    ssh_obj = SshUtils(bastion_server=cluster_base.bastion_server)
    sbcli_utils = SbcliUtils(
        cluster_api_url=cluster_base.api_base_url,
        cluster_id=cluster_base.cluster_id,
        cluster_secret=cluster_base.cluster_secret
    )
    _, storage_nodes = sbcli_utils.get_all_nodes_ip()
    for node in storage_nodes:
        logger.info(f"**Connecting to storage nodes** - {node}")
        ssh_obj.connect(
            address=node,
            bastion_server_address=cluster_base.bastion_server,
        )
    core_exist = False
    for node in storage_nodes:
        files = ssh_obj.list_files(node, "/etc/simplyblock/")
        logger.info(f"Files in /etc/simplyblock: {files}")
        if "core.react" in files:
            core_exist = True
            break

    for node, ssh in ssh_obj.ssh_connections.items():
        logger.info(f"Closing node ssh connection for {node}")
        ssh.close()
    return core_exist

def upload_logs():
    logger.info("Setting environment variables for log upload...")
    cluster_base = TestClusterBase()
    sbcli_utils = SbcliUtils(
        cluster_api_url=cluster_base.api_base_url,
        cluster_id=cluster_base.cluster_id,
        cluster_secret=cluster_base.cluster_secret
    )
    mgmt_nodes, _ = sbcli_utils.get_all_nodes_ip()
    storage_nodes_id = sbcli_utils.get_storage_nodes()
    sec_node = []
    primary_node = []
    for node in storage_nodes_id["results"]:
        if node['is_secondary_node']:
            sec_node.append(node["mgmt_ip"])
        else:
            primary_node.append(node["mgmt_ip"])

    os.environ["MINIO_ACCESS_KEY"] = os.getenv("MINIO_ACCESS_KEY", "admin")
    os.environ["MINIO_SECRET_KEY"] = os.getenv("MINIO_SECRET_KEY", "password")
    os.environ["BASTION_IP"] = os.getenv("BASTION_Server", mgmt_nodes[0])
    os.environ["USER"] = os.getenv("USER", "root")
    os.environ["STORAGE_PRIVATE_IPS"] = " ".join(primary_node)
    os.environ["SEC_STORAGE_PRIVATE_IPS"] = " ".join(sec_node)
    os.environ["MNODES"] = " ".join(mgmt_nodes)
    os.environ["CLIENTNODES"] = os.getenv("CLIENT_IP", os.getenv("MNODES", " ".join(mgmt_nodes)))
    suffix = time.strftime("%Y-%m-%d_%H-%M-%S")
    os.environ["GITHUB_RUN_ID"] = os.getenv("GITHUB_RUN_ID", f"Load-Run-{suffix}")

    script_path = os.path.join(os.getcwd(), "logs", "upload_logs_to_miniio.py")

    if os.path.exists(script_path):
        logger.info(f"Running upload script: {script_path}")
        try:
            subprocess.run(["python3", script_path], check=True)
            logger.info("Logs uploaded successfully to MinIO.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to upload logs: {e}")
    else:
        logger.error(f"Upload script not found at {script_path}")

parser = argparse.ArgumentParser(description="Run simplyBlock's Load Test Framework")
parser.add_argument('--testname', type=str, help="The name of the test to run", required=True)
parser.add_argument('--output', type=str, help="Path to the log file", default='lvol_outage_log.csv')
parser.add_argument('--max_lvols', type=int, help="Maximum number of lvols", default=1200)
parser.add_argument('--step', type=int, help="Step count for lvols", default=100)
parser.add_argument('--read_log', action='store_true', help="Only read log and generate graph")
parser.add_argument('--continue_from_log', action='store_true', help="Continue from existing log")
parser.add_argument('--send_debug_notification', action='store_true', help="Send Slack notification with summary")
parser.add_argument('--upload_logs', action='store_true', help="Upload logs to MinIO")

args = parser.parse_args()

# Fetch available tests
tests = get_load_tests()
selected_test = None

for cls in tests:
    if args.testname.lower() in cls.__name__.lower():
        selected_test = cls
        break

if not selected_test:
    available = ', '.join(cls.__name__ for cls in tests)
    raise TestNotFoundException(args.testname, available)

logger.info(f"Running Load Test: {selected_test.__name__}")

summary = ""
try:
    test_obj = selected_test(
        output_file=args.output,
        max_lvols=args.max_lvols,
        step=args.step,
        read_only=args.read_log,
        continue_from_log=args.continue_from_log
    )
    test_obj.setup()
    test_obj.cleanup_logs()
    test_obj.configure_sysctl_settings()
    test_obj.run()
    summary += f"{selected_test.__name__}: PASSED\n"
    logger.info(f"Test {selected_test.__name__} completed successfully")
except Exception as e:
    logger.error(traceback.format_exc())
    summary += f"{selected_test.__name__}: FAILED\n"
    if args.send_debug_notification or args.upload_logs:
        logger.error("Test failed. Logs and notification may still proceed.")
    raise MultipleExceptions({selected_test.__name__: [e]})
finally:
    test_obj.teardown()
    if check_for_dumps():
        logger.info("Found a core dump during test execution. Cluster is unstable.")
    if args.send_debug_notification:
        cluster_base = TestClusterBase()
        ssh_obj = SshUtils(bastion_server=cluster_base.bastion_server)
        sbcli_utils = SbcliUtils(
            cluster_api_url=cluster_base.api_base_url,
            cluster_id=cluster_base.cluster_id,
            cluster_secret=cluster_base.cluster_secret
        )
        common_utils = CommonUtils(sbcli_utils, ssh_obj)
        common_utils.send_slack_summary("Load Test Summary Report", summary)
    if args.upload_logs:
        upload_logs()
