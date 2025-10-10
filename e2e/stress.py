### simplyblock Stress tests
import argparse
import traceback
import os
import time
import subprocess
from __init__ import get_stress_tests
from logger_config import setup_logger
from exceptions.custom_exception import (
    TestNotFoundException,
    MultipleExceptions,
    SkippedTestsException
)
from e2e_tests.cluster_test_base import TestClusterBase
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils
from utils.common_utils import CommonUtils
from utils.manage_portal_util import (
    TestRunsAPI,
    detect_fe_be_tags,
    FAILURE_REASON_OTHER,
    resolve_environment_id_from_ip
)


PROFILE_KEY = "stress"         # fixed
JIRA_TICKET = ""            # always empty, per your note
COMPLETION_COMMENT = "Stress run"


def main():
    """Run complete test suite"""
    parser = argparse.ArgumentParser(description="Run simplyBlock's Stress Test Framework")
    parser.add_argument('--testname', type=str, help="The name of the test to run", default=None)
    parser.add_argument('--fio_debug', type=bool, help="Add debug flag to fio", default=False)
    
    # New arguments for ndcs, npcs, bs, chunk_bs with default values
    parser.add_argument('--ndcs', type=int, help="Number of data chunks (ndcs)", default=2)
    parser.add_argument('--npcs', type=int, help="Number of parity chunks (npcs)", default=1)
    parser.add_argument('--bs', type=int, help="Block size (bs)", default=4096)
    parser.add_argument('--chunk_bs', type=int, help="Chunk block size (chunk_bs)", default=4096)
    parser.add_argument('--run_k8s', type=bool, help="Run K8s tests", default=False)
    parser.add_argument('--run_ha', type=bool, help="Run HA tests", default=False)
    parser.add_argument('--send_debug_notification', type=bool, help="Send notification for debug", default=False)
    parser.add_argument('--upload_logs', type=bool, help="Upload Logs", default=False)
    

    args = parser.parse_args()
    
    tests = get_stress_tests()

    test_class_run = []
    if args.testname is None or len(args.testname.strip()) == 0:
        test_class_run = tests
    else:
        for cls in tests:
            if args.testname.lower() in cls.__name__.lower():
                test_class_run.append(cls)

    if not test_class_run:
        available_tests = ', '.join(cls.__name__ for cls in tests)
        print(f"Test '{args.testname}' not found. Available tests are: {available_tests}")
        raise TestNotFoundException(args.testname, available_tests)
    
    test_run_api = TestRunsAPI(PROFILE_KEY)
    try:
        cluster_base = TestClusterBase()
        ssh_obj = SshUtils(bastion_server=cluster_base.bastion_server)
        sbcli_utils = SbcliUtils(
            cluster_api_url=cluster_base.api_base_url,
            cluster_id=cluster_base.cluster_id,
            cluster_secret=cluster_base.cluster_secret
        )

        mgmt_nodes, storage_node = sbcli_utils.get_all_nodes_ip()
        mgmt_ip_for_env = mgmt_nodes[0]
        environment_id = resolve_environment_id_from_ip(mgmt_ip_for_env)
        if not environment_id:
            raise RuntimeError(f"Could not resolve environment for mgmt IP {mgmt_ip_for_env}")
        ssh_obj.connect(address=storage_node[0], bastion_server_address=cluster_base.bastion_server)

        fe_branch, fe_commit, be_branch, be_commit = detect_fe_be_tags(ssh_obj, storage_node[0])

        test_run_id = test_run_api.create_run(
            jira_ticket=JIRA_TICKET,
            github_branch_frontend=fe_branch or "unknown",
            github_branch_backend=be_branch or "unknown",
            github_commit_tag_frontend=fe_commit or "unknown",
            github_commit_tag_backend=be_commit or "unknown",
            environment_id=environment_id
        )
        logger.info(f"Test Run started: {test_run_id}")

        # Close the temp SSH connection used for tag detection
        for node, ssh in ssh_obj.ssh_connections.items():
            logger.info(f"Closing temp ssh connection for FE/BE detection: {node}")
            ssh.close()

    except Exception as e:
        logger.error("Failed to create Test Run; proceeding without external tracking.")
        logger.error(e)
        test_run_id = None

    errors = {}
    passed_cases = []
    for i, test in enumerate(test_class_run):
        logger.info(f"Running Test {test}")
        test_obj = test(fio_debug=args.fio_debug,
                        ndcs=args.ndcs,
                        npcs=args.npcs,
                        bs=args.bs,
                        chunk_bs=args.chunk_bs,
                        k8s_run=args.run_k8s)
        try:
            test_obj.setup()
            if i == 0:
                test_obj.cleanup_logs()
                test_obj.configure_sysctl_settings()
            test_obj.run()
            passed_cases.append(f"{test.__name__}")
        except Exception as exp:
            logger.error(traceback.format_exc())
            errors[f"{test.__name__}"] = [exp]
        try:
            if not args.run_k8s:
                test_obj.stop_docker_logs_collect()
            test_obj.fetch_all_nodes_distrib_log()
            if i == (len(test_class_run) - 1):
                test_obj.collect_management_details()
            all_nodes = test_obj._get_all_nodes()
            test_obj.ssh_obj.collect_final_docker_logs_simple(all_nodes, test_obj.docker_logs_path)
            test_obj.teardown(delete_lvols=False, close_ssh=True)
            # pass
        except Exception as _:
            logger.error(f"Error During Teardown for test: {test.__name__}")
            logger.error(traceback.format_exc())
        finally:
            if check_for_dumps():
                logger.info("Found a core dump during test execution. "
                            "Cannot execute more tests as cluster is not stable. Exiting")
                test_obj.collect_management_details()
                break

    failed_cases = list(errors.keys())
    skipped_cases = len(test_class_run) - (len(passed_cases) + len(failed_cases))
    logger.info(f"Number of Total Cases: {len(test_class_run)}")
    logger.info(f"Number of Passed Cases: {len(passed_cases)}")
    logger.info(f"Number of Failed Cases: {len(failed_cases)}")
    logger.info(f"Number of Skipped Cases: {skipped_cases}")

    summary = f"""
        *Total Test Cases:* {len(test_class_run)}
        *Passed Cases:* {len(passed_cases)}
        *Failed Cases:* {len(failed_cases)}
        *Skipped Cases:* {skipped_cases}

        *Test Wise Run Status:*
    """

    logger.info("Test Wise run status:")
    for test in test_class_run:
        if test.__name__ in passed_cases:
            logger.info(f"{test.__name__} PASSED CASE.")
            summary += f"✅ {test.__name__}: *PASSED*\n"
        elif test.__name__ in failed_cases:
            logger.info(f"{test.__name__} FAILED CASE.")
            summary += f"❌ {test.__name__}: *FAILED*\n"
        else:
            logger.info(f"{test.__name__} SKIPPED CASE.")
            summary += f"⚠️ {test.__name__}: *SKIPPED*\n"
    
    if args.send_debug_notification:
        # Send Slack notification
        cluster_base = TestClusterBase()
        ssh_obj = SshUtils(bastion_server=cluster_base.bastion_server)
        sbcli_utils = SbcliUtils(
            cluster_api_url=cluster_base.api_base_url,
            cluster_id=cluster_base.cluster_id,
            cluster_secret=cluster_base.cluster_secret
        )
        common_utils = CommonUtils(sbcli_utils, ssh_obj)
        common_utils.send_slack_summary("Stress Test Summary Report", summary)

    final_status = "completed" if not errors else "failed"
    failure_reason_id = FAILURE_REASON_OTHER if final_status == "failed" else None

    if test_run_id:
        try:
            test_run_api.complete_run(
                status=final_status,
                completion_comment=summary,
                completion_jira_ticket=JIRA_TICKET,
                failure_reason_id=failure_reason_id,
                errors=errors
            )
            logger.info(f"Test Run marked {final_status}.")
        except Exception as e:
            logger.error(f"Failed to update Test Run status: {e}")

    if args.upload_logs:
        upload_logs()

    if errors:
        raise MultipleExceptions(errors)
    if skipped_cases:
        raise SkippedTestsException("There are SKIPPED Tests. Please check!!")


def upload_logs():
    """Runs upload logs script on runner node."""
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
    os.environ["GITHUB_RUN_ID"] = os.getenv("GITHUB_RUN_ID", f"Stress-Run-{suffix}")

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


def check_for_dumps():
    """Validates whether core dumps present on machines
    
    Returns:
        bool: If there are core dumps or not
    """
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


logger = setup_logger(__name__)
main()
