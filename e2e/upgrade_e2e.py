import argparse
import traceback
from logger_config import setup_logger
from e2e_tests.cluster_test_base import TestClusterBase
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils
from utils.common_utils import CommonUtils
from exceptions.custom_exception import MultipleExceptions
from __init__ import get_upgrade_tests  # Assumes you have this defined somewhere

def main():
    parser = argparse.ArgumentParser(description="Run Upgrade Test Framework for simplyBlock")

    parser.add_argument('--base_version', type=str, required=True, help="Current installed version")
    parser.add_argument('--target_version', type=str, required=False, default="latest", help="Target version to upgrade to")
    parser.add_argument('--fio_debug', type=bool, help="Add debug flag to fio", default=False)
    parser.add_argument('--run_k8s', type=bool, help="Run K8s tests", default=False)
    parser.add_argument('--send_debug_notification', type=bool, help="Send notification for debug", default=False)

    args = parser.parse_args()

    logger.info(f"Running upgrade tests from version {args.base_version} to {args.target_version}")

    upgrade_tests = get_upgrade_tests()
    passed_cases = []
    errors = {}

    for i, test in enumerate(upgrade_tests):
        logger.info(f"Running Test {test}")
        test_obj = test(base_version=args.base_version,
                        target_version=args.target_version,
                        fio_debug=args.fio_debug,
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
            else:
                test_obj.stop_k8s_log_collect()
            test_obj.fetch_all_nodes_distrib_log()
            if i == (len(upgrade_tests) - 1) or check_for_dumps():
                test_obj.collect_management_details()
            test_obj.teardown()
            # pass
        except Exception as _:
            logger.error(f"Error During Teardown for test: {test.__name__}")
            logger.error(traceback.format_exc())
        finally:
            if check_for_dumps():
                logger.info("Found a core dump during test execution. "
                            "Cannot execute more tests as cluster is not stable. Exiting")
                break

    failed_cases = list(errors.keys())
    skipped_cases = len(upgrade_tests) - (len(passed_cases) + len(failed_cases))

    logger.info("Upgrade Test Summary:")
    logger.info(f"Total Cases: {len(upgrade_tests)}")
    logger.info(f"Passed: {len(passed_cases)}")
    logger.info(f"Failed: {len(failed_cases)}")
    logger.info(f"Skipped: {skipped_cases}")

    summary = f"""
        *Upgrade Test Suite:* from {args.base_version} to {args.target_version}
        *Total Test Cases:* {len(upgrade_tests)}
        *Passed Cases:* {len(passed_cases)}
        *Failed Cases:* {len(failed_cases)}
        *Skipped Cases:* {skipped_cases}
    """

    for test in upgrade_tests:
        if test.__name__ in passed_cases:
            summary += f"✅ {test.__name__}: *PASSED*\n"
        elif test.__name__ in failed_cases:
            summary += f"❌ {test.__name__}: *FAILED*\n"
        else:
            summary += f"⚠️ {test.__name__}: *SKIPPED*\n"

    if args.send_debug_notification:
        cluster_base = TestClusterBase()
        ssh_obj = SshUtils(bastion_server=cluster_base.bastion_server)
        sbcli_utils = SbcliUtils(
            cluster_api_url=cluster_base.api_base_url,
            cluster_id=cluster_base.cluster_id,
            cluster_secret=cluster_base.cluster_secret
        )
        common_utils = CommonUtils(sbcli_utils, ssh_obj)
        common_utils.send_slack_summary("Upgrade Test Summary Report", summary)

    if errors:
        raise MultipleExceptions(errors)

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
        if "core" in files and "tmp_cores" not in files:
            core_exist = True
            break


    for node, ssh in ssh_obj.ssh_connections.items():
        logger.info(f"Closing node ssh connection for {node}")
        ssh.close()
    return core_exist


logger = setup_logger(__name__)
main()
