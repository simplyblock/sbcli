### simplyblock e2e tests
import argparse
import traceback
from __init__ import get_all_tests, ALL_TESTS
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


def main():
    """Run complete test suite"""
    parser = argparse.ArgumentParser(description="Run simplyBlock's E2E Test Framework")
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
    parser.add_argument('--new-nodes', type=str, help="New nodes to add (space-separated)", default="")
    

    args = parser.parse_args()

    if args.ndcs == 0 and args.npcs == 0:
        tests = get_all_tests(custom=False, ha_test=args.run_ha)
    else:
        tests = get_all_tests(custom=True, ha_test=args.run_ha)

    test_class_run = []
    new_nodes = args.new_nodes.strip().split() if args.new_nodes else []
    skipped_cases = 0
    if args.testname is None or len(args.testname.strip()) == 0:
        for cls in tests:
            if cls.__name__ == "TestAddNodesDuringFioRun":
                if len(new_nodes) == 0 or len(new_nodes) % 2 != 0:
                    logger.warning("Skipping TestAddNodesDuringFioRun: requires --new-nodes with IPs in multiples of 2.")
                    skipped_cases += 1
                    continue
            if cls.__name__ == "TestRestartNodeOnAnotherHost":
                if len(new_nodes) == 0:
                    logger.warning("Skipping TestRestartNodeOnAnotherHost: requires --new-nodes with atleast 1 IP.")
                    skipped_cases += 1
                    continue
            test_class_run.append(cls)
    else:
        for cls in ALL_TESTS:
            if args.testname.lower() in cls.__name__.lower():
                if cls.__name__ == "TestAddNodesDuringFioRun" and (len(new_nodes) == 0 or len(new_nodes) % 2 != 0):
                    raise ValueError("TestAddNodesDuringFioRun requires --new-nodes with IPs in multiples of 2.")
                if cls.__name__ == "TestRestartNodeOnAnotherHost" and len(new_nodes) == 0:
                    raise ValueError("TestRestartNodeOnAnotherHost requires --new-nodes with atleast 1 new IP.")
                test_class_run.append(cls)

    if not test_class_run:
        available_tests = ', '.join(cls.__name__ for cls in tests)
        print(f"Test '{args.testname}' not found. Available tests are: {available_tests}")
        raise TestNotFoundException(args.testname, available_tests)

    errors = {}
    passed_cases = []
    for i, test in enumerate(test_class_run):
        logger.info(f"Running Test {test}")
        test_obj = test(fio_debug=args.fio_debug,
                        ndcs=args.ndcs,
                        npcs=args.npcs,
                        bs=args.bs,
                        chunk_bs=args.chunk_bs,
                        k8s_run=args.run_k8s,
                        new_nodes=new_nodes)
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
            if i == (len(test_class_run) - 1) or check_for_dumps():
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
    skipped_cases += len(test_class_run) - (len(passed_cases) + len(failed_cases))

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
        common_utils.send_slack_summary("E2E Test Summary Report", summary)

    if errors:
        raise MultipleExceptions(errors)
    if skipped_cases:
        raise SkippedTestsException("There are SKIPPED Tests. Please check!!")
    
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
