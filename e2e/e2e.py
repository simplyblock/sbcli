### simplyblock e2e tests
import argparse
import traceback
import os
from __init__ import get_all_tests
from logger_config import setup_logger
from exceptions.custom_exception import (
    TestNotFoundException,
    MultipleExceptions
)
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils


cluster_secret = os.environ.get("CLUSTER_SECRET")
cluster_id = os.environ.get("CLUSTER_ID")
api_base_url = os.environ.get("API_BASE_URL")
bastion_server = os.environ.get("BASTION_SERVER")


def main():
    """Run complete test suite
    """
    parser = argparse.ArgumentParser(description="Run simplyBlock's E2E Test Framework")
    parser.add_argument('--testname', type=str, help="The name of the test to run", default=None)
    parser.add_argument('--fio_debug', type=bool, help="Add debug flag to fio", default=False)

    args = parser.parse_args()

    tests = get_all_tests()
    # Find the test class based on the provided test name
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
    
    errors = {}
    for test in test_class_run:
        logger.info(f"Running Test {test}")
        test_obj = test(fio_debug=args.fio_debug)
        try:
            test_obj.setup()
            test_obj.run()
        except Exception as exp:
            logger.error(traceback.format_exc())
            errors[f"{test.__name__}"] = [exp]
        try:
            test_obj.teardown()
        except Exception as exp:
            logger.error(traceback.format_exc())
            errors[f"{test.__name__}"].append(exp)
        finally:
            if check_for_dumps():
                logger.info("Found a core dump during test execution. "
                            "Cannot execute more tests as cluster is not stable. Exiting")
                break
            

    failed_cases = list(errors.keys())
    logger.info(f"Number of Total Cases: {len(test_class_run)}")
    logger.info(f"Number of Passed Cases: {len(test_class_run) - len(failed_cases)}")
    logger.info(f"Number of Failed Cases: {len(failed_cases)}")
    
    logger.info("Test Wise run status:")
    for test in test_class_run:
        if test.__name__ not in failed_cases:
            logger.info(f"{test.__name__} PASSED CASE.")
        else:
            logger.info(f"{test.__name__} FAILED CASE.")

    if errors:
        raise MultipleExceptions(errors)


def generate_report():
    """
    If any of the above conditions are not true, the relevant outputs from logs or cli commands should be placed
    automatically in a bug report; we may just create a shared folder and place a textfile bug report per run
    there under the date of the run: No failure report file → everthing went ok.
    failure report file for a particular date and time → contains relevant logs of the run
    (fio output, output of sbcli sn list, sbcli sn list-devices, sbcli cluster status, sbcli cluster get-logs,
    sbcli lvol get, sbcli lvol get-cluster-map, spdk log)
    """
    pass


def check_for_dumps():
    """Validates whether core dumps present on machines
    
    Returns:
        bool: If there are core dumps or not
    """
    logger.info("Checking for core dumps!!")
    ssh_obj = SshUtils(bastion_server=bastion_server)
    sbcli_utils = SbcliUtils(
        cluster_api_url=api_base_url,
        cluster_id=cluster_id,
        cluster_secret=cluster_secret
    )
    _, storage_nodes = sbcli_utils.get_all_nodes_ip()
    for node in storage_nodes:
        logger.info(f"**Connecting to storage nodes** - {node}")
        ssh_obj.connect(
            address=node,
            bastion_server_address=bastion_server,
        )
    for node in storage_nodes:
        files = ssh_obj.list_files(node, "/etc/simplyblock/")
        logger.info(f"Files in /etc/simplyblock: {files}")
        if "core" in files:
            return True
    return False


logger = setup_logger(__name__)
main()
