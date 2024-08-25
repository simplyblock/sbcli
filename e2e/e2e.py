### simplyblock e2e tests
import argparse
import traceback
import os
import json
from __init__ import get_all_tests
from logger_config import setup_logger
from exceptions.custom_exception import (
    TestNotFoundException,
    MultipleExceptions
)
from e2e_tests.cluster_test_base import TestClusterBase
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils

def main():
    """Run complete test suite or only failed tests from last run."""
    parser = argparse.ArgumentParser(description="Run simplyBlock's E2E Test Framework")
    parser.add_argument('--testname', type=str, help="The name of the test to run", default=None)
    parser.add_argument('--fio_debug', type=bool, help="Add debug flag to fio", default=False)
    parser.add_argument('--failed-only', action='store_true', help="Run only failed tests from last run", default=False)
    parser.add_argument('--retry', type=int, help="Number of retries for failed cases", default=1)

    args = parser.parse_args()

    tests = get_all_tests()

    # Load previously failed cases if '--failed-only' is set
    failed_cases_file = 'failed_cases.json'
    if args.failed_only and os.path.exists(failed_cases_file):
        with open(failed_cases_file, 'r') as file:
            failed_tests = json.load(file)
            test_class_run = [cls for cls in tests if cls.__name__ in failed_tests]
    else:
        # Run all tests or selected ones
        test_class_run = []
        if args.testname is None or len(args.testname.strip()) == 0:
            test_class_run = tests
        else:
            for cls in tests:
                if args.testname.lower() in cls.__name__.lower():
                    test_class_run.append(cls)

    if not test_class_run:
        available_tests = ', '.join(cls.__name__ for cls in tests)
        logger.info(f"Test '{args.testname}' not found. Available tests are: {available_tests}")
        raise TestNotFoundException(args.testname, available_tests)

    errors = {}
    for test in test_class_run:
        logger.info(f"Running Test {test}")
        test_obj = test(fio_debug=args.fio_debug)

        # Retry logic
        for attempt in range(args.retry):
            try:
                test_obj.setup()
                test_obj.run()
                logger.info(f"Test {test.__name__} passed on attempt {attempt + 1}")
                break  # Test passed, no need for more retries
            except Exception as exp:
                logger.error(f"Attempt {attempt + 1} failed for test {test.__name__}")
                logger.error(traceback.format_exc())
                errors[f"{test.__name__}"] = [exp]

        try:
            test_obj.teardown()
        except Exception as _:
            logger.error(f"Error During Teardown for test: {test.__name__}")
            logger.error(traceback.format_exc())

    failed_cases = list(errors.keys())

    # Save failed cases for next run
    if failed_cases:
        with open(failed_cases_file, 'w', encoding='utf-8') as file:
            json.dump(failed_cases, file)

    logger.info(f"Number of Total Cases: {len(test_class_run)}")
    logger.info(f"Number of Passed Cases: {len(test_class_run) - len(failed_cases)}")
    logger.info(f"Number of Failed Cases: {len(failed_cases)}")

    if errors:
        raise MultipleExceptions(errors)

def check_for_dumps():
    """Validates whether core dumps are present on machines
    
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
        if "core" in files:
            core_exist = True
            break

    for node, ssh in ssh_obj.ssh_connections.items():
        logger.info(f"Closing node ssh connection for {node}")
        ssh.close()
    return core_exist


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


logger = setup_logger(__name__)
main()
