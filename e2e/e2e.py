### simplyblock e2e tests
import argparse
import os
import json
import traceback
from __init__ import get_all_tests
from logger_config import setup_logger
from exceptions.custom_exception import TestNotFoundException, MultipleExceptions
from e2e_tests.cluster_test_base import TestClusterBase
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils


def main():
    """Run the complete test suite or specific tests."""
    parser = argparse.ArgumentParser(description="Run simplyBlock's E2E Test Framework")
    parser.add_argument('--testname', type=str, help="The name of the test to run", default=None)
    parser.add_argument('--fio_debug', type=bool, help="Add debug flag to fio", default=False)
    parser.add_argument('--failed_only', action='store_true', help="Run only failed tests from last run", default=False)
    parser.add_argument('--unexecuted_only', action='store_true', help="Run only unexecuted tests from last run", default=False)
    parser.add_argument('--branch', type=str, help="Branch name to uniquely store test results", required=True)
    parser.add_argument('--retry', type=int, help="Number of retries for failed cases", default=1)

    args = parser.parse_args()

    tests = get_all_tests()

    # File to store failed test cases for the specific branch
    base_dir = os.path.join(os.path.expanduser('~'), 'e2e_test_runs_fail_unexec_json')
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    failed_cases_file = os.path.join(base_dir,
                                     f'failed_cases_{args.branch}.json')
    executed_cases_file = os.path.join(base_dir,
                                       f'executed_cases_{args.branch}.json')

    logger.info(f"Failed only: {args.failed_only}")
    logger.info(f"Unexecuted only: {args.unexecuted_only}")
    logger.info(f"Failed case file: {failed_cases_file}")
    logger.info(f"File exists: {os.path.exists(failed_cases_file)}")

    onlyfiles = [f for f in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, f))]
    logger.info(f"List of files: {onlyfiles}")

    # Load previously failed cases if '--failed_only' is set
    if args.failed_only and os.path.exists(failed_cases_file):
        logger.info("Running failed cases only")
        with open(failed_cases_file, 'r', encoding='utf-8') as file:
            failed_tests = json.load(file)
            test_class_run = [cls for cls in tests 
                              if any(ft in f'{cls.__name__}' for ft in failed_tests)]

            logger.info(f"Running failed cases only: {test_class_run}")
    elif args.unexecuted_only and os.path.exists(executed_cases_file):
        logger.info("Running unexecuted cases only")
        with open(executed_cases_file, 'r', encoding='utf-8') as file:
            executed_tests = json.load(file)
            test_class_run = [cls for cls in tests 
                              if all(unet not in f'{cls.__name__}' for unet in executed_tests)]
            logger.info(f"Running unexecuted cases only: {test_class_run}")
    else:
        # Run all tests or selected ones
        logger.info("Running all or selected cases")
        test_class_run = []
        if args.testname is None or len(args.testname.strip()) == 0:
            test_class_run = tests
        else:
            for cls in tests:
                if args.testname.lower() in cls.__name__.lower():
                    test_class_run.append(cls)

    logger.info(f"List of tests to run: {test_class_run}")
    if not test_class_run:
        available_tests = ', '.join(cls.__name__ for cls in tests)
        logger.info(f"Test '{args.testname}' not found. Available tests are: {available_tests}")
        raise TestNotFoundException(args.testname, available_tests)

    errors = {}
    executed_tests = []
    for test in test_class_run:
        logger.info(f"Running Test {test}")
        test_obj = test(fio_debug=args.fio_debug)

        for attempt in range(args.retry):
            try:
                test_obj.setup()
                executed_tests.append(test.__name__)
                test_obj.run()
                logger.info(f"Test {test.__name__} passed on attempt {attempt + 1}")
                if f"{test.__name__}" in errors:
                    del errors[f"{test.__name__}"]
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
        finally:
            if check_for_dumps():
                logger.info("Found a core dump during test execution. "
                            "Cannot execute more tests as cluster is not stable. Exiting")
                break

    failed_cases = list(errors.keys())

    # Save failed cases for next run
    if failed_cases:
        with open(failed_cases_file, 'w') as file:
            json.dump(failed_cases, file)
    else:
        if os.path.exists(failed_cases_file):
            os.remove(failed_cases_file)  # Clear file if all tests passed

    # Save executed cases for next run
    with open(executed_cases_file, 'w') as file:
        json.dump(executed_tests, file)

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
