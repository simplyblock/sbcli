### simplyblock e2e tests
import argparse
import traceback
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
    """Run complete test suite"""
    parser = argparse.ArgumentParser(description="Run simplyBlock's E2E Test Framework")
    parser.add_argument('--testname', type=str, help="The name of the test to run", default=None)
    parser.add_argument('--fio_debug', type=bool, help="Add debug flag to fio", default=False)
    
    # New arguments for ndcs, npcs, bs, chunk_bs with default values
    parser.add_argument('--ndcs', type=int, help="Number of data chunks (ndcs)", default=2)
    parser.add_argument('--npcs', type=int, help="Number of parity chunks (npcs)", default=1)
    parser.add_argument('--bs', type=int, help="Block size (bs)", default=4096)
    parser.add_argument('--chunk_bs', type=int, help="Chunk block size (chunk_bs)", default=4096)
    parser.add_argument('--run_k8s', type=bool, help="Run K8s setup", default=False)


    args = parser.parse_args()

    if args.ndcs == 0 and args.npcs == 0:
        tests = get_all_tests(custom=False, k8s_test=args.run_k8s)
    else:
        tests = get_all_tests(custom=True, k8s_test=args.run_k8s)

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
        test_obj = test(fio_debug=args.fio_debug,
                        ndcs=args.ndcs,
                        npcs=args.npcs,
                        bs=args.bs,
                        chunk_bs=args.chunk_bs,
                        k8s_run=args.run_k8s)
        try:
            test_obj.setup()
            test_obj.run()
        except Exception as exp:
            logger.error(traceback.format_exc())
            errors[f"{test.__name__}"] = [exp]
        try:
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
        if "core" in files:
            core_exist = True
            break

    for node, ssh in ssh_obj.ssh_connections.items():
        logger.info(f"Closing node ssh connection for {node}")
        ssh.close()
    return core_exist


logger = setup_logger(__name__)
main()
