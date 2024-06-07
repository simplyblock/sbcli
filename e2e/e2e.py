### simplyblock e2e tests
import traceback
from __init__ import get_all_tests
from logger_config import setup_logger


def main():
    """Run complete test suite
    """
    tests = get_all_tests()
    errors = {}
    for test in tests:
        logger.info(f"Running Test {test}")
        test_obj = test()
        try:
            test_obj.setup()
            test_obj.run()
        except Exception as exp:
            logger.error(traceback.format_exc())
            errors[f"{test}"] = [exp]
        try:
            test_obj.teardown()
        except Exception as exp:
            logger.error(traceback.format_exc())
            errors[f"{test}"].append(exp)

    for test, exception in errors.items():
        logger.error(f"Raising exception for test: {test}")
        for exc in exception:
            raise exc


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
