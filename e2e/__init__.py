from e2e_tests.single_node_outage import TestSingleNodeOutage
from e2e_tests.single_node_multi_fio_perf import TestSingleNodeMultipleFioPerfValidation


def get_all_tests():
    tests = [
        TestSingleNodeOutage,
        TestSingleNodeMultipleFioPerfValidation,
    ]
    return tests