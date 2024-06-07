from tests.single_node_outage import TestSingleNodeOutage
from tests.single_node_multi_fio_perf import TestSingleNodeMultipleFioPerfValidation


def get_all_tests():
    tests = [
        TestSingleNodeOutage,
        TestSingleNodeMultipleFioPerfValidation,
    ]
    return tests