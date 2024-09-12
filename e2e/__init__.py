from e2e_tests.single_node_outage import TestSingleNodeOutage
from e2e_tests.single_node_failure import TestSingleNodeFailure
from e2e_tests.single_node_multi_fio_perf import (
    TestLvolFioNpcs0, TestLvolFioNpcs1, TestLvolFioNpcs2, TestLvolFioNpcsCustom
)
from e2e_tests.cloning_and_snapshot.multi_lvol_snapshot_fio import TestMultiLvolFio



def get_all_tests(custom=True):
    tests = [
        TestSingleNodeOutage,
        TestLvolFioNpcs0,
        TestLvolFioNpcs1,
        TestLvolFioNpcs2,
        # TestSingleNodeFailure, # TODO:Enable test case when redeployment node is fixed. 
        # TestMultiLvolFio, - Enable when testing
    ]
    if custom:
        tests.append(TestLvolFioNpcsCustom)
        tests.remove(TestLvolFioNpcs0)
        tests.remove(TestLvolFioNpcs1)
        tests.remove(TestLvolFioNpcs2)
    return tests