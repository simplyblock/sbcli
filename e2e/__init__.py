from e2e_tests.single_node_outage import TestSingleNodeOutage
from e2e_tests.single_node_failure import TestSingleNodeFailure
from e2e_tests.single_node_multi_fio_perf import (
    TestLvolFioNpcs0, TestLvolFioNpcs1, TestLvolFioNpcs2, TestLvolFioNpcsCustom
)
from e2e_tests.cloning_and_snapshot.multi_lvol_snapshot_fio import TestMultiLvolFio
from e2e_tests.data_migration.data_migration_ha_fio import FioWorkloadTest



def get_all_tests(custom=True, k8s_test=False):
    tests = [
        TestSingleNodeOutage,
        TestLvolFioNpcsCustom,
        TestLvolFioNpcs0,
        TestLvolFioNpcs1,
        TestLvolFioNpcs2,
        TestSingleNodeFailure,
        FioWorkloadTest,
        # TestMultiLvolFio, - Enable when testing
    ]
    if not custom:
        tests.remove(TestLvolFioNpcsCustom)
    else:
        tests.remove(TestLvolFioNpcs0)
        tests.remove(TestLvolFioNpcs1)
        tests.remove(TestLvolFioNpcs2)
    if k8s_test:
        tests.remove(TestSingleNodeFailure)
    return tests