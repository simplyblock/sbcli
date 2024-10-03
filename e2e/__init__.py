from e2e_tests.single_node_outage import TestSingleNodeOutage
from e2e_tests.single_node_failure import TestSingleNodeFailure
from e2e_tests.single_node_multi_fio_perf import (
    TestLvolFioNpcs0, TestLvolFioNpcs1, TestLvolFioNpcs2,
)
from e2e_tests.cloning_and_snapshot.multi_lvol_snapshot_fio import TestMultiLvolFio
from e2e_tests.ha_journal.lvol_journal_device_node_restart import TestDeviceNodeRestart



def get_all_tests():
    tests = [
        TestSingleNodeOutage,
        TestLvolFioNpcs0,
        TestLvolFioNpcs1,
        TestLvolFioNpcs2,
        TestSingleNodeFailure,
        TestDeviceNodeRestart
        # TestMultiLvolFio, - Enable when testing
    ]
    return tests