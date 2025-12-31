from simplyblock_core.test import test_utils


def test_scheduler_below_25():
    print("\ntest_scheduler_below_25")
    testing_nodes_25 = [
        [
            {"uuid": "123", "node_size_util": 20, "lvol_count_util": 10},
            {"uuid": "456", "node_size_util": 20, "lvol_count_util": 10},
            {"uuid": "789", "node_size_util": 20, "lvol_count_util": 10}
        ],
        [
            {"uuid": "123", "node_size_util": 10, "lvol_count_util": 10},
            {"uuid": "456", "node_size_util": 15, "lvol_count_util": 50},
            {"uuid": "789", "node_size_util": 26, "lvol_count_util": 100}
        ],
    ]
    test_utils.run_lvol_scheduler_test(testing_nodes_25)


def test_scheduler_25_75():
    print("\ntest_scheduler_25_75")
    testing_nodes_25_75 = [
        [
            {"uuid": "123", "node_size_util": 25, "lvol_count_util": 10},
            {"uuid": "456", "node_size_util": 25, "lvol_count_util": 10},
            {"uuid": "789", "node_size_util": 25, "lvol_count_util": 10}
        ],
        [
            {"uuid": "123", "node_size_util": 75, "lvol_count_util": 10},
            {"uuid": "456", "node_size_util": 75, "lvol_count_util": 50},
            {"uuid": "789", "node_size_util": 75, "lvol_count_util": 20}
        ],
        [
            {"uuid": "123", "node_size_util": 20, "lvol_count_util": 10},
            {"uuid": "456", "node_size_util": 50, "lvol_count_util": 50},
            {"uuid": "789", "node_size_util": 50, "lvol_count_util": 70}
        ],
    ]
    test_utils.run_lvol_scheduler_test(testing_nodes_25_75)



def test_scheduler_75():
    print("\ntest_scheduler_75")
    testing_nodes_25_75 = [
        [
            {"uuid": "123", "node_size_util": 75, "lvol_count_util": 10},
            {"uuid": "456", "node_size_util": 80, "lvol_count_util": 10},
            {"uuid": "789", "node_size_util": 85, "lvol_count_util": 10}
        ],
        [
            {"uuid": "123", "node_size_util": 60, "lvol_count_util": 10},
            {"uuid": "456", "node_size_util": 80, "lvol_count_util": 10},
            {"uuid": "789", "node_size_util": 90, "lvol_count_util": 5}
        ],
        [
            {"uuid": "123", "node_size_util": 20, "lvol_count_util": 10},
            {"uuid": "456", "node_size_util": 80, "lvol_count_util": 20},
            {"uuid": "789", "node_size_util": 85, "lvol_count_util": 5}
        ],
    ]
    test_utils.run_lvol_scheduler_test(testing_nodes_25_75)

