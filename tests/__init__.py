from cluster_test import *
from node_fail_test import *
from corner_case_test import *


TESTS = [
    test_bootstrap,
    test_propose,
    test_transfer_leader,
    test_add_voter,
    test_add_learner,
    test_leader_crash,
    test_brain_split,
    test_brain_split_2,
    test_brain_split_3,
    test_replication,
]
