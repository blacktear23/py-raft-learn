from tests.utils import *


def test_multi_raft():
    transport_ex = QueueTransportExchange()
    nodes = init_nodes(5, transport_ex)
    cluster_1 = {
        'cluster_id': 1,
        'nodes': [
            {'id': 1, 'addr': 'node-1'},
            {'id': 2, 'addr': 'node-2'},
            {'id': 3, 'addr': 'node-3'},
        ],
    }

    cluster_2 = {
        'cluster_id': 2,
        'nodes': [
            {'id': 4, 'addr': 'node-3'},
            {'id': 5, 'addr': 'node-4'},
            {'id': 6, 'addr': 'node-5'},
        ],
    }

    cluster_3 = {
        'cluster_id': 3,
        'nodes': [
            {'id': 7, 'addr': 'node-2'},
            {'id': 8, 'addr': 'node-3'},
            {'id': 9, 'addr': 'node-4'},
        ],
    }

    bootstrap_raft_group(nodes, cluster_1)
    bootstrap_raft_group(nodes, cluster_2)
    bootstrap_raft_group(nodes, cluster_3)

    leaders = {
        1: None,
        2: None,
        3: None,
    }
    for i in range(20):
        tick_nodes(nodes)

        for i in range(3):
            cid = i + 1
            if leaders[cid] is None:
                leaders[cid] = on_leader(nodes, cid, lambda l: l)

        if None not in leaders.values():
            break

    if None in leaders.values():
        LOG.error('Multi-raft fail: some cluster not elect a leader: %s' % leaders)

    for i in range(10):
        tick_nodes(nodes)

        for cid, l in leaders.items():
            key = 'Key-%s-%s' % (cid, i)
            value = 'Data-%s-%s' % (cid, i)
            l.set(cid, key, value)

    for i in range(5):
        tick_nodes(nodes)

    for cid, l in leaders.items():
        fsm = l.get_cluster(cid).fsm
        if len(fsm.data) != 10:
            LOG.error('Cluster:', cid, 'lost data')

    stop_nodes(nodes)
