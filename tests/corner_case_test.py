from tests.utils import *


def test_replication():
    transport_ex = QueueTransportExchange()
    nodes = init_nodes(5, transport_ex)
    cluster_cfg = {
        'cluster_id': 1,
        'nodes': [
            {'id': 1, 'addr': 'node-1'},
            {'id': 2, 'addr': 'node-2'},
            {'id': 3, 'addr': 'node-3'},
            {'id': 4, 'addr': 'node-4'},
            {'id': 5, 'addr': 'node-5'},
        ],
    }
    bootstrap_raft_group(nodes, cluster_cfg)

    leader = None
    tlf = None
    for i in range(10):
        tick_nodes(nodes)

        if leader is None:
            leader = on_leader(nodes, 1, lambda l: l)
            if leader:
                if leader.get_cluster(1).id != 1:
                    tlf = leader.transfer_leader(1, 1)
                else:
                    break

        if tlf and tlf.finished:
            leader = on_leader(nodes, 1, lambda l: l)
            break

    future = None
    for i in range(20):
        tick_nodes(nodes)

        if not future:
            future = leader.set(1, 'Key1', 'Data 1')

        if future and future.finished:
            for i in range(3):
                tick_nodes(nodes)
            break

    leader.suspend()
    nodes[0].force_set(1, 'Key2', 'Data 2')
    nodes[1].force_set(1, 'Key2', 'Data 2')
    for node in nodes:
        rg = node.get_cluster(1)
        rg.expire_tick = 5
        rg.timeouted = True

    nodes[4].get_cluster(1).expire_tick = 0

    leader = None
    for i in range(30):
        tick_nodes(nodes)

        if not leader:
            leader = on_leader(nodes, 1, lambda l: l)
            if leader:
                break

    if leader.get_cluster(1).id != 5:
        LOG.error('Node 5 not select as Leader')

    leader.force_set(1, 'Key3', 'Data 3')
    leader.suspend()
    nodes[0].resume()
    for node in nodes:
        rg = node.get_cluster(1)
        rg.expire_tick = 5
        rg.timeouted = True

    nodes[0].get_cluster(1).expire_tick = 0
    leader = None
    for i in range(10):
        tick_nodes(nodes)

        if not leader:
            leader = on_leader(nodes, 1, lambda l: l)
            if leader:
                break

    leader.force_set(1, 'Key4', 'Data 4')
    leader.suspend()
    nodes[4].get_cluster(1).state = RaftState.Follower
    nodes[4].resume()

    for i in range(5):
        tick_nodes(nodes)

    leader.resume()
    future = None
    for i in range(5):
        tick_nodes(nodes)
        if future is None:
            future = on_leader(nodes, 1, lambda l: l.set(1, 'Key5', 'Data 5'))
        else:
            if future.finished:
                for i in range(2):
                    tick_nodes(nodes)
                break

    for node in nodes:
        rg = node.get_cluster(1)
        if rg.fsm.get('Key1') != 'Data 1':
            LOG.error('id: %s Key1 error' % rg.id)
        if rg.fsm.get('Key2') != 'Data 2':
            LOG.error('id: %s Key2 error' % rg.id)
        if rg.fsm.get('Key5') != 'Data 5':
            LOG.error('id: %s Key5 error' % rg.id)

    stop_nodes(nodes)
