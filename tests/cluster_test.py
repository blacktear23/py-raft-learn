from tests.utils import *


def test_all():
    transport_ex = QueueTransportExchange()
    nodes = init_nodes(5, transport_ex)
    cluster_1 = {
        'cluster_id': 1,
        'nodes': [
            {'id': 1, 'addr': 'node-1'},
            {'id': 2, 'addr': 'node-2'},
            {'id': 3, 'addr': 'node-3'},
        ]
    }
    bootstrap_raft_group(nodes, cluster_1)

    for i in range(40):
        LOG.info('---------- Tick', i, '----------')
        for node in nodes:
            node.tick()

        if i == 5:
            ret = on_leader(nodes, 1, lambda l: l.set(1, 'KEY1', 'Data'))
            if ret:
                ret.on_finish(lambda f: LOG.info(f))

        if i == 8:
            on_leader(nodes, 1, lambda l: l.suspend())

        if i == 15:
            ret = on_leader(nodes, 1, lambda l: l.set(1, 'KEY2', 'Data 2'))
            if ret:
                ret.on_finish(lambda f: LOG.info(f))

        if i == 19:
            for node in nodes:
                node.resume()

        if i >= 20 and i < 30:
            ret = on_leader(nodes, 1, lambda l: l.set(1, 'KEY3', 'DataN %d' % i))
            if ret:
                ret.on_finish(lambda f: LOG.info(f))

        if i == 25:
            node = nodes[3]
            LOG.info('Add Node:', node.addr)
            conf = {
                'id': 4,
                'mode': PeerMode.Learner,
                'addr': node.addr,
            }
            node.create_raft_group(1, conf['id'], conf['mode'], cluster_1['nodes'])
            ret = on_leader(nodes, 1, lambda l: l.add_node(1, conf))
            if ret:
                ret.on_finish(lambda f: LOG.info(f))

        if i == 30:
            target = None
            for node in nodes:
                rg = node.get_cluster(1)
                if not rg.is_leader():
                    target = rg.id
                    break

            if target:
                ret = on_leader(nodes, 1, lambda l: l.transfer_leader(1, target))
                if ret:
                    ret.on_finish(lambda f: LOG.info(f))

        time.sleep(0.05)
        if i % 1 == 0:
            for node in nodes:
                node.dump()

    stop_nodes(nodes)


def test_bootstrap():
    transport_ex = QueueTransportExchange()
    nodes = init_nodes(3, transport_ex)
    cluster_1 = {
        'cluster_id': 1,
        'nodes': [
            {'id': 1, 'addr': 'node-1'},
            {'id': 2, 'addr': 'node-2'},
            {'id': 3, 'addr': 'node-3'},
        ],
    }
    bootstrap_raft_group(nodes, cluster_1)

    success = False
    for i in range(10):
        tick_nodes(nodes)

        ret = on_leader(nodes, 1, lambda l: l)
        if ret:
            success = True

    if not success:
        LOG.error('Test Bootstrap Fail, No Leader found')
    stop_nodes(nodes)


def test_propose():
    transport_ex = QueueTransportExchange()
    nodes = init_nodes(3, transport_ex)
    cluster_2 = {
        'cluster_id': 2,
        'nodes': [
            {'id': 1, 'addr': 'node-1'},
            {'id': 2, 'addr': 'node-2'},
            {'id': 3, 'addr': 'node-3'},
        ],
    }
    bootstrap_raft_group(nodes, cluster_2)

    leader = None
    for i in range(20):
        tick_nodes(nodes)

        if leader is None:
            ret = on_leader(nodes, 2, lambda l: l)
            if ret:
                leader = ret
                continue

        if leader and i < 15:
            on_leader(nodes, 2, lambda l: l.set(2, 'KEY1', 'Data %d' % i))

    leader = on_leader(nodes, 2, lambda l: l)
    if leader is None:
        LOG.error('Test Propose Fail, No Leader found')

    rg = leader.get_cluster(2)
    if rg.fsm.get('KEY1') != 'Data 14':
        LOG.error('Test Propose Fail, Data not commited')
        LOG.info(rg.fsm.dump())

    stop_nodes(nodes)


def test_transfer_leader():
    transport_ex = QueueTransportExchange()
    nodes = init_nodes(3, transport_ex)
    cluster_3 = {
        'cluster_id': 3,
        'nodes': [
            {'id': 1, 'addr': 'node-1'},
            {'id': 2, 'addr': 'node-2'},
            {'id': 3, 'addr': 'node-3'},
        ],
    }
    bootstrap_raft_group(nodes, cluster_3)

    leader = None
    target = None
    transfer_future = None
    for i in range(20):
        tick_nodes(nodes)

        if leader is None:
            ret = on_leader(nodes, 3, lambda l: l)
            if ret:
                leader = ret
                continue

        if leader:
            rg = leader.get_cluster(3)
            if rg.id == 1:
                target = 2
            else:
                target = 1

        if transfer_future is None:
            transfer_future = on_leader(nodes, 3, lambda l: l.transfer_leader(3, target))

    if transfer_future is None:
        LOG.error('Transfer Leader Fail')

    if not transfer_future.finished:
        LOG.error('Transfer Leader Fail, Not finish')

    ret = on_leader(nodes, 3, lambda l: l)
    if ret:
        rg = ret.get_cluster(3)
        if rg.id != target:
            LOG.error('Transfer Leader Fail, Target is ', target, 'Current Leader is', rg.id)

    stop_nodes(nodes)


def test_add_voter():
    transport_ex = QueueTransportExchange()
    nodes = init_nodes(5, transport_ex)
    cluster_4 = {
        'cluster_id': 4,
        'nodes': [
            {'id': 1, 'addr': 'node-1'},
            {'id': 2, 'addr': 'node-2'},
            {'id': 3, 'addr': 'node-3'},
        ],
    }
    bootstrap_raft_group(nodes, cluster_4)

    leader = None
    add_node_future = None
    node = nodes[3]
    conf = {
        'id': 4,
        'mode': PeerMode.Voter,
        'addr': node.addr,
    }
    for i in range(20):
        tick_nodes(nodes)

        if leader is None:
            ret = on_leader(nodes, 4, lambda l: l)
            if ret:
                leader = ret
                continue

        if leader and not add_node_future:
            node.create_raft_group(4, conf['id'], conf['mode'], cluster_4['nodes'])
            add_node_future = on_leader(nodes, 4, lambda l: l.add_node(4, conf))

    if add_node_future is None:
        LOG.error('Add Node Voter Fail')

    if not add_node_future.finished:
        LOG.error('Add Node Voter Fail, Not finish')

    for node in nodes:
        rg = node.get_cluster(4)
        if rg is not None:
            if len(rg.peers) != 3:
                LOG.error('Add Node Voter Fail, node', rg.id, 'peers is', len(rg.peers), 'not 3')
            if rg.id != 4:
                if 4 not in rg.peers:
                    LOG.error('Add Node Voter Fail, new node not in peers')
                if rg.peers[4].mode != PeerMode.Voter:
                    LOG.error('Add Node Voter Fail, new node in peers with different mode')

    stop_nodes(nodes)


def test_add_learner():
    transport_ex = QueueTransportExchange()
    nodes = init_nodes(5, transport_ex)
    cluster_5 = {
        'cluster_id': 5,
        'nodes': [
            {'id': 1, 'addr': 'node-1'},
            {'id': 2, 'addr': 'node-2'},
            {'id': 3, 'addr': 'node-3'},
        ],
    }
    bootstrap_raft_group(nodes, cluster_5)

    leader = None
    add_node_future = None
    node = nodes[3]
    conf = {
        'id': 4,
        'mode': PeerMode.Learner,
        'addr': node.addr,
    }
    for i in range(20):
        tick_nodes(nodes)

        if leader is None:
            ret = on_leader(nodes, 5, lambda l: l)
            if ret:
                leader = ret
                continue

        if leader and not add_node_future:
            node.create_raft_group(5, conf['id'], conf['mode'], cluster_5['nodes'])
            add_node_future = on_leader(nodes, 5, lambda l: l.add_node(5, conf))

    if add_node_future is None:
        LOG.error('Add Node Learner Fail')

    if not add_node_future.finished:
        LOG.error('Add Node Learner Fail, Not finish')

    for node in nodes:
        rg = node.get_cluster(5)
        if rg is not None:
            if len(rg.peers) != 3:
                LOG.error('Add Node Learner Fail, node', rg.id, 'peers is', len(rg.peers), 'not 3')
            if rg.id != 4:
                if 4 not in rg.peers:
                    LOG.error('Add Node Learner Fail, new node not in peers')
                if rg.peers[4].mode != PeerMode.Learner:
                    LOG.error('Add Node Learner Fail, new node in peers with different mode')

    stop_nodes(nodes)
