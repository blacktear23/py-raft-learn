from tests.utils import *


def test_leader_crash():
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
    new_leader = None
    suspended = False
    for i in range(30):
        tick_nodes(nodes)

        if leader is None:
            leader = on_leader(nodes, 1, lambda l: l)
            continue

        if leader and not new_leader and not suspended:
            LOG.info('Suspend Leader', leader)
            leader.suspend()
            suspended = True
            continue

        if suspended and new_leader is None:
            ret = on_leader(nodes, 1, lambda l: l)
            if ret:
                new_leader = ret
                continue

        if new_leader and suspended:
            LOG.info('Resume Old Leader')
            leader.resume()
            suspended = False

    if leader is None or new_leader is None:
        LOG.error('Leader Crash Fail')
        return

    lrg = leader.get_cluster(1)
    nlrg = new_leader.get_cluster(1)
    if lrg.id == nlrg.id:
        LOG.error('Leader Crash Fail: new leader is old leader')

    if lrg.voted_for != nlrg.id:
        LOG.error('Leader Crash Fail: old leader not voted for new leader')

    if lrg.state != RaftState.Follower:
        LOG.error('Leader Crash Fail: old leader is not Follower:', lrg.get_state())

    if nlrg.term > 3:
        LOG.error('Leader Crash Fail: term too big', nlrg.term)

    stop_nodes(nodes)


def test_brain_split():
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
    transfer_future = None
    start_split = False
    split_tick = 0
    finish_split = False
    for i in range(30):
        tick_nodes(nodes)

        if leader is None:
            leader = on_leader(nodes, 1, lambda l: l)
            continue

        if leader and transfer_future is None:
            lrg = leader.get_cluster(1)
            if lrg.id != 1:
                transfer_future = on_leader(nodes, 1, lambda l: l.transfer_leader(1, 1))
            else:
                transfer_future = Future(None)
                transfer_future.set_result(True)
            continue

        if transfer_future and transfer_future.finished and not start_split:
            LOG.info('Start Brain Split')
            transport_ex.set_blacks('node-1', ['node-3', 'node-4', 'node-5'])
            transport_ex.set_blacks('node-2', ['node-3', 'node-4', 'node-5'])
            start_split = True
            split_tick = i
            continue

        if start_split and i - split_tick > 10 and not finish_split:
            transport_ex.set_blacks('node-1', ['node-3', 'node-4', 'node-5'], False)
            transport_ex.set_blacks('node-2', ['node-3', 'node-4', 'node-5'], False)
            finish_split = True
            continue

    num_leader = 0
    for node in nodes:
        rg = node.get_cluster(1)
        if rg.state == RaftState.Leader:
            num_leader += 1

    if num_leader > 1:
        LOG.error('Brain Split Fail, has more than 1 leader')

    leader = on_leader(nodes, 1, lambda l: l)
    rg = leader.get_cluster(1)
    if rg.term > 3:
        LOG.error('Brain Split Fail, term too big')

    if rg.id == 1:
        LOG.error('Brain Split Fail, Leader should not be 1')

    # dump_nodes(nodes)
    stop_nodes(nodes)


def test_brain_split_2():
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
    transfer_future = None
    start_split = False
    split_tick = 0
    finish_split = False
    for i in range(30):
        tick_nodes(nodes)
        # LOG.info('---------- Tick %s ----------' % i)
        # dump_nodes(nodes[0:2])

        if leader is None:
            leader = on_leader(nodes, 1, lambda l: l)
            continue

        if leader and transfer_future is None:
            lrg = leader.get_cluster(1)
            if lrg.id != 5:
                transfer_future = on_leader(nodes, 1, lambda l: l.transfer_leader(1, 5))
            else:
                transfer_future = Future(None)
                transfer_future.set_result(True)
            continue

        if transfer_future and transfer_future.finished and not start_split:
            LOG.info('Start Brain Split')
            transport_ex.set_blacks('node-1', ['node-3', 'node-4', 'node-5'])
            transport_ex.set_blacks('node-2', ['node-3', 'node-4', 'node-5'])
            start_split = True
            split_tick = i
            continue

        if start_split and i - split_tick > 10 and not finish_split:
            transport_ex.set_blacks('node-1', ['node-3', 'node-4', 'node-5'], False)
            transport_ex.set_blacks('node-2', ['node-3', 'node-4', 'node-5'], False)
            finish_split = True
            continue

    num_leader = 0
    for node in nodes:
        rg = node.get_cluster(1)
        if rg.state == RaftState.Leader:
            num_leader += 1

    if num_leader > 1:
        LOG.error('Brain Split Fail, has more than 1 leader')

    leader = on_leader(nodes, 1, lambda l: l)
    rg = leader.get_cluster(1)
    if rg.term > 3:
        LOG.error('Brain Split Fail, term too big')

    if rg.id in [1, 2]:
        LOG.error('Brain Split Fail, Leader should not be 1 or 2')

    # dump_nodes(nodes)
    stop_nodes(nodes)


def test_brain_split_3():
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
    transfer_future = None
    start_split = False
    split_tick = 0
    finish_split = False
    for i in range(30):
        tick_nodes(nodes)
        # LOG.info('---------- Tick %s ----------' % i)
        # dump_nodes(nodes[2:4])

        if leader is None:
            leader = on_leader(nodes, 1, lambda l: l)
            continue

        if leader and transfer_future is None:
            lrg = leader.get_cluster(1)
            if lrg.id != 1:
                transfer_future = on_leader(nodes, 1, lambda l: l.transfer_leader(1, 1))
            else:
                transfer_future = Future(None)
                transfer_future.set_result(True)
            continue

        if transfer_future and transfer_future.finished and not start_split:
            LOG.info('Start Brain Split')
            transport_ex.set_blacks('node-1', ['node-3', 'node-4'])
            transport_ex.set_blacks('node-2', ['node-3', 'node-4'])
            start_split = True
            split_tick = i
            continue

        if start_split and i - split_tick > 10 and not finish_split:
            transport_ex.set_blacks('node-1', ['node-3', 'node-4'], False)
            transport_ex.set_blacks('node-2', ['node-3', 'node-4'], False)
            finish_split = True
            continue

    num_leader = 0
    for node in nodes:
        rg = node.get_cluster(1)
        if rg.state == RaftState.Leader:
            num_leader += 1

    if num_leader > 1:
        LOG.error('Brain Split Fail, has more than 1 leader')

    leader = on_leader(nodes, 1, lambda l: l)
    rg = leader.get_cluster(1)
    if rg.term > 3:
        LOG.error('Brain Split Fail, term too big')

    if rg.id in [3, 4]:
        LOG.error('Brain Split Fail, Leader should not be 3 or 4')

    # dump_nodes(nodes)
    stop_nodes(nodes)
