from tests.utils import *


def init_servers(addrs):
    servers = []
    nodes = []
    for addr in addrs:
        server = RaftNetworkServer(addr)
        server.start()
        nodes.append(server.get_node())
        servers.append(server)

    return servers, nodes


def stop_servers(servers):
    for srv in servers:
        srv.stop()
        srv.wait_stop()
        LOG.info('Stop Server:', srv.addr)


def test_network_transport():
    LOG.level = Logger.DEBUG

    addrs = [
        '127.0.0.1:9901',
        '127.0.0.1:9902',
        '127.0.0.1:9903',
    ]
    servers, nodes = init_servers(addrs)

    cluster_cfg = {
        'cluster_id': 1,
        'nodes': [
            {'id': 1, 'addr': '127.0.0.1:9901'},
            {'id': 2, 'addr': '127.0.0.1:9902'},
            {'id': 3, 'addr': '127.0.0.1:9903'},
        ],
    }

    bootstrap_raft_group(nodes, cluster_cfg)

    leader = None
    for i in range(20):
        tick_nodes(nodes, i, 0.5)

        if leader is None:
            leader = on_leader(nodes, 1, lambda l: l)
        else:
            break

    if leader is None:
        LOG.error('No leader elected')
        dump_clusters(nodes)
        stop_servers(servers)
        return

    for i in range(10):
        tick_nodes(nodes, i, 0.5)

        key = 'Key%d' % i
        val = 'Data %d' % i
        leader.set(1, key, val)

    for i in range(3):
        tick_nodes(nodes, i, 0.5)

    dump_clusters(nodes)
    stop_servers(servers)
