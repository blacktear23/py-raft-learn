from raft import *
import time


def init_nodes(num, transport_ex):
    nodes = []
    for i in range(num):
        addr = 'node-%d' % (i + 1)
        node = Node(addr, transport_ex.create_transport(addr))
        node.start()
        nodes.append(node)

    return nodes


# config = {
#   cluster_id: int,
#   nodes: [{
#       id: int,
#       addr: str,
#       mode: RaftMode,
#   }...]
# }

def bootstrap_raft_group(nodes, config):
    cluster_id = config['cluster_id']

    for node in nodes:
        for peer in config['nodes']:
            if peer['addr'] == node.addr:
                node.create_raft_group(
                    cluster_id, peer['id'],
                    peer.get('mode', PeerMode.Voter),
                    config['nodes'],
                    InMemoryLogStorage(),
                )


def on_leader(nodes, cluster_id, func=None):
    if func is None:
        return None

    ret = None
    for node in nodes:
        if node.suspended:
            continue

        raft = node.raft_groups.get(cluster_id)
        if raft and raft.is_leader():
            ret = func(node)
            break

    return ret


def tick_nodes(nodes, dump=None):
    for node in nodes:
        node.tick()

    time.sleep(0.05)
    if dump is not None:
        LOG.info('---------- Tick %s ----------' % dump)
        dump_clusters(nodes)


def stop_nodes(nodes):
    for node in nodes:
        node.stop()
        node.join()


def dump_nodes(nodes):
    for node in nodes:
        running = 'R'
        if node.suspended:
            running = 'S'
        for cid, rg in node.raft_groups.items():
            msg = 'Node[%s]: %s Cluster: %d %s' % (running, node.addr, cid, rg)
            LOG.info(msg)


def dump_clusters(nodes):
    clusters = {}
    for node in nodes:
        for cid, rg in node.raft_groups.items():
            if cid in clusters:
                clusters[cid].append((node, rg))
            else:
                clusters[cid] = [(node, rg)]

    for cid, rgs in clusters.items():
        LOG.info('Cluster: %s' % cid)
        for node, rg in rgs:
            running = 'R'
            if node.suspended:
                running = 'S'
            msg = 'Node[%s]: %s\n  %s' % (running, node.addr, rg)
            LOG.info(msg)
