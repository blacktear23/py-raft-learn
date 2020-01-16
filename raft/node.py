from threading import *
from raft import *
from message import *
import queue


class Node(Thread):
    daemon = True

    def __init__(self, addr, transport):
        super(Node, self).__init__()
        self.addr = addr
        self.transport = transport
        self.raft_groups = {}
        self.suspended = False

    def create_raft_group(self, cluster_id, id, mode, peer_confs, log_storage):
        raft = Raft(id, cluster_id, self.addr, peer_confs, self.transport, log_storage, mode)
        self.raft_groups[cluster_id] = raft

    def run(self):
        while True:
            item = self.transport.recv_msg()
            cmd, params = item[0], item[1]
            if cmd == 'stop':
                LOG.info('Stop Node', self.addr)
                break

            if self.suspended:
                LOG.info('Node[S]: %s Skip cmd %s' % (self.addr, cmd))
                continue

            if cmd == 'tick':
                self.do_tick()
            elif cmd == 'propose':
                self.do_propose(params)
            elif cmd == 'transfer_leader':
                self.do_transfer_leader(params)
            elif cmd == 'add_node':
                self.do_add_node(params)
            elif cmd == 'delete_node':
                self.do_delete_node(params)
            else:
                self.on_message(item[0], item[1])

    def send_cmd(self, cmd, params=None):
        self.transport.send_msg(self.addr, [cmd, params])

    # API functions
    def suspend(self):
        self.suspended = True
        self.transport.suspended = True

    def resume(self):
        self.suspended = False
        self.transport.suspended = False

    def stop(self):
        if self.suspended:
            self.resume()
        self.send_cmd('stop')

    def tick(self):
        self.send_cmd('tick')

    def propose(self, cluster_id, data):
        future = Future(data)
        if cluster_id not in self.raft_groups:
            future.set_error('Cluster not found')
            return future
        self.send_cmd('propose', [cluster_id, future])
        return future

    def transfer_leader(self, cluster_id, target):
        future = Future(target)
        if cluster_id not in self.raft_groups:
            future.set_error('Cluster not found')
            return future
        self.send_cmd('transfer_leader', [cluster_id, future])
        return future

    def add_node(self, cluster_id, node_conf):
        future = Future(node_conf)
        if cluster_id not in self.raft_groups:
            future.set_error('Cluster not found')
            return future
        self.send_cmd('add_node', [cluster_id, future])
        return future

    def delete_node(self, cluster_id, node_id):
        future = Future(node_id)
        if cluster_id not in self.raft_groups:
            future.set_error('Cluster not found')
            return future
        self.send_cmd('delete_node', [cluster_id, future])
        return future

    def get(self, cluster_id, key, default=None):
        raft = self.raft_groups.get(cluster_id)
        if raft is None:
            return None
        raft.fsm.get(key, default)

    def set(self, cluster_id, key, value):
        return self.propose(cluster_id, 'SET %s %s' % (key, value))

    def delete(self, cluster_id, key):
        return self.propose(cluster_id, 'DEL %s NULL' % key)

    def get_cluster(self, cluster_id):
        return self.raft_groups.get(cluster_id)

    def dump(self):
        running = 'R'
        if self.suspended:
            running = 'S'
        for cid, raft in self.raft_groups.items():
            LOG.info('Node[%s]: %s, Cluster: %s' % (running, self.addr, cid))
            LOG.info('%s' % raft)
    # End API functions

    # API for test
    def force_set(self, cluster_id, key, value):
        rg = self.get_cluster(cluster_id)
        if rg:
            rg.force_append_log('SET %s %s' % (key, value))
    # End API for test

    # Helper Functions
    def do_propose(self, params):
        cluster_id, future = params[0], params[1]
        rg = self.raft_groups[cluster_id]
        rg.propose(future)

    def do_transfer_leader(self, params):
        cluster_id, future = params[0], params[1]
        rg = self.raft_groups[cluster_id]
        rg.transfer_leader(future)

    def do_add_node(self, params):
        cluster_id, future = params[0], params[1]
        rg = self.raft_groups[cluster_id]
        rg.add_node(future)

    def do_delete_node(self, params):
        cluster_id, future = params[0], params[1]
        rg = self.raft_groups[cluster_id]
        rg.delete_node(future)

    def do_tick(self):
        for rg in self.raft_groups.values():
            rg.tick()

    def route(self, msg):
        if msg.cluster_id in self.raft_groups:
            return self.raft_groups[msg.cluster_id]
        return None

    def on_message(self, msg_type, data):
        if msg_type == 'vote':
            msg = Vote.from_json(data)
            rg = self.route(msg)
            if rg:
                rg.on_vote(msg)
        elif msg_type == 'vote_reply':
            msg = VoteReply.from_json(data)
            rg = self.route(msg)
            if rg:
                rg.on_vote_reply(msg)
        elif msg_type == 'pre_vote':
            msg = PreVote.from_json(data)
            rg = self.route(msg)
            if rg:
                rg.on_pre_vote(msg)
        elif msg_type == 'pre_vote_reply':
            msg = PreVoteReply.from_json(data)
            rg = self.route(msg)
            if rg:
                rg.on_pre_vote_reply(msg)
        elif msg_type == 'append_logs':
            msg = AppendLogs.from_json(data)
            rg = self.route(msg)
            if rg:
                rg.on_append_logs(msg)
        elif msg_type == 'append_logs_reply':
            msg = AppendLogsReply.from_json(data)
            rg = self.route(msg)
            if rg:
                rg.on_append_logs_reply(msg)
        elif msg_type == 'timeout_now':
            msg = TimeoutNow.from_json(data)
            rg = self.route(msg)
            if rg:
                rg.on_timeout_now(msg)
        elif msg_type == 'timeout_now_reply':
            msg = TimeoutNowReply.from_json(data)
            rg = self.route(msg)
            if rg:
                rg.on_timeout_now_reply(msg)
        elif msg_type == 'install_snapshot':
            msg = InstallSnapshot.from_json(data)
            rg = self.route(msg)
            if rg:
                rg.on_install_snapshot(msg)
        elif msg_type == 'install_snapshot_reply':
            msg = InstallSnapshotReply.from_json(data)
            rg = self.route(msg)
            if rg:
                rg.on_install_snapshot_reply(msg)
        elif msg_type == 'configuration_change':
            msg = ConfigurationChange.from_json(data)
            rg = self.route(msg)
            if rg:
                rg.on_configuration_change(msg)
        elif msg_type == 'configuration_change_reply':
            msg = ConfigurationChangeReply.from_json(data)
            rg = self.route(msg)
            if rg:
                rg.on_configuration_change_reply(msg)
        else:
            LOG.warn('Unknown message type: %s' % msg_type)
    # End helper functions
