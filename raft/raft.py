from message import *
from state_machine import *
from random import random
from utils import *

STATES = [
    'PreCandidate',
    'Candidate',
    'Leader',
    'Follower',
]


class Future(object):
    def __init__(self, data):
        self.data = data
        self.finished = False
        self.on_finish_callback = None
        self.result = None
        self.error = None
        self.hint = None

    def resolve(self):
        if self.finished:
            return None, None
        return self.result, self.error

    def set_result(self, result):
        self.finished = True
        self.result = result
        if self.on_finish_callback:
            self.on_finish_callback(self)

    def set_error(self, error, hint=None):
        self.finished = True
        self.error = error
        self.hint = hint
        if self.on_finish_callback:
            self.on_finish_callback(self)

    def get_error_hint(self):
        return self.hint

    def __str__(self):
        return 'Future<F=%s, R=%s, E=%s> %s' % (self.finished, self.result, self.error, self.data)

    def on_finish(self, callback):
        self.on_finish_callback = callback


class PeerMode(object):
    Voter = 1
    Learner = 2


class Peer(object):
    def __init__(self, id, mode, transport):
        if id <= 0:
            raise Exception('ID should not less than 1')
        self.id = id
        self.first_contact = True
        self.voted_for = 0
        self.pre_voted_for = 0
        self.next_index = 1
        self.match_index = 0
        self.match_config_version = 0
        self.address = ''
        self.mode = mode
        self.in_install_snapshot = False
        self.transport = transport

    def get_mode(self):
        if self.mode == PeerMode.Voter:
            return 'Voter'
        elif self.mode == PeerMode.Learner:
            return 'Learner'
        else:
            return 'Unknown'

    def __str__(self):
        return 'id=%d addr=%s mode=%s voted_for=%d pre_voted_for=%d next=%d match=%d match_config=%d fist_contact=%s' % (
            self.id, self.address, self.get_mode()[0], self.voted_for, self.pre_voted_for,
            self.next_index, self.match_index, self.match_config_version, self.first_contact,
        )

    def can_vote(self):
        return self.mode == PeerMode.Voter

    def send_message(self, msg_type, msg):
        data = msg.to_json()
        self.transport.send_msg(self.address, [msg_type, data])

    def to_json(self):
        return json.dumps({
            'id': self.id,
            'mode': self.mode,
            'address': self.address,
        })

    @classmethod
    def from_json(self, msg):
        data = json.loads(msg)
        p = Peer(data['id'], data['mode'], None)
        p.address = data['address']
        return p


class RaftState(object):
    PreCandidate = 1
    Candidate = 2
    Leader = 3
    Follower = 4


class Raft(object):
    # Params:
    #   id: int
    #   cluster_id: int
    #   peer_configs: [(id, mode, addr)...]
    #   transport: Transport object
    def __init__(self, id, cluster_id, address, peer_configs, transport, log_storage, fsm_storage, mode=PeerMode.Voter):
        if id <= 0:
            raise Exception('ID should not less than 1')

        self.id = id
        self.cluster_id = cluster_id
        self.address = address
        self.mode = mode
        self.state = RaftState.Follower
        self.term = 0
        self.expire_tick = 0
        self.logs = log_storage
        self.commit_index = 0
        self.apply_index = 0
        self.voted_for = 0
        self.last_vote_term = 0
        self.timeouted = False
        self.in_leader_transfer = False
        self.leader_transfer_target = 0
        self.timeout_now_sended = False
        self.fsm = StateMachine(fsm_storage)
        self.last_snapshot = None
        self.transport = transport
        self.init_peers(peer_configs)
        self.reset_timer()
        self.jobs = {}

    def get_state(self):
        if self.state == RaftState.Leader:
            return 'Leader'
        elif self.state == RaftState.Follower:
            return 'Follower'
        elif self.state == RaftState.PreCandidate:
            return 'PreCandidate'
        elif self.state == RaftState.Candidate:
            return 'Candidate'
        else:
            return 'Unknown'

    def format_list(self, name, data):
        if len(data) == 0:
            return '\n    %s=[]' % name

        ret = '\n    %s=[\n' % name
        for item in data:
            ret += '        %s\n' % item
        ret += '    ]'
        return ret

    def __str__(self):
        tostr = 'X'
        if self.timeouted:
            tostr = 'O'

        ret = 'Raft<id=%s state=%s> addr=%s term=%s voted_for=%s cfg_ver=%s commit=%s apply=%s timeout=%d timeouted=%s' % (
            self.id, self.get_state()[0:1], self.address, self.term, self.voted_for, self.config_version,
            self.commit_index, self.apply_index, self.expire_tick, tostr,
        )
        ret += '\n    logs=[%s]' % self.logs.dump()
        ret += self.format_list('jobs', self.jobs.values())
        ret += self.format_list('peers', self.peers.values())
        if self.next_peers is not None:
            ret += self.format_list('next_peers', self.next_peers.values())
        ret += '\n    fsm=[%s]' % self.fsm.dump()
        return ret

    def reset_timer(self):
        self.expire_tick = int(random() * 10) % 5 + 2

    def init_peers(self, peer_configs):
        self.peers = {}
        self.peers_version = 1
        self.config_version = 1
        self.next_peers = None
        self.next_peers_version = None
        for conf in peer_configs:
            pid = conf['id']
            if pid == self.id:
                continue

            mode = conf.get('mode', PeerMode.Voter)
            addr = conf['addr']
            peer = Peer(pid, mode, self.transport)
            peer.address = addr
            self.peers[pid] = peer

    def create_next_config(self, peers, version=None):
        if self.next_peers is not None and version is not None and version < self.next_peers_version:
            return False

        if version is None:
            version = self.peers_version + 1

        self.next_peers = {}
        self.next_peers_version = version
        for pconf in peers:
            if pconf.id == self.id:
                continue
            peer = Peer(pconf.id, pconf.mode, self.transport)
            peer.address = pconf.address
            self.next_peers[peer.id] = peer

        return True

    def commit_next_config(self):
        if self.next_peers is None:
            return

        old_ver = self.peers_version
        new_ver = self.next_peers_version
        self.peers = self.next_peers
        self.peers_version = self.next_peers_version
        self.config_version = self.peers_version
        self.next_peers = None
        self.next_peers_version = None
        jkey = 'update-config-%s' % self.peers_version
        self.finish_job(jkey, True)
        LOG.debug('Commit Next Config:', self.id, 'from v %d to v %d' % (old_ver, new_ver))

    def try_commit_config(self, version):
        if version == self.next_peers_version:
            self.commit_next_config()

    def is_leader(self):
        return self.state == RaftState.Leader

    def is_follower(self):
        return self.state == RaftState.Follower

    def is_candidate(self):
        return self.state == RaftState.Candidate

    def is_pre_candidate(self):
        return self.state == RaftState.PreCandidate

    def tick(self):
        if self.is_leader():
            # in Leader state send heartbeat to followers
            self.do_heartbeat()
            return

        self.expire_tick -= 1
        if self.is_timeout():
            self.timeouted = True
            if self.is_candidate():
                # in Candidate state just re-vote
                self.do_vote()
            elif self.state == RaftState.PreCandidate:
                # in PreCandidate state just switch to Follower
                self.state = RaftState.Follower
            elif self.is_follower():
                # in Follower state and not vote any more do vote
                # else do pre-vote
                if self.voted_for == 0:
                    self.do_vote()
                else:
                    self.do_pre_vote()

    def is_timeout(self):
        if self.mode != PeerMode.Voter:
            return False

        return self.expire_tick < 0

    def add_peer(self, peer_id, peer_mode, peer_address):
        peer = Peer(peer_id, peer_mode, self.transport)
        peer.address = peer_address

    def get_last_log(self):
        log = self.logs.get_last_log()
        if log is None:
            return 0, self.term

        return log.index, log.term

    def get_log(self, idx):
        return self.logs.get_log(idx)

    def get_logs(self, start_idx):
        return self.logs.get_logs(start_idx)

    def prepare_msg(self, msg):
        msg.cluster_id = self.cluster_id
        msg.sender = self.id
        return msg

    def broadcast(self, msg_type, msg, peer_mode=None):
        for peer in self.peers.values():
            if peer_mode is not None and peer.mode != peer_mode:
                continue
            pmsg = msg.clone()
            pmsg.to = peer.id
            peer.send_message(msg_type, pmsg)

    def broadcast_voter(self, msg_type, msg):
        self.broadcast(msg_type, msg, PeerMode.Voter)

    def reply_message(self, pre_msg, msg_type, msg):
        pid = pre_msg.sender
        msg.to = pid
        if pid not in self.peers:
            LOG.warn('id:', self.id, 'not found peer', pid, 'ignore reply message', msg_type, self.format_list('peers', self.peers))
            return

        peer = self.peers[pid]
        peer.send_message(msg_type, msg)

    def send_message(self, peer_id, msg_type, msg):
        msg.to = peer_id
        peer = self.peers[peer_id]
        peer.send_message(msg_type, msg)

    def quorum(self):
        total = 0
        if self.mode == PeerMode.Voter:
            total += 1

        for peer in self.peers.values():
            if peer.mode == PeerMode.Voter:
                total += 1

        return int(total / 2) + 1

    def finish_job(self, key, result):
        job = self.jobs.get(key)
        if job:
            job.set_result(result)
            del self.jobs[key]

    def peer_has_gap(self, peer):
        last_log_idx, _ = self.get_last_log()
        return peer.match_index < last_log_idx

    # Send message functions
    def do_vote(self):
        if self.mode != PeerMode.Voter:
            return

        self.state = RaftState.Candidate
        self.term += 1
        self.last_vote_term = self.term
        self.voted_for = self.id
        LOG.debug('%s enter Candidate state' % self.id)
        last_log_idx, last_log_term = self.get_last_log()
        vote = self.prepare_msg(Vote())
        vote.term = self.term
        vote.candidate_id = self.id
        vote.last_log_index = last_log_idx
        vote.last_log_term = last_log_term
        for peer in self.peers.values():
            peer.voted_for = 0
            peer.first_contact = True

        self.reset_timer()
        self.broadcast_voter('vote', vote)

    def do_pre_vote(self):
        if self.mode != PeerMode.Voter:
            return

        self.state = RaftState.PreCandidate
        LOG.debug('%s enter PreCandidate state' % self.id)
        last_log_idx, last_log_term = self.get_last_log()
        vote = self.prepare_msg(PreVote())
        vote.term = self.term + 1
        vote.candidate_id = self.id
        vote.last_log_index = last_log_idx
        vote.last_log_term = last_log_term
        for peer in self.peers.values():
            peer.pre_voted_for = 0

        self.reset_timer()
        self.broadcast_voter('pre_vote', vote)

    def do_heartbeat(self, probe=False):
        for peer in self.peers.values():
            self.send_append_logs_to(peer, probe)
    # End Send message functions

    # On message functions
    def on_vote(self, vote):
        # Ignore vote message when not Voter
        if self.mode != PeerMode.Voter:
            return

        ret = self.prepare_msg(VoteReply())
        ret.term = vote.term
        ret.vote_granted = False

        # Reject node not in peers
        if self.peers.get(vote.candidate_id, None) is None:
            return

        # Reject old term
        if vote.term < self.term:
            self.reply_message(vote, 'vote_reply', ret)
            return

        # Update term
        if vote.term > self.term:
            self.state = RaftState.Follower
            self.term = vote.term

        # Check already voted
        if self.last_vote_term == vote.term:
            if self.voted_for == vote.candidate_id:
                ret.vote_granted = True
            self.reply_message(vote, 'vote_reply', ret)
            return

        # If last log term not match just reject
        last_log_idx, last_log_term = self.get_last_log()
        if last_log_term > vote.last_log_term:
            self.reply_message(vote, 'vote_reply', ret)
            return

        # If last lot term match but index is less than current, just reject
        if last_log_term == vote.last_log_term and last_log_idx > vote.last_log_index:
            self.reply_message(vote, 'vote_reply', ret)
            return

        ret.vote_granted = True
        self.voted_for = vote.candidate_id
        self.last_vote_term = vote.term
        self.reset_timer()
        self.timeouted = False
        self.reply_message(vote, 'vote_reply', ret)

    def on_vote_reply(self, vote_reply):
        if not self.is_candidate():
            if vote_reply.vote_granted:
                pid = vote_reply.sender
                peer = self.peers[pid]
                peer.voted_for = self.id
            return

        if vote_reply.term == self.term and vote_reply.vote_granted:
            pid = vote_reply.sender
            peer = self.peers[pid]
            peer.voted_for = self.id
            self.check_vote_status()

    def on_pre_vote(self, vote):
        ret = self.prepare_msg(PreVoteReply())
        ret.term = vote.term
        ret.vote_granted = False

        # Reject when node not timeout
        if vote.term <= self.term or not self.timeouted:
            self.reply_message(vote, 'pre_vote_reply', ret)
            return

        # Reject when log term not matched
        last_log_idx, last_log_term = self.get_last_log()
        if last_log_term > vote.last_log_term:
            self.reply_message(vote, 'pre_vote_reply', ret)
            return

        # Reject when log term match bug index is less than current
        if last_log_term == vote.last_log_term and last_log_idx > vote.last_log_index:
            self.reply_message(vote, 'pre_vote_reply', ret)
            return

        ret.vote_granted = True
        self.reply_message(vote, 'pre_vote_reply', ret)

    def on_pre_vote_reply(self, msg):
        if not self.is_pre_candidate():
            return

        if msg.vote_granted:
            pid = msg.sender
            peer = self.peers[pid]
            peer.pre_voted_for = self.id
            self.check_pre_vote_status()

    def on_append_logs(self, msg):
        LOG.debug('on-al', msg.sender, '->', self.id, msg.to_json())
        ret = self.prepare_msg(AppendLogsReply())
        ret.term = self.term
        ret.match_index = self.commit_index
        ret.match_config_version = self.last_config_version()
        ret.success = False

        # Ignore old term
        if msg.term < self.term:
            self.reply_message(msg, 'append_logs_reply', ret)
            return

        if msg.term > self.term or self.state != RaftState.Follower:
            self.term = msg.term
            self.state = RaftState.Follower
            ret.term = msg.term

        self.voted_for = msg.sender

        last_log_idx, last_log_term = self.get_last_log()

        if msg.prev_log_index > 0:
            log = self.get_log(msg.prev_log_index)
            if log is None:
                deleted = self.logs.delete_tail(ret.match_index + 1)
                self.reply_message(msg, 'append_logs_reply', ret)
                for log in deleted:
                    self.finish_job(log.index, False)
                return
            if log.term != msg.prev_log_term:
                deleted = self.logs.delete_tail(ret.match_index + 1)
                self.reply_message(msg, 'append_logs_reply', ret)
                for log in deleted:
                    self.finish_job(log.index, False)
                return

        new_logs = []
        deleted = []
        if len(msg.logs) > 0:
            for log in msg.logs:
                if log.index > last_log_idx:
                    new_logs.append(log)
                    continue

                clog = self.logs.get_log(log.index)
                if clog is None:
                    self.reply_message(msg, 'append_logs_reply', ret)
                    return

                if log.term != clog.term:
                    deleted = self.logs.delete_tail(log.index)
                    new_logs.append(log)
                    last_log_idx = log.index

        if len(new_logs) > 0:
            self.logs.append_logs(new_logs)

        nlast_log_idx, nlast_log_term = self.get_last_log()
        ret.match_index = nlast_log_idx
        if msg.prev_log_index >= last_log_idx and msg.leader_commit > self.commit_index:
            self.commit_index = min(msg.leader_commit, ret.match_index)
            self.do_apply()

        if msg.config_version > self.config_version:
            self.try_apply_config(msg)

        ret.success = True
        ret.match_config_version = self.last_config_version()
        self.reply_message(msg, 'append_logs_reply', ret)
        self.timeouted = False
        self.reset_timer()
        # Finish deleted logs as failed
        for log in deleted:
            self.finish_job(log.index, False)

    def on_append_logs_reply(self, msg):
        LOG.debug('on-alr', msg.sender, '->', self.id, msg.to_json())
        if msg.term > self.term:
            self.haneld_stale_term()
            return

        pid = msg.sender
        if pid not in self.peers:
            return

        peer = self.peers[pid]
        peer.next_index = msg.match_index + 1
        peer.match_index = msg.match_index
        peer.match_config_version = msg.match_config_version

        if peer.first_contact:
            peer.first_contact = False

        if self.peer_has_gap(peer):
            if self.need_send_snapshot(peer):
                self.send_install_snapshot(peer)
            else:
                self.send_append_logs_to(peer)
        else:
            if self.in_leader_transfer and not self.timeout_now_sended:
                msg = self.prepare_msg(TimeoutNow())
                self.send_message(self.leader_transfer_target, 'timeout_now', msg)
                self.timeout_now_sended = True

        self.try_update_config(peer)
        self.update_commit_index()

    def on_timeout_now(self, msg):
        self.do_vote()
        ret = self.prepare_msg(TimeoutNowReply())
        self.reply_message(msg, 'timeout_now_reply', ret)

    def on_timeout_now_reply(self, msg):
        if not self.in_leader_transfer:
            return

        # Set self as follower and reset timer
        self.state = RaftState.Follower
        self.reset_timer()
        # Finish leader transfer
        self.in_leader_transfer = False
        ifkey = 'transfer-leader-to-%s' % msg.sender
        self.finish_job(ifkey, True)

    def on_install_snapshot(self, msg):
        LOG.debug('on-is', msg.sender, '->', self.id, 'term:', msg.term)
        snap = Snapshot(msg.last_log_index, msg.last_log_term)
        snap.set_data(msg.data)
        self.peers_version = snap.config_version
        self.fsm.install_snapshot(snap)
        self.commit_index = msg.last_log_index
        self.apply_index = msg.last_log_index
        self.term = msg.term
        self.logs.loads(msg.logs)
        ret = self.prepare_msg(InstallSnapshotReply())
        ret.last_log_index = msg.last_log_index
        ret.last_log_term = msg.last_log_term
        ret.success = True
        self.reply_message(msg, 'install_snapshot_reply', ret)

    def on_install_snapshot_reply(self, msg):
        if not msg.success:
            return

        peer = self.peers[msg.sender]
        peer.match_index = msg.last_log_index
        peer.next_index = peer.match_index + 1
        peer.in_install_snapshot = False
        peer.voted_for = self.id
        if self.peer_has_gap(peer):
            self.send_append_logs_to(peer)

    def on_configuration_change(self, msg):
        LOG.debug('on-cc', msg.sender, '->', self.id, 'ver:', msg.version)
        # if peers is empty for join nodes
        if len(self.peers) > 0:
            if msg.sender not in self.peers:
                return

        success = self.create_next_config(msg.peers, msg.version)
        ret = self.prepare_msg(ConfigurationChangeReply())
        ret.success = success
        ret.version = msg.version
        self.reply_message(msg, 'configuration_change_reply', ret)

    def on_configuration_change_reply(self, msg):
        LOG.debug('on-ccr', msg.sender, '->', self.id, 'ver:', msg.version, 'success:', msg.success)
        if not msg.success:
            return
        pid = msg.sender
        peer = self.peers[pid]
        peer.match_config_version = msg.version
        if self.next_peers:
            self.try_update_config(peer)
    # End On message functions

    # Helper functions
    def haneld_stale_term(self):
        self.state = RaftState.Follower
        self.reset_timer()

    def check_vote_status(self):
        voted = 1
        for peer in self.peers.values():
            if peer.voted_for == self.id:
                voted += 1

        if voted >= self.quorum():
            last_index, _ = self.get_last_log()
            self.state = RaftState.Leader
            self.timeouted = False
            for peer in self.peers.values():
                peer.next_index = last_index + 1
                peer.first_contact = True

            LOG.info('%s enter Leader state' % self.id)
            self.add_nop()
            self.do_heartbeat()

    def add_nop(self):
        last_idx, _ = self.get_last_log()
        if self.commit_index < last_idx:
            log = Log(last_idx + 1, self.term, 'NOP KEY VALUE')
            self.logs.append(log)

    def check_pre_vote_status(self):
        voted = 1
        for peer in self.peers.values():
            if peer.pre_voted_for == self.id:
                voted += 1

        if voted >= self.quorum():
            self.do_vote()

    def send_append_logs_to(self, peer, probe=False):
        ret = self.prepare_msg(AppendLogs())
        ret.term = self.term
        log = self.get_log(peer.match_index)
        ret.prev_log_index = peer.match_index
        if log:
            ret.prev_log_term = log.term
        else:
            ret.prev_log_term = self.term

        ret.leader_commit = self.commit_index
        if peer.first_contact:
            probe = True

        if not probe:
            ret.logs = self.get_logs(peer.next_index)
        else:
            ret.logs = []

        ret.config_version = self.config_version
        self.send_message(peer.id, 'append_logs', ret)

    def do_apply(self):
        if self.apply_index >= self.commit_index:
            if self.need_do_snapshot():
                self.do_snapshot()
            return

        for i in range(self.commit_index - self.apply_index):
            log = self.get_log(self.apply_index + 1)
            if log is not None:
                self.fsm.apply([log])
                self.apply_index = log.index
                self.finish_job(log.index, True)

        if self.need_do_snapshot():
            self.do_snapshot()

    def need_do_snapshot(self):
        if self.logs.size() < 10:
            return False

        first_log = self.logs.get_first_log()
        if self.apply_index - first_log.index > 3:
            return True

        return False

    def do_snapshot(self):
        self.last_snapshot = self.fsm.create_snapshot()
        self.last_snapshot.config_version = self.peers_version
        self.logs.compact(self.last_snapshot.last_log_index - 2)
        self.last_snapshot.set_logs(self.logs.all())
        LOG.debug('do-snapshot', self.id, self.last_snapshot.last_log_term, self.last_snapshot.last_log_index)

    def calculate_max_commit_index(self):
        last_log_idx, _ = self.get_last_log()
        new_ci = self.commit_index
        quorum = self.quorum()
        while True:
            new_ci += 1
            if new_ci > last_log_idx:
                break
            success = 1
            for peer in self.peers.values():
                pmi = peer.match_index
                log = self.get_log(pmi)
                if log is None:
                    continue

                if pmi >= new_ci and log.term <= self.term:
                    success += 1

            if success < quorum:
                break

        return new_ci - 1

    def update_commit_index(self):
        new_ci = self.calculate_max_commit_index()
        log = self.get_log(new_ci)
        if log is None:
            return

        # Raft Leader only commit current term logs
        if log.term == self.term and new_ci > self.commit_index:
            self.commit_index = new_ci
            self.do_apply()

    def need_send_snapshot(self, peer):
        # Peer do not have any data and there has a snapshot
        # Just send snapshot
        # else means clean startup
        if peer.match_index == 0:
            return self.last_snapshot is not None

        log = self.get_log(peer.match_index)
        return log is None

    def get_last_snapshot(self):
        if self.last_snapshot is None:
            self.do_snapshot()

        return self.last_snapshot

    def send_install_snapshot(self, peer):
        snap = self.get_last_snapshot()
        peer.in_install_snapshot = True
        isn = self.prepare_msg(InstallSnapshot())
        isn.term = self.term
        isn.leader_id = self.id
        isn.last_log_index = snap.last_log_index
        isn.last_log_term = snap.last_log_term
        isn.data = snap.data
        isn.logs = snap.logs
        LOG.debug('send-is', self.id, '->', peer.id)
        self.send_message(peer.id, 'install_snapshot', isn)

    def try_apply_config(self, msg):
        cc_ver = msg.config_version
        if self.next_peers is not None and self.next_peers_version != cc_ver:
            return
        self.commit_next_config()

    def send_configuration_change_to(self, peer, send_next=True):
        ret = self.prepare_msg(ConfigurationChange())
        leader = Peer(self.id, self.mode, self.transport)
        leader.address = self.address
        ret.add_peer(leader)
        if send_next:
            ret.set_peers(self.next_peers)
            ret.version = self.next_peers_version
        else:
            ret.set_peers(self.peers)
            ret.version = self.peers_version
        self.send_message(peer.id, 'configuration_change', ret)

    def broadcast_configuration_change(self):
        for peer in self.peers.values():
            self.send_configuration_change_to(peer, True)

    def try_update_config(self, peer):
        # Need commit current
        if self.next_peers is not None:
            success = 0
            ncv = self.next_peers_version
            for peer in self.peers.values():
                if peer.match_config_version == ncv:
                    success += 1

            if success >= self.quorum():
                self.commit_next_config()
        else:
            ccv = self.config_version
            if peer.match_config_version != ccv:
                self.send_configuration_change_to(peer, False)

    def last_config_version(self):
        if self.next_peers is not None:
            return self.next_peers_version
        return self.peers_version

    def select_best_follower(self):
        current = 0
        choose = None
        for peer in self.peers.values():
            if peer.mode == PeerMode.Voter and peer.next_index > current:
                current = peer.next_index
                choose = peer.id

        return choose
    # End Helper functions

    # API for test
    def force_append_log(self, data):
        last_idx, _ = self.get_last_log()
        log = Log(last_idx + 1, self.term, data)
        self.logs.append(log)
    # End API for test

    # API functions
    def propose(self, future):
        data = future.data
        last_idx, _ = self.get_last_log()
        if self.is_leader() and not self.in_leader_transfer:
            log = Log(last_idx + 1, self.term, data)
            self.jobs[log.index] = future
            self.logs.append(log)
            for peer in self.peers.values():
                self.send_append_logs_to(peer)
        else:
            leader_id = self.voted_for
            future.set_error('Not Leader', leader_id)

    def transfer_leader(self, future):
        target = future.data
        if target is None:
            target = self.select_best_follower()
        else:
            tpeer = self.peers.get(target)
            if tpeer is None or tpeer.mode != PeerMode.Voter:
                future.set_error('Target not found or target is not voter')
                return

            if tpeer.in_install_snapshot:
                future.set_error('Target is not ready')
                return

        if not self.is_leader() or self.id == target:
            future.set_error('Target is leader')
            return

        if self.in_leader_transfer:
            future.set_error('In leader transfer')
            return

        jkey = 'transfer-leader-to-%s' % target
        self.jobs[jkey] = future
        self.in_leader_transfer = True
        self.timeout_now_sended = False
        self.leader_transfer_target = target
        peer = self.peers[target]
        if self.peer_has_gap(peer):
            self.send_append_logs_to(peer)
        else:
            msg = self.prepare_msg(TimeoutNow())
            self.send_message(peer.id, 'timeout_now', msg)
            self.timeout_now_sended = True

    # node_conf = {
    #   id: int
    #   mode: PeerMode
    #   address: str
    # }
    def add_node(self, future):
        node = future.data
        if node['id'] in self.peers or node['id'] == self.id:
            future.set_error('Already in cluster')
            return

        if self.next_peers is not None:
            future.set_error('Already in configuration change')
            return

        success = self.create_next_config(self.peers.values())
        if not success:
            future.set_error('Cannot create next configuration')
            return

        jkey = 'update-config-%s' % self.next_peers_version
        self.jobs[jkey] = future
        npeer = Peer(node['id'], node['mode'], self.transport)
        npeer.address = node['addr']
        self.next_peers[npeer.id] = npeer
        self.broadcast_configuration_change()

    def delete_node(self, future):
        node_id = future.data
        if node_id not in self.peers:
            future.set_error('Node not in cluster')
            return

        success = self.create_next_config(self.peers.values())
        if not success:
            future.set_error('Cannot create next configuration')
            return

        jkey = 'update-config-%s' % self.next_peers_version
        self.jobs[jkey] = future
        del self.next_peers[node_id]
        self.broadcast_configuration_change()
    # End API functions
