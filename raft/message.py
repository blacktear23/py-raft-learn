import json


class Log(object):
    def __init__(self, index, term, data):
        self.index = index
        self.term = term
        self.data = data

    def __str__(self):
        return "<T%s I%s>: %s" % (self.term, self.index, self.data)

    def to_json(self):
        return json.dumps({
            'index': self.index,
            'term': self.term,
            'data': self.data,
        })

    @classmethod
    def from_json(self, msg):
        data = json.loads(msg)
        log = Log(data['index'], data['term'], data['data'])
        return log


class AppendLogs(object):
    def __init__(self):
        self.cluster_id = 0
        self.sender = 0
        self.to = 0
        self.term = 0
        self.prev_log_index = 0
        self.prev_log_term = 0
        self.leader_commit = 0
        self.config_version = 0
        self.logs = []

    def to_json(self):
        logs = []
        for log in self.logs:
            logs.append(log.to_json())

        return json.dumps({
            'cluster_id': self.cluster_id,
            'sender': self.sender,
            'to': self.to,
            'term': self.term,
            'prev_log_index': self.prev_log_index,
            'prev_log_term': self.prev_log_term,
            'leader_commit': self.leader_commit,
            'config_version': self.config_version,
            'logs': logs,
        })

    @classmethod
    def from_json(cls, msg):
        data = json.loads(msg)
        al = AppendLogs()
        al.cluster_id = data['cluster_id']
        al.sender = data['sender']
        al.to = data['to']
        al.term = data['term']
        al.prev_log_index = data['prev_log_index']
        al.prev_log_term = data['prev_log_term']
        al.leader_commit = data['leader_commit']
        al.config_version = data['config_version']
        al.log = []
        logs_str = data['logs']
        for log_str in logs_str:
            log = Log.from_json(log_str)
            al.logs.append(log)
        return al


class AppendLogsReply(object):
    def __init__(self):
        self.cluster_id = 0
        self.sender = 0
        self.to = 0
        self.term = 0
        self.match_index = 0
        self.match_config_version = 0
        self.success = False

    def to_json(self):
        return json.dumps({
            'cluster_id': self.cluster_id,
            'sender': self.sender,
            'to': self.to,
            'term': self.term,
            'match_index': self.match_index,
            'match_config_version': self.match_config_version,
            'success': self.success,
        })

    @classmethod
    def from_json(cls, msg):
        data = json.loads(msg)
        alr = AppendLogsReply()
        alr.cluster_id = data['cluster_id']
        alr.sender = data['sender']
        alr.to = data['to']
        alr.term = data['term']
        alr.success = data['success']
        alr.match_index = data['match_index']
        alr.match_config_version = data['match_config_version']
        return alr


class Vote(object):
    def __init__(self):
        self.cluster_id = 0
        self.sender = 0
        self.to = 0
        self.term = 0
        self.candidate_id = 0
        self.last_log_index = 0
        self.last_log_term = 0

    def clone(self):
        obj = Vote()
        obj.cluster_id = self.cluster_id
        obj.sender = self.sender
        obj.to = self.to
        obj.term = self.term
        obj.candidate_id = self.candidate_id
        obj.last_log_index = self.last_log_index
        obj.last_log_term = self.last_log_term
        return obj

    def to_json(self):
        return json.dumps({
            'cluster_id': self.cluster_id,
            'sender': self.sender,
            'to': self.to,
            'term': self.term,
            'candidate_id': self.candidate_id,
            'last_log_index': self.last_log_index,
            'last_log_term': self.last_log_term,
        })

    @classmethod
    def from_json(cls, msg):
        data = json.loads(msg)
        v = Vote()
        v.cluster_id = data['cluster_id']
        v.sender = data['sender']
        v.to = data['to']
        v.term = data['term']
        v.candidate_id = data['candidate_id']
        v.last_log_index = data['last_log_index']
        v.last_log_term = data['last_log_term']
        return v


class VoteReply(object):
    def __init__(self):
        self.cluster_id = 0
        self.sender = 0
        self.to = 0
        self.term = 0
        self.vote_granted = False

    def to_json(self):
        return json.dumps({
            'cluster_id': self.cluster_id,
            'sender': self.sender,
            'to': self.to,
            'term': self.term,
            'vote_granted': self.vote_granted,
        })

    @classmethod
    def from_json(cls, msg):
        data = json.loads(msg)
        vr = VoteReply()
        vr.cluster_id = data['cluster_id']
        vr.sender = data['sender']
        vr.to = data['to']
        vr.term = data['term']
        vr.vote_granted = data['vote_granted']
        return vr


class TimeoutNow(object):
    def __init__(self):
        self.cluster_id = 0
        self.sender = 0
        self.to = 0

    def to_json(self):
        return json.dumps({
            'cluster_id': self.cluster_id,
            'sender': self.sender,
            'to': self.to,
        })

    @classmethod
    def from_json(cls, msg):
        data = json.loads(msg)
        tn = TimeoutNow()
        tn.cluster_id = data['cluster_id']
        tn.sender = data['sender']
        tn.to = data['to']
        return tn


class TimeoutNowReply(object):
    def __init__(self):
        self.cluster_id = 0
        self.sender = 0
        self.to = 0

    def to_json(self):
        return json.dumps({
            'cluster_id': self.cluster_id,
            'sender': self.sender,
            'to': self.to,
        })

    @classmethod
    def from_json(cls, msg):
        data = json.loads(msg)
        tnr = TimeoutNowReply()
        tnr.cluster_id = data['cluster_id']
        tnr.sender = data['sender']
        tnr.to = data['to']
        return tnr


class PreVote(object):
    def __init__(self):
        self.cluster_id = 0
        self.sender = 0
        self.to = 0
        self.term = 0
        self.candidate_id = 0
        self.last_log_index = 0
        self.last_log_term = 0

    def clone(self):
        obj = PreVote()
        obj.cluster_id = self.cluster_id
        obj.sender = self.sender
        obj.to = self.to
        obj.term = self.term
        obj.candidate_id = self.candidate_id
        obj.last_log_index = self.last_log_index
        obj.last_log_term = self.last_log_term
        return obj

    def to_json(self):
        return json.dumps({
            'cluster_id': self.cluster_id,
            'sender': self.sender,
            'to': self.to,
            'term': self.term,
            'candidate_id': self.candidate_id,
            'last_log_index': self.last_log_index,
            'last_log_term': self.last_log_term,
        })

    @classmethod
    def from_json(cls, msg):
        data = json.loads(msg)
        v = PreVote()
        v.cluster_id = data['cluster_id']
        v.sender = data['sender']
        v.to = data['to']
        v.term = data['term']
        v.candidate_id = data['candidate_id']
        v.last_log_index = data['last_log_index']
        v.last_log_term = data['last_log_term']
        return v


class PreVoteReply(object):
    def __init__(self):
        self.cluster_id = 0
        self.sender = 0
        self.to = 0
        self.term = 0
        self.vote_granted = False

    def to_json(self):
        return json.dumps({
            'cluster_id': self.cluster_id,
            'sender': self.sender,
            'to': self.to,
            'term': self.term,
            'vote_granted': self.vote_granted,
        })

    @classmethod
    def from_json(cls, msg):
        data = json.loads(msg)
        vr = PreVoteReply()
        vr.cluster_id = data['cluster_id']
        vr.sender = data['sender']
        vr.to = data['to']
        vr.term = data['term']
        vr.vote_granted = data['vote_granted']
        return vr


class InstallSnapshot(object):
    def __init__(self):
        self.cluster_id = 0
        self.sender = 0
        self.to = 0
        self.term = 0
        self.leader_id = 0
        self.last_log_index = 0
        self.config_version = 0
        self.logs = []
        self.data = ''

    def to_json(self):
        logs_data = []
        for log in self.logs:
            logs_data.append(log.to_json())

        return json.dumps({
            'cluster_id': self.cluster_id,
            'sender': self.sender,
            'to': self.to,
            'term': self.term,
            'leader_id': self.leader_id,
            'config_version': self.config_version,
            'last_log_index': self.last_log_index,
            'last_log_term': self.last_log_term,
            'logs': logs_data,
            'data': self.data,
        })

    @classmethod
    def from_json(cls, msg):
        data = json.loads(msg)
        snap = InstallSnapshot()
        snap.cluster_id = data['cluster_id']
        snap.sender = data['sender']
        snap.to = data['to']
        snap.term = data['term']
        snap.leader_id = data['leader_id']
        snap.last_log_index = data['last_log_index']
        snap.last_log_term = data['last_log_term']
        snap.config_version = data['config_version']
        snap.data = data['data']
        for log_str in data['logs']:
            log = Log.from_json(log_str)
            snap.logs.append(log)

        return snap


class InstallSnapshotReply(object):
    def __init__(self):
        self.cluster_id = 0
        self.sender = 0
        self.to = 0
        self.last_log_index = 0
        self.last_log_term = 0
        self.success = False

    def to_json(self):
        return json.dumps({
            'cluster_id': self.cluster_id,
            'sender': self.sender,
            'to': self.to,
            'last_log_index': self.last_log_index,
            'last_log_term': self.last_log_term,
            'success': self.success,
        })

    @classmethod
    def from_json(cls, msg):
        data = json.loads(msg)
        isr = InstallSnapshotReply()
        isr.cluster_id = data['cluster_id']
        isr.sender = data['sender']
        isr.to = data['to']
        isr.last_log_index = data['last_log_index']
        isr.last_log_term = data['last_log_term']
        isr.success = data['success']
        return isr


class ConfigurationChange(object):
    def __init__(self):
        self.cluster_id = 0
        self.sender = 0
        self.to = 0
        self.version = 0
        self.peers = []

    def add_peer(self, peer):
        peer_json = peer.to_json()
        self.peers.append(peer_json)

    def set_peers(self, peers):
        for peer in peers.values():
            self.add_peer(peer)

    def to_json(self):
        return json.dumps({
            'cluster_id': self.cluster_id,
            'sender': self.sender,
            'to': self.to,
            'version': self.version,
            'peers': self.peers,
        })

    @classmethod
    def from_json(cls, msg):
        data = json.loads(msg)
        cc = ConfigurationChange()
        cc.cluster_id = data['cluster_id']
        cc.sender = data['sender']
        cc.to = data['to']
        cc.version = data['version']
        for peer_json in data['peers']:
            from raft import Peer
            peer = Peer.from_json(peer_json)
            cc.peers.append(peer)

        return cc


class ConfigurationChangeReply(object):
    def __init__(self):
        self.cluster_id = 0
        self.sender = 0
        self.to = 0
        self.success = False
        self.version = 0

    def to_json(self):
        return json.dumps({
            'cluster_id': self.cluster_id,
            'sender': self.sender,
            'to': self.to,
            'success': self.success,
            'version': self.version,
        })

    @classmethod
    def from_json(cls, msg):
        data = json.loads(msg)
        ccr = ConfigurationChangeReply()
        ccr.cluster_id = data['cluster_id']
        ccr.sender = data['sender']
        ccr.to = data['to']
        ccr.success = data['success']
        ccr.version = data['version']
        return ccr
