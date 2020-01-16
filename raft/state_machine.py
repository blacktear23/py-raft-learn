import json


class Snapshot(object):
    def __init__(self, index, term):
        self.last_log_index = index
        self.last_log_term = term
        self.config_version = 0
        self.logs = []
        self.data = {}

    def set_data(self, data):
        self.data = data

    def set_logs(self, logs):
        self.logs = logs


class StateMachine(object):
    def __init__(self, storage):
        self.storage = storage
        idx, term = storage.get_last_log()
        self.last_log_index = idx
        self.last_log_term = term

    def process_command(self, cmd, key, value):
        cmd = cmd.upper()
        if cmd == 'SET':
            self.storage.set(key, value)
        elif cmd == 'DEL':
            self.storage.delete(key, value)
        return True

    def apply(self, logs):
        last_log = None
        for log in logs:
            success = self.handle_log_data(log.data)
            if success:
                last_log = log

        if last_log is not None:
            self.last_log_index = last_log.index
            self.last_log_term = last_log.term
            self.storage.set_last_log(last_log.index, last_log.term)

        return len(logs)

    def handle_log_data(self, data):
        parts = data.split(' ', 2)
        if len(parts) != 3:
            return False
        cmd, key, val = parts[0], parts[1], parts[2]
        return self.process_command(cmd, key, val)

    def create_snapshot(self):
        snap = Snapshot(self.last_log_index, self.last_log_term)
        data = self.storage.dump()
        snap.set_data(data)
        return snap

    def install_snapshot(self, snapshot):
        self.storage.load(snapshot.data)
        self.last_log_index = snapshot.last_log_index
        self.last_log_term = snapshot.last_log_term
        self.storage.set_last_log(self.last_log_index, self.last_log_term)

    def get(self, key, default=None):
        return self.storage.get(key, default)

    def dump(self):
        ret = '(T%d I%d) %s' % (self.last_log_term, self.last_log_index, self.storage)
        return ret

    def size(self):
        return self.storage.size()
