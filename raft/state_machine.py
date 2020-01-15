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
    def __init__(self):
        self.last_log_index = 0
        self.last_log_term = 0
        self.data = {}

    def process_command(self, cmd, key, value):
        cmd = cmd.upper()
        if cmd == 'SET':
            self.data[key] = value
        elif cmd == 'DEL':
            del self.data[key]
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

        return len(logs)

    def handle_log_data(self, data):
        parts = data.split(' ', 2)
        if len(parts) != 3:
            return False
        cmd, key, val = parts[0], parts[1], parts[2]
        return self.process_command(cmd, key, val)

    def create_snapshot(self):
        snap = Snapshot(self.last_log_index, self.last_log_term)
        snap.set_data(json.dumps(self.data))
        return snap

    def install_snapshot(self, snapshot):
        self.data = json.loads(snapshot.data)
        self.last_log_index = snapshot.last_log_index
        self.last_log_term = snapshot.last_log_term

    def get(self, key, default=None):
        return self.data.get(key, default)

    def dump(self):
        ret = ['(T%d I%d)' % (self.last_log_term, self.last_log_index)]
        for k, v in self.data.items():
            ret.append('%s => %s' % (k, v))

        return ', '.join(ret)
