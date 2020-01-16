import json


class LogStorage(object):
    def get_first_log(self):
        raise Exception('Not Implements')

    def get_last_log(self):
        raise Exception('Not Implements')

    def get_log(self, idx):
        raise Exception('Not Implements')

    def get_logs(self, start_idx):
        raise Exception('Not Implements')

    def size(self):
        raise Exception('Not Implements')

    def append(self, log):
        raise Exception('Not Implements')

    def loads(self, logs):
        raise Exception('Not Implements')

    def delete_tail(self, start_idx):
        raise Exception('Not Implements')

    def compact(self, end_idx):
        raise Exception('Not Implements')

    def all(self):
        raise Exception('Not Implements')

    def dump(self):
        return ''


class StateMachineStorage(object):
    def set_last_log(self, index, term):
        raise Exception('Not Implements')

    def get_last_log(self):
        raise Exception('Not Implements')

    def set(self, key, value):
        raise Exception('Not Implements')

    def get(self, key, default=None):
        raise Exception('Not Implements')

    def delete(self, key):
        raise Exception('Not Implements')

    def dump(self):
        raise Exception('Not Implements')

    def load(self, data):
        raise Exception('Not Implements')

    def size(self):
        raise Exception('Not Implements')

    def __str__(self):
        return ''


class InMemoryLogStorage(LogStorage):
    def __init__(self):
        self.logs = []

    def get_first_log(self):
        if len(self.logs) == 0:
            return None
        return self.logs[0]

    def get_last_log(self):
        if len(self.logs) == 0:
            return None
        return self.logs[-1]

    def get_log(self, idx):
        for log in self.logs:
            if log.index == idx:
                return log

        return None

    def get_logs(self, start_idx):
        ret = []
        for log in self.logs:
            if log.index >= start_idx:
                ret.append(log)

        return ret

    def append(self, log):
        self.logs.append(log)

    def append_logs(self, logs):
        for log in logs:
            self.logs.append(log)

    def size(self):
        return len(self.logs)

    def loads(self, logs):
        self.logs = logs

    def delete_tail(self, start_idx):
        new_logs = []
        deleted = []
        for log in self.logs:
            if log.index < start_idx:
                new_logs.append(log)
            else:
                deleted.append(log)

        self.logs = new_logs
        return deleted

    def all(self):
        return self.logs

    def compact(self, end_idx):
        new_logs = [log for log in self.logs if log.index >= end_idx]
        self.logs = new_logs

    def dump(self):
        return ', '.join([str(log) for log in self.logs])


class InMemoryStateMachineStorage(StateMachineStorage):
    def __init__(self):
        self.last_log_index = 0
        self.last_log_term = 0
        self.data = {}

    def set_last_log(self, index, term):
        self.last_log_index = index
        self.last_log_term = term

    def get_last_log(self):
        lli, llt = self.last_log_index, self.last_log_term
        return lli, llt

    def set(self, key, value):
        self.data[key] = value

    def get(self, key, default=None):
        return self.data.get(key, default)

    def delete(self, key):
        if key in self.data:
            del self.data[key]

    def dump(self):
        return json.dumps(self.data)

    def load(self, data):
        self.data = json.loads(data)

    def size(self):
        return len(self.data)

    def __str__(self):
        items = []
        for k, v in self.data.items():
            items.append('%s => %s' % (k, v))

        return ', '.join(items)
