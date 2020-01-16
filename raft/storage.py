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
