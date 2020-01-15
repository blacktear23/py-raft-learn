from threading import *


class Logger(object):
    DEBUG = 4
    INFO = 3
    WARN = 2
    ERROR = 1

    def __init__(self, level=None):
        if level is None:
            level = self.INFO
        self.level = level
        self.lock = Lock()

    def sync_print(self, *args):
        self.lock.acquire()
        print(" ".join([str(i) for i in args]))
        self.lock.release()

    def info(self, *args):
        if self.level >= self.INFO:
            self.sync_print('[INFO]', *args)

    def warn(self, *args):
        if self.level >= self.WARN:
            self.sync_print('[WARN]', *args)

    def error(self, *args):
        if self.level >= self.ERROR:
            self.sync_print('[ERROR]', *args)

    def debug(self, *args):
        if self.level >= self.DEBUG:
            self.sync_print('[DEBUG]', *args)


LOG = Logger()
