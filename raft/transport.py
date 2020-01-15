from queue import Queue
from utils import LOG


class Transport(object):
    def send_msg(self, addr, data):
        raise Exception('Not Implements')

    def recv_msg(self):
        raise Exception('Not Implements')


class QueueTransport(Transport):
    def __init__(self, address, queue, exchange):
        self.address = address
        self.suspended = False
        self.queue = queue
        self.exchange = exchange

    def send_msg(self, addr, data):
        if self.suspended:
            return

        self.exchange.send_msg(self.address, addr, data)

    def recv_msg(self):
        return self.queue.get()


class QueueTransportExchange(object):
    def __init__(self):
        self.queues = {}
        self.black = {}

    def set_black_one(self, node_a, node_b, value):
        key1 = '%s-%s' % (node_a, node_b)
        key2 = '%s-%s' % (node_b, node_a)
        self.black[key1] = value
        self.black[key2] = value

    def set_blacks(self, target, sources, value=True):
        for source in sources:
            self.set_black_one(target, source, value)

    def register(self, address, queue):
        self.queues[address] = queue

    def create_transport(self, address):
        queue = Queue()
        self.register(address, queue)
        return QueueTransport(address, queue, self)

    def _check_in_black(self, sender, target):
        key = '%s-%s' % (sender, target)
        return self.black.get(key, False)

    def send_msg(self, sender, target, data):
        if self._check_in_black(sender, target):
            LOG.info('Packet block %s->%s' % (sender, target))
            return

        queue = self.queues.get(target)
        if queue is not None:
            queue.put(data)
        else:
            raise Exception('Cannot send data')
