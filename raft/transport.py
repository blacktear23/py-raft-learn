from queue import Queue


class Transport(object):
    def send_msg(self, addr, data):
        raise Exception('Not Implements')

    def recv_msg(self):
        raise Exception('Not Implements')


class QueueTransport(Transport):
    def __init__(self, queue, exchange):
        self.queue = queue
        self.exchange = exchange

    def send_msg(self, addr, data):
        self.exchange.send_msg(addr, data)

    def recv_msg(self):
        return self.queue.get()


class QueueTransportExchange(object):
    def __init__(self):
        self.queues = {}

    def register(self, address, queue):
        self.queues[address] = queue

    def create_transport(self, address):
        queue = Queue()
        self.register(address, queue)
        return QueueTransport(queue, self)

    def send_msg(self, addr, data):
        queue = self.queues.get(addr)
        if queue is not None:
            queue.put(data)
        else:
            raise Exception('Cannot send data')
