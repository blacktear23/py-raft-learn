import socket
import struct
import logging
import asyncore
from node import *
from utils import *
from queue import Queue
from threading import *
from transport import Transport


class NetworkTransport(Transport):
    def __init__(self, addr):
        self.queue = Queue()
        self.local_addr = addr
        self.peer_connections = {}

    def put_msg(self, msg):
        self.queue.put(msg)

    def send_msg(self, addr, msg):
        if addr == self.local_addr:
            self.put_msg(msg)
        else:
            data = self.encode_msg(msg)
            self.send_data(addr, data)

    def recv_msg(self):
        return self.queue.get()

    def encode_msg(self, msg):
        if len(msg) != 2:
            return
        raw_msg = '%s|%s' % (msg[0], msg[1])
        msg_len = len(raw_msg)
        data = struct.pack('L', msg_len)
        data += raw_msg
        return data

    def get_peer_conn(self, addr):
        if addr in self.peer_connections:
            return self.peer_connections[addr]
        host, port = addr.split(':')
        conn = NetworkMessageSender(host, int(port))
        self.peer_connections[addr] = conn
        return conn

    def send_data(self, addr, data):
        conn = self.get_peer_conn(addr)
        conn.append_buffer(data)

    def close(self):
        for conn in self.peer_connections.values():
            try:
                conn.close()
            except Exception:
                pass


class NetworkMessageSender(asyncore.dispatcher):
    def __init__(self, host, port):
        asyncore.dispatcher.__init__(self)
        self.lock = Lock()
        self.target_addr = '%s:%d' % (host, port)
        self.buffer = ''
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((host, port))

    def handle_connect(self):
        pass

    def handle_close(self):
        self.close()

    def handle_read(self):
        self.recv(8192)

    def writable(self):
        return (len(self.buffer) > 0)

    def handle_write(self):
        sent = self.send(self.buffer)
        self.lock.acquire()
        try:
            self.buffer = self.buffer[sent:]
        finally:
            self.lock.release()

    def append_buffer(self, data):
        self.lock.acquire()
        try:
            self.buffer += data
        finally:
            self.lock.release()


class NetworkMessageHandler(asyncore.dispatcher_with_send):
    def __init__(self, sock, transport):
        self.lock = Lock()
        self.out_buffer = ''
        self.transport = transport
        self.buffer = ''
        asyncore.dispatcher_with_send.__init__(self, sock)

    def handle_read(self):
        while True:
            try:
                data = self.recv(8192)
                if data:
                    self.buffer += data
                else:
                    break
            except Exception as e:
                break

        self.dispatch_message()

    def dispatch_message(self):
        while True:
            msg = self.decode_one_message()
            if msg is None:
                break
            else:
                # LOG.debug('Got message:', msg)
                self.transport.put_msg(msg)

    def decode_one_message(self):
        size_field_len = struct.calcsize('L')
        self.lock.acquire()
        buflen = len(self.buffer)
        if buflen <= size_field_len:
            self.lock.release()
            return None

        try:
            msg_size = struct.unpack('L', self.buffer[0:size_field_len])[0]
            if buflen < msg_size + size_field_len:
                self.lock.release()
                return None

            msg_body = str(self.buffer[size_field_len:size_field_len + msg_size])
            self.buffer = self.buffer[size_field_len + msg_size:]
            cmd, data = msg_body.split('|')
            self.lock.release()
            return [cmd, data]
        except Exception as e:
            logging.exception(e)
            self.lock.release()
            return None


class NetworkServerWorker(asyncore.dispatcher):
    def __init__(self, addr, transport):
        asyncore.dispatcher.__init__(self)
        host, port = addr.split(':')
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, int(port)))
        self.listen(1024)
        self.transport = transport

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            sock, addr = pair
            NetworkMessageHandler(sock, self.transport)


class RaftNetworkServer(Thread):
    def __init__(self, addr):
        super(RaftNetworkServer, self).__init__()
        self.addr = addr
        self.transport = NetworkTransport(self.addr)
        self.node = Node(addr, self.transport)
        self.server = NetworkServerWorker(addr, self.transport)
        self.running = True
        self.node.start()

    def run(self):
        while self.running:
            try:
                asyncore.loop(1)
            except Exception as e:
                logging.exception(e)

    def stop(self):
        self.running = False
        self.server.close()
        self.node.stop()

    def get_node(self):
        return self.node

    def wait_stop(self):
        self.node.join()
        self.join()
