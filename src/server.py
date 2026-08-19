"""
Single-Threaded nonblocking TCP server with a select-based event loop.

"""

import errno
import socket
import select
import time 

from . import commands
from . import resp
from .config import parse_args
from .storage import Storage


class RedisServer:
    def __init__(self, host="0.0.0.0", port=6379):
        self.host = host
        self.port = port
        self.running = True

        self.client_sockets = set()
        self.readers = {}
        self.out_buffers = {}
        self.client_addresses = {}

        self.storage = Storage()


    def start(self):
        self.server_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        self.server_socket.setblocking(False)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(128)

        print(f"Redis-like server listening on {self.host}:{self.port}")
        self._event_loop()

    def stop(self):
        self.running = False

    def _event_loop(self):
        while self.running:
            readable = [self.server_socket] + list(self.client_sockets)

            ready_read, ready_write, ready_exception = select.select(readable, [], [], 1.0)

            if self.server_socket in ready_read:
                self._accept_new_connection()
                ready_read.remove(self.server_socket)

            for sock in ready_read:
                self._handle_client_readable(sock)

    def _accept_new_connection(self):
        try:
            client_socket, address = self.server_socket.accept()
        except socket.error:
            return
        client_socket.setblocking(False)
        self.client_sockets.add(client_socket)
        self.readers[client_socket] = resp.RESPReader()
        self.out_buffers[client_socket] = bytearray()
        self.client_addresses[client_socket] = address
        print(f"Connection from address{address}") 

    def _handle_client_readable(self, client_socket):
        if client_socket not in self.client_sockets:
            return
        try:
            data = client_socket.recv(4096)
        except socket.error as e:
            if e.errno != errno.EWOULDBLOCK:
                self._disconnect_client(client_socket)
            return

        if not data:
            self._disconnect_client(client_socket)
            return

        reader = self.readers[client_socket]
        reader.feed(data)

        while True:
            try:
                command = reader.try_parse_command()
            except resp.RESPParseError:
                self._queue_write(client_socket, resp.encode_error("ERR Protocol error"))
                self._disconnect_client(client_socket)
                return
            if command is None:
                break
            if command:  # skip empty inline pings
                reply = commands.dispatch(self.storage, command)
                self._queue_write(client_socket, reply)

    def _handle_client_writable(self, client_socket):
        buf = self.out_buffers.get(client_socket)
        if not buf:
            return
        try:
            sent = client_socket.send(buf)
            del buf[:sent]
        except socket.error as e:
            if e.errno != errno.EWOULDBLOCK:
                self._disconnect_client(client_socket)

    def _queue_write(self, client_socket, data: bytes):
        if not data:
            return
        buf = self.out_buffers.setdefault(client_socket, bytearray())
        buf.extend(data)
        # Best-effort immediate flush so latency stays low under light load.
        self._handle_client_writable(client_socket)

    def _disconnect_client(self, client_socket):
        address = self.client_addresses.get(client_socket, "unknown")
        if client_socket in self.client_sockets:
            self.client_sockets.remove(client_socket)
        self.readers.pop(client_socket, None)
        self.out_buffers.pop(client_socket, None)
        self.client_addresses.pop(client_socket, None)
        try:
            client_socket.close()
        except socket.error:
            pass
        print(f"Disconnected {address}")


def main():
    config = parse_args()
    server = RedisServer(host=config.host, port=config.port)
    server.start()

if __name__ == "__main__":
    main()