"""
Single-Threaded nonblocking TCP server with a select-based event loop.

"""

import errno
import socket
import select
import secrets

from . import commands
from . import resp
from .config import parse_args
from .storage import Storage
from .connection_state import ConnectionState


class RedisServer:
    def __init__(self, host="0.0.0.0", port=6379, replicaof_host=None, replicaof_port=None):
        self.host = host
        self.port = port
        self.running = True

        self.client_sockets = set()
        self.readers = {}
        self.out_buffers = {}
        self.client_addresses = {}

        self.storage = Storage()
        self.conn_state = ConnectionState(self.storage, self._queue_write)

        self.replication_state = ReplicationState(
            replicaof_host,
            replicaof_port
        )

    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.setblocking(False)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(128)

        print(f"REdislite server listening on {self.host}:{self.port}")
        self._event_loop()

    def stop(self):
        self.running = False

    def _event_loop(self):
        while self.running:
            readable = [self.server_socket] + list(self.client_sockets)
            writable = [s for s in self.client_sockets if self.out_buffers.get(s)]

            ready_read, ready_write, ready_exception = select.select(readable, writable, [], 1.0)

            if self.server_socket in ready_read:
                self._accept_new_connection()
                ready_read.remove(self.server_socket)

            for sock in ready_read:
                self._handle_client_readable(sock)

            self.conn_state.check_timeouts()

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
        print(f"Connected (address): {address}")

    def _handle_info(self, client_socket, args):
        body = self.replication_state.info_selection()
        self._queue_write(client_socket, resp.encode_bulk_string(body))

    def _handle_replconf(self, client_socket, args):
        print(f"REPLCONF received: {args}")
        self._queue_write(client_socket, resp.encode_simple_string("OK"))

    def _empty_rdb_bytes(self) -> bytes:
        import base64
        b64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
        return base64.b64decode(b64)

    def _handle_psync(self, client_socket, args):
        print(f"PSYNC received: {args}")
        replid = self.replication_state.master_replid
        offset = self.replication_state.master_repl_offset
        self._queue_write(client_socket, resp.encode_simple_string(f"FULLRESYNC {replid} {offset}"))

        rdb_bytes = self._empty_rdb_bytes()
        self._queue_write(client_socket, f"${len(rdb_bytes)}\r\n".encode() + rdb_bytes)

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
            if not command:
                continue
            # for stateless commands
            if self.conn_state.dispatch(client_socket, command):
                continue

            cmd_name = command[0].upper()

            if cmd_name == "INFO":
                self._handle_info(client_socket, command[1:])
                continue

            if cmd_name == "REPLCONF":
                self._handle_replconf(client_socket, command[1:])
                continue

            if cmd_name == "PSYNC":
                self._handle_psync(client_socket, command[1:])
                continue

            reply = commands.dispatch(self.storage, command)
            self._queue_write(client_socket, reply)

            # for stateful commands
            if len(command) >= 2:
                self.conn_state.on_write_command(cmd_name, command[1])

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
        print(f"Disconnected (address): {address}")


class ReplicationState:
    def __init__(self, replicaof_host, replicaof_port):
        self.master_replid = secrets.token_hex(20)
        self.master_repl_offset = 0

        self.replicaof_host = replicaof_host
        self.replicaof_port = replicaof_port
        self.role = "slave" if replicaof_host is not None else "master"

    def info_selection(self) -> str:
        "Returns the replication section body as REDIS's info format."
        lines = [
            "# Replication",
            f"role:{self.role}",
            f"master_replid:{self.master_replid}",
            f"master_repl_offset:{self.master_repl_offset}",
        ]
        return "\r\n".join(lines) + "\r\n"


def main():
    config = parse_args()
    server = RedisServer(
        host=config.host, 
        port=config.port,
        replicaof_host=config.replicaof_host,
        replicaof_port=config.replicaof_port,
    )
    server.start()


if __name__ == "__main__":
    main()