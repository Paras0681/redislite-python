"""
Single-Threaded nonblocking TCP server with a select-based event loop.

"""
import os
import errno
import socket
import select
import secrets

from . import commands
from . import resp
from .config import parse_args
from .storage import Storage
from .connection_state import ConnectionState
from . import rdb

class RedisServer:
    def __init__(
            self, 
            host="0.0.0.0", 
            port=6379, 
            replicaof_host=None, 
            replicaof_port=None, 
            dir='.', 
            dbfilename="dump.rdb",
            appendonly="no",
            appenddirname='appendonlydir',

        ):
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

        self.replica_sockets = set()
        self.master_conn = None
        self.master_reader = None

        self.config_dir = dir
        self.config_dbfilename = dbfilename

        self.config_appendonly = appendonly
        self.config_appenddirname = appenddirname


    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.setblocking(False)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(128)

        filepath = os.path.join(self.config_dir, self.config_dbfilename)
        rdb.load_rdb(self.storage, filepath)

        if self.config_appendonly.lower() == "yes":
            aof_dir = os.path.join(self.config_dir, self.config_appenddirname)
            os.makedirs(aof_dir, exist_ok=True)
            self.aof_path = os.path.join(aof_dir, "appendonly.aof")
            open(self.aof_path, "ab").close()
            self._load_aof()
        else:
            self.aof_path = None

        if self.replication_state.replicaof_host is not None:
            self.master_conn = self._handle_replica_handshake()
            self.master_conn.setblocking(False)
            self.master_reader = resp.RESPReader()

        print(f"REdislite server listening on {self.host}:{self.port}")
        self._event_loop()

    def stop(self):
        self.running = False

    def _event_loop(self):
        while self.running:
            readable = [self.server_socket] + list(self.client_sockets)
            if self.master_conn is not None:
                readable.append(self.master_conn)
            writable = [s for s in self.client_sockets if self.out_buffers.get(s)]

            ready_read, ready_write, ready_exception = select.select(readable, writable, [], 1.0)

            if self.server_socket in ready_read:
                self._accept_new_connection()
                ready_read.remove(self.server_socket)

            if self.master_conn is not None and self.master_conn in ready_read:
                self._handle_master_readable()
                ready_read.remove(self.master_conn)

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

        if len(args) >= 2 and args[0].upper() == "GETACK" and args[1] == "*":
            ack = resp.encode_array(["REPLCONF","ACK",str(self.replication_state.master_repl_offset)])
            self._queue_write(client_socket, ack)
            return
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

        self.replica_sockets.add(client_socket)

    def _handle_replica_handshake(self):
        host = self.replication_state.replicaof_host
        port = self.replication_state.replicaof_port

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))

        def send_command(*parts):
            encoded = resp.encode_array([p.encode() if isinstance(p, str) else p for p in parts])
            sock.sendall(encoded)

        def read_line():
            buf = b""
            while not buf.endswith(b"\r\n"):
                chunk = sock.recv(1)
                if not chunk:
                    raise ConnectionError("Master closed connection during handshake")
                buf += chunk
            return buf[:-2]
        
        send_command("PING")
        print(f"Handshake: sent PING, got {read_line()!r}")

        send_command("REPLCONF", "listening-port", str(self.port))
        print(f"Handshake: sent REPLCONF listening-port, got {read_line()!r}")

        send_command("REPLCONF", "capa", "psync2")
        print(f"Handshake: sent REPLCONF capa psync2, got {read_line()!r}")

        send_command("PSYNC", "?", "-1")
        fullresync_line = read_line()
        print(f"Handshake: sent PSYNC, got {fullresync_line!r}")

        length_line = read_line()
        rdb_len = int(length_line[1:])
        rdb_bytes = b""
        while len(rdb_bytes) < rdb_len:
            chunk = sock.recv(rdb_len - len(rdb_bytes))
            if not chunk:
                raise ConnectionError("Master closed connection during RDB transfer")
            rdb_bytes += chunk
        print(f"Handshake: received {len(rdb_bytes)} bytes of RDB, handshake complete")

        return sock

    def _propagate_to_replicas(self, command):
        if not self.replica_sockets:
            return
        print(f"Propagating to {len(self.replica_sockets)} replica(s): {command}")
        encoded = resp.encode_array([p.encode() if isinstance(p, str) else p for p in command])
        for replica_socket in list(self.replica_sockets):
            self._queue_write(replica_socket, encoded)
        self.replication_state.master_repl_offset += len(encoded)

    def _handle_config(self, client_socket, args):
        if len(args) < 2 or args[0].upper() != "GET":
            self._queue_write(client_socket, resp.encode_error("ERR syntax error."))
            return
        param = args[1].lower()
        values = {"dir": self.config_dir, "dbfilename": self.config_dbfilename}
        if param not in values:
            self._queue_write(client_socket, resp.encode_array([]))
            return
        self._queue_write(client_socket, resp.encode_array([param, values[param]]))


    def _append_to_aof(self, command):
        if not self.aof_path:
            return
        encoded = resp.encode_array([
            arg.encode() if isinstance(arg, str) else arg
            for arg in command
        ])

        with open(self.aof_path, "ab") as f:
            f.write(encoded)
            f.flush()

    def _load_aof(self):
        if not self.aof_path or not os.path.exists(self.aof_path):
            return

        with open(self.aof_path, "rb") as f:
            reader = resp.RESPReader()

            while True:
                data = f.read(4096)
                if not data:
                    break
                reader.feed(data)

                while True:
                    command = reader.try_parse_command()
                    if command is None:
                        break
                    if command:
                        commands.dispatch(self.storage, command)

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

        WRITE_COMMANDS = {"SET", "DEL", "RPUSH", "LPUSH", "LPOP", "RPOP", "INCR", "XADD"}

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

            if cmd_name == "CONFIG":
                self._handle_config(client_socket, command[1:])
                continue

            reply = commands.dispatch(self.storage, command)
            self._queue_write(client_socket, reply)

            # for stateful commands
            if len(command) >= 2:
                self.conn_state.on_write_command(cmd_name, command[1])

            if cmd_name in WRITE_COMMANDS and not reply.startswith(b"-"):
                self._append_to_aof(command)
                self._propagate_to_replicas(command)

    def _handle_master_readable(self):
        try:
            data = self.master_conn.recv(4096)
        except socket.error as e:
            if e.errno != errno.EWOULDBLOCK:
                print(f"Master connection error: {e}.")
            return
        if not data:
            print(f"Master closed the connection.")
            self.master_conn = None
            return

        self.master_reader.feed(data)

        while True:
            try:
                command = self.master_reader.try_parse_command()
            except resp.RESPParseError:
                print("Protocol error on master connection")
                return
            if command is None:
                break
            if not command:
                continue

            cmd_name = command[0].upper()

            encoded = resp.encode_array([p.encode() if isinstance(p, str) else p for p in command])

            # REPLCONF GETACK
            if (
                cmd_name == "REPLCONF"
                and len(command) >= 3
                and command[1].upper() == "GETACK"
                and command[2] == "*"
            ):
                ack = resp.encode_array([
                    "REPLCONF",
                    "ACK",
                    str(self.replication_state.master_repl_offset)
                ])
                self.master_conn.sendall(ack)
                # self.master_conn.send(ack)
                continue
            commands.dispatch(self.storage, command)

            self.replication_state.master_repl_offset += len(encoded)
            print(f"Applied from master: {command}")

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
        dir=config.dir,
        dbfilename=config.dbfilename,
        appendonly=config.appendonly,
        appenddirname=config.appenddirname,
    )
    server.start()


if __name__ == "__main__":
    main()