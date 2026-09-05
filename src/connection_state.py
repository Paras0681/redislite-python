"""
Owns all per-connection state and stateful command handling:
BLPOP, XREAD BLOCK, MULTI/EXEC/DISCARD, WATCH/UNWATCH.

Anything here needs client_socket identity and/or shared mutable
state across connections -- that's what distinguishes it from
commands.py's stateless command table.
"""

import time
from collections import deque
import hashlib

from . import commands
from . import resp

class ConnectionState:
    def __init__(self, storage, queue_write):
        self.storage = storage
        self.queue_write = queue_write  # callback: (client_socket, bytes) -> None

        # For BLPOP command
        self.blocked_clients = {}
        self.waiters_by_key = {}

        # For XREAD command
        self.blocked_xread_clients = {}
        self.waiters_by_stream = {}

        # For MULTI, EXEC command
        self.transactions = {}

        # For WATCH/UNWATCH command
        self.watched_keys = {}

        # Pub/Sub
        self.subscriptions = {}

        #Authentication
        self.authenticated = {}


    # COMMAND entry point from the server
    def dispatch(self, client_socket, command):
        """
        Called from server.py for every parsed command. Returns True if
        this was a stateful command and has already been fully handled
        (including writing the reply) -- server.py should `continue` its
        loop. Returns False if this wasn't a stateful command, so
        server.py should fall through to its normal commands.dispatch(...)
        path and its own RPUSH/LPUSH/XADD side-effect hooks.
        """
        cmd_name = command[0].upper()

        if (
            not self.storage.users["default"]["nopass"] 
            and not self.authenticated.get(client_socket, False)
            and cmd_name not in {"AUTH"}
        ):
            self.queue_write(
                client_socket,
                resp.encode_error("NOAUTH Authentication required.")
            )
            return True

        if client_socket in self.subscriptions and self.subscriptions[client_socket]:
            if cmd_name == "PING":
                message = " ".join(command[1:])

                self.queue_write(
                    client_socket,
                    resp.encode_array([
                        "pong",
                        message
                    ])
                )
                return True

        #Pub/Sub subscribed mode
        if client_socket in self.subscriptions and self.subscriptions[client_socket]:
            allowed = {"SUBSCRIBE", "UNSUBSCRIBE", "PING"}
            if cmd_name not in allowed:
                self.queue_write(
                    client_socket,
                    resp.encode_error("ERR can't execute this command in SUBSCRIBED MODE.")
                )
                return True

        if cmd_name == "BLPOP":
            self._handle_blpop(client_socket, command[1:])
            return True

        if cmd_name == "XREAD":
            args_upper = [a.upper() for a in command[1:]]
            if "BLOCK" in args_upper:
                self._handle_xread_block(client_socket, command[1:])
                return True
            return False

        if cmd_name == "MULTI":
            if client_socket in self.transactions:
                reply = resp.encode_error("ERR MULTI calls cannot be nested.")
            else:
                self.transactions[client_socket] = {"queue": [], "dirty": False}
                reply = resp.encode_simple_string("OK")
            self.queue_write(client_socket, reply)
            return True

        if cmd_name == "DISCARD":
            if client_socket not in self.transactions:
                reply = resp.encode_error("ERR DISCARD cannot be without 'EXEC' command.")
            else:
                self.transactions.pop(client_socket)
                self.watched_keys.pop(client_socket, None)
                reply = resp.encode_simple_string("OK")
            self.queue_write(client_socket, reply)
            return True

        if cmd_name == "EXEC":
            self._handle_exec(client_socket)
            return True

        if cmd_name == "WATCH":
            self._handle_watch(client_socket, command[1:])
            return True

        if cmd_name == "UNWATCH":
            self._handle_unwatch(client_socket, command[1:])
            return True

        if cmd_name == "SUBSCRIBE":
            self._handle_subscribe(client_socket, command[1:])
            return True

        if cmd_name == "UNSUBSCRIBE":
            self._handle_unsubscribe(client_socket, command[1:])
            return True

        if cmd_name == "PUBLISH":
            if len(command) < 3:
                self.queue_write(
                    client_socket, 
                    resp.encode_error("ERR wrong number of arguments for 'PUBLISH' command.")
                )
            self._handle_publish(client_socket, command[1], " ".join(command[2:]))
            return True

        if client_socket in self.transactions:
            if cmd_name not in commands.COMMAND:
                self.transactions[client_socket]["dirty"] = True
                self.queue_write(client_socket, resp.encode_error(f"ERR unknown command '{cmd_name}'."))
            else:
                self.transactions[client_socket]["queue"].append(command)
                self.queue_write(client_socket, resp.encode_simple_string("QUEUED"))
            return True

        if cmd_name == "AUTH":
            if len(command) != 2:
                self.queue_write(
                    client_socket,
                    resp.encode_error(
                        "ERR wrong number of arguments for 'AUTH' command."
                    )
                )
                return True
            password = command[1]
            user = self.storage.users["default"]
            password_hash = hashlib.sha256(password.encode()).hexdigest()

            if password_hash not in user["passwords"]:
                self.queue_write(
                    client_socket,
                    resp.encode_error("WRONGPASS username-password pair is invalid.")
                )
                return True
            self.authenticated[client_socket] = True
            self.queue_write(client_socket, resp.encode_simple_string("OK"))
            return True

        if cmd_name == "ACL":
            if len(command) < 2:
                self.queue_write(
                    client_socket,
                    resp.encode_error("ERR wrong number of arguments for 'ACL' command.")
                )
                return True
            sub_command = command[1].upper()
            if sub_command == "WHOAMI":
                self.queue_write(
                    client_socket,
                    resp.encode_bulk_string(b"default")
                )
                return True

            if sub_command == "GETUSER":
                if len(command) != 3:
                    self.queue_write(
                        client_socket,
                        resp.encode_error(
                            "ERR wrong number of arguments for 'ACL GETUSER' command."
                        )
                    )
                    return True

                username = command[2]
                user = self.storage.users.get(username)
                if user is None:
                    self.queue_write(client_socket, resp.encode_null())
                    return True
                reply = [
                    "flags",
                    ["on", "nopass"] if user["nopass"] else ["on"],
                    "passwords",
                    list(user["passwords"]),
                    "commands",
                    "+@all",
                    "keys",
                    "~*",
                    "channels",
                    "",
                    "selectors",
                    "",
                ]
                self.queue_write(
                    client_socket,
                    resp.encode_array(reply)
                )
                return True
        return False

    def on_write_command(self, cmd_name, key):
        """Called by server.py after every successfully dispatched
        stateless write, so blocked BLPOP/XREAD waiters get resolved."""
        if cmd_name in ("RPUSH", "LPUSH"):
            self._resolve_waiters(key)
        if cmd_name == "XADD":
            self._resolve_xread_waiters(key)

    # EXEC command
    def _handle_exec(self, client_socket):
        if client_socket not in self.transactions:
            self.queue_write(
                client_socket,
                resp.encode_error("ERR EXEC cannot be without 'MULTI' command.")
            )
            return

        txn = self.transactions.pop(client_socket)
        watched = self.watched_keys.get(client_socket, {})
        invalidated = any(
            self.storage.get_version(key) != version
            for key, version in watched.items()
        )
        self.watched_keys.pop(client_socket, None)
        
        if txn["dirty"]:
            self.queue_write(client_socket, resp.encode_error("EXECABORT transaction discard."))
            return
        
        if invalidated:
            self.queue_write(client_socket, resp.encode_null_array())
            return


        results = [commands.dispatch(self.storage, queued) for queued in txn["queue"]]
        self.queue_write(client_socket, resp.encode_array_of_replies(results))

    # WATCH command
    def _handle_watch(self, client_socket, args):
        if client_socket in self.transactions:
            self.queue_write(client_socket, resp.encode_error("ERR WATCH inside MULTI is not allowed."))
            return
        if len(args) < 1:
            self.queue_write(client_socket, resp.encode_error("ERR wrong number of arguments for 'WATCH' command."))
            return

        watched = self.watched_keys.setdefault(client_socket, {})
        for key in args:
            watched[key] = self.storage.get_version(key)
        self.queue_write(client_socket, resp.encode_simple_string("OK"))

    # # UNWATCH command
    def _handle_unwatch(self, client_socket, args):
        self.watched_keys.pop(client_socket, None)
        self.queue_write(client_socket, resp.encode_simple_string("OK"))

    #  XREAD BLOCK
    def _handle_xread_block(self, client_socket, args):
        args_upper = [a.upper() for a in args]

        if "STREAMS" not in args_upper:
            self.queue_write(client_socket, resp.encode_error("ERR syntax error"))
            return
        streams_idx = args_upper.index("STREAMS")

        block_idx = args_upper.index("BLOCK")
        if block_idx + 1 >= len(args):
            self.queue_write(client_socket, resp.encode_error("ERR syntax error"))
            return
        try:
            block_ms = int(args[block_idx + 1])
        except ValueError:
            self.queue_write(client_socket, resp.encode_error("ERR timeout is not an integer or out of range"))
            return
        if block_ms < 0:
            self.queue_write(client_socket, resp.encode_error("ERR timeout is negative"))
            return

        count = None
        if "COUNT" in args_upper:
            count_idx = args_upper.index("COUNT")
            if count_idx + 1 >= len(args):
                self.queue_write(client_socket, resp.encode_error("ERR syntax error"))
                return
            try:
                count = int(args[count_idx + 1])
            except ValueError:
                self.queue_write(client_socket, resp.encode_error("ERR value is not an integer or out of range."))
                return

        rest = args[streams_idx + 1:]
        if len(rest) < 2 or len(rest) % 2 != 0:
            self.queue_write(client_socket, resp.encode_error("ERR wrong number of arguments for 'XREAD' command."))
            return
        n = len(rest) // 2
        keys = rest[:n]
        ids = rest[n:]

        # Resolve "$" NOW, at call-start, per stream.
        resolved = {}
        for key, start_id in zip(keys, ids):
            if start_id == "$":
                resolved[key] = self.storage.last_stream_id(key)
            else:
                resolved[key] = start_id

        # Try immediately, non-blocking, before registering as a waiter.
        streams_result = []
        try:
            for key, start_id in resolved.items():
                entries = self.storage.x_read(key, start_id, count)
                if entries:
                    streams_result.append((key, entries))
        except TypeError:
            self.queue_write(client_socket, resp.encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
            return

        if streams_result:
            self.queue_write(client_socket, resp.encode_xread_result(streams_result))
            return

        deadline = None if block_ms == 0 else time.time() + block_ms / 1000
        self.blocked_xread_clients[client_socket] = {"streams": resolved, "count": count, "deadline": deadline}
        for key in resolved:
            self.waiters_by_stream.setdefault(key, deque()).append(client_socket)

    # ---- BLPOP ----

    def _handle_blpop(self, client_socket, args):
        """
        BLPOP is Block-LPOP which blocks the key value and holds the client in waiting list.
        It Blocks until timeout gets expired or until new data added inside key.
        The client is registered as a waiter and gets NO reply until
        it will be answered later by _resolve_waiters() (when someone
        pushes) or check_timeouts() (if the timeout expires first).
        """
        if len(args) < 2:
            self.queue_write(client_socket, resp.encode_error("ERR wrong number of arguments for 'BLPOP' command."))
            return

        *keys, timeout_str = args
        try:
            key_timeout = float(timeout_str)
        except ValueError:
            self.queue_write(client_socket, resp.encode_error("ERR timeout value is not float or out of range."))

        if key_timeout < 0:
            self.queue_write(client_socket, resp.encode_error("ERR timeout cannot be negative."))

        for key in keys:
            try:
                item = self.storage.lpop(key)
            except TypeError:
                self.queue_write(client_socket, resp.encode_error("WRONGTYPE Openration against key is holding wrong kind of value."))
                return
            if item is not None:
                self.queue_write(client_socket, resp.encode_array([key, item]))
                return
        deadline = None if key_timeout == 0 else time.time() + key_timeout
        self.blocked_clients[client_socket] = {"keys": keys, "deadline": deadline}
        for key in keys:
            self.waiters_by_key.setdefault(key, deque()).append(client_socket)

    def _resolve_waiters(self, key):
        """
        resolve_waiters is called when any key is PUSH to handle the oldest waiter in the queue.
        """
        queue = self.waiters_by_key.get(key)
        if not queue:
            return
        while queue:
            client_socket = queue[0]
            if client_socket not in self.blocked_clients:
                queue.popleft()
                continue
            item = self.storage.lpop(key)
            if item is None:
                break  # nothing left to operate on
            queue.popleft()
            self.queue_write(client_socket, resp.encode_array([key, item]))
            self._unblock(client_socket, self.blocked_clients, self.waiters_by_key,
                          lambda info: info["keys"])

        if not queue:
            self.waiters_by_key.pop(key, None)

    def _resolve_xread_waiters(self, key):
        queue = self.waiters_by_stream.get(key)
        if not queue:
            return
        for client_socket in list(queue):
            info = self.blocked_xread_clients.get(client_socket)
            if info is None:
                continue
            streams_result = []
            for k, start_id in info["streams"].items():
                entries = self.storage.x_read(k, start_id, info["count"])
                if entries:
                    streams_result.append((k, entries))
            if streams_result:
                self.queue_write(client_socket, resp.encode_xread_result(streams_result))
                self._unblock(client_socket, self.blocked_xread_clients, self.waiters_by_stream,
                              lambda i: i["streams"].keys())

    def _unblock(self, client_socket, blocked_dict, waiters_dict, key_iter):
        """
        Removes client from the blocked_dict and waiter queue
        in waiter_dict it was registered under. key_iter info must return
        keys/streams that client was waiting on.
        """
        info = blocked_dict.pop(client_socket, None)
        if not info:
            return
        for key in key_iter(info):
            q = waiters_dict.get(key)
            if q and client_socket in q:
                q.remove(client_socket)
            if q is not None and not q:
                waiters_dict.pop(key, None)

    def check_timeouts(self):
        """Called once per event-loop tick from server.py -- runs both
        BLPOP's and XREAD's timeout sweep."""
        self._check_timeouts_for(
            self.blocked_clients,
            self.waiters_by_key,
            lambda info: info["keys"],
            resp.encode_null_array()
        )
        self._check_timeouts_for(
            self.blocked_xread_clients,
            self.waiters_by_stream,
            lambda info: info["streams"].keys(),
            resp.encode_null_array()
        )

    def _check_timeouts_for(self, blocked_dict, waiters_dict, key_iter, timeout_reply):
        now = time.time()
        for client_socket, info in list(blocked_dict.items()):
            deadline = info["deadline"]
            if deadline is not None and now >= deadline:
                self.queue_write(client_socket, timeout_reply)
                self._unblock(client_socket, blocked_dict, waiters_dict, key_iter)

    def _handle_subscribe(self, client_socket, channels):
        if not channels:
            self._queue_write(
                client_socket, 
                resp.encode_error("ERR wrong number of arguments for 'SUBSCRIBE' command.")
            )
        subscribed = self.subscriptions.setdefault(client_socket, set())

        for channel in channels:
            subscribed.add(channel)
            reply = resp.encode_array([
                "subscribe",
                channel,
                str(len(subscribed))
            ])
            self.queue_write(client_socket, reply)


    def _handle_publish(self, client_socket, channel, message):
        count = 0
        for subscriber_socket, channels in self.subscriptions.items():
            if channel in channels:
                self.queue_write(
                    subscriber_socket,
                    resp.encode_array([
                        "message",
                        channel,
                        message
                    ])
                )
                count+=1
            self.queue_write(
                client_socket,
                resp.encode_integer(count)
            )

    def _handle_unsubscribe(self, client_socket, channels):
        subscribed = self.subscriptions.get(client_socket, set())
        if channels:
            for channel in channels:
                subscribed.discard(channel)
                self.queue_write(
                    client_socket,
                    resp.encode_array([
                        "UNSUBSCRIBE",
                        channel,
                        str(len(subscribed))
                    ])
                )
        else:
            while subscribed:
                channel = subscribed.pop()
                self.queue_write(client_socket, resp.encode_array([
                    "UNSUBSCRIBE",
                    channel,
                    str(len(subscribed))
                ]))
