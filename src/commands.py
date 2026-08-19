"""
This file handles Built-in commands to handle input data.
Each handler function takes storage and args as arguments where args is the list of arguments
AFTER the command name and returns byte-string i.e RESP-encoded byte string as a respone
"""

from . import resp

def ping_command(storage, args):
    if not args:
        return resp.encode_simple_string("PONG")
    return resp.encode_bulk_string(args[0])

def pong_command(storage, args):
    if not args:
        return resp.encode_simple_string("PING")
    return resp.encode_bulk_string(args[0])

def echo_command(storage, args):
    if len(args) != 1:
        return resp.encode_error("ERR wrong number of arguments for 'ECHO' command.") 
    return resp.encode_bulk_string(args[0])

def set_command(storage, args):
    if len(args) < 2:
        return resp.encode_error("ERR wrong number of arguments for 'SET' command.")
    key, value = args[0], args[1]
    px = None
    ex = None
    storage.set(key, value, px=px, ex=ex)
    return resp.encode_simple_string("OK")

def get_command(storage, args):
    if len(args) != 1:
        return resp.encode_error("ERR wrong number of arguments for 'GET' command.")
    val = storage.get(args[0])
    return resp.encode_bulk_string(val)

def rpush_command(storage, args):
    if len(args) < 2:
        return resp.encode_error("ERR wrong number of arguments for 'RPUSH' command.")
    key, values = args[0], args[1:]
    try:
        new_len = storage.rpush(key, values)
    except TypeError:
        return resp.encode_error("WRONGTYPE wrong type of operation for key holding wrong kind of value.")
    return resp.encode_integer(new_len)

def lpush_command(storage, args):
    if len(args) < 2:
        return resp.encode_error("ERR wrong number of arguements for 'LPUSH' command.")
    key, values = args[0], args[1:]
    try:
        new_len = storage.lpush(key, values)
    except TypeError:
        return resp.encode_error("WRONGTYPE wrong type of operation for key holding wrong kind of value.")
    return resp.encode_integer(new_len)

def llen_command(storage, args):
    if len(args) != 1:
        return resp.encode_error("ERR wrong number of arguments for 'LLEN' command.")
    try:
        val = storage.llen(args[0])
    except TypeError:
        return resp.encode_error("WRONGTYPE wrong type of operation for key holding wrong kind of value.")
    return resp.encode_integer(val)

def lrange_command(storage, args):
    if len(args) != 3:
        return resp.encode_error("ERR wrong number of arguments for 'LRANGE' command.")
    key, start, stop = args
    try:
        start=int(start)
        stop=int(stop)
    except ValueError:
        return resp.encode_error("ERR value is not an integer or out of range.")
    try:
        items = storage.lrange(key, start, stop)
    except TypeError:
        return resp.encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
    return resp.encode_array(items)

def _pop_command(storage, args, pop_fn, name):
    if len(args) not in (1,2):
        return resp.encode_error(f"ERR wrong number of arguments for '{name}' command.")

    key = args[0]
    count = None
    if len(args) == 2:
        try:
            count = int(args[1])
        except ValueError:
            return resp.encode_error("ERR Value is not an integer or out of bounds.")
        if count < 0:
            return resp.encode_error("ERR value is out of range, must be positive.")
    try:
        result = pop_fn(key, count)
    except TypeError:
        return resp.encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")

    if count is None:
        return resp.encode_bulk_string(result)
    return resp.encode_array(result)

def lpop_command(storage, args):
    return _pop_command(storage, args, storage.lpop, "LPOP")

def rpop_command(storage, args):
    return _pop_command(storage, args, storage.rpop, "RPOP")

    
COMMAND = {
    "PING": ping_command,
    "PONG": pong_command,
    "ECHO": echo_command,
    "SET": set_command,
    "GET": get_command,
    "RPUSH": rpush_command,
    "LLEN": llen_command,
    "LRANGE": lrange_command,
    "LPOP": lpop_command,
    "RPOP": rpop_command,

}
def dispatch(storage, parts):
    """
    parts is a list[str]: [COMMAND_NAME ARGS1 ARGS2 ...] which returns RESP byte string.
    """
    if not parts:
        return b""
    cmd_name = parts[0].upper()
    cmd_handler = COMMAND.get(cmd_name)
    if cmd_handler is None:
        return resp.encode_error(f"ERR unknown command - {cmd_name}")
    return cmd_handler(storage, parts[1:])