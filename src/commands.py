"""
This file handles Built-in commands to handle input data.
Each handler function takes storage and args as arguments where args is the list of arguments
AFTER the command name and returns byte-string i.e RESP-encoded byte string as a respone
"""

from . import resp
from . import storage as storage_module
def ping_command(storage, args):
    """PING command is for the client to know that the server is working."""
    if not args:
        return resp.encode_simple_string("PONG")
    return resp.encode_bulk_string(args[0])

def pong_command(storage, args):
    """PONG command is same as the PING to check the working server."""
    if not args:
        return resp.encode_simple_string("PING")
    return resp.encode_bulk_string(args[0])

def echo_command(storage, args):
    """ECHO command copies the input text of the server"""
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

def del_command(storage, args):
    if len(args) != 1:
        return resp.encode_error("ERR wrong number of arguments for 'DEL' command.")
    cnt = 0
    for key in args:
            if storage.delete(key):
                cnt+=1
    return resp.encode_integer(cnt)

def rpush_command(storage, args):
    if len(args) < 2:
        return resp.encode_error("ERR wrong number of arguments for 'RPUSH' command.")
    key, values = args[0], args[1:]
    try:
        new_len = storage.rpush(key, values)
    except TypeError:
        return resp.encode_error("WRONGTYPE of operation for key holding wrong kind of value.")
    return resp.encode_integer(new_len)

def lpush_command(storage, args):
    if len(args) < 2:
        return resp.encode_error("ERR wrong number of arguements for 'LPUSH' command.")
    key, values = args[0], args[1:]
    try:
        new_len = storage.lpush(key, values)
    except TypeError:
        return resp.encode_error("WRONGTYPE of operation for key holding wrong kind of value.")
    return resp.encode_integer(new_len)

def llen_command(storage, args):
    if len(args) != 1:
        return resp.encode_error("ERR wrong number of arguments for 'LLEN' command.")
    try:
        val = storage.llen(args[0])
    except TypeError:
        return resp.encode_error("WRONGTYPE of operation for key holding wrong kind of value.")
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
        return resp.encode_error("WRONGTYPE of operation against a key holding the wrong kind of value")
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
        return resp.encode_error("WRONGTYPE of operation against a key holding the wrong kind of value")

    if count is None:
        return resp.encode_bulk_string(result)
    return resp.encode_array(result)

def lpop_command(storage, args):
    return _pop_command(storage, args, storage.lpop, "LPOP")

def rpop_command(storage, args):
    return _pop_command(storage, args, storage.rpop, "RPOP")

def type_command(storage, args):
    if len(args) != 1:
        return resp.encode_error("ERR wrong number of arguments for 'TYPE' command.")
    key = args[0]
    TYPE_NAMES = {"str": "string", "list": "list", "NoneType": "none"}
    name = storage.get_type(key)
    if name is None:
        type_name = "none"
    else:
        type_name = TYPE_NAMES.get(name, name)
    return resp.encode_simple_string(type_name)

def xadd_command(storage, args):
    if len(args) < 3 or len(args)%2 != 0:
        return resp.encode_error("ERR wrong number of arguments for 'XADD' command.")
    key, raw_id = args[0], args[1]
    fields = args[2:]

    if raw_id == "*":
        ms_str = str(storage._now_ms())
        seq_str = "*"
    else:
        ms_str, _, seq_str = raw_id.partition("-")
    try:
       new_id = storage.xadd(key, ms_str, seq_str, fields)
    except TypeError:
        return resp.encode_error("WRONGTYPE of operation against key holding wrong value.")
    except storage_module.StreamIDError as e:
        return resp.encode_error(str(e))
    return resp.encode_simple_string(new_id)

def x_range_command(storage, args):
    if len(args) < 3:
        return resp.encode_error("ERR wrong number of arguements for 'XRANGE' command.")
    key = args[0]
    start_str = args[1]
    end_str = args[2]
    count = None
    if len(args) == 5 and args[3].upper() == "COUNT":
        count = int(args[4])
    result =  storage.x_range(key, start_str, end_str, count)
    return resp.encode_nested_array(result)

def x_read_command(storage, args):
    args_upper = [a.upper() for a in args]
    if "STREAMS" not in args_upper:
        return resp.encode_error("ERR syntax error")
    streams_idx = args_upper.index("STREAMS")
    
    count = None
    if "COUNT" in args_upper:
        count_idx = args_upper.index("COUNT")
        if count_idx + 1 >= len(args):
            return resp.encode_error("ERR syntax error")
        try:
            count = int(args[count_idx + 1])
        except ValueError:
            return resp.encode_error("ERR value is not an integer or out of range.")

    streams_result = []
    rest = args[streams_idx + 1:]
    if len(rest) < 2 or len(rest) % 2 != 0:
        return resp.encode_error("ERR wrong number of arguments for 'XREAD' command.")

    n = len(rest) // 2
    keys = rest[:n]
    ids = rest[n:]
    try:
        for key, start_id in zip(keys, ids):
            entries = storage.x_read(key, start_id, count)
            if entries:
                streams_result.append((key, entries))
    except TypeError:
        return resp.encode_error("WRONGTYPE of operation against a key holding the wrong kind of value.")

    return resp.encode_xread_result(streams_result)

def incr_command(storage, args):
    if len(args) != 1:
        return resp.encode_error("ERR wrong number of arguments for 'INCR' command.")
    key = args[0]
    try: 
        result = storage.incr(key)
    except TypeError:
        return resp.encode_error("WRONGTYPE of operation against a key holding the wrong kind of value.")
    except ValueError as e:
        return resp.encode_error(str(e))
    return resp.encode_integer(result)

COMMAND = {
    "PING": ping_command,
    "PONG": pong_command,
    "ECHO": echo_command,
    "SET": set_command,
    "GET": get_command,
    "DEL": del_command,
    "RPUSH": rpush_command,
    "LLEN": llen_command,
    "LRANGE": lrange_command,
    "LPOP": lpop_command,
    "RPOP": rpop_command,
    "LPUSH": lpush_command,
    "RPUSH": rpush_command,
    "TYPE": type_command,
    "XADD": xadd_command,
    "XRANGE": x_range_command,
    "XREAD": x_read_command,
    "INCR": incr_command,
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