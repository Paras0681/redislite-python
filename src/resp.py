"""
This file handles RESP (REdis Serializatin Protocol) for encoding and decoding of intputs/outputs.
This is the wire protocol real Redis clients (redis-cli, redis.py etc) responds. 
Every stage from here assumes commands arrives as RESP arrays of bulk strings, 
and that we reply using the RESP types below.

References: https://github.com/mehadiproman/Building-Redis-From-Scratch/
References: https://redis.io/docs/reference/protocol-spec/
"""


CRLF = b"\r\n"

class RESPParseError(Exception):
    """Raised when buffer contains malformed RESP."""

class NeedMoreData(Exception):
    """Raised internally when the buffer doesn't yet hold a full frame."""

def encode_simple_string(s: str) -> bytes:
    """Basic string to RESP byte-string."""
    return f"+{s}\r\n".encode()

def encode_error(msg: str) -> bytes:
    """Error RESP byte string."""
    return f"-{msg}\r\n".encode()

def encode_integer(n: int) -> bytes:
    """Int in RESP byte string."""
    return f":{n}\r\n".encode()

def encode_bulk_string(s: str) -> bytes:
    """s can be str, bytes or None i.e null bulk string."""
    if s is None:
        return b"$-1\r\n"
    if isinstance(s, str):
        s = s.encode()
    return b"$" + str(len(s)).encode() + CRLF + s + CRLF

def encode_null_array() -> bytes:
    """Null array in RESP byte string."""
    return b"*-1\r\n"

def encode_array(items: list) -> bytes:
    """Array in RESP byte string."""
    if items is None:
        return encode_null_array()
    out = [f"*{len(items)}\r\n".encode()]
    for item in items:
        out.append(encode_bulk_string(item))
    return b"".join(out)

def encode_nested_array(entries: list) -> bytes:
    """Array in RESP """
    out = [f"*{len(entries)}\r\n".encode()]
    for entry_id, fields in entries:
        out.append(b"*2\r\n")
        out.append(encode_bulk_string(entry_id)) 
        out.append(encode_array(fields))
    return b"".join(out) 

def encode_xread_result(streams_result: list) -> bytes:
    """Stream encoding for RESP format output."""
    if not streams_result:
        return b"*-1\r\n"
    out = [f"*{len(streams_result)}\r\n".encode()]
    for key, entries in streams_result:
        out.append(b"*2\r\n")
        out.append(encode_bulk_string(key))
        out.append(encode_nested_array(entries))
    return b"".join(out)

def encode_array_of_replies(replies: list) -> bytes:
    """Wraps a list of already-RESP-encoded replies into one array, without re-encoding them."""
    out = [f"*{len(replies)}\r\n".encode()]
    out.extend(replies)
    return b"".join(out)


# Ref: https://github.com/mehadiproman/Building-Redis-From-Scratch/
class RESPReader:
    """
    Incrementally parses RESP frames out of a growing byte buffer.

    Usage: feed all newly-received bytes with feed(), then repeatedly call
    try_parse_command() until it returns None (meaning "not enough data yet").
    """

    def __init__(self):
        self._buf = bytearray()

    def feed(self, data: bytes):
        self._buf.extend(data)

    def __len__(self):
        return len(self._buf)

    def try_parse_command(self):
        """
        Attempt to parse a single command (a RESP Array of Bulk Strings) from
        the buffer. Returns a list[str] on success and advances the buffer,
        or None if there isn't a complete command yet.

        Also supports the "inline command" fallback (plain text separated by
        spaces, terminated by \\r\\n) for simple manual testing with netcat,
        matching real Redis behaviour.
        """
        if not self._buf:
            return None

        try:
            if self._buf[0:1] == b"*":
                return self._parse_array()
            else:
                return self._parse_inline()
        except NeedMoreData:
            return None

    # -- internals ---------------------------------------------------

    def _read_line(self, start):
        """Return (line_bytes, index_after_crlf) or raise NeedMoreData."""
        idx = self._buf.find(CRLF, start)
        if idx == -1:
            raise NeedMoreData()
        return bytes(self._buf[start:idx]), idx + 2

    def _parse_array(self):
        pos = 0
        line, pos = self._read_line(pos)
        if not line.startswith(b"*"):
            raise RESPParseError("expected array header")
        count = int(line[1:])

        if count <= 0:
            self._consume(pos)
            return []

        items = []
        for _ in range(count):
            type_byte = self._buf[pos:pos + 1]
            if not type_byte:
                raise NeedMoreData()
            if type_byte != b"$":
                raise RESPParseError(f"expected bulk string, got {type_byte!r}")
            len_line, pos = self._read_line(pos + 1)
            blen = int(len_line)
            if blen == -1:
                items.append(None)
                continue
            data_end = pos + blen
            if len(self._buf) < data_end + 2:
                raise NeedMoreData()
            items.append(bytes(self._buf[pos:data_end]).decode("utf-8", errors="replace"))
            pos = data_end + 2  # skip trailing CRLF

        self._consume(pos)
        return items

    def _parse_inline(self):
        line, pos = self._read_line(0)
        self._consume(pos)
        text = line.decode("utf-8", errors="replace").strip()
        if not text:
            return []
        return text.split()

    def _consume(self, n):
        del self._buf[:n]