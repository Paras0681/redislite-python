"""
This file handles interaction with REDIS database snapshot file i.e rdb files.
rdb.py — parses Redis RDB (persistence) files.
RDB format reference: https://rdb.fnordig.de/file_format.html
"""
import os

#Opcodes
OP_EOF = 0xFF
OP_SELECTDB = 0xFE
OP_EXPIRETIME_MS = 0xFC
OP_EXPIRETIME = 0xFD
OP_RESIZEDB = 0xFB
OP_AUX = 0xFA

TYPE_STRING = 0x00

class RDBParseError(Exception):
    """Raised when the file doesn't follow the expected RDB structure."""

def load_rdb(storage, filepath):
    if not os.path.exists(filepath):
        return
    with open(filepath, "rb") as f:
        data = f.read()

    if not data.startswith(b"REDIS"):
        raise ValueError("Not a valid RDB file (missing REDIS header).")

    version = data[5:9]
    print(f"RDB version: {version.decode()}")

    pos = 9
    _parse_body(storage, data, pos)


def _parse_body(storage, data, pos):
    while True:
        opcode = data[pos]

        if opcode == OP_EOF:
            break
        if opcode == OP_SELECTDB:
            pos += 1
            _, pos = _read_length(data, pos)
            continue

        if opcode == OP_RESIZEDB:
            pos += 1
            _, pos = _read_length(data, pos)
            _, pos = _read_length(data, pos)
            continue

        # milliseconds
        if opcode == OP_EXPIRETIME_MS:
            pos += 1
            expiry = int.from_bytes(
                data[pos:pos + 8],
                byteorder="little"
            )
            pos += 8

        # seconds
        elif opcode == OP_EXPIRETIME:
            pos += 1
            expiry = int.from_bytes(
                data[pos:pos + 4],
                byteorder="little"
            ) * 1000
            pos += 4

        value_type = data[pos]
        pos += 1

        key_bytes, pos = _read_string_raw(data, pos)
        key = key_bytes.decode("utf-8")

        if value_type != TYPE_STRING:
            raise RDBParseError(f"Unsupported value type 0x{value_type:02x} (only string are supported for now).")

        value_bytes, pos = _read_string_raw(data, pos)
        storage.set(key, value_bytes.decode("utf-8", errors="replace"))

    return pos

def _read_length(data, pos):
    """
    Decode an RDB length-encoded integer starting at pos.
    Returns (length, new_pos). Does not handle the special-encoded (0b11)
    form — callers that might hit that should use _read_string_raw instead,
    which checks for it explicitly.
    """

    first = data[pos]
    data_bits = (first & 0b11000000) >> 6

    if data_bits == 0b00:
        return first & 0b00111111, pos + 1
    if data_bits == 0b01:
        second = data[pos+1]
        length = ((first & 0b00111111) << 8) | second
        return length, pos+2
    if data_bits == 0b10:
        length = int.from_bytes(data[pos + 1: pos + 5], byteorder="big")
        return length, pos + 5

    raise RDBParseError("Encountered special-encoded length where a plain length was expected.")

def _read_string_raw(data, pos):
    """
    Reads an RDB-encoded string starting at pos. Returns (bytes, new_pos).
    Handles the special-encoded (0b11) integer forms by converting them into their
    ASCII-digit byte representation, so callers always get something i.e
    string-shaped back regardless of encoding form.
    """
    first = data[pos]
    top_bits = (first & 0b11000000) >> 6

    if top_bits == 0b11:
        special_type = first & 0b00111111
        # for 8-bit integer
        if special_type == 0:
            value = int.from_bytes(data[pos + 1:pos + 2], byteorder="little", signed=True)
            return str(value).encode(), pos + 2
        # for 16-bit integer
        if special_type == 1:
            value = int.from_bytes(data[pos + 1:pos + 3], byteorder="little", signed=True)
            return str(value).encode(), pos + 3
        # for 32-bit integer
        if special_type == 2:
            value = int.from_bytes(data[pos + 1:pos + 5], byteorder="little", signed=True)
            return str(value).encode(), pos + 5
        raise RDBParseError(f"Unsupported special string encoding type {special_type} (e.g. LZF compression)")

    length, pos = _read_length(data, pos)
    return data[pos:pos + length], pos + length