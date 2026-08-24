"""
This file handles how the expiry is handled for the storage. 
In-Memory data storage which stores string, list, Streams, Sorted sets.
Lazy expiry: A key past TTL-TimeToLive manages to expire the exisiting data.
"""

import time


class Storage:
    def __init__(self):
        """
        _data has the key value pair
        _expires has argument which sets the expire time i.e px or ex milisec or sec
        """
        self._data = {}
        self._expires = {}

    def _now_ms(self):
        return int(time.time() * 1000)

    # Expiry helper functions 
    def _is_expired(self, key) -> bool:
        exp = self._expires.get(key)
        return exp is not None and self._now_ms >= exp

    def _purge_if_expired(self, key):
        if key in self._data and self._is_expired(key):
            del self._data[key]
            self._expires.pop(key, None)

    # Generic helper functionss
    def exists(self, key) -> bool:
        self._purge_if_expired(key)
        return key in self._data

    def delete(self, key) -> bool:
        self._purge_if_expired(key)
        existed = key in self._data
        self._data.pop(key, None)
        self._expires.pop(key, None)
        return existed

    def get_type(self, key):
        self._purge_if_expired(key)
        val = self._data.get(key)
        if val is None:
            return None
        return val[0] if isinstance(val, tuple) else type(val).__name__

    # String ops
    def set(self, key, value: str, px: int=None, ex: int=None):
        self._data[key] = value
        self._expires.pop(key, None)
        if ex is not None:
            self._expires[key] = self._now_ms() + ex * 1000
        elif px is not None:
            self._expires[key] = self._now_ms() + px

    def get(self, key):
        self._purge_if_expired(key)
        val = self._data.get(key)
        if val is None or not isinstance(val, str):
            return None
        return val

    # List ops
    def _get_list(self, key):
        self._purge_if_expired(key)
        val = self._data.get(key)
        if val is None:
            return []
        if not isinstance(val, list):
            raise TypeError("WRONGTYPE")
        return val

    def rpush(self, key, values):
        lst = self._get_list(key)
        lst.extend(values)
        self._data[key] = lst
        return len(lst)

    def lpush(self, key, values):
        lst = self._get_list(key)
        lst=values[::-1]+lst
        self._data[key]=lst
        return len(lst)

    def llen(self, key):
        lst = self._get_list(key)
        return len(lst) 

    def lrange(self, key, start, stop):
        lst = self._get_list(key)
        n = len(lst)
        if start < 0:
            start = max(n+start, 0)
        if stop < 0:
            stop = n + stop
        stop = min(stop, n-1)
        if start>stop or start>=n:
            return []
        return lst[start:stop+1]


    def lpop(self, key, count=None):
        val = self._get_list(key)
        if not val:
            return None if count is None else []
        if count is None:
            item = val.pop(0)
            if not val:
                del self._data[key]
            return item
        n = min(count, len(val))
        popped = val[:n]
        del val[:n]
        if not val:
            del self._data[key]
        return popped

    def rpop(self, key, count=None):
        val = self._get_list(key)
        if not val:
            return None if count is None else []
        if count is None:
            item = val.pop()
            if not val:
                del self._data[key]
            return item
        n = min(count, len(val))
        popped = val[-n:][::-1] if n else []
        if n:
            del val[-n:]
        if not val:
            del self._data[key]
        return popped

    # Stream ops
    def xadd(self, key, ms_str, seq_str, fields):
        """fields is a flat list [f1 v1 f2 v2 ...] in RESP order."""
        self._purge_if_expired(key)

        if key not in self._data:
            self._data[key] = Stream()
        stream = self._data[key]
        if not isinstance(stream, Stream):
            raise TypeError("WRONGTYPE")

        if seq_str == "*":
            ms = int(ms_str)
            last_ms, last_seq = stream.last_id
            if ms == last_ms:
                seq = last_seq+1
            elif ms > last_ms:
                seq = 0
            else:
                raise StreamIDError(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item."
                )
        else:
            ms, seq = int(ms_str), int(seq_str)
        if (ms, seq) == (0, 0):
            raise StreamIDError(
                "ERR The ID specified in XADD must be greater than 0-0"
            )
        if (ms,seq) <= stream.last_id:
            raise StreamIDError(
                "ERR the ID specified in XADD is equal or smaller than the target stream top item."
            )
        stream.entries.append((f"{ms-seq}", fields))
        stream.last_id = (ms,seq)
        return f"{ms}-{seq}"

    def parse_stream_id(self, id_str, is_start=True):
        if "-" in id_str:
            ms_str, seq_str = id_str.split("-")
            return int(ms_str), int(seq_str)
        ms = int(id_str)
        return (ms, 0) if is_start else (ms, float("inf"))
        
    def x_range(self, key, start_str, end_str, count=None):
        self._purge_if_expired(key)
        stream = self._data.get(key)
        if stream is None:
            return []
        if not isinstance(stream, Stream):
            raise TypeError("WRONGTYPE")

        start = (0,0) if start_str == "-" else self.parse_stream_id(start_str, is_start=True)
        end = (float("inf"),float("inf")) if end_str == "+" else self.parse_stream_id(end_str, is_start=False)

        result=[]
        for id_str, fields in stream.entries:
            entry_id = self.parse_stream_id(id_str)
            if start <= entry_id <= end:
                result.append((id_str, fields))
                if len(result) == count:
                    break
        return result

    def x_read(self, key, start_id, count=None):
        self._purge_if_expired(key)
        stream = self._data.get(key)
        if stream is None:
            return []
        if not isinstance(stream, Stream):
            raise TypeError("WRONGTYPE")
        start = self.parse_stream_id(start_id, is_start=True)
        result = []
        for id_str, fields in stream.entries:
            entry_id = self.parse_stream_id(id_str)
            if entry_id > start:
                result.append((id_str, fields))
                if len(result) == count:
                    break
        return result

    

class StreamIDError(Exception):
    """
    Special Error class for stream datatype errors.
    """

class Stream:
    """
    Stream is a datatype in REDIS used to store dict data in ordered format
    where it has ID-sequence key-field and a value-field i.e a dict
    Important note: The individual entry is immutable but overall stream data is mutable as we can add, remove data
    """
    __slots__ = ("entries", "last_id")

    def __init__(self):
        self.entries = [] # list of (id_str, fields) in insertion order
        self.last_id = (0,0) # (ms, seq) for Auto-ID and ordering
