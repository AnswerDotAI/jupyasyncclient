from __future__ import annotations

import json
import struct
from typing import Any, Dict

try: from jupyter_client.jsonutil import json_default as _json_default
except ImportError: from jupyter_client.jsonutil import date_default as _json_default  # pragma: no cover

from jupyter_client.jsonutil import extract_dates


def dumps(msg: Dict[str, Any]) -> str: return json.dumps(msg, default=_json_default)


def loads(s: str) -> Dict[str, Any]:
    msg = json.loads(s)
    if isinstance(msg, dict):
        for k in ("header", "parent_header"):
            if isinstance(msg.get(k), dict): msg[k] = extract_dates(msg[k])
    return msg


def serialize_binary_message(msg: Dict[str, Any]) -> bytes:
    msg = msg.copy()
    buffers = list(msg.pop("buffers"))
    bmsg = dumps(msg).encode("utf8")
    buffers.insert(0, bmsg)
    nbufs = len(buffers)
    offsets = [4 * (nbufs + 1)]
    for buf in buffers[:-1]: offsets.append(offsets[-1] + len(buf))
    offsets_buf = struct.pack("!" + "I" * (nbufs + 1), nbufs, *offsets)
    buffers.insert(0, offsets_buf)
    return b"".join(buffers)


def deserialize_binary_message(bmsg: bytes) -> Dict[str, Any]:
    nbufs = struct.unpack("!i", bmsg[:4])[0]
    offsets = list(struct.unpack("!" + "I" * nbufs, bmsg[4 : 4 * (nbufs + 1)]))
    offsets.append(None)
    bufs = [bmsg[start:stop] for start, stop in zip(offsets[:-1], offsets[1:])]
    msg = loads(bufs[0].decode("utf8"))
    msg["buffers"] = [memoryview(b) for b in bufs[1:]]
    return msg
