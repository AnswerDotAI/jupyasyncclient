from __future__ import annotations

from datetime import datetime, timezone

from jupyasyncclient._ws import deserialize_binary_message, dumps, loads, serialize_binary_message


def test_ws_dumps_loads_roundtrip_extracts_dates():
    msg = {
        "channel": "iopub",
        "header": {"msg_id": "abc", "msg_type": "status", "date": datetime.now(timezone.utc)},
        "parent_header": {"msg_id": "p", "msg_type": "execute_request", "date": datetime.now(timezone.utc)},
        "metadata": {},
        "content": {"execution_state": "idle"},
    }

    msg2 = loads(dumps(msg))
    assert msg2["channel"] == "iopub"
    assert msg2["header"]["msg_id"] == "abc"
    assert msg2["header"]["msg_type"] == "status"
    # jsonutil.extract_dates should convert header dates back to datetimes
    assert isinstance(msg2["header"].get("date"), datetime)
    assert isinstance(msg2["parent_header"].get("date"), datetime)


def test_ws_binary_message_roundtrip_preserves_buffers():
    msg = {
        "channel": "iopub",
        "header": {"msg_id": "abc", "msg_type": "display_data"},
        "parent_header": {"msg_id": "p", "msg_type": "execute_request"},
        "metadata": {},
        "content": {"data": {"text/plain": "hi"}, "metadata": {}},
        "buffers": [b"hello", b"world"],
    }

    bmsg = serialize_binary_message(msg)
    msg2 = deserialize_binary_message(bmsg)

    assert msg2["channel"] == "iopub"
    assert msg2["header"]["msg_id"] == "abc"
    assert msg2["header"]["msg_type"] == "display_data"
    assert msg2["content"]["data"]["text/plain"] == "hi"

    bufs = [memoryview(b).tobytes() for b in msg2.get("buffers") or []]
    assert bufs == [b"hello", b"world"]
