import asyncio, inspect, os, sys, time, uuid, httpx, websockets
from contextlib import suppress
from queue import Empty
from typing import Any, Awaitable, Callable, Dict, Optional
from urllib.parse import urlencode, urlsplit, urlunsplit

from jupyter_client.client import validate_string_dict
from jupyter_client.session import Session

from ._ws import deserialize_binary_message, dumps, loads, serialize_binary_message


def _join_url(base, path, ws=False, params=None):
    u = urlsplit(base)
    scheme = {"http": "ws", "https": "wss"}.get(u.scheme, u.scheme) if ws else u.scheme
    base_path = u.path.rstrip("/")
    full_path = f"{base_path}/{path.lstrip('/')}" if path else base_path
    query = urlencode({k: v for k, v in (params or {}).items() if v is not None})
    return urlunsplit((scheme, u.netloc, full_path, query, ""))


class JupyAsyncKernelClient:
    "AsyncKernelClient-ish API over Jupyter Server HTTP + websocket."

    allow_stdin = True

    def __init__(self, base_url, kernel_id=None, token=None, session_id=None, username=None,
                 headers=None, timeout=30, http_client=None):
        self.kernel_id,self.token,self.base_url = kernel_id,token or "",base_url.rstrip("/")
        self.session_id = session_id or uuid.uuid4().hex
        self.session = Session(session=self.session_id, username=username or os.environ.get("USER") or "")
        self._timeout,self._http = timeout,http_client
        self._headers = {**(headers or {})}
        if self.token and "Authorization" not in self._headers: self._headers["Authorization"] = f"token {self.token}"
        self._ws,self._start_task,self._send_task,self._recv_task,self._close_task = [None]*5
        self._send_q = asyncio.Queue()
        self._queues = {k: asyncio.Queue() for k in ("shell", "iopub", "stdin", "control")}
        self._reply_waiters = {k: {} for k in ("shell", "control")}

    def _kpath(self, suffix=""):
        res = '/api/kernels'
        if self.kernel_id: res += f'/{self.kernel_id}'
        return res + suffix

    # --- HTTP ---

    def _ensure_http(self):
        if self._http and not self._http.is_closed: return self._http
        self._http = httpx.AsyncClient(headers=self._headers)
        return self._http

    async def _request(self, method, path, **kwargs):
        http = self._ensure_http()
        r = await http.request(method, _join_url(self.base_url, path), **kwargs)
        r.raise_for_status()
        if r.status_code==204: return True
        ct = (r.headers.get("content-type") or "").split(";")[0]
        return r.json() if ct=="application/json" else r.text

    async def kernel_request(self, method, suffix="", **kwargs):
        if not self.kernel_id: return None
        return await self._request(method, self._kpath(suffix), **kwargs)

    async def start_kernel(self, kernel_name= "python3", **kwargs):
        model = await self._request("POST", self._kpath(), json={"name": kernel_name, **kwargs})
        self.kernel_id = model["id"]
        return model

    async def shutdown_kernel(self): return await self.kernel_request("DELETE")
    async def interrupt_kernel(self): return await self.kernel_request("POST", "/interrupt")
    async def restart_kernel(self): return await self.kernel_request("POST", "/restart")

    async def is_alive(self):
        try: return bool(await self.kernel_request("GET"))
        except Exception: return False

    # --- websocket channels ---

    def start_channels(self, shell=True, iopub=True, stdin=True, control=True):
        if not (shell or iopub or stdin or control): return
        if self._start_task and not self._start_task.done(): return
        self._start_task = asyncio.create_task(self._start_ws())
        return self

    async def _start_ws(self):
        if self._ws and self._ws.close_code is None: return
        if not self.kernel_id: raise RuntimeError("kernel_id required")
        params = {"session_id": self.session_id}
        if self.token: params["token"] = self.token
        ws_url = _join_url(self.base_url, self._kpath("/channels"), ws=True, params=params)
        self._ws = await websockets.connect(ws_url, additional_headers=self._headers, ping_interval=30)
        self._send_task = asyncio.create_task(self._send_loop())
        self._recv_task = asyncio.create_task(self._recv_loop())

    @property
    def channels_running(self): return bool(self._ws and self._ws.close_code is None)

    def stop_channels(self):
        if self._close_task and not self._close_task.done(): return
        self._close_task = asyncio.create_task(self.aclose())

    async def aclose(self):
        for t in (self._send_task, self._recv_task):
            if t and not t.done(): t.cancel()
        if self._ws and self._ws.close_code is None: await self._ws.close()
        for t in (self._send_task, self._recv_task):
            if t:
                with suppress(asyncio.CancelledError, Exception): await t
        for d in self._reply_waiters.values():
            for fut in d.values():
                if not fut.done(): fut.cancel()
            d.clear()
        self._ws = None
        if self._http and not self._http.is_closed: await self._http.aclose()

    async def _send_loop(self):
        assert self._ws is not None
        while True:
            payload = await self._send_q.get()
            if payload is None: return
            try: await self._ws.send(payload)
            except Exception as e: return print('failed to send', e)

    async def _recv_loop(self):
        assert self._ws is not None
        with suppress(websockets.ConnectionClosed):
            async for data in self._ws:
                if isinstance(data, str): msg = loads(data)
                elif isinstance(data, bytes): msg = deserialize_binary_message(data)
                else: continue
                channel = msg.pop("channel", None) or "shell"
                msg.setdefault("msg_id", msg.get("header", {}).get("msg_id"))
                msg.setdefault("msg_type", msg.get("header", {}).get("msg_type"))
                msg.setdefault("buffers", [])
                parent_msg_id = msg.get("parent_header", {}).get("msg_id")
                if channel in self._reply_waiters and parent_msg_id:
                    fut = self._reply_waiters[channel].pop(parent_msg_id, None)
                    if fut and not fut.done():
                        fut.set_result(msg)
                        continue
                q = self._queues.get(channel)
                if q: q.put_nowait(msg)

    async def _ensure_started(self):
        if not self._start_task: self.start_channels()
        if self._start_task: await self._start_task

    def _queue_msg(self, msg, channel: str):
        msg = dict(msg)
        msg["channel"] = channel
        bufs = msg.get("buffers")
        if bufs:
            msg["buffers"] = [bytes(b) for b in bufs]
            payload = serialize_binary_message(msg)
        else:
            msg.pop("buffers", None)
            payload = dumps(msg)
        self._send_q.put_nowait(payload)
        return msg["header"]["msg_id"]

    # --- message getters ---

    async def _get_msg(self, channel, timeout=None):
        await self._ensure_started()
        q = self._queues[channel]
        try:
            if timeout is None: return await q.get()
            return await asyncio.wait_for(q.get(), timeout)
        except asyncio.TimeoutError as e: raise Empty from e

    async def get_shell_msg(self, timeout=None): return await self._get_msg("shell", timeout)
    async def get_iopub_msg(self, timeout=None): return await self._get_msg("iopub", timeout)
    async def get_stdin_msg(self, timeout=None): return await self._get_msg("stdin", timeout)
    async def get_control_msg(self, timeout=None): return await self._get_msg("control", timeout)

    async def _await_reply_waiter(self, msg_id, fut, timeout=None, channel="shell"):
        try:
            if timeout is None: return await fut
            async with asyncio.timeout(timeout): return await fut
        except asyncio.TimeoutError as e: raise TimeoutError("Timeout waiting for reply") from e
        finally:
            if self._reply_waiters[channel].get(msg_id) is fut: self._reply_waiters[channel].pop(msg_id, None)

    async def _async_recv_reply(self, msg_id, timeout=None, channel= "shell"):
        await self._ensure_started()
        if channel not in self._reply_waiters: raise ValueError(f"Unsupported reply channel: {channel}")
        fut = self._reply_waiters[channel].get(msg_id)
        if fut is None: raise RuntimeError(f"No pending reply waiter for msg_id={msg_id!r} on channel={channel!r}")
        return await self._await_reply_waiter(msg_id, fut, timeout=timeout, channel=channel)
    _recv_reply = _async_recv_reply

    async def wait_for_ready(self, timeout=None):
        await self._ensure_started()
        await self.kernel_info(reply=True, timeout=timeout)
        while True:
            try: await self.get_iopub_msg(timeout=0.05)
            except Empty: return

    # --- request/reply methods (AsyncKernelClient-style: return msg_id or awaitable) ---

    def __getattr__(self, name):
        if name.startswith("_"): raise AttributeError(name)
        def _f(reply=False, timeout=None, channel="shell", **kwargs):
            msg = self.session.msg(name) if not kwargs else self.session.msg(name, kwargs)
            msg_id = msg["header"]["msg_id"]
            if not reply: return self._queue_msg(msg, channel)
            if channel not in self._reply_waiters: raise ValueError(f"Unsupported reply channel: {channel}")
            fut = asyncio.get_running_loop().create_future()
            self._reply_waiters[channel][msg_id] = fut
            self._queue_msg(msg, channel)
            return self._await_reply_waiter(msg_id, fut, timeout=timeout, channel=channel)
        return _f

    def execute(self, code, silent=False, store_history=True, user_expressions=None, allow_stdin=None,
                stop_on_error=True, reply=False, timeout=None):
        user_expressions = {} if user_expressions is None else user_expressions
        allow_stdin = self.allow_stdin if allow_stdin is None else allow_stdin
        if not isinstance(code, str): raise ValueError(f"code {code!r} must be a string")
        validate_string_dict(user_expressions)
        return self.execute_request(reply=reply, timeout=timeout, code=code, silent=silent, store_history=store_history,
                                    user_expressions=user_expressions, allow_stdin=allow_stdin, stop_on_error=stop_on_error)

    def complete(self, code, cursor_pos=None, reply=False, timeout=None):
        cursor_pos = len(code) if cursor_pos is None else cursor_pos
        return self.complete_request(code=code, cursor_pos=cursor_pos, reply=reply, timeout=timeout)

    def inspect(self, code, cursor_pos=None, detail_level=0, reply=False, timeout=None):
        cursor_pos = len(code) if cursor_pos is None else cursor_pos
        return self.inspect_request(reply=reply, timeout=timeout, code=code, cursor_pos=cursor_pos, detail_level=detail_level)

    def history(self, raw=True, output=False, hist_access_type="range", reply=False, timeout=None, **kwargs):
        if hist_access_type=="range":
            kwargs.setdefault("session", 0)
            kwargs.setdefault("start", 0)
        return self.history_request(reply=reply, timeout=timeout, raw=raw, output=output, hist_access_type=hist_access_type, **kwargs)

    def comm_info(self, target_name=None, reply=False, timeout=None):
        return self.comm_info_request(reply=reply, timeout=timeout, target_name=target_name)

    def kernel_info(self, reply=False, timeout=None): return self.kernel_info_request(reply=reply, timeout=timeout)
    def is_complete(self, code, reply=False, timeout=None): return self.is_complete_request(code=code, reply=reply, timeout=timeout)
    def input(self, string: str): self.input_reply(value=string, channel="stdin")
    def shutdown(self, restart=False, reply=False, timeout=None):
        return self.shutdown_request(restart=restart, reply=reply, timeout=timeout, channel="control")
