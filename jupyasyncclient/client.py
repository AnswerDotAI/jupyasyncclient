import asyncio, inspect, os, sys, time, uuid, httpx, websockets
from getpass import getpass
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

    def _api_url(self, path: str): return _join_url(self.base_url, path)

    def _ws_url(self):
        if not self.kernel_id: raise RuntimeError("kernel_id required")
        params = {"session_id": self.session_id}
        if self.token: params["token"] = self.token
        return _join_url(self.base_url, f"/api/kernels/{self.kernel_id}/channels", ws=True, params=params)

    # --- HTTP ---

    def _ensure_http(self):
        if self._http and not self._http.is_closed: return self._http
        self._http = httpx.AsyncClient(headers=self._headers)
        return self._http

    async def _request(self, method, path, **kwargs):
        http = self._ensure_http()
        r = await http.request(method, self._api_url(path), **kwargs)
        r.raise_for_status()
        if r.status_code == 204: return None
        ct = (r.headers.get("content-type") or "").split(";")[0]
        return r.json() if ct == "application/json" else r.text

    async def start_kernel(self, kernel_name= "python3", **kwargs):
        model = await self._request("POST", "/api/kernels", json={"name": kernel_name, **kwargs})
        self.kernel_id = model["id"]
        return model

    async def shutdown_kernel(self):
        if self.kernel_id: await self._request("DELETE", f"/api/kernels/{self.kernel_id}")

    async def interrupt_kernel(self):
        if self.kernel_id: await self._request("POST", f"/api/kernels/{self.kernel_id}/interrupt")

    async def restart_kernel(self):
        if not self.kernel_id: raise RuntimeError("kernel_id required")
        return await self._request("POST", f"/api/kernels/{self.kernel_id}/restart")

    async def is_alive(self):
        if not self.kernel_id: return False
        try:
            await self._request("GET", f"/api/kernels/{self.kernel_id}")
            return True
        except Exception: return False

    # --- websocket channels ---

    def start_channels(self, shell=True, iopub=True, stdin=True, control=True):
        if not (shell or iopub or stdin or control): return
        if self._start_task and not self._start_task.done(): return
        try: loop = asyncio.get_running_loop()
        except RuntimeError as e: raise RuntimeError("start_channels() needs a running asyncio loop") from e
        self._start_task = loop.create_task(self._start_ws())

    async def _start_ws(self):
        if self._ws and self._ws.close_code is None: return
        self._ws = await websockets.connect(self._ws_url(), additional_headers=self._headers, ping_interval=30)
        self._send_task = asyncio.create_task(self._send_loop())
        self._recv_task = asyncio.create_task(self._recv_loop())

    @property
    def channels_running(self): return bool(self._ws and self._ws.close_code is None)

    def stop_channels(self):
        if self._close_task and not self._close_task.done(): return
        try: loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.aclose())
            return
        self._close_task = loop.create_task(self.aclose())

    async def aclose(self):
        for t in (self._send_task, self._recv_task):
            if t and not t.done(): t.cancel()
        if self._ws and self._ws.close_code is None: await self._ws.close()
        for t in (self._send_task, self._recv_task):
            if t:
                try: await t
                except asyncio.CancelledError: pass
                except Exception: pass
        self._ws = None
        if self._http and not self._http.is_closed: await self._http.aclose()

    async def _send_loop(self):
        assert self._ws is not None
        while True:
            payload = await self._send_q.get()
            if payload is None: return
            try: await self._ws.send(payload)
            except Exception: return

    async def _recv_loop(self):
        assert self._ws is not None
        try:
            async for data in self._ws:
                if isinstance(data, str): msg = loads(data)
                elif isinstance(data, bytes): msg = deserialize_binary_message(data)
                else: continue
                channel = msg.pop("channel", None) or "shell"
                msg.setdefault("msg_id", msg.get("header", {}).get("msg_id"))
                msg.setdefault("msg_type", msg.get("header", {}).get("msg_type"))
                msg.setdefault("buffers", [])
                q = self._queues.get(channel)
                if q: q.put_nowait(msg)
        except websockets.ConnectionClosed: pass

    async def _ensure_started(self):
        if not self._start_task: self.start_channels()
        if self._start_task: await self._start_task

    def _queue_msg(self, msg, channel: str):
        msg = dict(msg)
        msg["channel"] = channel
        bufs = msg.get("buffers")

        if bufs:
            msg = dict(msg)
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

    async def _async_recv_reply(self, msg_id, timeout=None, channel= "shell"):
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            t = None if deadline is None else max(0, deadline - time.monotonic())
            try: reply = await (self.get_control_msg(timeout=t) if channel == "control" else self.get_shell_msg(timeout=t))
            except Empty as e: raise TimeoutError("Timeout waiting for reply") from e
            if reply.get("parent_header", {}).get("msg_id") != msg_id: continue
            return reply
    _recv_reply = _async_recv_reply

    async def wait_for_ready(self, timeout=None):
        await self._ensure_started()
        msg_id = self.kernel_info()
        await self._async_recv_reply(msg_id, timeout=timeout)
        while True:
            try: await self.get_iopub_msg(timeout=0.05)
            except Empty: return

    # --- request/reply methods (AsyncKernelClient-style: return msg_id or awaitable) ---

    def execute(self, code, silent=False, store_history=True, user_expressions=None, allow_stdin=None,
                stop_on_error=True, reply=False, timeout=None):
        user_expressions = {} if user_expressions is None else user_expressions
        allow_stdin = self.allow_stdin if allow_stdin is None else allow_stdin
        if not isinstance(code, str): raise ValueError(f"code {code!r} must be a string")
        validate_string_dict(user_expressions)
        content = dict(code=code, silent=silent, store_history=store_history, user_expressions=user_expressions,
                       allow_stdin=allow_stdin, stop_on_error=stop_on_error)
        msg_id = self._queue_msg(self.session.msg("execute_request", content), "shell")
        return msg_id if not reply else self._async_recv_reply(msg_id, timeout=timeout)

    def complete(self, code, cursor_pos=None, reply=False, timeout=None):
        cursor_pos = len(code) if cursor_pos is None else cursor_pos
        msg_id = self._queue_msg(self.session.msg("complete_request", {"code": code, "cursor_pos": cursor_pos}), "shell")
        return msg_id if not reply else self._async_recv_reply(msg_id, timeout=timeout)

    def inspect(self, code, cursor_pos=None, detail_level= 0, reply=False, timeout=None):
        cursor_pos = len(code) if cursor_pos is None else cursor_pos
        content = dict(code=code, cursor_pos=cursor_pos, detail_level=detail_level)
        msg_id = self._queue_msg(self.session.msg("inspect_request", content), "shell")
        return msg_id if not reply else self._async_recv_reply(msg_id, timeout=timeout)

    def history(self, raw=True, output=False, hist_access_type= "range", reply=False, timeout=None, **kwargs):
        if hist_access_type == "range":
            kwargs.setdefault("session", 0)
            kwargs.setdefault("start", 0)
        content = dict(raw=raw, output=output, hist_access_type=hist_access_type, **kwargs)
        msg_id = self._queue_msg(self.session.msg("history_request", content), "shell")
        return msg_id if not reply else self._async_recv_reply(msg_id, timeout=timeout)

    def kernel_info(self, reply=False, timeout=None):
        msg_id = self._queue_msg(self.session.msg("kernel_info_request"), "shell")
        return msg_id if not reply else self._async_recv_reply(msg_id, timeout=timeout)

    def comm_info(self, target_name=None, reply=False, timeout=None):
        content = {} if target_name is None else {"target_name": target_name}
        msg_id = self._queue_msg(self.session.msg("comm_info_request", content), "shell")
        return msg_id if not reply else self._async_recv_reply(msg_id, timeout=timeout)

    def is_complete(self, code, reply=False, timeout=None):
        msg_id = self._queue_msg(self.session.msg("is_complete_request", {"code": code}), "shell")
        return msg_id if not reply else self._async_recv_reply(msg_id, timeout=timeout)

    def input(self, string: str): self._queue_msg(self.session.msg("input_reply", {"value": string}), "stdin")

    def shutdown(self, restart=False, reply=False, timeout=None):
        msg_id = self._queue_msg(self.session.msg("shutdown_request", {"restart": restart}), "control")
        return msg_id if not reply else self._async_recv_reply(msg_id, timeout=timeout, channel="control")

    # --- interactive execution ---

    def _output_hook_default(self, msg: dict):
        msg_type = msg["header"]["msg_type"]
        c = msg.get("content", {})
        if msg_type == "stream": getattr(sys, c.get("name", "stdout"), sys.stdout).write(c.get("text", ""))
        elif msg_type in {"display_data", "execute_result"}: sys.stdout.write(c.get("data", {}).get("text/plain", ""))
        elif msg_type == "error": sys.stderr.write("\n".join(c.get("traceback", [])))

    async def _stdin_hook_default(self, msg: dict):
        c = msg.get("content", {})
        prompt = getpass if c.get("password") else input
        try: raw = prompt(c.get("prompt", ""))
        except EOFError: raw = "\x04"
        except KeyboardInterrupt:
            sys.stdout.write("\n")
            return
        self.input(raw)

    async def execute_interactive(self, code, silent=False, store_history=True, user_expressions=None,
                                  allow_stdin=None, stop_on_error=True, timeout=None, output_hook=None, stdin_hook=None):
        allow_stdin = self.allow_stdin if allow_stdin is None else allow_stdin
        msg_id = self.execute(code, silent=silent, store_history=store_history,
            user_expressions=user_expressions, allow_stdin=allow_stdin, stop_on_error=stop_on_error)
        assert isinstance(msg_id, str)
        output_hook = self._output_hook_default if output_hook is None else output_hook
        stdin_hook = self._stdin_hook_default if stdin_hook is None else stdin_hook
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            t = None if deadline is None else max(0, deadline - time.monotonic())
            tasks = [asyncio.create_task(self.get_iopub_msg())]
            if allow_stdin: tasks.append(asyncio.create_task(self.get_stdin_msg()))
            done, pending = await asyncio.wait(tasks, timeout=t, return_when=asyncio.FIRST_COMPLETED)
            for p in pending: p.cancel()
            if not done: raise TimeoutError("Timeout waiting for output")
            msg = done.pop().result()

            if msg["header"]["msg_type"] == "input_request":
                res = stdin_hook(msg)
                if inspect.isawaitable(res): await res
                continue

            if msg.get("parent_header", {}).get("msg_id") != msg_id: continue
            output_hook(msg)
            if msg["header"]["msg_type"] == "status" and msg.get("content", {}).get("execution_state") == "idle": break

        t = None if deadline is None else max(0, deadline - time.monotonic())
        return await self._async_recv_reply(msg_id, timeout=t)

