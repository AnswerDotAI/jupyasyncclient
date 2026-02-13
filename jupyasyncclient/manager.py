from __future__ import annotations

import uuid
from typing import Any, Dict, Optional, Type
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx

from .client import JupyAsyncKernelClient


def _join_url(base, path, params=None):
    u = urlsplit(base)
    base_path = u.path.rstrip("/")
    full_path = f"{base_path}/{path.lstrip('/')}" if path else base_path
    query = urlencode({k: v for k, v in (params or {}).items() if v is not None})
    return urlunsplit((u.scheme, u.netloc, full_path, query, ""))


class JupyAsyncKernelManager:
    "AsyncKernelManager-ish wrapper over Jupyter Server's /api/kernels."

    client_class = JupyAsyncKernelClient

    def __init__(self, base_url, token=None, kernel_id=None, kernel_name="python3", username=None,
                 headers=None, timeout=30, http_client=None):
        self.base_url = base_url.rstrip("/")
        self.token = token or ""
        self.kernel_id = kernel_id
        self.kernel_name = kernel_name
        self.username = username
        self._timeout = timeout

        self._headers = {**(headers or {})}
        if self.token and "Authorization" not in self._headers: self._headers["Authorization"] = f"token {self.token}"

        self._http = http_client
        self._own_http = http_client is None

    @property
    def has_kernel(self): return bool(self.kernel_id)

    def _ensure_http(self):
        if self._http and not self._http.is_closed: return self._http
        self._http = httpx.AsyncClient(headers=self._headers)
        self._own_http = True
        return self._http

    async def _request(self, method, path, **kwargs):
        http = self._ensure_http()
        r = await http.request(method, _join_url(self.base_url, path), **kwargs)
        r.raise_for_status()
        if r.status_code == 204: return None
        ct = (r.headers.get("content-type") or "").split(";")[0]
        return r.json() if ct == "application/json" else r.text

    async def start_kernel(self, kernel_name= None, **kwargs):
        name = kernel_name or self.kernel_name
        model = await self._request("POST", "/api/kernels", json={"name": name, **kwargs})
        self.kernel_id = model["id"]
        self.kernel_name = model.get("name", name)
        return model

    async def shutdown_kernel(self, now=False, restart= False):
        if not self.kernel_id: return
        try: await self._request("DELETE", f"/api/kernels/{self.kernel_id}")
        finally:
            if not restart: self.kernel_id = None

    async def interrupt_kernel(self):
        if self.kernel_id: await self._request("POST", f"/api/kernels/{self.kernel_id}/interrupt")

    async def restart_kernel(self, now=False, newports= False, **kw):
        if not self.kernel_id: raise RuntimeError("kernel_id required")
        return await self._request("POST", f"/api/kernels/{self.kernel_id}/restart")

    async def is_alive(self):
        if not self.kernel_id: return False
        try:
            await self._request("GET", f"/api/kernels/{self.kernel_id}")
            return True
        except Exception: return False

    def client(self, **kwargs):
        kernel_id = kwargs.pop("kernel_id", None) or self.kernel_id
        if not kernel_id: raise RuntimeError("kernel_id required (call start_kernel first)")

        http = self._http if (self._http and not self._http.is_closed) else None
        return self.client_class(self.base_url, kernel_id=kernel_id, token=self.token,
            username=kwargs.pop("username", None) or self.username, headers=kwargs.pop("headers", None),
            timeout=kwargs.pop("timeout", None) or self._timeout,
            http_client=kwargs.pop("http_client", None) or http,
            session_id=kwargs.pop("session_id", None) or uuid.uuid4().hex)

    async def aclose(self):
        await self.shutdown_kernel(now=True)
        if self._http and self._own_http and not self._http.is_closed: await self._http.aclose()

    async def __aenter__(self):
        self._ensure_http()
        return self

    async def __aexit__(self, *exc): await self.aclose()


async def start_new_server_kernel(base_url, token=None, kernel_name="python3", startup_timeout=60, **kwargs):
    km = JupyAsyncKernelManager(base_url, token=token, kernel_name=kernel_name)
    await km.start_kernel(kernel_name, **kwargs)
    kc = km.client()
    kc.start_channels()
    try: await kc.wait_for_ready(timeout=startup_timeout)
    except Exception:
        await kc.aclose()
        await km.aclose()
        raise
    return km, kc

