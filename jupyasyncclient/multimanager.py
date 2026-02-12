from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional, Type

import httpx

from .manager import JupyAsyncKernelManager, _join_url


class JupyAsyncMultiKernelManager:
    "AsyncMultiKernelManager-ish wrapper over Jupyter Server's /api/kernels."

    kernel_manager_class = JupyAsyncKernelManager

    def __init__(self, base_url: str, *, token: str|None=None, kernel_name: str="python3", username: str|None=None,
                 headers: dict|None=None, timeout: float=30, http_client: httpx.AsyncClient|None=None):
        self.base_url = base_url.rstrip("/")
        self.token = token or ""
        self.kernel_name = kernel_name
        self.username = username
        self._timeout = timeout

        self._headers = {**(headers or {})}
        if self.token and "Authorization" not in self._headers: self._headers["Authorization"] = f"token {self.token}"

        self._http = http_client
        self._own_http = http_client is None

        self._kernels = {}
        self._owned = set()
        self._keys = {}

    # --- HTTP ---

    def _ensure_http(self) -> httpx.AsyncClient:
        if self._http and not self._http.is_closed: return self._http
        self._http = httpx.AsyncClient(headers=self._headers)
        self._own_http = True
        return self._http

    def _ensure_http_now(self) -> None:
        if self._http and not self._http.is_closed: return
        self._http = httpx.AsyncClient(headers=self._headers)
        self._own_http = True

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        http = self._ensure_http()
        r = await http.request(method, _join_url(self.base_url, path), **kwargs)
        r.raise_for_status()
        if r.status_code == 204: return None
        ct = (r.headers.get("content-type") or "").split(";")[0]
        return r.json() if ct == "application/json" else r.text

    # --- kernel lifecycle ---

    async def list_kernels(self) -> list[dict]: return await self._request("GET", "/api/kernels")

    async def list_kernel_ids(self) -> list[str]: return [k["id"] for k in await self.list_kernels()]

    async def start_kernel(self, kernel_name: str | None = None, **kwargs: Any) -> str:
        name = kernel_name or self.kernel_name
        model = await self._request("POST", "/api/kernels", json={"name": name, **kwargs})
        kid = model["id"]
        self._owned.add(kid)
        return kid

    async def shutdown_kernel(self, kernel_id: str, now: bool = False, restart: bool = False) -> None:
        try: await self._request("DELETE", f"/api/kernels/{kernel_id}")
        finally:
            self._owned.discard(kernel_id)
            self._kernels.pop(kernel_id, None)
            for k, v in list(self._keys.items()):
                if v == kernel_id: self._keys.pop(k, None)

    async def interrupt_kernel(self, kernel_id: str) -> None: await self._request("POST", f"/api/kernels/{kernel_id}/interrupt")

    async def restart_kernel(self, kernel_id: str, now: bool = False, newports: bool = False, **kw: Any) -> dict:
        return await self._request("POST", f"/api/kernels/{kernel_id}/restart")

    async def is_alive(self, kernel_id: str) -> bool:
        try:
            await self._request("GET", f"/api/kernels/{kernel_id}")
            return True
        except Exception: return False

    async def shutdown_all(self, now: bool = False, only_owned: bool = True) -> None:
        kids = sorted(self._owned) if only_owned else await self.list_kernel_ids()
        await asyncio.gather(*(self.shutdown_kernel(k, now=now) for k in kids), return_exceptions=True)
        if only_owned: self._owned.clear()
        self._kernels.clear()
        self._keys = {k: v for k, v in self._keys.items() if v in self._owned}

    # --- convenience: reuse kernels by key ---

    async def ensure_kernel(self, key: str|None=None, *, kernel_name: str|None=None, restart: bool=False, **kwargs: Any) -> str:
        if key is None: return await self.start_kernel(kernel_name, **kwargs)

        kid = self._keys.get(key)
        if kid and await self.is_alive(kid):
            if restart: await self.restart_kernel(kid)
            return kid

        kid = await self.start_kernel(kernel_name, **kwargs)
        self._keys[key] = kid
        return kid

    # --- KernelManager-ish surface ---

    def get_kernel(self, kernel_id: str) -> JupyAsyncKernelManager:
        km = self._kernels.get(kernel_id)
        if km: return km
        self._ensure_http_now()
        km = self.kernel_manager_class(self.base_url, token=self.token, kernel_id=kernel_id,
            kernel_name=self.kernel_name, username=self.username, timeout=self._timeout, http_client=self._http)
        self._kernels[kernel_id] = km
        return km

    def client(self, kernel_id: str, **kwargs: Any): return self.get_kernel(kernel_id).client(**kwargs)

    async def aclose(self) -> None:
        if self._http and self._own_http and not self._http.is_closed: await self._http.aclose()

    async def __aenter__(self) -> "JupyAsyncMultiKernelManager":
        self._ensure_http()
        return self

    async def __aexit__(self, *exc: Any) -> None: await self.aclose()
