"Many-kernel manager over the Jupyter kernels HTTP API, with keyed reuse."

import asyncio

from .core import KernelApi
from .manager import JupyAsyncKernelManager

__all__ = ["JupyAsyncMultiKernelManager"]


class JupyAsyncMultiKernelManager(KernelApi):
    "AsyncMultiKernelManager-ish wrapper over the kernels API."
    kernel_manager_class = JupyAsyncKernelManager
    def __init__(self, base_url, *, token=None, kernel_name="python3", username=None, headers=None, timeout=30, http_client=None):
        super().__init__(base_url, token=token, headers=headers, timeout=timeout, http_client=http_client)
        self.kernel_name,self.username = kernel_name,username
        self._kernels,self._owned,self._keys = {},set(),{}

    async def list_kernels(self): return await self.kernel_request("GET")
    async def list_kernel_ids(self): return [k["id"] for k in await self.list_kernels()]

    async def start_kernel(self, kernel_name=None, **kwargs):
        model = await self.kernel_request("POST", json={"name": kernel_name or self.kernel_name, **kwargs})
        self._owned.add(model["id"])
        return model["id"]

    async def shutdown_kernel(self, kernel_id, now=False, restart=False):
        try: await self.kernel_request("DELETE", kernel_id)
        finally:
            self._owned.discard(kernel_id)
            self._kernels.pop(kernel_id, None)
            self._keys = {k:v for k,v in self._keys.items() if v != kernel_id}

    async def interrupt_kernel(self, kernel_id): await self.kernel_request("POST", kernel_id, "/interrupt")
    async def restart_kernel(self, kernel_id, **kw): return await self.kernel_request("POST", kernel_id, "/restart")

    async def is_alive(self, kernel_id):
        try:
            await self.kernel_request("GET", kernel_id)
            return True
        except Exception: return False

    async def shutdown_all(self, now=False, only_owned=True):
        kids = sorted(self._owned) if only_owned else await self.list_kernel_ids()
        await asyncio.gather(*(self.shutdown_kernel(k, now=now) for k in kids), return_exceptions=True)
        self._kernels.clear()

    async def ensure_kernel(self, key=None, *, kernel_name=None, restart=False, **kwargs):
        "Kernel id for `key`: the live one if registered, else a fresh kernel remembered under `key`."
        if key is None: return await self.start_kernel(kernel_name, **kwargs)
        kid = self._keys.get(key)
        if kid and await self.is_alive(kid):
            if restart: await self.restart_kernel(kid)
            return kid
        kid = await self.start_kernel(kernel_name, **kwargs)
        self._keys[key] = kid
        return kid

    def get_kernel(self, kernel_id):
        if (km := self._kernels.get(kernel_id)): return km
        self._ensure_http()
        km = self.kernel_manager_class(self.base_url, token=self.token, kernel_id=kernel_id, kernel_name=self.kernel_name,
            username=self.username, timeout=self._timeout, http_client=self._http)
        self._kernels[kernel_id] = km
        return km

    def client(self, kernel_id, **kwargs): return self.get_kernel(kernel_id).client(**kwargs)

    async def aclose(self): await self.aclose_http()

    async def __aenter__(self):
        self._ensure_http()
        return self

    async def __aexit__(self, *exc): await self.aclose()