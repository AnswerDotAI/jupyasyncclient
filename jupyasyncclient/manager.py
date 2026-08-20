"One-kernel lifecycle manager over the Jupyter kernels HTTP API."

from .core import KernelApi, JupyAsyncKernelClient

__all__ = ["JupyAsyncKernelManager", "start_new_server_kernel"]


class JupyAsyncKernelManager(KernelApi):
    "AsyncKernelManager-ish wrapper for one kernel's lifecycle."
    client_class = JupyAsyncKernelClient
    def __init__(self, base_url, token=None, kernel_id=None, kernel_name="python3", username=None, headers=None, timeout=30, http_client=None, verify=True):
        super().__init__(base_url, token=token, headers=headers, timeout=timeout, http_client=http_client, verify=verify)
        self.kernel_id,self.kernel_name,self.username = kernel_id,kernel_name,username

    @property
    def has_kernel(self): return bool(self.kernel_id)

    def _kid(self):
        if not self.kernel_id: raise RuntimeError("kernel_id required")
        return self.kernel_id

    async def start_kernel(self, kernel_name=None, **kwargs):
        model = await self.api.kernels.create_kernel(name=kernel_name or self.kernel_name, **kwargs)
        self.kernel_id = model["id"]
        self.kernel_name = model.get("name", kernel_name or self.kernel_name)
        return model

    async def shutdown_kernel(self, now=False, restart=False):
        try: await self.api.kernels.delete_kernel(kid=self._kid())
        finally:
            if not restart: self.kernel_id = None

    async def interrupt_kernel(self): return await self.api.kernels.interrupt(kid=self._kid())
    async def restart_kernel(self, **kw): return await self.api.kernels.restart(kid=self._kid())

    async def is_alive(self):
        try: return bool(await self.api.kernels.get_kernel(kid=self._kid()))
        except Exception: return False

    def client(self, kernel_id=None, session_id=None, username=None, headers=None, timeout=None, http_client=None):
        kernel_id = kernel_id or self.kernel_id
        if not kernel_id: raise RuntimeError("kernel_id required (call start_kernel first)")
        http = self.transport.client
        return self.client_class(self.base_url, kernel_id=kernel_id, token=self.token, username=username or self.username,
            headers=headers, timeout=timeout or self._timeout, http_client=http_client or http, session_id=session_id, verify=self.verify)

async def start_new_server_kernel(base_url, token=None, kernel_name="python3", startup_timeout=60, verify=True, **kwargs):
    "Start a kernel and a ready client for it in one call; returns `(manager, client)`."
    km = JupyAsyncKernelManager(base_url, token=token, kernel_name=kernel_name, verify=verify)
    await km.start_kernel(kernel_name, **kwargs)
    kc = km.client().start_channels()
    try: await kc.wait_for_ready(timeout=startup_timeout)
    except Exception:
        await kc.aclose()
        await km.shutdown_kernel(now=True)
        raise
    return km,kc
