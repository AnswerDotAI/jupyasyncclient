"HTTP clients and JSON requests over HTTP or a kernel comm."

import asyncio
import httpx2
from fastcore.meta import delegates

__all__ = ['CommTransport', 'http_client', 'request_json']


class _CommStream(httpx2.AsyncByteStream):
    "Body chunks of one proxied reply, read until rustygate closes the comm"
    def __init__(self, comm, q, first): self.comm,self.q,self.first,self.done = comm,q,first,False
    async def __aiter__(self):
        for b in self.first: yield bytes(b)
        while True:
            kind,m = await self.q.get()
            if kind=='close':
                self.done = True
                if err := (m['content'].get('data') or {}).get('error'): raise httpx2.ReadError(err)
                return
            for b in m['buffers']: yield bytes(b)
    async def aclose(self):
        if not self.done: self.comm.close()

class CommTransport(httpx2.AsyncBaseTransport):
    "An httpx2 transport sending each request to rustygate over a `rustygate` comm"
    async def handle_async_request(self, request):
        from comm import create_comm
        q,loop = asyncio.Queue(),asyncio.get_running_loop()
        put = lambda kind: lambda m: loop.call_soon_threadsafe(q.put_nowait, (kind,m))
        c = create_comm(target_name='rustygate', buffers=[await request.aread()],
            data=dict(route=request.url.raw_path.decode(), method=request.method, headers=dict(request.headers)))
        c.on_msg(put('msg'))
        c.on_close(put('close'))
        kind,m = await q.get()
        if kind=='close':
            raise httpx2.ConnectError((m['content'].get('data') or {}).get('error', 'rustygate closed the request'), request=request)
        head = m['content']['data']
        return httpx2.Response(head['status'], headers=head['headers'], stream=_CommStream(c, q, m['buffers']), request=request)


@delegates(httpx2.AsyncClient)
def http_client(base_url, transport=None, **kwargs):
    "An httpx2 client for an explicit server URL and transport"
    return httpx2.AsyncClient(base_url=base_url, transport=transport, **kwargs)

@delegates(http_client)
async def request_json(method, route, base_url, json=None, params=None, transport=None, **kwargs):
    "Request JSON with a fresh client, raising HTTPStatusError and returning None for an empty body"
    async with http_client(base_url, transport=transport, **kwargs) as c:
        r = await c.request(method, route, json=json, params={k:v for k,v in (params or {}).items() if v is not None})
        r.raise_for_status()
        return r.json() if r.content else None
