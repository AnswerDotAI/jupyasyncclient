import asyncio, httpx, pytest
from jupyasyncclient import JupyAsyncKernelClient, JmsgQueues
from jupyasyncclient.core import dumps

TIMEOUT = 5


@pytest.fixture
async def kc(jp_server):
    async with httpx.AsyncClient() as http:
        r = await http.post(jp_server["base_url"] + "/api/kernels", json={"name": "python3"})
        r.raise_for_status()
        kernel_id = r.json()["id"]
        client = JupyAsyncKernelClient(jp_server["base_url"], kernel_id=kernel_id).start_channels()
        await client.wait_for_ready(timeout=TIMEOUT)
        try: yield client
        finally:
            await client.aclose()
            try: await http.delete(jp_server["base_url"] + f"/api/kernels/{kernel_id}")
            except Exception: pass


class TestJupyAsyncKernelClient:
    async def test_reply(self, kc):
        reply = await kc.reply("print('hello')", timeout=TIMEOUT)
        assert reply["content"]["status"]=="ok"


    async def test_input_request(self, kc):
        qs = JmsgQueues(kc, queues=("jmsg",), merge=dict(iopub="jmsg", stdin="jmsg"))
        task = asyncio.create_task(kc.reply("x = input()\nx", timeout=TIMEOUT))
        msg = await qs.jmsg_for("input_request", timeout=TIMEOUT)
        assert msg["header"]["msg_type"]=="input_request"
        kc.input("test")
        reply = await task
        assert reply["content"]["status"]=="ok"


    async def test_request_reply_roundtrip_and_shutdown(self, kc):
        rep = await kc.kernel_info(timeout=TIMEOUT)
        assert rep["header"]["msg_type"]=="kernel_info_reply"
        rep = await kc.complete_request(code="impor", cursor_pos=5, timeout=TIMEOUT)
        assert rep["header"]["msg_type"]=="complete_reply"
        rep = await kc.shutdown(timeout=TIMEOUT)
        assert rep["header"]["msg_type"]=="shutdown_reply"


    async def test_concurrent_reply_waiters_route_by_parent_msg_id(self, kc):
        # Start two replies, then start waiter 2 first so the awaits are intentionally inverted.
        c1 = kc.reply("import time; time.sleep(0.1); 1", timeout=2)
        c2 = kc.reply("2", timeout=2)
        t2 = asyncio.create_task(c2)
        await asyncio.sleep(0)
        t1 = asyncio.create_task(c1)
        r1,r2 = await asyncio.gather(t1, t2)
        assert r1["header"]["msg_type"]=="execute_reply"
        assert r2["header"]["msg_type"]=="execute_reply"
        assert r1["content"]["status"]=="ok"
        assert r2["content"]["status"]=="ok"

    async def test_concurrent_replies_after_orphan_execute(self, kc):
        kc.execute("print('orphan')")
        await asyncio.sleep(0.3)
        slow = kc.reply("import time; time.sleep(0.3)", timeout=TIMEOUT)
        fast = kc.reply("1+1", timeout=TIMEOUT)
        rslow,rfast = await asyncio.gather(slow, fast)
        assert rslow["header"]["msg_type"]=="execute_reply"
        assert rfast["header"]["msg_type"]=="execute_reply"
        assert rslow["content"]["status"]=="ok"
        assert rfast["content"]["status"]=="ok"

    async def test_two_replies_can_be_awaited_together(self, kc):
        a = kc.reply("x=2", timeout=TIMEOUT)
        b = kc.reply("y=3", timeout=TIMEOUT)
        reps = await asyncio.wait_for(asyncio.gather(a,b), timeout=2)
        assert len(reps)==2
        assert all(rep["header"]["msg_type"]=="execute_reply" for rep in reps)
        assert len({rep["parent_header"]["msg_id"] for rep in reps})==2

    async def test_reply_sends_at_call_time(self):
        "The call files the future and queues the frame, so wire order is call order, whatever happens to the await."
        kc = JupyAsyncKernelClient("http://127.0.0.1:1", kernel_id="k")
        coro = kc.reply("1", timeout=0.2)
        assert len(kc.replies) == 1
        assert not kc._send_q.empty()
        task = asyncio.create_task(coro)
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
        assert not kc.replies   # a cancelled await pops its entry

    async def test_timed_out_reply_reaches_on_jmsg_when_late(self):
        class _OneMsgWS:
            def __init__(self, payload): self.payload,self._done = payload,False
            def __aiter__(self): return self
            async def __anext__(self):
                if self._done: raise StopAsyncIteration
                self._done = True
                return self.payload

        kc = JupyAsyncKernelClient("http://127.0.0.1:1", kernel_id="k", reconnect=False)
        seen = []
        kc.on_jmsg = seen.append
        w = kc.reply("1", timeout=0.01)
        [msg_id] = kc.replies.keys()
        with pytest.raises(TimeoutError): await w
        assert not kc.replies

        late = dict(channel="shell", metadata={}, content=dict(status="ok", execution_count=1))
        late["header"] = dict(msg_id="late-reply", msg_type="execute_reply")
        late["parent_header"] = dict(msg_id=msg_id, msg_type="execute_request")
        kc._ws = _OneMsgWS(dumps(late))
        await kc._recv_loop()
        assert seen[-1]["msg_type"] == "execute_reply"


    async def test_start_kernel_and_shutdown_kernel_http_helpers(self, jp_server):
        kc = JupyAsyncKernelClient(jp_server["base_url"])
        try:
            model = await kc.start_kernel("python3")
            assert kc.kernel_id == model["id"]
            kc.start_channels()
            await kc.wait_for_ready(timeout=TIMEOUT)
            rep = await kc.reply("2+2", timeout=TIMEOUT)
            assert rep["content"]["status"] == "ok"
            await kc.shutdown_kernel()
            assert not await kc.is_alive()
        finally: await kc.aclose()


    async def test_reconnect_survives_drop_and_gives_up_when_kernel_dies(self, kc):
        "A pending reply resolves across a forced disconnect (jupyter_server buffers and replays); once the kernel is gone, pending futures fail fast."
        t = asyncio.create_task(kc.reply("import time; time.sleep(1); 'back'", timeout=30))
        await asyncio.sleep(0.3)
        kc._ws.transport.abort()                # the network dies mid-cell
        rep = await t
        assert rep["content"]["status"] == "ok"
        rep = await kc.reply("1+1", timeout=TIMEOUT)  # live traffic flows again on the redialed socket
        assert rep["content"]["status"] == "ok"
        kc.reconnect_ceiling = 10
        t = asyncio.create_task(kc.reply("import time; time.sleep(30)", timeout=60))
        await asyncio.sleep(0.3)
        await kc.api.kernels.delete_kernel(kid=kc.kernel_id)  # the kernel goes away for real, behind the connection's back...
        kc._ws.transport.abort()                # ...and then the connection dies: the redial probe finds the kernel gone
        with pytest.raises(RuntimeError, match="gone"): await t
