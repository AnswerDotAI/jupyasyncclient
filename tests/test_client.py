import asyncio, httpx, pytest
from jupyasyncclient import JupyAsyncKernelClient

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
    async def test_execute_reply(self, kc):
        reply = await kc.execute("print('hello')", reply=True, timeout=TIMEOUT)
        assert reply["content"]["status"]=="ok"


    async def test_input_request(self, kc):
        task = asyncio.create_task(kc.execute("x = input()\nx", reply=True, timeout=TIMEOUT))
        msg = await kc.get_stdin_msg(timeout=TIMEOUT)
        assert msg["header"]["msg_type"]=="input_request"
        kc.input("test")
        reply = await task
        assert reply["content"]["status"]=="ok"


    async def test_request_reply_roundtrip_and_shutdown(self, kc):
        rep = await kc.kernel_info(reply=True, timeout=TIMEOUT)
        assert rep["header"]["msg_type"]=="kernel_info_reply"
        rep = await kc.complete("impor", reply=True, timeout=TIMEOUT)
        assert rep["header"]["msg_type"]=="complete_reply"
        rep = await kc.shutdown(reply=True, timeout=TIMEOUT)
        assert rep["header"]["msg_type"]=="shutdown_reply"


    async def test_concurrent_reply_waiters_route_by_parent_msg_id(self, kc):
        # Start two execute requests, then start waiter 2 first so queue consumers are intentionally inverted.
        c1 = kc.execute("import time; time.sleep(0.1); 1", reply=True, timeout=2)
        c2 = kc.execute("2", reply=True, timeout=2)
        t2 = asyncio.create_task(c2)
        await asyncio.sleep(0)
        t1 = asyncio.create_task(c1)
        r1,r2 = await asyncio.gather(t1, t2)
        assert r1["header"]["msg_type"]=="execute_reply"
        assert r2["header"]["msg_type"]=="execute_reply"
        assert r1["content"]["status"]=="ok"
        assert r2["content"]["status"]=="ok"


    async def test_start_kernel_and_shutdown_kernel_http_helpers(self, jp_server):
        kc = JupyAsyncKernelClient(jp_server["base_url"])
        try:
            model = await kc.start_kernel("python3")
            assert kc.kernel_id == model["id"]
            kc.start_channels()
            await kc.wait_for_ready(timeout=TIMEOUT)
            rep = await kc.execute("2+2", reply=True, timeout=TIMEOUT)
            assert rep["content"]["status"] == "ok"
            await kc.shutdown_kernel()
            assert not await kc.is_alive()
        finally: await kc.aclose()
