import asyncio, httpx, pytest
from jupyasyncclient import JupyAsyncKernelClient

TIMEOUT = 60

@pytest.fixture
async def kc(jp_server):
    async with httpx.AsyncClient() as http:
        r = await http.post(jp_server["base_url"] + "/api/kernels", json={"name": "python3"})
        r.raise_for_status()
        model = r.json()
        kernel_id = model["id"]
        client = JupyAsyncKernelClient(jp_server["base_url"], kernel_id=kernel_id)
        client.start_channels()
        await client.wait_for_ready(timeout=TIMEOUT)
        try: yield client
        finally:
            await client.aclose()
            try: await http.delete(jp_server["base_url"] + f"/api/kernels/{kernel_id}")
            except Exception: pass

class TestJupyAsyncKernelClient:
    async def test_execute_interactive_and_output_hook(self, kc):
        got = asyncio.Event()
        def hook(msg):
            if msg["header"]["msg_type"] == "stream" and "hello" in msg["content"].get("text", ""): got.set()
        reply = await kc.execute_interactive("print('hello')", timeout=TIMEOUT, output_hook=hook)
        assert reply["content"]["status"] == "ok"
        assert got.is_set()

    async def test_input_request(self, kc):
        def handle_stdin(msg): kc.input("test")
        reply = await kc.execute_interactive("x = input()\nx", stdin_hook=handle_stdin, timeout=TIMEOUT)
        assert reply["content"]["status"] == "ok"

    async def test_request_reply_roundtrip_and_shutdown(self, kc):
        rep = await kc.kernel_info(reply=True, timeout=TIMEOUT)
        assert rep["header"]["msg_type"] == "kernel_info_reply"
        rep = await kc.complete("impor", reply=True, timeout=TIMEOUT)
        assert rep["header"]["msg_type"] == "complete_reply"
        rep = await kc.shutdown(reply=True, timeout=TIMEOUT)
        assert rep["header"]["msg_type"] == "shutdown_reply"

