from jupyasyncclient import start_new_server_kernel

TIMEOUT = 5

class TestJupyAsyncKernelManager:
    async def test_start_new_server_kernel(self, jp_server):
        km, kc = await start_new_server_kernel(jp_server["base_url"], kernel_name="python3", startup_timeout=TIMEOUT)
        try:
            reply = await kc.execute("1+1", reply=True, timeout=TIMEOUT)
            assert reply["content"]["status"] == "ok"
        finally:
            await kc.aclose()
            await km.shutdown_kernel(now=True)
            await km.aclose()
