from jupyasyncclient import JupyAsyncMultiKernelManager

TIMEOUT = 5


class TestJupyAsyncMultiKernelManager:
    async def test_ensure_kernel_reuse_and_shutdown_all(self, jp_server):
        mkm = JupyAsyncMultiKernelManager(jp_server["base_url"], kernel_name="python3")
        clients = []
        try:
            kid1 = await mkm.ensure_kernel("a")
            kid2 = await mkm.ensure_kernel("a")
            assert kid1 == kid2

            kc = mkm.client(kid1)
            clients.append(kc)
            kc.start_channels()
            await kc.wait_for_ready(timeout=TIMEOUT)
            rep = await kc.execute("2+2", reply=True, timeout=TIMEOUT)
            assert rep["content"]["status"] == "ok"

            kidb = await mkm.ensure_kernel("b")
            assert kidb != kid1
            kcb = mkm.client(kidb)
            clients.append(kcb)
            kcb.start_channels()
            await kcb.wait_for_ready(timeout=TIMEOUT)
            rep2 = await kcb.kernel_info(reply=True, timeout=TIMEOUT)
            assert rep2["header"]["msg_type"] == "kernel_info_reply"
        finally:
            for c in clients:
                try: await c.aclose()
                except Exception: pass
            await mkm.shutdown_all(now=True)
            await mkm.aclose()
