import asyncio, pytest
from jupyasyncclient import JmsgQueues, start_new_server_kernel

TIMEOUT = 5


async def test_walkthrough(jp_server):
    "The reference-server end-to-end: start through the manager, execute, answer stdin, shut down."
    km,kc = await start_new_server_kernel(jp_server["base_url"], startup_timeout=TIMEOUT)
    try:
        qs = JmsgQueues(kc, queues=("jmsg",), merge=dict(iopub="jmsg", stdin="jmsg"))
        rep = await kc.reply("print('hello'); 6*7", timeout=TIMEOUT)
        assert rep["content"]["status"] == "ok"
        task = asyncio.create_task(kc.reply("x = input()", timeout=TIMEOUT))
        await qs.jmsg_for("input_request", timeout=TIMEOUT)
        kc.input("test")
        assert (await task)["content"]["status"] == "ok"
    finally:
        await kc.aclose()
        await km.shutdown_kernel(now=True)


async def test_reconnect_survives_drop_and_gives_up_when_kernel_dies(jp_server):
    "A pending reply resolves across a forced disconnect (jupyter_server buffers and replays); once the kernel is gone, pending futures fail fast."
    km,kc = await start_new_server_kernel(jp_server["base_url"], startup_timeout=TIMEOUT)
    try:
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
    finally: await kc.aclose()
