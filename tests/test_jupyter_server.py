import asyncio, pytest
from jupyasyncclient import JmsgQueues, start_new_server_kernel
from jupywire.route import DeadKernelError

TIMEOUT = 5


async def test_walkthrough(jp_server):
    "The reference-server end-to-end: start through the manager, execute, answer stdin, shut down."
    km,kc = await start_new_server_kernel(jp_server["base_url"], startup_timeout=TIMEOUT)
    try:
        qs = JmsgQueues(kc, queues=("jmsg",), merge=dict(iopub="jmsg", stdin="jmsg"))
        rep = await kc.reply("print('hello'); 6*7", timeout=TIMEOUT)
        assert rep["content"]["status"] == "ok"
        task = asyncio.create_task(kc.reply("x = input()", allow_stdin=True, timeout=TIMEOUT))
        await qs.jmsg_for("input_request", timeout=TIMEOUT)
        kc.input("test")
        assert (await task)["content"]["status"] == "ok"
    finally:
        await kc.aclose()
        await km.shutdown_kernel(now=True)


async def test_reconnect_ends_old_work_and_gives_up_when_kernel_dies(jp_server):
    "A gap ends old work without resending it; redial serves new work or reports that the kernel is gone."
    km,kc = await start_new_server_kernel(jp_server["base_url"], startup_timeout=TIMEOUT)
    try:
        reconnected, qs = asyncio.Event(), JmsgQueues(kc)
        kc.on_reconnect = reconnected.set
        t = asyncio.create_task(kc.reply(r"import time; time.sleep(1); count = globals().get('count', 0) + 1", timeout=30))
        await qs.jmsg_for('status', pred=lambda m: m['content']['execution_state'] == 'busy', queue='iopub', timeout=TIMEOUT)
        kc._ws.transport.abort()                # the network dies mid-cell
        with pytest.raises(ConnectionError): await t
        await asyncio.wait_for(reconnected.wait(), TIMEOUT)
        rep = await kc.reply('count', timeout=TIMEOUT)
        assert rep["content"]["status"] == "ok"
        assert await kc.eval_expr('count') == 1
        kc.reconnect_ceiling = 10
        t = asyncio.create_task(kc.reply("import os, signal; os.kill(os.getpid(), signal.SIGKILL)", timeout=60))
        await asyncio.sleep(0.3)
        await kc.api.kernels.delete_kernel(kid=kc.kernel_id)  # the kernel goes away for real, behind the connection's back...
        reader = kc._recv_task
        kc._ws.transport.abort()                # ...and then the connection dies: the redial probe finds the kernel gone
        with pytest.raises((ConnectionError, DeadKernelError)): await t
        await reader
        with pytest.raises(DeadKernelError, match="gone"): await kc._start_task
    finally: await kc.aclose()
