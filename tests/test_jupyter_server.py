import asyncio, time, pytest
from jupyasyncclient import JmsgQueues, JupyAsyncKernelClient, JupyAsyncTerminalClient
from rustygate.tools import start_gateway
from jupywire.route import DeadKernelError

TIMEOUT = 5


async def test_walkthrough(jp_server):
    "The reference-server end-to-end: connect, execute, answer stdin, shut down."
    async with await JupyAsyncKernelClient.connect(jp_server["base_url"], timeout=TIMEOUT) as kc:
        qs = JmsgQueues(kc, queues=("jmsg",), merge=dict(iopub="jmsg", stdin="jmsg"))
        rep = await kc.reply("print('hello'); 6*7", timeout=TIMEOUT)
        assert rep["content"]["status"] == "ok"
        task = asyncio.create_task(kc.reply("x = input()", allow_stdin=True, timeout=TIMEOUT))
        await qs.jmsg_for("input_request", timeout=TIMEOUT)
        kc.input("test")
        assert (await task)["content"]["status"] == "ok"


async def test_reconnect_ends_old_work_and_gives_up_when_kernel_dies(jp_server):
    "A gap ends old work without resending it; redial serves new work or reports that the kernel is gone."
    async with await JupyAsyncKernelClient.connect(jp_server["base_url"], timeout=TIMEOUT) as kc:
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


async def _read_until(fr, pat: str, timeout=TIMEOUT) -> str:
    buf, end = "", time.monotonic() + timeout
    while pat not in buf:
        item = await asyncio.wait_for(anext(fr), end - time.monotonic())
        if item[0] == "stdout": buf += item[1]
    return buf


async def test_terminal_interop(jp_server):
    "The same terminal client works end-to-end against both reference jupyter_server and rustygate."
    rg = start_gateway()
    try:
        for base_url in (jp_server["base_url"], rg.url):
            tc, tc2 = JupyAsyncTerminalClient(base_url), None
            try:
                model = await tc.start_terminal()
                assert model["name"] in [t["name"] for t in await tc.list_terminals()]
                assert (await tc.connect())[0] == "setup"
                fr = tc.frames()
                await tc.write("echo wired $((6*7))\n")
                assert "wired 42" in await _read_until(fr, "wired 42")
                await tc.resize(50, 120)
                await tc.write("stty size\n")
                assert "50 120" in await _read_until(fr, "50 120")
                tc2 = JupyAsyncTerminalClient(base_url, name=tc.name)
                assert (await tc2.connect())[0] == "setup"
                assert "wired 42" in await _read_until(tc2.frames(), "wired 42")
                await tc2.shutdown_terminal()
                item = await asyncio.wait_for(anext(fr), TIMEOUT)
                while item[0] == "stdout": item = await asyncio.wait_for(anext(fr), TIMEOUT)
                assert item[0] == "disconnect"
                assert tc.name not in [t["name"] for t in await tc.list_terminals()]
            finally:
                await tc.aclose()
                if tc2: await tc2.aclose()
    finally: rg.stop()
