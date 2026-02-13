import time
from queue import Empty

import httpx, pytest
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


async def _wait_for_iopub_status(kc, parent_msg_id: str, state: str, timeout: float = TIMEOUT):
    "Wait for a status message with a specific parent_msg_id + execution_state."
    deadline = time.monotonic() + timeout
    while True:
        t = deadline - time.monotonic()
        if t <= 0: raise TimeoutError(f"no iopub status={state!r} for {parent_msg_id!r}")
        try: msg = await kc.get_iopub_msg(timeout=t)
        except Empty as e: raise TimeoutError(f"no iopub status={state!r} for {parent_msg_id!r}") from e
        if msg.get("header", {}).get("msg_type") != "status": continue
        if msg.get("parent_header", {}).get("msg_id") != parent_msg_id: continue
        if msg.get("content", {}).get("execution_state") != state: continue
        return msg


async def _wait_for_shell_reply(kc, parent_msg_id: str, msg_type: str, timeout: float = TIMEOUT):
    "Wait for a shell reply message with a specific parent_msg_id + msg_type."
    deadline = time.monotonic() + timeout
    while True:
        t = deadline - time.monotonic()
        if t <= 0: raise TimeoutError(f"no shell {msg_type!r} for {parent_msg_id!r}")
        try: msg = await kc.get_shell_msg(timeout=t)
        except Empty as e: raise TimeoutError(f"no shell {msg_type!r} for {parent_msg_id!r}") from e
        if msg.get("header", {}).get("msg_type") != msg_type: continue
        if msg.get("parent_header", {}).get("msg_id") != parent_msg_id: continue
        return msg


class TestSubshells:
    async def test_subshell_lifecycle_create_delete(self, kc):
        rep = await kc.create_subshell(reply=True, timeout=TIMEOUT)
        assert rep["header"]["msg_type"] == "create_subshell_reply"
        assert rep["content"].get("status") == "ok"
        sid = rep["content"].get("subshell_id")
        assert sid

        rep = await kc.delete_subshell(sid, reply=True, timeout=TIMEOUT)
        assert rep["header"]["msg_type"] == "delete_subshell_reply"
        assert rep["content"].get("status") == "ok"

        rep = await kc.list_subshell(reply=True, timeout=TIMEOUT)
        assert rep["header"]["msg_type"] == "list_subshell_reply"
        assert rep["content"].get("status") == "ok"
        assert sid not in (rep["content"].get("subshell_id") or [])

    async def test_subshell_concurrency_execute_vs_complete(self, kc):
        rep = await kc.create_subshell(reply=True, timeout=TIMEOUT)
        assert rep["header"]["msg_type"] == "create_subshell_reply"
        assert rep["content"].get("status") == "ok"
        sid = rep["content"].get("subshell_id")
        assert sid

        rep = await kc.list_subshell(reply=True, timeout=TIMEOUT)
        assert rep["header"]["msg_type"] == "list_subshell_reply"
        assert rep["content"].get("status") == "ok"
        assert sid in (rep["content"].get("subshell_id") or [])

        # Run a long execute on the subshell.
        exec_id = kc.execute("import time; time.sleep(1.0); 123", subshell_id=sid, reply=False)
        await _wait_for_iopub_status(kc, exec_id, "busy", timeout=TIMEOUT)

        # While subshell is busy, we should still be able to do completions on the main shell.
        t0 = time.perf_counter()
        comp = await kc.complete("impor", reply=True, timeout=0.6)
        assert comp["header"]["msg_type"] == "complete_reply"
        assert time.perf_counter() - t0 < 0.6

        rep = await _wait_for_shell_reply(kc, exec_id, "execute_reply", timeout=TIMEOUT)
        assert rep["content"].get("status") == "ok"

        rep = await kc.delete_subshell(sid, reply=True, timeout=TIMEOUT)
        assert rep["header"]["msg_type"] == "delete_subshell_reply"
        assert rep["content"].get("status") == "ok"

        rep = await kc.list_subshell(reply=True, timeout=TIMEOUT)
        assert sid not in (rep["content"].get("subshell_id") or [])
