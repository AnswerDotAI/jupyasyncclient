import os, socket, subprocess, sys, time, urllib.request, pytest


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope="session")
def jp_server(tmp_path_factory):
    port = _free_port()
    root = tmp_path_factory.mktemp("jp-root")
    sandbox = tmp_path_factory.mktemp("jp-sandbox")
    runtime_dir,config_dir,data_dir,ipython_dir = [sandbox/p for p in ("runtime", "config", "data", "ipython")]
    for d in (runtime_dir, config_dir, data_dir, ipython_dir): d.mkdir(parents=True, exist_ok=True)
    base_url = f"http://127.0.0.1:{port}"

    cmd = [sys.executable, "-m", "jupyter_server", "--no-browser", f"--ServerApp.port={port}", "--ServerApp.port_retries=0",
           "--ServerApp.token=", "--ServerApp.password=", "--ServerApp.disable_check_xsrf=True", "--ServerApp.allow_root=True",
           f"--ServerApp.root_dir={root}", "--ServerApp.open_browser=False", "--ServerApp.log_level=50"]

    env = dict(os.environ)
    env.update(JUPYTER_PLATFORM_DIRS="1", JUPYTER_NO_CONFIG="1", JUPYTER_RUNTIME_DIR=str(runtime_dir),
               JUPYTER_CONFIG_DIR=str(config_dir), JUPYTER_DATA_DIR=str(data_dir), IPYTHONDIR=str(ipython_dir))
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, env=env)
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                urllib.request.urlopen(base_url + "/api/kernelspecs", timeout=1).read()
                break
            except Exception: time.sleep(0.2)
        else: raise RuntimeError("Jupyter Server did not start")
        yield {"base_url": base_url, "token": ""}
    finally:
        proc.terminate()
        try: proc.wait(timeout=10)
        except Exception: proc.kill()
