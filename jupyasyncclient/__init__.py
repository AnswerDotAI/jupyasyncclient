"""Async kernel client for Jupyter Server via HTTP/WebSocket

Modules:

- `jupyasyncclient.files`: Files and cells over the gateway's contents and cells APIs
- `jupyasyncclient.sessions`: Async client for Jupyter sessions over `/api/sessions`
- `jupyasyncclient.ssh`: SSH-over-WebSocket ProxyCommand client, session store, and `.ssh/config` updater for Rustygate, Solveit, and Colab
- `jupyasyncclient.term`: A client for gateway-hosted terminals: REST lifecycle plus one websocket attachment"""

__version__ = "0.2.32"









from jupywire.route import JmsgQueues
from .core import JupyAsyncKernelClient, DeadKernelError, Gateway, start_gateway
from .term import JupyAsyncTerminalClient
from .files import JupyAsyncFilesClient, JupyAsyncCellsClient, apply_ops
from .sessions import JupyAsyncSessionsClient
from .http import CommTransport, http_client, request_json

__all__ = ["JupyAsyncKernelClient", "JmsgQueues", "DeadKernelError", "Gateway", "start_gateway", "JupyAsyncTerminalClient", "JupyAsyncFilesClient", "JupyAsyncCellsClient", "JupyAsyncSessionsClient", "apply_ops", "CommTransport", "http_client", "request_json"]
