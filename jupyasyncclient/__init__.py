"""Async kernel client for Jupyter Server via HTTP/WebSocket

Modules:

- `jupyasyncclient.files`: Files and cells over the gateway's contents and cells APIs
- `jupyasyncclient.term`: A client for gateway-hosted terminals: REST lifecycle plus one websocket attachment"""

__version__ = "0.2.29"



from jupywire.route import JmsgQueues
from .core import JupyAsyncKernelClient, DeadKernelError
from .term import JupyAsyncTerminalClient
from .files import JupyAsyncFilesClient, JupyAsyncCellsClient, apply_ops
from .http import CommTransport, http_client, request_json

__all__ = ["JupyAsyncKernelClient", "JmsgQueues", "DeadKernelError", "JupyAsyncTerminalClient", "JupyAsyncFilesClient", "JupyAsyncCellsClient", "apply_ops", "CommTransport", "http_client", "request_json"]
