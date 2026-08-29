"""Async kernel client for Jupyter Server via HTTP/WebSocket

Modules:

- `jupyasyncclient.files`: Files and cells over the gateway's contents and cells APIs
- `jupyasyncclient.term`: A client for gateway-hosted terminals: REST lifecycle plus one websocket attachment"""

__version__ = "0.2.10"

from jupywire.route import JmsgQueues
from .core import JupyAsyncKernelClient, DeadKernelError, dumps, loads, serialize_binary_message, deserialize_binary_message
from .manager import JupyAsyncKernelManager, start_new_server_kernel
from .multimanager import JupyAsyncMultiKernelManager
from .term import JupyAsyncTerminalClient
from .files import JupyAsyncFilesClient, JupyAsyncCellsClient, HashMismatch, apply_ops

__all__ = ["JupyAsyncKernelClient", "JmsgQueues", "DeadKernelError", "JupyAsyncKernelManager", "JupyAsyncMultiKernelManager", "JupyAsyncTerminalClient", "JupyAsyncFilesClient", "JupyAsyncCellsClient", "HashMismatch", "apply_ops", "start_new_server_kernel"]
