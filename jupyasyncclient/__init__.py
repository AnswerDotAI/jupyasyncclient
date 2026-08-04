"""Async kernel client for Jupyter Server via HTTP/WebSocket

Modules:

- `jupyasyncclient.term`: A client for gateway-hosted terminals: REST lifecycle plus one websocket attachment"""

__version__ = "0.2.1"

from .core import JupyAsyncKernelClient, Run, DeadKernelError, dumps, loads, serialize_binary_message, deserialize_binary_message
from .manager import JupyAsyncKernelManager, start_new_server_kernel
from .multimanager import JupyAsyncMultiKernelManager
from .term import JupyAsyncTerminalClient

__all__ = ["JupyAsyncKernelClient", "Run", "DeadKernelError", "JupyAsyncKernelManager", "JupyAsyncMultiKernelManager", "JupyAsyncTerminalClient", "start_new_server_kernel"]
