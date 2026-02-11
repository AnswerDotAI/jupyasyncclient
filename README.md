# jupyasyncclient

`JupyAsyncKernelClient` is an `AsyncKernelClient`-style client that talks to a Jupyter Server over HTTP + WebSocket (instead of ZMQ).

`JupyAsyncKernelManager` is a tiny `AsyncKernelManager`-style wrapper around Jupyter Server's `/api/kernels`, so you can do the familiar `km.client()` flow.

`JupyAsyncMultiKernelManager` is an `AsyncMultiKernelManager`-style wrapper that can list/start/restart/shutdown multiple server kernels and hand you per-kernel `JupyAsyncKernelManager`/`JupyAsyncKernelClient` objects.

This is intentionally small and pragmatic.
