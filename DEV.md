# Developer guide

The client's design and behavior are documented where they are implemented: `nbs/00_core.ipynb` builds `JupyAsyncKernelClient` bottom-up with every public method demonstrated against a live rustygate (the notebooks spawn the binary via `rustygate.tools.start_gateway`, provided by the `dev` extra). This file covers the project structure and the traps discovered while building it.

## Project structure

- `core.py` is generated from `nbs/00_core.ipynb` (never edit it; `nbdev-export` overwrites). It holds the wire codec, `KernelApi` (shared HTTP plumbing), and the client.
- `manager.py` and `multimanager.py` are plain hand-written modules. They are thin HTTP wrappers over `KernelApi` with no narrative worth a notebook; putting them in one would only make giant cells. Their behavior is covered by the test suite and shown in `index.ipynb`.
- The public API is re-exported from `__init__.py`; `_ws` (the old codec module) folded into `core` in 0.2.0, so `jupyasyncclient._ws` imports are gone while `manager`/`multimanager` paths survive.
- `rg_spec.py` is rustygate's spec in fastspec's compact pre-parsed form (`SpecParser.save`), which `rg_spec()` loads into the ops on every client's `api`. Regenerate it with `tools/build.py`, with a gateway built at the wanted rev running at `http://localhost:8787`, then run the notebooks. Bundling (rather than fetching at connect time) keeps construction offline and lets the same client talk to servers that serve no spec, such as jupyter_server.

## Traps worth knowing

- **`@patch` on `__getattr__` breaks the generated module.** `patch` returns the name's previous module-global (or builtin) so decorated names don't clobber anything - but `__getattr__` has no previous binding, so the module ends up with `__getattr__ = None`, which PEP 562 treats as the module's attribute hook: importing the module then dies with `NoneType is not callable` on the first missing-attribute lookup. Notebook execution can't reproduce it (cells aren't a module). Hence the explicit `JupyAsyncKernelClient.__getattr__ = _gen_request` assignment. A fastcore-side guard (returning a plain raise-AttributeError hook when there is no prior binding) would make `@patch` safe here; until then, don't `@patch` dunders that PEP 562 gives module-level meaning.
- **Generated request methods are gated to `*_request`/`*_reply` names.** Before that guard, any attribute access fabricated a sender: a typo like `kc.exeucte(...)` silently sent a bogus message type to the kernel, and an attribute looked up before its `@patch` cell had run did the same. Anything outside the two suffixes raises `AttributeError`.
- **`wait_for_ready` deliberately calls `kernel_info_request`,** the generated name, not the typed `kernel_info` wrapper: the wrapper is defined later in the notebook, and machinery must not depend on surface defined after it.
- **Unit tests that drive `_recv_loop` by hand need `reconnect=False`.** The receive loop's tail schedules `_reconnect` on any non-deliberate close, so a fake-websocket unit test against a dead base URL would leak a background task retrying for `reconnect_ceiling` seconds. Real connections are unaffected: `aclose` sets `_closing` and cancels the reconnect task.

## Tests

`pytest -q` runs the suite against a real jupyter_server spawned per session (`tests/conftest.py`), deliberately not rustygate: the notebooks prove the client against rustygate, the suite proves it against the reference server, so both wire implementations are exercised (reconnect included: the suite test rides jupyter_server's own buffering and replay, the notebook demo rides rustygate's). pytest-timeout bounds everything. Style is fastai; run `chkstyle` on `manager.py` and `multimanager.py` (hand-written) - `core.py` style is fixed in the notebook.
