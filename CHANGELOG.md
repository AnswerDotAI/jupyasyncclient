# Release notes

<!-- do not remove -->

## 0.2.14

### New Features

- Add fields query param to /api/search for ancestor-walk content ([#25](https://github.com/AnswerDotAI/jupyasyncclient/issues/25))


## 0.2.13

### New Features

- search wrapper, `list_kernels` forwards its filters, and the regenerated spec ([#24](https://github.com/AnswerDotAI/jupyasyncclient/pull/24)), thanks to [@jph00](https://github.com/jph00)


## 0.2.12

### New Features

- Import APIError from fasttransport ([#23](https://github.com/AnswerDotAI/jupyasyncclient/pull/23)), thanks to [@jph00](https://github.com/jph00)


## 0.2.11

### New Features

- Support stable subshell ids in `create_subshell`, rename eval flags ([#22](https://github.com/AnswerDotAI/jupyasyncclient/issues/22))


## 0.2.10

### New Features

- Teach the streaming run() in the core notebook ([#21](https://github.com/AnswerDotAI/jupyasyncclient/pull/21)), thanks to [@jph00](https://github.com/jph00)


## 0.2.9

### New Features

- Add OpenAPI-derived kwargs to kernel list/start methods via delegates ([#20](https://github.com/AnswerDotAI/jupyasyncclient/issues/20))
- Add `on_stdin` callback to scope `input_request` handling to a single run ([#18](https://github.com/AnswerDotAI/jupyasyncclient/issues/18))


## 0.2.8

### New Features

- Move message routing to jupywire RouterOps: awaitable `*_request` senders, collecting run(), JmsgQueues for pull-style consumers ([#17](https://github.com/AnswerDotAI/jupyasyncclient/issues/17))
- Add JupyAsyncKernelClient.model to fetch the gateway kernel model, and use it in `is_alive` and reconnect checks ([#16](https://github.com/AnswerDotAI/jupyasyncclient/issues/16))
- Add `build_spec` to regenerate the bundled rustygate spec from a running gateway openapi.json ([#14](https://github.com/AnswerDotAI/jupyasyncclient/issues/14))
- Replace hand-rolled HTTP calls with spec-generated OpenAPI ops from bundled rustygate spec across kernel, terminal, and files clients ([#13](https://github.com/AnswerDotAI/jupyasyncclient/issues/13))
- Remove priority param from reply() ([#12](https://github.com/AnswerDotAI/jupyasyncclient/issues/12))


## 0.2.7

### New Features

- Send requests at call time, add release() for held executes, unique/parents file options, and `jmsg_for`/`jmsg_flush` message helpers ([#11](https://github.com/AnswerDotAI/jupyasyncclient/issues/11))


## 0.2.6

### New Features

- Add JupyAsyncFilesClient.edit, drop conditional apply, make `apply_ops` bend like the server, and coerce Path args to str ([#10](https://github.com/AnswerDotAI/jupyasyncclient/issues/10))


## 0.2.5

### New Features

- Flush pending sends on aclose, auto-close after `shutdown_kernel`, add `jmsg_flush` and `jmsg_for` pred, drop drain from `wait_for_ready` ([#9](https://github.com/AnswerDotAI/jupyasyncclient/issues/9))


## 0.2.4

### New Features

- Replace httpx client plumbing with fasttransport AsyncTransport; drop `aclose_http` ([#8](https://github.com/AnswerDotAI/jupyasyncclient/issues/8))


## 0.2.3

### New Features

- Merge iopub/stdin/cells into one jmsg stream with `jmsg_for`; add `kernel_for`, custom `msg_id`, `max_size` ([#7](https://github.com/AnswerDotAI/jupyasyncclient/issues/7))


## 0.2.2

### New Features

- Add files/cells client with cells channel and `apply_ops`, TLS verify option, and switch dev server and docs from jupygate to rustygate ([#6](https://github.com/AnswerDotAI/jupyasyncclient/issues/6))
- Raise DeadKernelError instead of RuntimeError when reconnect probe finds the kernel is gone ([#5](https://github.com/AnswerDotAI/jupyasyncclient/issues/5))


## 0.2.1

### New Features

- Add Run streaming execution API and connect/context-manager startup; add JupyAsyncTerminalClient; make complete/inspect/check awaited helpers ([#4](https://github.com/AnswerDotAI/jupyasyncclient/issues/4))


## 0.2.0

### New Features

- Move session serialization to jupywire, add EvalOps mixin with reply/eval sugar and ipyfuncs helpers; replace `jupyter_client` dep ([#3](https://github.com/AnswerDotAI/jupyasyncclient/issues/3))
- Add websocket auto-reconnect with backoff and unsent-payload retry; use fastcore xdumps/`pack_frames` helpers ([#2](https://github.com/AnswerDotAI/jupyasyncclient/issues/2))
- Convert to nbdev; add KernelApi base, docs notebooks, and reply-parenting/fail-pending features ([#1](https://github.com/AnswerDotAI/jupyasyncclient/issues/1))

## 0.1.0

- Init release
