# Release notes

<!-- do not remove -->

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
