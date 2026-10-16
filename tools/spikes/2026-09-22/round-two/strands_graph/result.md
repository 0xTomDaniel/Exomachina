# Strands Graph round-two countertrial

22 September 2026 · bounded synthetic result; no product runtime selected.

## Decision signal

Core Strands Graph remains a plausible small outer runtime, but only with an Exomachina-owned run claim and effect identity/reconciliation boundary. The local probe closed the earlier observed duplicate **accepted synthetic delivery** with 119 lines of claim/effect code layered over the reused Graph fixture. It did not establish exactly-once external A2A effects or prove that full product recovery can stay this small. A lease can expire after an owner passes its fence; the overlap probe then produced **two submission attempts** for one accepted effect. The remote endpoint's idempotency key, or an equivalent authoritative reconciliation contract, is essential.

## Pin, topology, and sources

- `strands-agents==1.57.0`, CPython 3.14.3, macOS 26.5.2 arm64. The reused [countertrial lockfile](../../countertrials/strands_graph/uv.lock) has SHA-256 `166ef59aa5dd9ac96d97151952531d58cdfe3bc1a3fb5257ba105dcf074b8149`. Installed package metadata declares Apache-2.0.
- Reused the earlier [Graph builder, immutable v1/v2 definitions, interrupt, and deterministic model fixture](../../countertrials/strands_graph/runner.py) by import. This directory adds no copied Strands SDK source. Runtime databases, session files, and rendezvous sentinels are under `/tmp/exomachina-round-two/strands_graph`.
- One Python process hosts both v1 and v2 at publication time. Fresh Python processes resume each. The contested trials launch separate processes for each owner against one run and one local `FileSessionManager` session directory. Local SQLite stores a run claim and effect ledger; a **separate SQLite database represents only a synthetic remote service** that accepts a stable effect key. There is no HTTP/A2A transport, model call, external provider, queue, or complete factory harness.
- The [current Python session guide](https://strandsagents.com/docs/user-guide/concepts/agents/session-management/) says Graph/Swarm should use repository-based `FileSessionManager` and that `SnapshotSessionManager` does not support Graph. The earlier pinned trial happened to work with `SnapshotSessionManager`; this round tested the documented manager instead. The same guide says session managers take no distributed lock.

## Observed process tests

The [file manager probe](file_session_probe.py) constructed v1, paused before delivery, published v2 in the same process, and paused v2. It then exited. Separate new processes restored and completed v1 and v2. V1 had no `verify` node; v2 did. The pinned definition digests remained `13006bf8…e1e56` and `8d8a56bd…04379`. Exact execution orders and statuses are in [file_session_observations.json](file_session_observations.json).

The [fenced probe](fenced_probe.py) uses a SQLite `BEGIN IMMEDIATE` claim with an increasing epoch and lease. It checks owner, epoch and lease immediately before the synthetic effect, records `pending`, looks up the stable effect key remotely, submits if absent, then records `accepted`. These were the observed interleavings in [observations.json](observations.json):

| Forced case | Observed result |
| --- | --- |
| Second owner while first lease valid | Claim rejected before Graph resume. |
| Owner A pauses at delivery; lease expires; B takes epoch 2 and completes; A resumes with epoch 1 | A raised `stale run claim`; B alone submitted; one accepted effect. A third owner was rejected after completion. |
| A passes fence, then pauses before remote submit; lease expires; B submits and completes; A continues | Both A and B submitted the same key, in order B then A. The synthetic remote accepted one effect. A's final claim completion failed because its epoch was stale. This demonstrates the remaining submit race. |
| First owner submits and the remote accepts, then process receives SIGKILL before local acknowledgement | Local effect stayed `pending` while remote had one accepted record. After lease expiry, epoch 2 owner restored the Graph, found the remote record by key, recorded `accepted`, and completed without another submit. |

The process tests used real OS processes and SQLite transactions. The remote is a synthetic ledger whose lookup and unique-key behavior are defined by this probe; it does **not** establish an external provider's actual idempotency, query, timeout, or acknowledgement contract. The lost-ack test kills a process while it is held immediately after a committed synthetic remote accept. It does not cover a network partition, remote acceptance with delayed visibility, or an effect executed outside the fenced model wrapper.

## Owned code and operating cost

This round added 386 source lines: 81 in `file_session_probe.py` and 305 in `fenced_probe.py`. The claim, effect-key, synthetic remote, delivery, and claim-finalization functions occupy about 119 lines of `fenced_probe.py`; the remainder is process orchestration, assertions, CLI and measurements. The earlier Graph fixture remains 272 lines and is imported, not copied. The code is a measurement of this narrow local prototype, not a production estimate. There is **no timer loop, worker discovery loop, lease renewal, general definition interpreter, provider adapter, or scheduler** in the new code.

One warm process holding two interrupted `FileSessionManager` Graph runs measured **73,248 KiB RSS (71.5 MiB)**. Its state directory measured **60 KiB** at the pause; the pinned Python environment was **82,524 KiB** on disk. See [file_session_measurement.json](file_session_measurement.json). RSS includes shared pages and excludes a real model, A2A server/client, artifact storage, worker processes, remote service and product policy. This is not a whole-install memory comparison with other candidates.

The run claim needs renewal or an execution bound longer than real node work; this 1.5–5 second lease was only a test control. Recovery discovery, long waits without a retained process, concurrent commands, cross-host durable storage, crash consistency between Graph's session and the separate claim/effect ledger, completed-run retention, and multi-node effects remain open. A process can lose its claim after passing the fence, so the product must require stable remote effect IDs with provider-confirmed duplicate suppression or an authoritative status query/reconciliation path. Without that provider contract, this probe's single accepted effect does not transfer to A2A. If those pieces demand a persistent scheduler or broad execution ledger beyond the factory's existing obligations, Graph's simplicity advantage narrows substantially.

## Reproduce

From this directory, after syncing the earlier pinned project:

```sh
cd ../../countertrials/strands_graph
UV_PROJECT_ENVIRONMENT=/tmp/exomachina-countertrials/strands_graph/.venv uv sync --frozen
cd ../../round-two/strands_graph
/tmp/exomachina-countertrials/strands_graph/.venv/bin/python file_session_probe.py exercise --root /tmp/exomachina-round-two/strands_graph/file-session-repeat --output file_session_observations.json
/tmp/exomachina-countertrials/strands_graph/.venv/bin/python file_session_probe.py measure --root /tmp/exomachina-round-two/strands_graph/file-measure-repeat --output file_session_measurement.json
/tmp/exomachina-countertrials/strands_graph/.venv/bin/python fenced_probe.py exercise --root /tmp/exomachina-round-two/strands_graph/fenced-repeat --output observations.json
```

Use fresh `--root` paths because the published revisions and run IDs are immutable. The measure command kills its held process; the fenced probe kills only its own synthetic lost-ack process. All probe processes had exited when these observations were recorded.
