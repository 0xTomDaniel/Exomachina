# Core Strands Graph outer-runtime countertrial

22 September 2026 · **Bounded probes completed; product engine unselected**

## Question and exact topology

Can the existing Python Strands SDK provide a lightweight, durable graph inside a factory-capable harness while agent-authored factory definitions change as data, without moving graph progress into a new application scheduler?

This trial used **Strands Agents 1.57.0**, CPython **3.14.3**, macOS **26.5.2 arm64**, and the checked-in `uv.lock`. The installed package declares Apache-2.0. There were **two independent Python processes**, each hosting two core SDK Graph runs in one process, with one `SnapshotSessionManager` and `LocalFileStorage` namespace per run. There was no separate workflow server, database process, Docker, model-provider call, real A2A service, Strands A2A endpoint, or complete S2 factory harness. The model was a deterministic external-provider fixture that emitted fixed node markers. State and the virtual environment lived under `/tmp/exomachina-countertrials/strands_graph`; only source and sanitized measurements are in this directory.

Each process published `v1.json`, built a Graph and stopped at a `BeforeNodeCallEvent` interrupt before `deliver`; **in that same process**, it then published a structurally different `v2.json`, built another Graph and stopped at its own interrupt. V2 adds `verify` between the join and review. These two definitions were manually written fixtures, so agent authoring quality was not tested. The small application-owned publisher accepted only a fixed node/condition vocabulary, wrote immutable revision files, recorded the digest and revision in each run binding, and reconstructed the exact graph from that binding on resume. Python Graph's default incoming-edge behavior is OR, so both research-to-join edges used an explicit **both-complete** condition. A direct `join → deliver` bypass was rejected by the fixture validator. This is one bypass case, not a complete publication-policy proof.

## Observed result

The [countertrial runner](countertrial.py) completed with all assertions. For each of the two distinct harness identities, both v1 and v2 reached `interrupted`. The parent then sent **SIGKILL** to each held Python process. New Python processes reconstructed each run from its pinned definition and saved session, sent the saved interrupt response, and reached `completed`. V1 resumed without the v2 `verify` node; v2 retained it. Append-only fixture events show every pre-interrupt node ran **once** and only `deliver` ran after restart, once per run. Both research branches completed before the single join. The exact per-run statuses and event counts are in [measurements.json](measurements.json).

This proves a useful **single-host, settled-checkpoint** path. It does not prove a crash while a node is executing, a lost remote submission acknowledgement, product-level idempotency, or a complete versioned factory catalog. Core Graph supplied traversal, the node snapshot and interrupt mechanics; the versioned document loader, digest binding, approval vocabulary and process-resume command were trial code.

### Contested resume: the missing ownership guarantee is observed

The bounded [contended-resume follow-up](contended.py) launched **two separate Python processes against the same `run-v1` session and definition**. A fixture rendezvous held both after they had restored the interrupted graph and accepted the saved response. Owner A was released and completed; the fixture event counter showed one `deliver`. Then stale in-memory owner B was released **after A completed**. B also returned `completed` with exit code 0, and the counter showed **two `deliver` executions**. Earlier nodes remained at one each. The exact result is [contention.json](contention.json).

The `deliver` node only appended a deterministic local test event; no external publication or duplicate customer effect occurred. The result nevertheless shows that the core SDK's saved graph/session did **not** fence an already-restored stale owner or deduplicate a contested side-effect node. It does not test a product-level Director incarnation token or an Exomachina-owned lock, because neither was implemented. Those become required product work: a durable exclusive per-run resume claim with fencing/CAS, plus idempotent effect identities and reconciliation for ambiguous completion. A file lock alone would not resolve remote-effect uncertainty after a process dies.

| Measurement | Observed |
| --- | ---: |
| Warm paused process RSS, one harness alive | 73,664 KiB (71.9 MiB) |
| Warm paused process RSS, two harnesses alive | 73,664 KiB each; 147,328 KiB naive RSS sum |
| Time from Python process launch through import and two paused runs | 0.789 s, 0.719 s |
| Installed Python environment on disk | 82,524 KiB |
| Initialized state after resume | 48 KiB per harness root |

These are **not whole-product resource figures**. The process did not host the S2 A2A server, real model provider, artifact store, remote workers or full factory policy. RSS includes shared pages, so adding process RSS is not a non-double-counted whole-install memory measure. Temporal and Dagu trials were running concurrently on this host; whole-machine pressure and these start times are not isolated comparisons. No peak, sustained workload, cold-install or serial repeat was measured. The measured 71.9 MiB per Python process is much smaller than the observed warm Kestra JVM alone, but the topologies and workloads are not equivalent and this is **not** a product selection or resource-budget pass.

## Product work exposed by the trial

1. **Factory authoring and publication.** Core Graph is built through `GraphBuilder`, not a managed versioned document service. Exomachina would own a definition schema/loader, capability binding, approved node and condition registry, policy validation across all paths, immutable publication, migration/compatibility rules, authorization, dependency pinning, and rollout/rollback. This fixture implements a deliberately tiny portion. Its `runner.py` contains 272 lines including the deterministic model, rendezvous fixture and CLI; that line count is evidence of trial scope, not an estimate of production effort.
2. **Ownership and restart.** Session snapshots persist graph state after nodes and interrupts. The SDK session guidance explicitly says session managers are not thread-safe and take no distributed lock. The contested trial observed both owners complete one logical run and execute the delivery node twice. Exomachina would need one fenced resume owner per run, discovery of pending runs after process launch, concurrent-command deduplication, timers/deadlines, retention, backups, and recovery across partial publication/run/session writes. No production fencing or reconciler was implemented.
3. **Remote work.** Strands Graph documents A2AAgent nodes, but this trial used none. Lost acceptance replies, task-ID recovery, duplicate dispatch, asynchronous waits, cancellation, remote artifact retrieval and exact-revision acceptance remain unrun. A completed Graph node does not independently establish that its remote output passed Quality's acceptance gate. The existing S2 harness proof and S4 Kestra ledger fixture do not transfer these guarantees automatically to core Graph.
4. **Operating model.** The fixture used one local file store per run and two separate harness state roots. It did not test a single shared store, concurrent same-run access, same-user hostile writes, host loss, large artifacts, long-lived idle waits, process supervision, packaging, upgrades or cross-platform behavior.

The next decision gate is **not another happy-path graph**. The competing-owner test has now shown the missing fence. A subsequent trial should first assess whether one small application-owned lock/CAS Interface plus effect idempotency can prevent that duplicate while preserving recovery; then test a kill during remote dispatch or an ambiguous reply, a long wait with no retained worker/thread, and a publication policy that rejects indirect review bypass. Count every new durable table, lock, scheduler loop and definition semantic against the no-bespoke-engine objective. If satisfying those cases requires Exomachina to own a second workflow scheduler or broad interpreter, Strands Graph loses its apparent simplicity advantage despite the low observed process RSS.

## Reproduce

From this directory:

```sh
UV_PROJECT_ENVIRONMENT=/tmp/exomachina-countertrials/strands_graph/.venv uv sync --frozen
UV_PROJECT_ENVIRONMENT=/tmp/exomachina-countertrials/strands_graph/.venv uv run --frozen python countertrial.py \
  --root /tmp/exomachina-countertrials/strands_graph/repeat-01 \
  --output measurements.json
UV_PROJECT_ENVIRONMENT=/tmp/exomachina-countertrials/strands_graph/.venv uv run --frozen python contended.py \
  --root /tmp/exomachina-countertrials/strands_graph/contention-repeat-01 \
  --output contention.json
```

The first command launches and SIGKILLs its two held subprocesses. The second launches two contending resume processes and releases them in sequence. All owned processes had stopped when this result was written. Use a fresh `--root` for each repeat because published revisions and run IDs are intentionally immutable.

Primary Strands references: [Graph](https://strandsagents.com/docs/user-guide/sdk/multi-agent/graph/), [multi-agent session checkpoints and concurrency limits](https://strandsagents.com/docs/user-guide/sdk/agents/session-management/), and [Graph interrupts](https://strandsagents.com/docs/user-guide/sdk/interrupts-multi-agent/). The SDK [Workflow page](https://strandsagents.com/docs/user-guide/sdk/multi-agent/workflow/) is a code-pattern guide, not a managed factory-definition runtime. The separate Tools [graph](https://github.com/strands-agents/tools/blob/main/src/strands_tools/graph.py) and [workflow](https://github.com/strands-agents/tools/blob/main/src/strands_tools/workflow.py) utilities were not used: the former holds graphs in a process-global manager, and the latter currently returns an error for pause/resume.
