# S2 — Strands harness / factory identity / A2A spike

This records the **initial fixture-only proof**. The subsequent [Kestra integration result](kestra-result.md) proves the bounded native-engine path; its seven-test combined result and `kestra-result.json` are the latest evidence. Limits below describe this initial phase.

Assessment date: 2026-09-22. **Harness/transport fixture proof passed; full S2 engine-integrated acceptance remains open.** No sandbox or container work. No repository files changed.

## Pins and actual runtime

- CPython **3.14.3** on macOS arm64, uv **0.6.9**.
- `strands-agents[a2a] == 1.57.0`; exact transitive dependencies in `uv.lock`.
- Installed **a2a-sdk 0.3.26**; Agent Card advertises **A2A protocol 0.3.0**, JSON-RPC over HTTP.
- Strands 1.57.0 declares `a2a-sdk >=0.3.0,<0.4.0` for this extra. This does **not** prove compatibility with A2A SDK/protocol 1.x.
- pytest 9.0.2; aiosqlite 0.22.1 pinned explicitly. The owned ledger uses Python SQLite; the A2A SDK is not maintaining a second execution ledger.

## Reproduce

From `/tmp/exomachina-spikes/s2`:

```sh
uv sync --frozen
uv run pytest -q --basetemp=.test-state
```

Observed final suite: **6 passed in 11.34s**. Initial failing import recorded in `red-01.log`; the process test then failed before `server.py` existed, and the child correlation test failed before that Interface was implemented. These were repaired through the actual implementation, without mocking owned Modules.

The two real subprocess probes record their protocol evidence in:

- `.test-state/test_real_a2a_process_kill_res0/evidence.json`
- `.test-state/test_parent_child_over_a2a_wit0/evidence.json`

Run either configured role with the same entry point and installed environment:

```sh
uv run python server.py --state demo-capability --role capability --port 28092
uv run python server.py --state demo-factory --role factory --port 28093
```

Use different terminal sessions/ports and state paths. These foreground commands stop with Ctrl-C. `serve_capability.py` starts the bounded shared integration target in the background, records its PID in `shared-capability/server.pid`, and logs only to that S2 directory. Send SIGTERM to that PID to stop it. No S0/S1 database is used.

## What ran and passed

| Boundary | Actual proof |
| --- | --- |
| Same harness, two roles | Same Python code, environment, Strands `Agent`, plugin and tool Interface run an ordinary capability and a factory Director. Only role/state/port configuration changes. This is not a packaged installer or built wheel proof. |
| Stable owned state | Each instance has its own durable identity, organization, role/config file, SQLite ledger, workspace directory, pending runs and accepted artifacts. Authoritative artifacts are stored in the ledger; workspace filesystem recovery was not stress-tested. |
| Real Strands execution | A custom deterministic **model fixture** emits tool calls through the real SDK event loop. The actual plugin calls the owned durable Module. Every invocation deliberately starts a fresh conversation. |
| Real A2A transport | Official A2A SDK `DefaultRequestHandler`, JSON-RPC HTTP app, Agent Card, `message/send`, `tasks/get` and `tasks/cancel` run in real local server processes. |
| Restart | SIGKILL the waiting factory server, restart the same state path and port, retrieve the old A2A task, observe the same logical run still waiting, then complete it. The ordinary capability's identity, incarnation and task remained unchanged. |
| Ownership fencing | Startup atomically increments durable incarnation. A request carrying the previous incarnation is rejected. A still-running predecessor process is rejected after a successor takes the same state path. Checks and owned mutations share a SQLite `BEGIN IMMEDIATE` transaction. |
| Authorization | Unauthenticated HTTP request gets 401; observer credential can read but a Director mutation is rejected. Actor is bound by middleware and plugin construction, not accepted from the model's command body. Credentials are explicit local test fixtures. |
| Duplicate assignment | 24 concurrent in-process submissions (12 threads) and 16 concurrent HTTP submissions (8 threads) produce one logical assignment; reuse of an idempotency key with changed input is rejected. |
| Completion contract | Factory chat/tool completion leaves A2A task `input-required` with no accepted artifact. A Director continuation plus the evaluator fixture produces `completed` and a structured accepted artifact. All delivery aliases project that same final state. |
| Nested factory | A parent calls another instance of the same factory harness over real A2A HTTP. Durable child identity/correlation survives parent SIGKILL and restart. Repeated child-start reuses one logical child. Parent retrieves its accepted output. A separate pending parent/child pair is canceled and both terminal states are retrieved. Premature parent acceptance while child is pending is rejected. |

## Why a reusable adapter is present

The stock Strands A2A server is an agent-conversation adapter. Its conversation completion is not sufficient evidence that an outstanding factory has completed. This spike keeps the real Strands tool loop and uses a small reusable A2A `AgentExecutor`/`TaskStore` Adapter. A2A task state and artifacts are projected from the authoritative owned ledger; chat state cannot overwrite them.

The ordinary role and Director role use that same Adapter. New A2A deliveries may have different protocol task IDs; durable aliases map them to **one logical assignment**. Logical run identity is exposed as `metadata.run_id`. Reconnect through `tasks/get` uses a previously returned protocol task ID. This proves no duplicate logical assignment, not one protocol task ID across independent new `message/send` calls.

## Explicit fixtures and unresolved obligations

1. **No selected engine integration in this suite.** `Harness` contains a fixed one-checkpoint engine fixture, not a general scheduler or definition language. Conductor S0 was blocked; Kestra is being investigated separately. This does not prove native graph publication, bundled engine restart, engine/ledger reconciliation, or real model autonomy. A Kestra HTTP call to the supplied capability endpoint would establish one transport path only.
2. **Acceptance is an evaluator fixture.** It returns a deterministic digest/result; no real independent reviewer, agent quality, repair loop or judgment was evaluated.
3. **Local ownership only.** SQLite transaction fencing works on this host/state path. No remote multi-host ownership, lease expiry, host-loss recovery, filesystem attacks, malicious code isolation, sandbox security, resource quotas or production authentication was tested.
4. **Crash windows are bounded.** Assignment, dedup record and initial A2A delivery alias commit together. The tests kill a settled waiting process. They do not inject a crash at every transaction boundary or lost HTTP response; child preparation is persisted before dispatch but a production outbox/reconciler and external side-effect idempotency remain obligations.
5. **Child behavior is basic.** Durable correlation, duplicate reuse, explicit polling, result retrieval and observed cancellation work. Deadlines, budgets, cancellation during an in-flight remote action, concurrent parent completion/cancel races, and remote incarnation fencing are untested. A predecessor's already-authorized remote request cannot be recalled by this local check. No background recovery owner or second scheduler was added.
6. **Definition/version claim is narrow.** The fixture stores a fixed `fixture.factory@1` or `fixture.capability@1` identity in each run/artifact. Updating definitions, compatible migration, publication authorization, and immutable native engine snapshots are not tested.
7. **A2A subset only.** No streaming, push notifications, OAuth, SSE reconnect, binary artifact transfer or protocol 1.x interoperability proof. Test credentials authorize the entire single-organization fixture; they are not per-factory production authorization.

## Integration target

Live target requested by parent: `http://127.0.0.1:28092/`.

Agent Card: `http://127.0.0.1:28092/.well-known/agent-card.json`.

HTTP headers: `Content-Type: application/json`, `Authorization: Bearer director-test-token` (public fixture value).

`rpc-request.json` is a reproducible JSON-RPC request. Expected response: `result.kind = task`, `result.status.state = completed`, `result.metadata.run_id` is the stable assignment, `result.artifacts[0].parts[0].data` contains the fixture result, digest, reviewer and definition. The actual sample response is recorded as `rpc-response.json` when the live target probe runs.

```sh
curl --fail-with-body http://127.0.0.1:28092/ \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer director-test-token' \
  --data-binary @rpc-request.json
```

## Primary references and implementation evidence

- Strands A2A documentation: https://strandsagents.com/docs/user-guide/sdk/multi-agent/agent-to-agent/
- Strands plugin documentation: https://strandsagents.com/docs/user-guide/sdk/plugins/
- Pinned distribution: https://pypi.org/project/strands-agents/1.57.0/
- Installed source inspected: `.venv/lib/python3.14/site-packages/strands/multiagent/a2a/server.py`, `strands/models/model.py`, `a2a/server/request_handlers/default_request_handler.py`, `a2a/server/tasks/task_store.py`, `a2a/server/tasks/task_manager.py`.
- Executed owned Modules: `harness.py`, `server.py`, `child_adapter.py`; public-boundary tests: `test_harness.py`, `test_a2a_process.py`.

Result: **enough evidence to continue with the customized Strands harness shape; insufficient evidence to close S2's real durable-engine integration or overall product decision.**
