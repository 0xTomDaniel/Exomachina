# Qualification spikes A–C after the merged prototype

23 September 2026 · branch `qual/factory-spikes` (local only, from `origin/main` 374b27a) · macOS arm64, Python 3.12.9, temporalio 1.33.0, a2a-sdk 0.3.26, strands-agents 1.57.0, pi-ai 0.87.1.

The prototype README ends with three technical gaps:
- the external A2A node assumption was fixture-only;
- a second harness instance on one runner was designed but never run;
- a broker-backed Director was not built.

These three spikes test those gaps with falsifiable checks, which were committed before any code change (`f3244f8`, `briefs/spike-{a,b,c}.md`). The architecture is unchanged:
- the Strands harness in factory mode sits behind its normal A2A identity;
- one shared, lazy Temporal/PostgreSQL runner serves each install home;
- external agents are black boxes.

There is no directory, no separate factory endpoint, and no new tenancy, sandbox, memory ceiling or installer. Commercial subscription entitlement is an external release gate, not a technical spike.

Labels follow the README:
- **observed-real** means real processes (harness, A2A, runner, Temporal and, where stated, the live broker);
- **observed-synthetic** means real code against a mock;
- **unit-tested** means an isolated test.

A passing fixture is never counted as live proof.

## Verdict

**All three spikes pass their pre-registered checks, on each spike branch and again on the merged tree** (final combined run below). Each spike also exposed one real defect that the checks or the review caught before integration:

| Spike | Result | What changed architecturally |
| --- | --- | --- |
| A: independent delayed A2A agent | **Pass** A-1..A-5, observed-real (fixture Director) | External nodes are now pinned by **identity + url-less Agent Card digest + served contract digest**, not by URL. A static snapshot resolves the URL at call time. The remote Task id is journaled and polled. A lost response is reconciled by an idempotent resend **only** when the pinned contract document declares it; otherwise it is an incident. |
| B: two instances in one home | **Pass** B-1..B-6, observed-real (fixture Director); **B-6 failed on the unmodified code** | Runner ports belong to the **install home** (`$H/runner-config.json`), not to `instance.json`. Provisioning rejects port collisions. A per-instance process lock is taken before the Director claims an incarnation. |
| C: broker-backed Director | **Pass** C-1..C-5, **live** `gpt-6-sol` through the signed-in broker, plus synthetic | A text brief on the normal A2A endpoint reaches a Strands Director with **semantic tools only**. Code owns the action ids, principal, Task/run binding and Temporal authority. Inspect/abort now also require the run owner and the original context. |

## Spike A: one independent delayed agent

**Question.** Can the factory rely on a real-shaped external agent that answers `working`, finishes later, may move, and may lose its response?

**Built.**
- A1: `services/delayed_agent.py`, a separate a2a-sdk server that imports nothing from `src/`. It has a durable identity, a `urn:exomachina:a2a-action-contract:v1` card extension carrying `contract_digest`, the contract `action-idempotent-async@1`, durable delayed completion, and test-only `/_test/*` controls.
- A2: the factory side, in `agent_binding.py`, `long_client.py`, `adapter.py`, `a2a_outcome.py`, `factory.py` and `binding.py`, plus the additive testbed snapshot `$H/testbed/agent_snapshot.json`.

| Check | Verdict | Decisive evidence (`evidence/spike-a/delayed.json`, home `/tmp/exo-qual-a-delayed-20260923-a2-postreview2`) |
| --- | --- | --- |
| A-1 async Task | Pass | First remote state `working`; Task id journaled; the caller Task completed with one artifact |
| A-2 pinned card/contract/identity | Pass | Pinned and observed card sha256, extension identity and contract digest are equal before every send and poll |
| A-3a mapping moved mid-run | Pass | `working` before the move; `tasks/get` polled the **original** remote Task on the new port; effect count 1 |
| A-3b mapping swapped to an impostor | Pass | `pinned-agent-verification-failed` incident; 0 impostor effects before and after; no release |
| A-4 lost response + restart | Pass | Journal `unknown` with **no** Task id; runner, harness and agent restarted; the resend returned the **original** Task id; effect count 1 |
| A-5 mismatched artifact | Pass | `async-artifact-inconsistent` incident; 0 releases; no acceptance |

Global invariants in the evidence:
- every delayed action has at most one effect;
- every accepted delayed artifact matches its pinned identity and its action/run/definition/content binding.

**Independent review** (`handoff/spike-a-review.md`, read-only, by a second worker) found five issues in the first passing version. All were fixed before integration, and A-1..A-5 were re-run on the final code:
1. A timed-out Activity's thread could overwrite a newer journal decision, and the heartbeat never ran. Fix: the heartbeat runs on the event loop, HTTP timeouts are 10 s, and journal transitions are compare-and-set on the prior phase and sequence. Terminal rows are immutable.
2. Publication granted idempotent resend without reading the served contract document. Fix: resend is granted only when the document declares the action-id key, same-payload replay to the original Task, and commit-before-response; otherwise the pin is `opaque`.
3. An incomplete async pin fell back to the legacy URL path. It is now an incident before any send.
4. and 5. The scenario verdicts could pass without their decisive transition. They now require it.

**Preserved failures:** `delayed-attempt-{1..3}.json` (probe port, parent-vs-child action key, cleanup signal) and `delayed-post-review-failure-1.json` (the evidence reader raced journal creation). The pre-review passes are `delayed-attempt-4.json` and `delayed-pre-review.json`; they do not count as post-review proof.

**Implications.** A real external agent contract must declare:
- a durable identity;
- url-less card and contract digests;
- action-id binding;
- whether an identical-payload replay returns the original Task after a durable commit;
- `tasks/get`;
- artifact fields that bind action, run, definition, author and content digest.

`a2a-idempotent-resend` is safe only with those clauses declared and pinned. An opaque contract cannot resolve a lost response and must produce an incident. A URL move for the same identity, card and contract can resume a pinned run. A changed card or contract, or a swapped identity, stops the pinned run before another send.

**Limits.**
- The agent is a local fixture process, the Director is a fixture, and the release receiver is still an HTTP fixture.
- Resend retries every 0.5 s during an outage, with no backoff.
- Async polling is bounded by the Activity retry policy (about 12 × 70 s); a longer remote Task fails the child explicitly. Real long-running agents need workflow-level durable polling.
- The snapshot path is derived from the journal location.
- No trust infrastructure (signed cards, remote auth) was built.

## Spike B: two factory-mode harness instances in one home

**Question.** Can two harness processes, each with its own durable identity, catalog, port and card, share one install home and one lazy runner, without contamination and without either one shutting it down?

| Check | Unmodified | Fixed | Evidence (`evidence/spike-b/`) |
| --- | --- | --- | --- |
| B-1 cold concurrent first requests | Pass | Pass | Exactly one `serve-ready`, one `start` and one `attach`; one runner PID; both Tasks completed with their own receipt |
| B-2 no cross-instance contamination | Pass | Pass | Task ids unknown across instances; journal, releases and `director.sqlite3` partition by identity |
| B-3 one instance down, the other continues | Pass | Pass | Beta completed while alpha was stopped; runner PID unchanged; no stop event |
| B-4 restarted instance reattaches | Pass | Pass | Same identity, incarnation +1; `recover-unfinished` **attached**; abort on the original Task gave `aborted` with 0 releases |
| B-5 hard kill during an active run | Pass | Pass | The run completed while alpha was down; projected on the original Task after restart |
| B-6 same-home port/config handling | **Fail** | Pass | Unmodified code accepted alpha's harness port for a third instance and a divergent runner port config. A duplicate process for a live instance raised the stored incarnation, fencing the live process. Fixed: all are rejected, and the peer is undisturbed |

The first unmodified run (`before.json`) had a checker bug: it compared release receipts to the parent instead of the child run. It is preserved. The unmodified code was re-run as `before-corrected.json`, which is the baseline.

**Implications.**
- Runner configuration is install-scoped.
- An instance needs a process lock held for its serving lifetime, acquired **before** the incarnation claim.

**Limits.**
- `create_app()` embedding bypasses that lock.
- Only two instances were tested; no load or stress testing.
- Re-provisioning an instance created before this change, whose `instance.json` still carries runner ports, is refused as "configured differently". Serving such an instance still passes those ports to `Runner`, which seeds or checks the home config.

## Spike C: broker-backed Director

**Question.** Can a real, broker-backed model be the factory Director behind the instance's normal A2A identity, while deterministic code keeps all authority?

**Built.**
- `src/director_agent.py`: the Strands tools `start_research(question, outcome_mode)`, `inspect_run()` and `decide_wait(action, revision, sha256, rationale)`, with a hard per-turn budget of 4 model calls, 4 tool calls and 90 s.
- A text part on A2A routes to this Director. The structured DataPart fixture path stays the default.
- A `fixture-observer` principal authenticates but is authorized for nothing.
- The broker is used only through `model_broker.py`, with its default home. The worker never logged in, and no token was read or printed.

| Check | Live (`evidence/spike-c/live.json`, `/tmp/exo-qual-c-live-3`) | Synthetic | Notes |
| --- | --- | --- | --- |
| C-1 brief → authorized start | Pass | Pass | One accepted `start_research`; Task observed `working` → `input-required` at the Director wait after two repairs |
| C-2 one wait decision on the original Task | Pass | Pass | `inspect_run`, then exactly one accepted `decide_wait(abort, r3, <current sha256>)`; Task `completed`/`aborted`; 0 releases |
| C-3 invalid inputs rejected by code | Pass | Pass | The live model **did** request `outcome_mode: "forbidden"`; `authorize_run_inputs` rejected it. For "choose graph v99" and unknown-input briefs, the model folded the request into the question; those runs used the active publication and declared inputs only. Extra fields are rejected structurally, proven **fixture-injected** |
| C-4 stale/unauthorized rejected | Pass | Pass | Stale r2 digest, the `fixture-observer` principal (tool path and a real A2A bearer) and an unbound Task were all rejected. These probes are **fixture-injected**, not spontaneous model behaviour |
| C-5 budget and credential hygiene | Pass | Pass | Every turn within budget; `gpt-6-sol`, account `sha256:188b022d6e97` (from `status` only); leak scan 0 real-store hits over 2,029 files, positive control detected |

**Live attempts, all preserved:**
1. `live-first-failure.json`: C-1 **failed** on an observation race. The model's start was accepted and the run reached its wait, but the A2A send's reply came back only after the Task was already `input-required`, so `working` was never observed. The scenario now polls the Task concurrently with the send.
2. `live-2-with-regex-guard.json`: passed, but **superseded**. Orchestrator review rejected a regex heuristic over the caller's brief. It was scenario-shaped, it performed two of the C-3 rejections, and it would false-reject ordinary questions. It was removed.
3. `live.json`: the final result above.

The synthetic first failure (`synthetic-first-failure.json`, turn-object serialization) and the pre-removal synthetic pass are also kept.

**Implications.**
- The model chooses semantic operations and supplies the question, mode, revision and rationale. Code owns everything that carries authority.
- A model request cannot bypass input authorization, fencing, the original-Task binding, owner checks or stale-revision checks.
- The Director token already travels in Workflow history. That matters more now that a model can trigger its use, although the token is never a tool argument or result.

**Limits.**
- One account, one model, one decision type (abort).
- The C-1 brief wrote `outcome_mode: never` literally instead of in prose.
- The abort was directed by the caller's follow-up, not decided autonomously.
- Quality, capabilities and release remain fixtures.
- `message/send` blocks for the whole Director turn.
- Model reliability across broader briefs is unmeasured.

## Final combined run on the merged tree

All of these ran on merge `8189412` (A, B and C together), each on a fresh home in its own port block, with A, B and C running concurrently. Worker scenarios write to the per-spike files above, so each combined result was copied to a `final-combined*` name. The committed per-spike originals were then restored unchanged; each copy was byte-compared before the restore.

| Run | Result | Label | Evidence |
| --- | --- | --- | --- |
| Unit suite (under lock) | 112 tests OK | unit-tested | `evidence/qualification-final-unit.txt` |
| Spike A scenario, `/tmp/exo-qual-a-combined-1` | A-1..A-5 pass, with every strengthened condition true; both global invariants true | observed-real, fixture Director | `evidence/spike-a/final-combined.json` |
| Spike B scenario, `/tmp/exo-qual-b-combined-1` | B-1..B-6 pass | observed-real, fixture Director | `evidence/spike-b/final-combined.json` |
| Spike C synthetic, `/tmp/exo-qual-c-combined-syn-1` | C-1..C-5 pass | observed-synthetic | `evidence/spike-c/final-combined-synthetic.json` |
| Spike C **live**, `/tmp/exo-qual-c-combined-live-1` | C-1..C-5 pass | observed-real, **live** `gpt-6-sol`, account `sha256:188b022d6e97` | `evidence/spike-c/final-combined-live.json` |

The combined live run in detail:
- 5 text turns, **11 model calls and 6 tool calls** in total. The heaviest turn used 3 model calls and 2 tool calls, against a limit of 4/4/90 s.
- The Task was observed `working` → `input-required`, followed by exactly one accepted abort and 0 releases.
- The live model again requested `outcome_mode: "forbidden"`, and code rejected it.
- Its in-run leak scan covered 2,075 files with **0 real-store hits**, and the positive control was detected (`checks.C-5.leak_scan`).

An intermediate B+C merge re-run (`evidence/spike-b/merged-bc.json` and `evidence/spike-c/merged-bc-synthetic.json`, before A merged) also passed.

**Leak-scan copy correction.** The C worker's first copy of `final-combined-leak-scan.json` duplicated the earlier sidecar summary from `/tmp/exo-qual-c-live-3`. It was stale, and byte-identical to the committed `final-leak-scan.json`. It was replaced by a fresh sidecar scan, which the orchestrator ran immediately before commit through the broker's own `leak-scan` interface. That scan covered the final commit-candidate set of this branch, the combined trial homes and the model home logs, and ran a new synthetic positive control (`/tmp/exo-qual-final-sidecar-leak-control`). The result was **0 real-store hits across 7,825 files**, including all 164 commit-candidate files. The control detected both planted synthetic copies, with 9 hits. The combined live run's own in-run scan above is kept as it was.

## Cross-spike notes

- The existing unit suite hard-codes ports (the 46140 broker mock, harness 45300, testbed 45200/45302). Concurrent runs across worktrees collided and produced spurious failures in `test_authoring_budget` and `test_harness_modes`. All counts in this document were taken under `/usr/bin/lockf -k /tmp/exo-qual-suite.lock`. The suite should move to allocated ports.
- Merged unit suite: **112 tests OK** (90 baseline + 15 A + 2 B + 5 C) under the lock, `evidence/qualification-final-unit.txt`.
- Worker orchestration used the matching Herdr 0.8.2 client (`/opt/homebrew/Cellar/herdr/0.8.2/bin/herdr`), because the installed 0.9.0 client refuses the running 0.8.2 server.

## Remaining gaps after qualification

- External agents: no signed or attested cards, no remote authentication, no general directory. Workflow-level polling for long remote Tasks is not built.
- The Director: live proof covers one account, one model and abort only. The Director token in history is unchanged.
- Topology: two instances only; embedded `create_app()` has no instance lock; no stress testing.
- Still fixtures: Quality, capability content and the HTTP release receiver.
- Carried from the README: old-build retirement, contract attestation, operational hardening, and the memory ceiling are out of scope and unbuilt.
