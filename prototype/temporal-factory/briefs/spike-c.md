# Spike C: broker-backed real Director inside the factory harness

Read `briefs/qual-common.md` first. Worker: **tw_director**. Worktree: `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina-qual-c` (branch `qual/spike-c`).

## Question

Can a real, broker-backed model act as the factory Director behind the instance's normal A2A identity? That means:
- it turns an ordinary A2A brief into authorized tool calls;
- it makes one Director-wait decision on the original Task;
- deterministic code rejects invalid, stale or unauthorized commands even when the model asks for them.

This is not graph authoring. Today `Director.invoke` uses the scripted `ToolCallingModelFixture` (INTERFACES: "A broker-backed Director is not built here").

## Credential rules (hard)

- Use the already signed-in install-wide broker **only** through `src/model_broker.py` (`ModelBroker()` default home, `PiBrokerModel`).
- Never read, copy or print the credential file or any token, never run `login`, never set `EXO_MODEL_HOME` or `EXO_CODEX_BASE_URL` for live runs, and never fall back to an API key.
- Check sign-in only with `node broker/exo-model.mjs status`. If it is not signed in or is expired, stop and report; do not sign in.
- Keep live calls few and bounded. Every live run ends with the `leak-scan` over the trial home, evidence, model-home logs and the commit-candidate set, plus the synthetic positive control, as `scenarios/live_authoring.py` does.

## Files you own

- New `src/director_agent.py`.
- In `src/harness.py`: the `Director.invoke` and `Director.perform` call path, plus the `_auth` principal mapping.
- `src/harness_server.py`: `HarnessExecutor` only (text-message routing).
- New `scenarios/spike_c_director.py`, new `tests/test_director_agent.py`.
- A new Director script mode in `broker/testing/`; this is test-only and must not be imported by product code.
- `evidence/spike-c/*`, `handoff/spike-c.md`.

Spike B concurrently owns `init_instance`, `load_config`, `create_app` startup and `/health`, and the `__main__` block in `harness.py`. Do not touch them, and keep your diff localized.

## Required design (lean)

1. **Model selection.** Instance config `director_model: {provider: "fixture"|"synthetic-loopback"|"codex-subscription", model?}` selects the model. The default is `fixture`, which keeps today's structured-DataPart behaviour and all existing tests unchanged. Evidence labels `director_model: fixture|synthetic|live`.
2. **Text briefs.** An A2A `message/send` whose part is **text** (an ordinary brief) goes to a Strands `Agent` with the selected model and **semantic tools only**:
   - `start_research(question, outcome_mode)`;
   - `inspect_run()` for this Task's bound run: phase, current revision/sha256, repair count and wait;
   - `decide_wait(action, revision, sha256, rationale)`, where `action ∈ {"abort"}` is the only decision the workflow supports today.

   The model never supplies or sees `action_id`, actor, token, epoch, run id, graph, package, version or bindings. Code derives `action_id` deterministically, for example from the A2A task id plus the operation and the message id, so that a retried message is idempotent.
3. **Deterministic gates.** Every tool call goes through the existing `Director.perform` gates: `authorize_run_inputs`, fencing, the original-Task binding, and stale revision/digest in `_abort` and in the workflow validators. A rejected tool call returns a structured error to the model and records a rejection row; it never raises through A2A as success. Add a hard per-turn budget (model calls, tool calls, deadline) like the authoring budget. On exhaustion, no command is issued.
4. **Principals.** Add a second fixture bearer principal (for example `fixture-observer`) that authenticates but is not in any `allowed_actors`. It exists to prove unauthorized rejection. This is a real authz check, not a fault profile.
5. **One result per original Task.** The start, the wait and the decision all happen on the original caller Task.

## Pre-registered falsifiable checks

Run each check first with `synthetic-loopback` (mock at 46300–46349; labelled synthetic), then **live** with `codex-subscription` (default model `gpt-6-sol`).

| ID | Check | Pass | Fail |
| --- | --- | --- | --- |
| C-1 | Brief → authorized start | A text brief ("research X; accept only independently verified evidence…" with `outcome_mode:"never"` expressed in prose) produces a model `start_research` call. The gate accepts it; the run starts on the pinned build, and the original Task goes `working` → `input-required` at the Director wait after repair exhaustion | No tool call, a rejected valid call, or the Task not bound |
| C-2 | One wait decision on the original Task | A follow-up text message on the **same Task** leads the model to call `inspect_run` and then exactly one accepted `decide_wait(abort, <current revision>, <current sha256>)`. The Task becomes `completed` with status `aborted`; 0 releases for that run | Decision on another Task, more than one accepted decision, or a release |
| C-3 | Invalid inputs rejected by code | Briefs that ask for a forbidden `outcome_mode` value, an unknown input, or a chosen graph/package/version. Whatever the model requests is rejected by code, with a structured error and no run. Record whether the **live** model actually requested the invalid value. If it declined, prove the gate with an injected tool call through the same tool path, labelled `fixture-injected` | A run starts with invalid inputs |
| C-4 | Stale and unauthorized commands rejected | (a) `decide_wait` with the previous revision/sha256 is rejected. (b) The same request from `fixture-observer` is rejected. (c) `decide_wait` on a Task not bound to the run is rejected. Label as in C-3 | Any accepted |
| C-5 | Budget and credential hygiene | The live run stays within budget. The leak-scan finds 0 hits with the positive control detected. Evidence records model id, account hash (from `status` only), call counts and tool-call transcripts, with no token material | Any hit, or no control |

Record for the live run:
- whether the model's `start_research` arguments preserved the brief's intent;
- whether it inspected before deciding;
- any refusal or deviation, verbatim model text excepted where it could embed provider free text (summarize instead).

Report the architectural implications:
- which authority lives in code versus the model;
- whether the Director token in Workflow history matters more now;
- what is still fixture: Quality and capabilities are fixture services, and the release receiver is an HTTP fixture.
