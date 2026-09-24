# Spike A: one independent delayed A2A agent (highest priority)

Read `briefs/qual-common.md` first. Worktree: `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina-qual-a` (branch `qual/spike-a`). Two workers share it under strict file ownership:
- **A1 (tw_version)** builds the black-box agent.
- **A2 (tw_quality)** changes the factory side and runs the scenario.

## Problem

Today, `src/long_client.py` requires `message/send` to return a `completed` Task and reconciles through the fixture-only `GET /fixture/actions/{id}`. `services/testbed.py` hand-builds bindings, and the binding URL is frozen in the package. A real external agent may:
- answer with a `working` Task and finish later;
- move to another endpoint during a run;
- commit an action while its HTTP response is lost.

## Wire contract between A1 and A2 (fixed; changes go through the orchestrator)

**Agent.** A separate process, `services/delayed_agent.py`, that imports nothing from `src/` and uses its own a2a-sdk server. It holds durable state in `--state DIR` (SQLite, FULL sync) and serves A2A 0.3.0 at `--port`, with fixture bearer auth `Bearer fixture-token` and unauthenticated `/health` and `/.well-known/agent-card.json`.

**Agent Card.**
- One skill: `counter_evidence@1`.
- `capabilities.extensions` holds exactly one `AgentExtension`, with `uri: "urn:exomachina:a2a-action-contract:v1"`, `required: true`, and `params: {identity, contract: "action-idempotent-async@1", contract_digest}`.
- `identity` is a durable UUID created on first start and kept across restarts and port changes.
- `contract_digest` is sha256 of the canonical JSON of the contract document served at `GET /contract`.
- The card's `url` reflects the current port. **The card digest used for pinning is sha256 of canonical card JSON with `url` removed.**

**`action-idempotent-async@1`.**
- Request: `message/send` with one DataPart `{op:"assign", action_id, run_id, definition_digest, brief}` and `configuration.blocking=false`.
- Response: a Task in state `submitted` or `working`, with `metadata {action_id, run_id, definition_digest, agent_identity}`. The agent commits the action and task mapping durably **before** responding.
- Completion: the Task becomes `completed` after `--delay-seconds` (default 15), measured from first acceptance and stored durably, so it survives restart. On completion it carries exactly one artifact whose DataPart is `{revision:"r2", sha256, author: identity, content, action_id, run_id, definition_digest}`, where `sha256 = sha256(content)`. Content must be derivable from the brief only, deterministically.
- Idempotency: a repeat `message/send` with the same `action_id` and identical canonical payload fingerprint returns **the original Task id**, with no new effect. A different payload under the same `action_id` gets a JSON-RPC error.
- `tasks/get` returns the current Task.
- An effect means a first acceptance. The effect count is observable **only** on test endpoint `GET /_test/effects` → `{identity, effects:{action_id: count}, total}`. Product code must never call `/_test/*`.

**Test-only fault controls.** Startup flags or `POST /_test/faults`, never reachable from product code:
- `drop_response_once_for=<action_id>`: commit, then close the connection or kill the process before the response.
- `mismatch_artifact_for=<action_id>`: the completed artifact names a different `run_id`.
- `--identity-file` override: lets a test start an impostor with a different identity on the old or new port.

## A1 (tw_version): files you own

`services/delayed_agent.py`, `tests/test_delayed_agent.py`, `evidence/spike-a/agent-*.json`, and `handoff/spike-a-agent.md`.

Unit and in-process tests must cover all of the following:
- card and extension shape;
- a `working` response on the first send;
- completion after the delay;
- idempotent resend returns the same Task and effect count stays 1;
- a conflicting payload is rejected;
- durable completion across a process restart, including a port change with the same identity;
- `drop_response_once_for` commits the action exactly once.

Also run a smoke on a real process pair using ports 45410–45419. Finish A1 first, because A2 integrates against it. Tell the orchestrator as soon as the agent works; A2 will pick it up.

## A2 (tw_quality): files you own

`src/long_client.py`, `src/adapter.py`, `src/a2a_outcome.py`, `src/factory.py` (binding pass-through only), new `src/agent_binding.py`, `services/testbed.py` (additive: optional delayed agent plus snapshot), new `definitions/spike-a-template.json`, new `scenarios/spike_a_delayed.py`, `tests/test_agent_binding.py`, `tests/test_long_client_async.py`, `evidence/spike-a/*` except `agent-*`, and `handoff/spike-a.md`. Do not edit `src/harness.py` or `src/runner.py`.

Required design, within the lean scope:
1. **Static binding snapshot, not a directory.** `$H/testbed/agent_snapshot.json` is `{snapshot_version, agents:{identity:{url}}}`. It is read at call time. The package binding keeps `url` as the URL at publication, and the pinned closure contract for the delayed binding adds `{card_sha256, a2a_extension:{uri, contract, contract_digest}, reconcile:"a2a-idempotent-resend"}`. Existing fixture services keep `reconcile:"fixture-lookup"`, and their behaviour must not change.
2. **Before any send or resend**, the Activity resolves the pinned identity to its current URL through the snapshot. It fetches the card there and requires the url-less card digest, the extension identity and the `contract_digest` to equal the pin. It never follows the binding name to a different identity. On mismatch or absence it produces an explicit `incident`, with no send.
3. **Async Task.** Journal the remote `task_id` durably as soon as the `working` Task is returned. Then poll `tasks/get` until the Task is terminal, with heartbeats or bounded durable polling that survives worker or runner restart. The artifact must match `action_id`, `run_id`, `definition_digest`, the pinned author identity and the content digest. Otherwise the result is an incident, never acceptance.
4. **Lost response.** With no journaled `task_id` after an uncertain send, reconciliation is allowed only if the pinned contract declares `a2a-idempotent-resend`. It re-sends the identical payload and requires the same action binding on the returned Task. An `opaque` or undeclared contract becomes an incident.

## Pre-registered falsifiable checks

Each check runs through the real harness A2A → lazy runner → Temporal → delayed agent. Use a spike template that binds `counter_beta` to the delayed agent; the other services are the fixture testbed.

| ID | Check | Pass | Fail |
| --- | --- | --- | --- |
| A-1 | Async working Task | The agent returned `working` first; the run reached `accepted`; one artifact on the original caller Task | The client required `completed`, or the run stalled |
| A-2 | Pinned card, contract and identity | The Activity verified the card digest, extension identity and contract digest; evidence shows the pin and the observed values | Any send happened without verification |
| A-3a | Mapping moved mid-run | While the action is `working`, the agent restarts on a new port with the same state and identity and the snapshot is updated. The run follows identity to the new URL and reconciles the original remote Task id, with effect count 1 | Duplicate effect, a new Task, or a stall |
| A-3b | Mapping swapped to an impostor mid-run | The snapshot maps the pinned identity to a URL serving a different identity. The result is an explicit incident, with 0 impostor effects and no accepted artifact | The impostor is contacted for an effect, or the run is accepted |
| A-4 | Lost response plus restart | `drop_response_once_for` on the action; the worker or runner **and** the agent are restarted while unresolved. The original action is reconciled to the same remote Task id with effect count 1, or an explicit incident is produced | Effect count 2, or a silent success on an unverified Task |
| A-5 | Mismatched artifact | `mismatch_artifact_for` → an explicit incident, the release effect count is 0, and no acceptance | Acceptance, or a release |

Across all checks, the invariant is:
- sum of effects per `action_id` ≤ 1;
- every accepted artifact's binding equals its pin.

Record per check: `/_test/effects` before and after, remote Task ids, the journal rows (`outcomes.sqlite3`), the parent/child workflow status, the harness Task state, and the incident payload where relevant.

Report the architectural implications in the handoff. Answer:
- What must a real external agent contract declare?
- Which reconcile modes are safe?
- What do contract, identity or directory changes do to pinned runs?
