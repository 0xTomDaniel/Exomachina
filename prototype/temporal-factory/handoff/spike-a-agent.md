# Spike A1: delayed A2A agent handoff

## Result and owned files

The independent agent is ready for A2. It imports no `src/` module and uses the a2a-sdk A2A 0.3.0 FastAPI server. SQLite records the identity, action to Task mapping, first acceptance time, completion deadline, and faults with `synchronous=FULL`. The first `message/send` returns `working` after the action is committed; `tasks/get` returns the original Task, and completion is derived from the stored deadline.

Files changed: `services/delayed_agent.py`, `tests/test_delayed_agent.py`, `evidence/spike-a/agent-smoke.json`, and this handoff. No commit was made. A2 owns the other concurrent changes in this worktree.

## Exact CLI for A2

Run from `prototype/temporal-factory/`:

```sh
PY=/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python
STATE=/tmp/exo-qual-a-your-trial/services/delayed-counter
$PY -B services/delayed_agent.py --state "$STATE" --port 45410 --delay-seconds 15
```

For a mapped port move, stop that process and start the same state on another port. The Agent Card `url` changes, while the identity and url-less canonical card SHA-256 remain the same:

```sh
$PY -B services/delayed_agent.py --state "$STATE" --port 45411 --delay-seconds 15
```

The startup fault flags are `--drop-response-once-for ACTION_ID` and `--mismatch-artifact-for ACTION_ID`. They can be combined. Configure before first acceptance of the named action. Equivalently, authenticated `POST /_test/faults` accepts `{"drop_response_once_for":"ACTION_ID"}` or `{"mismatch_artifact_for":"ACTION_ID"}`. A test impostor should use a **different** `--state` directory and `--identity-file /tmp/exo-qual-a-your-trial/impostor-id`; the file is created as a UUID on first start and reused. Product code must not call `/_test/*`.

The Agent Card and `/health` are unauthenticated. `/contract`, A2A RPC, and `/_test/*` require `Authorization: Bearer fixture-token`. These calls show the exact request shape (replace the example IDs with A2's action values):

```sh
curl -s http://127.0.0.1:45410/.well-known/agent-card.json
curl -s -H 'Authorization: Bearer fixture-token' http://127.0.0.1:45410/contract
curl -s -H 'Authorization: Bearer fixture-token' -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":"send-1","method":"message/send","params":{"message":{"role":"user","messageId":"message-1","parts":[{"kind":"data","data":{"op":"assign","action_id":"action-1","run_id":"run-1","definition_digest":"digest-1","brief":"counter brief"}}]},"configuration":{"blocking":false}}}' \
  http://127.0.0.1:45410/
curl -s -H 'Authorization: Bearer fixture-token' -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":"get-1","method":"tasks/get","params":{"id":"TASK_ID"}}' \
  http://127.0.0.1:45410/
curl -s -H 'Authorization: Bearer fixture-token' http://127.0.0.1:45410/_test/effects
```

The `message/send` DataPart must contain exactly `op`, `action_id`, `run_id`, `definition_digest`, and `brief`; `configuration.blocking` must be false. Resending the identical DataPart with a new RPC/message ID returns the original Task ID and does not add an effect. A changed DataPart under the same `action_id` produces JSON-RPC error `-32602`. The completed artifact content is `fixture-result:` plus the brief, with SHA-256 of those UTF-8 bytes. Its `author` and Task metadata `agent_identity` equal the card extension identity. The mismatch fault changes only the completed artifact's `run_id` to the requested run ID plus `-mismatch`.

The one Agent Card extension has URI `urn:exomachina:a2a-action-contract:v1`, `required: true`, and params `identity`, `contract: action-idempotent-async@1`, and `contract_digest`. The observed contract digest is `09c95ad2d3a5196a8b24f26002979418b6bc4dc75d6c26b06d6cab2a0229f5be`; compute it from canonical JSON at `/contract` rather than hardcoding it. The url-less card digest is SHA-256 of sorted, compact UTF-8 JSON after removing the top-level `url` key. This smoke observed `059d58dfb9cf9a080265133af1b9863b487ad24b27cf7efbca810f4afb6ed08f` on both ports.

## Checks and evidence

| Check | Verdict | Evidence |
| --- | --- | --- |
| Card, extension, contract digest, bearer rules | Pass, `unit-tested` and `observed-real` | `tests/test_delayed_agent.py`; `evidence/spike-a/agent-smoke.json` |
| First send `working`, completion after delay, one artifact | Pass, `unit-tested` and `observed-real` | Same |
| Identical resend same Task and effect count one; conflicting payload error | Pass, `unit-tested` and `observed-real` | Same |
| Completion survives restart and port change with same identity and original Task ID | Pass, `unit-tested` and `observed-real` | Same |
| Lost response commits once before process exit and survives restart | Pass, `unit-tested` and `observed-real` | Same |
| Mismatched artifact fault and identity-file override | Pass, `unit-tested` | `tests/test_delayed_agent.py` |

The real-process smoke used ports 45410 and 45411, sent one normal action and one action whose response was dropped, restarted the agent on the new port, and fetched both original Tasks. Both effects remained at one. The recorded first Task state, durable Task IDs, card/contract digests, before/after effect counts, completed artifacts, conflict error, and process exit are in `evidence/spike-a/agent-smoke.json` (`classification: observed-real`, `director_model: fixture`). Trial directory `/tmp/exo-qual-a-agent-smoke-b475779f` remains in place. Both smoke processes were stopped; `lsof -nP -iTCP:45400-45419 -sTCP:LISTEN` showed no listener. The four A1 unit tests also stopped their subprocess and left their `/tmp/exo-qual-a-unit-*` state directories in place.

The pre-registered A-1 through A-5 checks require the real harness → lazy runner → Temporal path and belong to A2's integration run. A1's agent-level checks above pass, but they are not claims that A-1 through A-5 pass.

## Commands and observed output

From the worktree root, before A1 changes:

```sh
/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest discover -s tests
# Ran 90 tests in 10.324s; OK
```

The first focused run after implementation passed four tests. A later focused run caught a test expectation that assumed zero delay would make the first response `completed`; the agent correctly returned `working` (`Ran 4 tests; FAILED (errors=1)`). The test now fetches completion through `tasks/get`.

An interim full suite run while A2's files were changing, before the prescribed offline broker install, reported `Ran 97 tests; FAILED (failures=2, errors=2)` in broker-budget and harness-mode tests. No A1 test failed. The failure remains recorded rather than treated as a pass. Then:

```sh
npm --prefix prototype/temporal-factory/broker ci --offline --cache /tmp/exomachina-pi-strands-debate/npm-cache
# added 85 packages; found 0 vulnerabilities; exit 0
/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest discover -s tests
# Ran 100 tests in 15.034s; OK
```

From `prototype/temporal-factory/` after the final A1 change:

```sh
/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest discover -s tests -p test_delayed_agent.py -v
# Ran 4 tests in 3.332s; OK
```

After that focused run, a further full-suite invocation reported `Ran 100 tests in 12.320s; FAILED (failures=4)` in `test_authoring_budget` and `test_harness_modes`; all four A1 tests passed. The earlier 100-test full-suite pass is recorded above. The cause of this intermittent full-suite result was not established, and A1 did not edit those test or product files.

The real-process smoke command was an inline Python driver using the specified Python interpreter and `subprocess.Popen` for the two exact service CLIs above, with `--delay-seconds 0.7 --drop-response-once-for smoke-lost` on port 45410 and `--delay-seconds 100` on port 45411. It exited 0 and printed `verdict: pass`, all seven agent smoke checks true, and `remaining_listeners: []`. Its recorded trial state and requests are in the evidence JSON.

## Contract implications and limits

A real external agent must declare stable identity, the exact contract and digest, an idempotent action key with same-payload replay returning the original Task, durable commitment before response, a way to obtain Task completion, and artifact bindings that the client can verify. `a2a-idempotent-resend` is safe after an uncertain response only when that declared contract is pinned and verified again at the resolved URL. The existing fixture lookup mode is safe only for its participating fixture lookup contract. Opaque or undeclared reconciliation must produce an incident.

The binding snapshot may change a URL for the same identity when its url-less card and contract digests still match. A changed identity, contract, or url-less card should stop a pinned run before any send and produce an incident. A2 must prove those client-side rules in A-1 through A-5. A1 did not run the full harness/Temporal scenario or a live Director model.
