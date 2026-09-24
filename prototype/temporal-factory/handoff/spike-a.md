# Spike A2 factory-side handoff

## Verdict and evidence

The final run is `observed-real` for the harness A2A, lazy PostgreSQL/Temporal runner, worker, independent delayed A2A process, and HTTP release fixture. The Director model is `fixture`. Final evidence: `evidence/spike-a/delayed.json`, trial home `/tmp/exo-qual-a-delayed-20260923-a2-postreview2`. The pre-review passing run is preserved as `evidence/spike-a/delayed-pre-review.json`; it is not used as post-review proof.

| Check | Verdict | Final evidence |
| --- | --- | --- |
| A-1 async working Task | **Pass** | `delayed.json` → `checks.A-1`: first remote state `working`, original remote Task ID journaled and completed, caller Task `completed` with one artifact. |
| A-2 card, contract, identity pin | **Pass** | `checks.A-2`: pin and observed url-less card SHA-256, extension identity and contract digest match before send. |
| A-3a mapping moved | **Pass** | `checks.A-3a`: journal `working` before stop; `tasks/get` polls the original Task ID on port 45406; effect count one. |
| A-3b impostor mapping | **Pass** | `checks.A-3b`: `pinned-agent-verification-failed` incident, impostor effects zero before and after, caller Task failed, no release. |
| A-4 lost response and restart | **Pass** | `checks.A-4`: first journal row `unknown` with no Task ID; runner, harness and agent restarted; post-restart resend returned the original Task ID; effect count one. |
| A-5 mismatched artifact | **Pass** | `checks.A-5`: `async-artifact-inconsistent` incident, caller Task failed, zero release rows and no acceptance. |

Every check records the delayed agent's test-only effect counts before and after, the remote Task ID, full `outcomes.sqlite3` row, parent and child workflow status, harness Task, and incident where applicable. The final `global_invariants` are both true: every delayed action has at most one effect, and every accepted delayed artifact matches its pinned identity and action/run/definition/content bindings. `remaining_listeners` is empty.

## Factory-side changes

- `src/long_client.py`: async `message/send` with `blocking=false`, Task and artifact validation, ten-second HTTP timeout. Card resolution is imported from the single `agent_binding` implementation.
- `src/agent_binding.py`: reads the static identity snapshot at call time; verifies the URL-less card digest, extension identity, and digest of the served contract. Publication grants `a2a-idempotent-resend` only when the same contract document declares action-ID keying, same-payload replay to the original Task, and commitment before response; otherwise it pins `opaque`.
- `src/a2a_outcome.py`: durable `working` Task ID, payload fingerprint and pinned identity, plus phase/sequence compare-and-set. A stale Activity cannot overwrite a confirmed or incident row.
- `src/adapter.py`: mode-based dispatch, pin verification before each send/resend/poll, exact-payload resend after an uncertain response, terminal artifact checks, and explicit incidents. The Activity heartbeats from the event loop while its synchronous I/O runs in a thread.
- `src/factory.py`: passes the pinned binding and closure contract to assign; assign Activities have a 15-second heartbeat timeout.
- `src/binding.py`: includes `agent_binding.py` in the immutable worker source digest and build snapshot, required after deduplicating card resolution.
- `services/testbed.py`: optional delayed `counter_beta`, pinned contract and `$H/testbed/agent_snapshot.json`; ordinary fixture mode retains its existing metadata and lookup behavior.
- `definitions/spike-a-template.json`, `scenarios/spike_a_delayed.py`, `tests/test_agent_binding.py`, `tests/test_long_client_async.py`: publication template, pre-registered real-process driver, and unit tests with an in-test A2A wire stub.

A1 independently owns `services/delayed_agent.py`, `tests/test_delayed_agent.py`, `evidence/spike-a/agent-*`, and `handoff/spike-a-agent.md`; A2 did not edit them. No commit, push, merge, rebase, or branch switch was made.

## Review response

- **HIGH-1:** assign now has event-loop heartbeats and a 15-second heartbeat timeout; each A2A HTTP call has a 10-second timeout. Journal transitions require the persisted prior phase and sequence and use an exact-row compare-and-set. `test_stale_attempt_cannot_overwrite_incident` and `test_heartbeat_runs_on_activity_event_loop` pass. The final worker log has no `coroutine ... was never awaited` warning.
- **HIGH-2:** `pin` parses the served contract document used for its digest. A self-consistent opaque contract pins `opaque`; its lost response causes an incident with one send. Both cases have unit tests.
- **MED-3:** async and opaque modes require the complete pin before any send; an incomplete pin returns `async-pin-incomplete` and never invokes the legacy sender. Unit tested.
- **MED-4/5:** scenario verdicts require A-4's unknown/no-ID and original-ID resend, A-3a's working state and new-URL Task poll, and A-3b's exact pin incident.
- **Deduplication:** `agent_binding` is the only card/snapshot verifier; `long_client` imports its resolver. The worker build includes it.

## Commands and failures retained

Run from `prototype/temporal-factory/` with Python `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python`:

| Command | Observed output |
| --- | --- |
| `python -B -m unittest discover -s tests` before offline broker install | `Ran 88 tests; FAILED (errors=1)`: broker budget setup lacked installed pi-ai. |
| `npm --prefix prototype/temporal-factory/broker ci --offline --cache /tmp/exomachina-pi-strands-debate/npm-cache` from worktree root | Exit 0; added 85 packages, 0 vulnerabilities. |
| `python -B -m unittest discover -s tests` after install, before A2 changes | `Ran 90 tests; OK`. |
| `python -B -m unittest tests/test_agent_binding.py tests/test_long_client_async.py tests/test_binding.py` after deduplication | `Ran 16 tests; OK`. |
| `/usr/bin/lockf -k /tmp/exo-qual-suite.lock python -B -m unittest discover -s tests` on final code (substitute the absolute Python above) | `Ran 105 tests in 18.426s; OK` on the post-review code before dedup, then `Ran 105 tests in 18.779s; OK` on the final packaged code. |
| `python -B scenarios/spike_a_delayed.py --home /tmp/exo-qual-a-delayed-20260923-a2-postreview2` | Exit 0; `A-1`, `A-2`, `A-3a`, `A-3b`, `A-4`, `A-5`: all `pass`. |
| `git diff --check` | Exit 0. |

Initial scenario attempts were recorded before fixing their failures:

| Trial home | Failure/evidence | Correction |
| --- | --- | --- |
| `/tmp/exo-qual-a-delayed-20260923-a2` | `delayed-attempt-1.json`: test probe used 45404 for `counter_beta`, causing HTTP 404 before a run. | Corrected to testbed port 45403. |
| `/tmp/exo-qual-a-delayed-20260923-a2b` | `delayed-attempt-2.json`: collector looked up parent-run assignment ID; timed out although the actual child run accepted. | Used child workflow ID in action keys and faults. |
| `/tmp/exo-qual-a-delayed-20260923-a2c` | `delayed-attempt-3.json`: cleanup signaled an agent that had exited after the drop fault; `PermissionError`. A-1 through A-3b had passed. | Check the port before signaling. |
| `/tmp/exo-qual-a-delayed-20260923-a2-final` | `delayed-post-review-failure-1.json`: evidence reader raced initial journal creation (`no such table: outcomes`), before any check verdict. | Reader treats only that missing-table state as not yet initialized and waits. |

Other preserved runs: `/tmp/exo-qual-a-delayed-20260923-a2d` (`delayed-attempt-4.json`, pre-review pass), `/tmp/exo-qual-a-delayed-20260923-a2e` (`delayed-pre-review.json`, pre-review pass), and `/tmp/exo-qual-a-delayed-20260923-a2-review` (post-review pass before final dedup). Unit test state directories `/tmp/exo-qual-a-unit-*` and `/tmp/exo-qual-a-bind-*` remain. All started harness, service, runner and agent processes were stopped; the final scenario and an independent socket sweep both found no listeners in A's assigned runner, harness, service and mock port blocks.

## Contract implications and limitations

A real external agent must declare a durable identity, url-less card and contract digests, stable action-ID binding, whether identical-payload replay returns the original Task after durable commitment, `tasks/get`, and artifact fields binding action, run, definition, author and content digest. `a2a-idempotent-resend` is safe only with those declared and pinned idempotency clauses; `fixture-lookup` is safe for the existing participating fixtures; opaque or undeclared reconciliation cannot resolve a lost response and yields an incident. A URL change for the same pinned identity and card/contract can resume polling. A missing identity, changed card or contract, or mapping to an impostor stops the pinned run with an incident before another send.

This prototype's Director model is a fixture, the release receiver is an HTTP fixture, and the delayed agent is a local A2A fixture with a real process and durable SQLite state. During an agent outage, resend mode retries the identical idempotent payload every 0.5 seconds without backoff. Async polling is bounded by roughly 12 Activity attempts of 70 seconds; a Task beyond that window explicitly fails the child, so real long-running agents need workflow-level durable polling. The snapshot location is derived from the journal location as `<EXO_HOME>/testbed/agent_snapshot.json`, a stand-in coupling. No general directory or remote trust infrastructure was built.
