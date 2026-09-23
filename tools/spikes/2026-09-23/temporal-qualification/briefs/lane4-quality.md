# Lane 4 (`tw_quality`): Quality identity/verdict consistency and A2A unknown-outcome reconciliation

Owned directory: `tools/spikes/2026-09-23/temporal-quality-reconciliation/`. Ports 44000–44999. State `/tmp/exo-tq-quality-*`. Follow [`../COORDINATION.md`](../COORDINATION.md), including the safety boundary. **Important:** the Effect Quality verdict-disagreement fault-injection follow-up was rejected by automatic approval review. Do not send tampered, forged, or disagreeing verdicts through live services in any form.

## Questions

1. **Quality authority.** In the Temporal candidate (`temporal-recovery-scale/adapter.py`, `factory.py`, `director_server.py`, the common fixtures under `tools/spikes/2026-09-22/arbitration/common/`), is the Quality verdict that drives acceptance bound to: the pinned Quality service identity (Agent Card / endpoint / key) from the run's closure; the exact artifact revision digest under review; the run/assignment/attempt; and a Quality actor independent of the author? Is the A2A Task verdict payload cross-checked against the action-lookup verdict (the gap found in Effect's `effect-parity/bridge.py`)? Cite lines.
2. **Unknown-outcome reconciliation.** When an A2A `message/send` outcome is ambiguous (timeout, lost reply, connection reset after send), what does the Temporal adapter do for (a) a participating service with caller-action-ID lookup, (b) an opaque service without lookup? It must never blindly resubmit a non-idempotent assignment or release to an opaque service; an unresolvable effect must become a durable `unknown` state/incident requiring authority, not a silent retry or acceptance.

## Work

1. Source audit with a concise table of each binding/consistency property: present / absent / partial, with file:line.
2. Implement in your directory a small, well-typed `quality_authority` module: a verdict-acceptance function that requires identity match with the pinned closure, artifact digest match, run/assignment/attempt match, author≠Quality, and Task-payload vs action-lookup agreement; otherwise returns a typed *inconsistent* outcome that routes to an incident and never to acceptance. Implement an `a2a_outcome` reconciliation state machine: `submitted → confirmed | unknown → (lookup) confirmed | still-unknown → incident`, distinguishing participating vs opaque receivers, with bounded lookups and a durable record.
3. Evidence: **pure unit tests** with in-memory inputs covering every branch, including mismatched identity/digest/attempt and payload-vs-lookup disagreement as data fed to the pure function (no live services, no network). Plus an ordinary happy-path live run through the real Director showing the new checks accept a genuine consistent verdict unchanged, if you can wire them in without touching the interpreter semantics. Show how the functions would be called from the Temporal Activity (source), and whether their decision is replay-safe (deterministic inputs recorded in history).
4. Live inconsistent-verdict or ambiguous-outcome scenarios remain **unproved** unless done with purely ordinary operations; do not simulate lost replies or tamper with services.

## Output

`result.md`, `observed.json`, module, tests. Verdict: Quality identity/verdict consistency (source/unit/observed happy path), A2A unknown-outcome reconciliation (source/unit), explicit remaining gaps and the smallest safe next proof.
