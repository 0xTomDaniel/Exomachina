# A2 withheld composition — revealed after both implementation freezes

Reveal time: 2026-09-23 03:48:50 UTC

Evaluator receipts recorded before this file was created:
- Dagu inventory: `79c3e81d2037cb44639840120921ff5e1de2a257efd886fa042404f36421290f`
- Temporal inventory: `aaf68094e8ecbc958e3727f1ef6f1278a913b249b3a527210475e51ee72b3099`

This is one additional graph arrangement from the predeclared [vocabulary](VOCABULARY.md), exercised under two immutable definition bindings. It is a semantic input, not an engine-specific template or program. Only authored native factory definitions and probe drivers may change after the freeze. The frozen publisher, validator, interpreter, A2A adapter, reconciler, Director, supervisor, and common services may not change. Verify each manifest before and after.

## Arrangement

A new parent pins a new `verified_research` child and its full transitive closure. Within the child, start **three** named assignments concurrently:

1. `source_primary`: `source_evidence@1`
2. `source_crosscheck`: a second independent assignment of the **same approved** `source_evidence@1` contract
3. `counter_scope`: `counter_evidence@1`

Use distinct stable action IDs and artifact receipts for all three. A typed join waits for and validates **all three** named predecessors, including both source instances; it produces `evidence_join@1`. The joined result must include source IDs from both source instances, the counter objections, exact branch artifact digests, and the declared `route_status` / `requires_scope` fields.

Add a typed `route` **after the join and before synthesis** on the validated `join.route_status` enum:

- `requires_scope`: synthesize unresolved r1, obtain independent Quality rejection, then use the declared bounded repair/review path. If two repairs still fail, wait for the nested child Director's current-revision authorized abort. No acceptance or release follows abort.
- `clear`: synthesize resolved r1, obtain independent Quality acceptance of exact r1, commit authoritative acceptance, then release r1 once. This branch may not bypass Quality or the acceptance invariant.

The parent returns only the pinned child's public accepted or aborted result. Keep an already-running v3 exhaustion case waiting while the new definitions publish; it must retain v3 bindings through the new executions and a normal restart.

## Two bindings of that same arrangement

Publish one immutable version with `counter_scope.scope_status = requires_scope` and run (a) one-repair success and (b) two-repair exhaustion followed by a valid nested Director abort. Publish a second immutable version with `counter_scope.scope_status = clear` and run direct r1 acceptance. Use native version names/digests supported by the candidate. These may be consecutive versions (for example v4 and v5). Only the authored definitions and their pinned closure differ; no engine-facing source change or worker rollout is allowed.

Retain the authored definitions, validation/publication diagnostics, old/new closure digests, branch timing and typed receipts, Quality verdicts, authoritative acceptances, release receipts, Director command evidence, and both `freeze.py verify` outputs. If the frozen implementation cannot express or run this graph, report the precise limitation as an A2 failure or partial result. Do not modify source to make the hidden case pass.
