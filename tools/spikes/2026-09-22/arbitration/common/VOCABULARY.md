# Arbitration v3 semantic vocabulary

This is the **visible** contract to freeze against before an evaluator supplies
one additional valid composition. It defines behavior and types, not a common
serialization or a mandatory universal DSL. Dagu may use native YAML; Temporal
may use a stable interpreter over approved documents; Kestra and Effect may use
their supported authoring models. The implementation must accept a new approved
composition from this vocabulary without a worker/engine code change or a new
hard-coded graph case. Preserve native documents and their closure digests as
evidence.

## Types and permitted blocks

| Semantic block | Input → output | Required property |
| --- | --- | --- |
| `assign` | Scoped work + pinned capability contract → A2A Task and typed result | Persist a stable action/run/definition binding before send; reconcile an uncertain result when the receiver supports lookup. Multiple named instances of either declared result type are allowed. |
| `join` | Named typed branch results → validated composite | Require every declared predecessor and exact result type; one branch's output cannot stand in for another. V3 uses `source_evidence@1` and `counter_evidence@1` to make `evidence_join@1`. |
| `synthesize` | Validated join + revision → `verified_research_candidate@1` | Content has a digest and immutable revision. A changed candidate makes a new revision. |
| `quality` | Exact candidate revision/digest + run/definition binding → independent verdict | The reviewer is the verified Quality service identity, distinct from author. The response is a verdict, not authoritative acceptance. |
| `route` | Typed verdict or declared result → one permitted successor | Conditions are over validated enum/boolean fields only; unknown or malformed values fail closed. |
| `repair` | Rejected current candidate → next immutable revision | At most **two** repairs after r1. A Quality verdict is required for each revision. |
| `director_wait` | Declared unresolved state → persisted authorized decision or expiry | On exhausted repair, the nested child waits; it cannot release an artifact. Authorization and current revision are checked before recording a command. |
| `nested_factory` | Pinned child version/closure + parent assignment → public child result | The parent treats the child as a capability. Its binding includes the child's transitive definitions, contracts, and approved service revisions. |
| `release` | Authoritative acceptance for current revision → receiver receipt | Use one stable logical release identity and reconcile uncertainty. A receiving fixture enforces duplicate handling. |

Allowed edges are typed and explicit. A release is reachable only after a
positive verified Quality verdict for the current candidate and authoritative
acceptance. A rejected verdict routes to repair while the bound allows it, then
to the child Director wait. A Director abort ends the child with a public
`aborted` result and no release. Arbitrary code, shell/HTTP destinations,
unapproved capability revisions, unbounded loops/fan-out, implicit latest-child
references, and syntactically valid review bypass are outside the vocabulary.
Candidate-owned authoring can express additional approved routing arrangements
using these same blocks; the fixture does not dictate engine syntax.

`counter_evidence@1.scope_status` is the validated enum `requires_scope` or
`clear`. A typed join exposes the same `route_status` enum and its matching
`requires_scope` boolean. A route may test either field; any value outside the
declared type fails validation. `fixture.py` can instantiate the two result
types under distinct action IDs and validate an arbitrary declared set of
branch instances. This supports composition changes without adding a new
capability type or changing the engine code.

## Visible v3 reference composition

The parent starts a **pinned** `verified_research` child. Inside it, two useful
capability branches start concurrently: source evidence supplies claims with
source IDs, while counterevidence supplies objections with source IDs. Their
distinct typed results join only after both validate. Synthesis makes r1 with
unresolved objections. The independent Quality fixture rejects this *valid*
candidate. One repair creates r2 that addresses the objections; a fresh Quality
verdict accepts r2. Authoritative acceptance then releases r2 once and the
parent receives the child's public accepted result and receipt.

A second child keeps the objections unresolved in r1, r2, and r3. Those are
three Quality rejections after exactly two repairs. It enters a persisted child
Director wait while the parent remains pending. An authorized, current-revision
Director abort completes the child and parent as `aborted`; neither records an
acceptance or release. An expired/unauthorized/stale command cannot abort or
publish. No indefinite automatic repair is allowed.

The old v2 run must already be waiting when v3 publishes. It retains its v2
definition and transitive child/capability closure; new work binds v3. The
publication must not replace engine or worker code to achieve this. A later
additional composition will be disclosed **only after implementation hashes
are recorded and handed to the evaluator**. Its exact arrangement is not in
this repository now.

## Service interface and authority

`fixture.py` supplies deterministic branch briefs and typed result validation.
Launch **two separate** instances of the existing
`decision-round/common/harness_server.py --role capability`, one for each
branch, and this directory's `quality_server.py` for Quality. They are three
real local Strands/A2A 0.3.0 processes with distinct durable identities. A2A
commands and the fixture-specific action lookup are documented in
`decision-round/common/CONTRACT.md`; use `decision-round/common/client.py` for
the wire calls. The Quality service returns `accepted: false` on a digest-valid
candidate with unresolved objections, or `accepted: true` when addressed. It
binds revision and SHA-256. Its `accepted_count` inherited action field means
**receiver effect count even for rejection**; it is never a Quality-acceptance,
product-acceptance, or release count.

The caller must verify A2A Task metadata, action/run/definition identity, the
service's pinned identity, artifact digest, and current candidate revision. A
caller-supplied reviewer string is insufficient. The product's authoritative
acceptance may live in engine history or compact product state; this contract
does not require an external acceptance ledger. Every path into continuation
must cross the same acceptance invariant.
