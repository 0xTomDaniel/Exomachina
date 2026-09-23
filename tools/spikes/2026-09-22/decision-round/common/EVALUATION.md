# Decision-round comparison contract

Each candidate report must distinguish `observed pass`, `observed failure`,
`partial`, and `not tested`. The common remote harness is a deterministic fixture,
so a passing result proves orchestration across real Strands/A2A processes, not
model quality, protocol 1.x support, or exactly-once effects with arbitrary peers.

| Check | Required evidence |
| --- | --- |
| Versioned publication | Publish structurally different v2 while v1 is waiting; start both without replacing running worker/engine code; each run retains its own full definition and capability-reference closure. |
| Harness identity | Launch at least capability and Quality as separate instances of the same customized Strands harness package, with distinct durable identities and state. State explicitly whether the Director is itself a Strands harness process. |
| A2A assignment | Show actual `message/send` and structured Task artifact, with pinned action/run/definition binding. A plain HTTP fixture is insufficient. |
| Remote uncertainty | Kill the receiver after committing a new assignment but before reply; restart it, reconcile by stable action ID and binding, and show accepted receiver effect count and attempt count. Note that action lookup is a fixture-specific extension. |
| Same-run contention | Launch two independent owner processes against the same run. Show owner decisions and remote attempt/effect counts. One receiver-accepted effect can result from receiver idempotency even when both owners submit. |
| Exact acceptance | Record current artifact revision and digest; reject stale revision, author self-review, duplicate acceptance, and mismatched definition/run; accept only an independent Quality result for the current revision. Quality's response and product acceptance are distinct events. |
| Acceptance/engine gap | Crash after product acceptance commits but before engine continuation, then resume through a stable outbox command; show no duplicate logical delivery. A local marker does not prove external side-effect exactly-once. |
| Restart | Kill the engine/helper/Director at a defined checkpoint and demonstrate recovered run state without silently republishing or swapping v1 to v2. |
| Packaging and operating cost | List processes, source/lock versions, persistent stores, required ports, startup/shutdown owner, measured warm/active RSS, disk, and product-owned code. Whole-install metrics need an identical workload and platform. |
| Product rights | Identify exact shipped artifact and license/feature gate. Flag unresolved obligations; a source-code license alone does not clear a packaged product. |

The decision is based on the total product Module, including the engine Adapter,
publication validator, acceptance ledger, remote reconciliation, lifecycle, and
distribution. A candidate's engine does not get credit for behavior supplied by
the shared receiver or custom product code.
