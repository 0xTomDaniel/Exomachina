# Decision-round factory runtime comparison

22 September 2026. **Dagu is the narrow provisional preference for the first
agent-authored factory, Effect Workflow/Cluster is the strongest alternative,
and Strands Graph remains the simplest harness integration but the most
product-owned outer runtime.** This is a development judgment, not an engine
selection or production qualification. The [independent judgment](independent-judgment.md)
weighs the same evidence without treating a feature score as the decision.

The comparison used a [common deterministic Strands/A2A fixture](common/CONTRACT.md)
and [evidence contract](common/EVALUATION.md). Each candidate had to publish a
structurally different v2 while v1 waited, run separate capability and Quality
harness instances, reconcile a receiver that committed work then dropped its
reply, accept an exact artifact revision, and expose the factory through a
Strands Director. These are real local processes and A2A 0.3.0 messages, but
the model and Quality decision are deterministic test fixtures. The receiver's
action lookup and duplicate suppression are **product-test extensions**, not
generic A2A guarantees supplied by any engine.

| Decision concern | Dagu Community v2.17.0 | Effect Workflow/Cluster v3 | Strands Graph v1.57.0 |
| --- | --- | --- | --- |
| Agent-authored factory | Native YAML DAG and named child DAGs. Product publisher checks a narrow approved vocabulary and closure; transactional publication remains open. | Durable code substrate. The product interpreter currently handles only two fixed linear JSON shapes; flexible approved graphs need substantial new product code. | Code-defined Graph. Product constructs it from fixed JSON, with no managed definition language or publication service. |
| Version and recovery | V1 retained its root/child closure while v2 published. Dagu waits survived graceful restart and a later helper hard stop; a lost A2A acknowledgment was reconciled by product adapter. | V1/v2 pinned in one helper. SQL-backed Activities/waits survived hard stops during remote uncertainty and on both sides of the acceptance signal. | V1/v2 Graph sessions restored. Product ledger, scheduler/ownership and session files are separate state owners. |
| Exact acceptance and release | Product ledger enforced current `r2`/digest; targeted Dagu retry after a post-acceptance failure resumed the same run, with one accepted logical revision and one local release marker. No automatic recovery outbox was shown. | Product ledger/outbox replayed one local delivery marker across signal crashes. Director's reviewer authority remains fixture-only. | Product ledger/claim/outbox enforced a bounded case; parallel branch results were not semantically joined for Quality. |
| Director as factory service | Separate follow-on Strands Director exposed A2A Tasks; original IDs survived SIGKILL/restart and projected accepted artifacts. A stale incarnation was rejected before a command. | Separate follow-on Strands Director exposed durable A2A Tasks and recovered original IDs after SIGKILL. | Director follow-on reused the Strands harness/Graph session and restored original A2A Tasks after SIGKILL. |
| Multi-instance ownership | Different Dagu homes with identical workflow/run IDs collided through a host-global run socket; distinct run IDs succeeded. Every harness must allocate host-unique run IDs. In-flight Director command fencing remains open. | Two local helpers raced the same new run in one SQLite state and admitted it once. Remote in-flight two-owner and multi-host ownership remain open. | Local valid-lease contention fenced one Director delivery; a prior forced-expiry race sent twice and relied on receiver deduplication. |
| One-command local topology | A copied Dagu/Python/Strands bundle's long-running supervisor started five children. A separate probe killed/relaunched the supervisor on the same state, then killed Dagu and one Director; original Tasks and the other Director survived. Host-unique run IDs were bound through Director and adapter. | A copied Node/Python/Strands bundle's long-running supervisor started five children. A separate probe killed/relaunched it on the same state, then recovered original A2A Tasks and local delivery. Child Director/helper SIGKILL also passed. | No copied one-command package in this round; Graph is inside the Strands process but product state/lifecycle remain to assemble. |
| Product rights | GPL-3.0-or-later permits commercial distribution under its terms. Separate CLI/server path is eligible for [GPL-compliant bundle review](dagu/gpl-bundle-review.md); present trial bundle lacks Corresponding Source delivery. | Lockfile license screen found no paid runtime gate; final artifact/notice audit remains. | Strands Apache-2.0 package metadata; transitive bundle audit remains. |

The [Dagu result](dagu/result.md), [Effect result](effect/result.md), and
[Graph result](strands_graph/result.md) identify exact commands and observed
limits. Effect's independent [copied-bundle trial](effect/packaging-result.md)
measured **645 MiB summed warm RSS** and **674 MiB with two waiting runs** for a
supervisor, one shared helper, two Directors, capability and Quality; bundle
disk was **393 MiB**. Dagu's supervised copied trial measured about **676 MiB
summed RSS** with two waiting runs and **314 MiB** bundle disk including state.
The separate outer-supervisor SIGKILL/relaunch probes passed for
[Dagu](dagu/result.md) and [Effect](effect/packaging-result.md). These are
single-host process RSS sums, which can double-count shared pages, and the
active workloads were not precisely matched. Neither is a clean-machine,
relocatable, strictly offline, multi-platform or redistributable installer.
The earlier ~650 MiB Kestra number measured its JVM alone, not this topology.

The concrete reason to prefer Dagu for development is its **existing document
language**: an authoring agent can submit a new composition of approved
capabilities and pinned nested factories without deploying new orchestration
code. Dagu does not supply Exomachina's semantic safety policy, publication
transaction, A2A reconciliation, independent acceptance, Director authority,
incident controller or experiment evaluator. Effect offers stronger observed
local crash recovery and copied-bundle supervision, but its present document
surface is too narrow to demonstrate the required flexible graphs. Strands
Graph gives direct harness fit; the wider publication and durable ownership
system would largely be built by Exomachina.

The next *development* discriminator is a genuinely agent-authored v3 factory
containing a typed parallel join, bounded review/repair, and a pinned nested
factory while v2 remains active. Run it on Dagu's native YAML and an Effect
approved-block interpreter, counting validator/adapter/recovery code and
testing rejection paths. This tests whether Dagu's native DSL still saves
meaningful work once safety policy is applied. It is not a reason to resume a
broad six-engine survey before building the first useful factory.

No candidate has passed the full E01–E10 factory scenarios, Maintenance
M01–M08, optional autoresearch R01–R05, real model/provider use, A2A 1.x
interoperability, production credentials, customer isolation, external
exactly-once effects, clean-machine installation, or release licensing.
Those remain explicit development and release boundaries.
