# Temporal factory authoring evaluation

> **Current development direction, 23 September 2026:** The [fresh arbitration](engine-reassessment-2026-09-23.md) favors Temporal with a stable Python factory interpreter, rather than the Zigflow route, for the first development slice. Exomachina may automatically restart or roll out bundled orchestration workers, but active runs must retain their exact factory and compatible worker versions, and independent A2A services remain untouched. The [Zigflow rollout reassessment](../tools/spikes/2026-09-23/zigflow-reassessment/result.md) keeps it eligible as a conditional alternative. The older decision text below records its stage of evaluation, not the current rank.

> **Later stable-interpreter trial:** [Temporal round two](../tools/spikes/2026-09-22/round-two/temporal/result.md) published v2 while v1 waited with one unchanged Python worker on a non-development server, then recovered both after server/worker/PostgreSQL restart. The Zigflow-specific findings below remain valid for Zigflow; they no longer block Temporal with a product-owned interpreter.

22 September 2026 · Primary-source review plus bounded local probes · No production engine selected

> **Later comparison result:** a [bounded non-development Temporal countertrial](../tools/spikes/2026-09-22/countertrials/temporal/result.md) ran with PostgreSQL persistence/visibility and Zigflow v0.15.2. It preserved a waiting run through helper restart and old/new pinned workers returned their respective versions. Adding a valid new definition while a worker stayed up still failed, confirming the assessed Zigflow worker-refresh requirement on this topology. The SQLite `start-dev` run recorded below does **not** measure the non-development package. Zigflow's authoring limit is not a Temporal execution-engine limit; a stable interpreter would transfer definition semantics and publication ownership to Exomachina unless a maintained permitted layer is proven. The original observations below are unchanged.

## Decision

**Promote Temporal to a direct conditional candidate alongside Conductor and Kestra.** The default comparison is Conductor, Kestra and Temporal. Argo remains a provisional challenger and stays selectable. The separate weighted top-four control remains Conductor, Kestra, Argo and Temporal in coverage order. No feature ratings, weights or scores change.

Temporal's authoring layer remains a decision to resolve, rather than grounds to dismiss its execution strengths. **Zigflow is a credible existing declarative layer, but its tested release requires worker lifecycle changes to publish new definitions.** It is suitable only if that rollout model is accepted and the remaining contracts pass. An official sample shows that an input-driven interpreter can avoid per-composition deployment, but adopting it means owning that interpreter and its publication facilities.

The [durable evidence record](exomachina-temporal-evaluation.json) preserves the source audits, alternatives, probe observations and limits. The [engine evaluation](engine-evaluation.md) remains the authority for the full reference factory and required scenarios.

## Why the overall score understated Temporal's case

The unchanged [matrix](exomachina-feature-matrix.json) scores Temporal at **56.5%**. Among Conductor, Kestra, Argo and Temporal, it leads Graph (**91.2%**), Operations (**71.4%**) and Experience (**72.7%**), and ties Argo on Recovery (**81.4%**). Governance (**34.8%**) and Maintenance & Engineering (**37.8%**) equal Conductor.

Its 17-point deficit against Conductor comprises 16 points in Authoring and 13 in Remote services, offset by a 12-point advantage across the other six sections. Native documents, publication without worker deployment and agent authoring are distinct outcomes, but partly correlated consequences of the same architecture choice.

Native A2A support remains useful integration credit, not a selection gate: a reusable adapter is already accepted. Artifact acceptance, external effect deduplication, shared budgets, independent Quality and the Engineering improvement loop also require Exomachina or participating-service behavior across candidates. Those shared gaps do not uniquely disqualify Temporal.

| Responsibility | Temporal route | Conductor/Kestra route |
| --- | --- | --- |
| Durable execution | Server and SDK Workers provide process history, waits, retries and recovery primitives. | Native engines execute JSON/YAML process definitions. |
| Remote agent lifecycle | Reusable Activity/service adapter implements A2A correlation and reconciliation. | Conductor supplies more documented native lifecycle support; Kestra's assessed integration is narrower. Each still needs protocol and recovery proof. |
| Acceptance, budgets, Engineering | Exomachina policies and services use engine primitives. | The same domain responsibilities remain, with different supplied primitives. |
| Factory publication | Adopt a separately assessed layer or own bounded definition semantics, validation, storage and upgrades. | Native languages, registration, revisions and interpreters already exist; product policy remains additional. |

The specific Temporal cost is an authoring/publication subsystem, not rebuilding a durable scheduler. Compare its total ownership against the native engines' integration and operating costs. [Temporal workflow model](https://docs.temporal.io/workflow-definition), [Conductor definition lifecycle](https://docs.conductor-oss.org/architecture/json-native.html), [Kestra flow lifecycle](https://kestra.io/docs/workflow-components/flow).

## What was actually tested

The probes used official **Zigflow v0.15.2**, released at commit `a18538ba585e580c466ad3f743874af642ccad68`, and **Temporal CLI v1.9.1** on local Darwin ARM64. Temporal ran with `server start-dev` and a persistent SQLite file. The workflow used a deterministic local HTTP fixture and a signal-controlled wait. Each reported case ran **once**. [Zigflow release](https://github.com/zigflow/zigflow/releases/tag/v0.15.2), [Temporal CLI release](https://github.com/temporalio/cli/releases/tag/v1.9.1).

| Probe | Observed result | Supported conclusion |
| --- | --- | --- |
| Valid definition | Validation returned exit 0 and `valid: true`. | The selected definition passed this release's validator. |
| Extra top-level `surprise` field | Also returned exit 0 and `valid: true`. | Unknown-field rejection cannot be assumed. This input was not established as invalid under the actual schema; acceptance alone is not a validator defect. |
| Missing `document.taskQueue` | Exit 1 with a missing-property error at `$.document`. | Required-field validation worked in this case. |
| Unsupported call | Exit 1 with a schema `oneOf` error. | Unsupported syntax was rejected; error specificity still matters for authoring. |
| Server and worker crash during wait | Both processes were killed, then restarted against the same SQLite state. The same workflow completed after its signal. HTTP counts were `/before: 1`, `/after: 1`. | This recorded wait recovered without repeating the already recorded HTTP step. |
| New file with worker already running | Starting its new workflow type produced a Workflow Task failure. After worker refresh, that execution completed. | File publication alone did not register the new type in the running worker. |
| Unversioned definition replacement | A run started with revision `1.0.0`; after a wait, definition replacement and worker restart, its result reported `2.0.0`. | `document.version` alone did not bind the waiting run to its original implementation. |
| Explicit pinned worker deployments | The old worker remained available. After making v2 current, the old run returned `1.0.0` and a new run returned `2.0.0`. | Explicit version routing preserved the two tested behaviors. |

Raw observations originate from `probe-results.json` and `validation-results.json` and are preserved in the [evidence record](exomachina-temporal-evaluation.json). The runner reported all owned processes stopped afterward.

These are local functional observations, **not production, HA, load, A2A or full E01–E10 results**. The crash occurred at a recorded wait, not in the lost-acknowledgement window of an external submission. HTTP counts therefore do not prove exactly-once effects. Nested factories, remote cancellation, concurrent admission, protected acceptance and Engineering were not exercised.

## Zigflow's useful scope and remaining limits

The inspected release is Apache 2.0 and supplies a declarative language, schema validation, authoring assistance, conditionals, sequential loops, declared parallel branches, named child workflows and durable waits/messages. Its MCP tools assist discovery and validation; they are not a factory publication service. [Pinned license](https://github.com/zigflow/zigflow/blob/a18538ba585e580c466ad3f743874af642ccad68/LICENSE), [MCP surface](https://github.com/zigflow/zigflow/blob/a18538ba585e580c466ad3f743874af642ccad68/docs/docs/cli/mcp-server.md).

Production publication remains tied to loaded worker definitions. `--watch` replaces workers and is documented for development. A managed rollout can reuse the binary while changing its definition bundle, but that still changes worker lifecycle. Explicit pinning requires immutable build identities and retention of workers serving older runs. [Startup](https://github.com/zigflow/zigflow/blob/a18538ba585e580c466ad3f743874af642ccad68/cmd/run/command.go), [watch implementation](https://github.com/zigflow/zigflow/blob/a18538ba585e580c466ad3f743874af642ccad68/cmd/run/watch.go), [version configuration](https://github.com/zigflow/zigflow/blob/a18538ba585e580c466ad3f743874af642ccad68/cmd/run/workers.go).

**Do not transfer the whole Temporal SDK matrix to Zigflow.** The release audit identified narrower behavior that remains untested here:

- Runtime collection iteration is sequential; declared forks do not establish runtime-sized parallel mapping.
- Named child workflows need proof of input projection, schema enforcement, version selection and context isolation.
- Listen `acceptIf` is not a pre-acceptance Update validator: source stores incoming data before evaluating it. Root-to-child command routing and `listen.any` completion also need targeted tests.
- Schema-valid script/shell/container tasks and selectable endpoints/Activity queues require an approved-primitive boundary. Environment propagation is not a credential isolation contract.

These findings are grounded in the pinned [loop](https://github.com/zigflow/zigflow/blob/a18538ba585e580c466ad3f743874af642ccad68/pkg/zigflow/tasks/task_builder_for.go), [child invocation](https://github.com/zigflow/zigflow/blob/a18538ba585e580c466ad3f743874af642ccad68/pkg/zigflow/tasks/task_builder_run.go), [listener](https://github.com/zigflow/zigflow/blob/a18538ba585e580c466ad3f743874af642ccad68/pkg/zigflow/tasks/task_builder_listen.go) and [execution](https://github.com/zigflow/zigflow/blob/a18538ba585e580c466ad3f743874af642ccad68/pkg/zigflow/activities/run.go) implementations. Source concerns are distinguished from reproduced failures in the evidence record.

The inspected main commit `44d7f1cbaa6de0c4ba7f1fea0758d5bbe776880f` was two commits ahead of this release and included stronger fork-cancellation closure behavior. That change was **not** tested as v0.15.2 behavior. Documentation for main, released binaries and third-party forks must remain separate. [Exact comparison](https://github.com/zigflow/zigflow/compare/a18538ba585e580c466ad3f743874af642ccad68...44d7f1cbaa6de0c4ba7f1fea0758d5bbe776880f).

## Other authoring paths

**Tracecat** is a credible Temporal-backed document platform with an existing publication lifecycle, but it is a larger security-automation product. Current inspected source has AGPL core plus Enterprise and gating-code exceptions; production and redistribution rights are not cleared for the proposed product. Its current-main audit is not certification of release 1.0.1. Keep it a conditional reference, not a permissive drop-in substitute. [Pinned README and exceptions](https://github.com/TracecatHQ/tracecat/blob/712426150cc07596eafd8255a7e481e699b54540/README.md), [Enterprise terms](https://github.com/TracecatHQ/tracecat/blob/712426150cc07596eafd8255a7e481e699b54540/packages/tracecat-ee/LICENSE.md).

**The official Temporal Go DSL sample** registers one interpreter and supplies YAML as input. New compositions within its Activity/Sequence/Parallel vocabulary need no new worker implementation. It is a reference implementation, not a managed factory catalog: Exomachina would own the extended language, validation, publication, inspection and compatibility policy. A bounded interpreter is a viable hypothesis, not an existing turnkey product. [Pinned interpreter](https://github.com/temporalio/samples-go/blob/e9f36fee3251a5c7ad9b798cab710a6e7283b295/dsl/workflow.go), [starter](https://github.com/temporalio/samples-go/blob/e9f36fee3251a5c7ad9b798cab710a6e7283b295/dsl/starter/main.go).

Direct agent-authored SDK code is another explicit architecture choice. It needs code review, isolation, build/deployment and version management; it does not silently satisfy document-only publication.

## Next proof gates

1. **Prove the accepted publication contract.** Automate Zigflow's managed, version-pinned worker rollout and test activation, old-run retention, rollback and complete cost; compare it with a stable interpreter that does not refresh workers. Preserve clear validation and non-bypassable publication checks.
2. **Bind the complete execution.** Pin definition digest, interpreter semantics, capability/profile revisions and acceptance policy. Exercise child queues, Continue-As-New, old-worker retention, replay compatibility and rollback. Worker pinning alone does not freeze remote dependencies.
3. **Prove the actual authoring layer.** Test the Zigflow limitations above using its DSL, rather than substituting SDK examples. Reject unauthorized primitives and malformed public inputs/results.
4. **Run the reference factory's fault cases.** E01–E10 cover nested contracts, ambiguous submission, competing attempts, artifact revisions, director waits, cancellation, delivery recovery and shared admission. The local probes do not mark those scenarios passed.
5. **Run required maintenance and improvement.** Apply M01–M08, including recovery-owner failure and protected candidate promotion; apply R01–R05 only for enabled autoresearch. Replay compatibility is not evidence of improved agent quality.
6. **Measure the whole production stack.** Use a pinned production server/storage topology and overlapping concurrent requests. Compare operating burden, owned persistent state and recovery paths consistently against Conductor/Kestra.

Temporal remains a direct candidate with unresolved authoring conditions. Zigflow's rollout is allowed but unqualified; an owned interpreter remains an explicit alternative cost. Selection follows the full observed contract, not a feature total or one successful restart.
