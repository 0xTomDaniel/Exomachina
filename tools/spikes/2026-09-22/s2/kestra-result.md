# S2 follow-on — native Kestra execution inside the Strands factory harness

2026-09-22. **Passed the bounded native integration.** Final combined suite: **7 passed in 18.39 seconds**. This closes the demonstrated gap between the Strands/A2A factory endpoint and a real durable engine for one native flow and one Director wait. It does not qualify every S2 or production requirement.

`kestra-result.json` contains the exact flow/execution IDs, definition revision, identities and incarnations, accepted artifact, native state history, observed task runs, and source hashes. The full local protocol record remains at `.kestra-final/test_kestra_factory_native_pau0/kestra-evidence.json`.

## What was integrated

The existing `Harness.command` Interface now selects the Kestra-backed implementation when its instance state directory contains `kestra.json`. Both the ordinary capability and factory Director still use the same package, Strands `Agent`, plugin, A2A adapter and server entry point. No separate bespoke Director application was added.

`kestra_adapter.py` implements the external HTTP Adapter. `kestra_factory.py` implements factory ownership, assignment intent, review/acceptance, command deduplication and engine-state reconciliation using the existing owned ledger. Kestra owns graph progress through native YAML:

**Return candidate → Pause for Director → Return accepted-artifact digest.**

The YAML is `kestra-flow.yaml`; the test publishes it with a unique `exomachina_s2_factory_*` ID in namespace `exomachina.spikes`. Each assignment retains a specific native revision, execution ID and logical assignment correlation. The engine is not a fixture in this follow-on. Model decisions, evaluator quality and local A2A authentication remain explicit fixtures.

## Executed proof

1. A real HTTP A2A `message/send` reached the real Strands tool loop. Its factory command created a native Kestra execution of the published revision. Kestra produced a candidate and entered `PAUSED`.
2. Eight concurrent repeat start deliveries (four client threads) reused one logical assignment and native execution. Kestra's flow-specific execution listing contained exactly one execution.
3. The Director process received SIGKILL. Restarting its same state directory preserved its identity, incremented its incarnation, and retrieved the original waiting A2A task and native execution. A separate capability instance remained unchanged.
4. A decision without an accepted artifact was rejected. An observer's review mutation was rejected. The evaluator fixture read the actual native candidate from Kestra, verified its expected content, and committed an accepted artifact and exact native Pause task-run identity to the owned ledger. Kestra remained paused; A2A still exposed no completed artifact.
5. The Director received SIGKILL again after acceptance was committed and before resume. A second restart restored that acceptance. A decision carrying the previous incarnation was rejected.
6. Eight concurrent repeat decision deliveries (four client threads) produced **one observed resume HTTP request** and **one successful downstream `after` task run**. Kestra reached `SUCCESS`; the downstream task output matched the accepted artifact's digest. All eight A2A replies reported completion with the same accepted artifact.
7. Ordinary A2A `tasks/get` reconciles native engine state directly; polling does not start a model turn. The authoritative A2A completion requires both native success and matching ledger acceptance.

Exactly-once wording is limited to this observed one-Pause fixture and contention batch. No general exactly-once external side-effect guarantee is asserted.

## Reproduce

Prerequisites: the parent-owned Kestra OSS **2.0.3** instance remains available at `127.0.0.1:28081`, with test Basic Auth configuration in `/tmp/exomachina-spikes/s1/auth.json`. That credential file is read directly and is **not copied or printed**. The parent reports JDK25 and PostgreSQL16.15 for that server. S2 neither restarts nor stops it, and does not access its database directly.

From this evidence directory, with the same prerequisite paths:

```sh
uv sync --frozen
EXOMACHINA_S2_KESTRA_LIVE=1 uv run pytest -q --basetemp=.kestra-final
```

Without the opt-in, the live test is skipped and the original six harness/transport tests can run independently. Each live test creates only its uniquely named flow and associated run. The test starts and stops its own S2 Director/capability subprocesses on free local ports.

Pins: CPython **3.14.3**, Strands **1.57.0**, A2A SDK **0.3.26**, advertised wire protocol **A2A 0.3.0**. `uv.lock` preserves the exact Python dependency graph. There is no protocol 1.x compatibility claim.

The earlier shared capability service on `127.0.0.1:28092` was left untouched for the parent's other probes.

## Concrete API findings

- Native creation uses multipart inputs at `/api/v1/main/executions/{namespace}/{flowId}` and explicitly supplies `revision`. The adapter adds logical assignment and instance labels.
- Native resume uses multipart input at `/api/v1/main/executions/{id}/actions/resume`, carrying the accepted artifact digest.
- Kestra 2.0.3 task outputs are separate from execution JSON. The actual pinned endpoint is **`/api/v1/main/outputs/tasks/{executionId}/{taskRunId}`**. The current migration page's shorter `/outputs/{executionId}/{taskRunId}` returned 404 in this probe. Reading the pinned controller and testing its route resolved the mismatch.
- Execution creation mints a new native ID. The adapter persists intent before calling it and refuses blind re-creation when a previous create outcome is unresolved. Labels provide correlation evidence, but automatic lost-response reconciliation is not implemented or tested.

The initial live attempt correctly exposed the missing output fetch: the native execution reached `PAUSED`, but the evaluator had no candidate in the execution JSON and refused acceptance. The corrected adapter retrieves the dedicated task output. Intermediate failed-probe flows/runs use separate S2 IDs; no existing S1 flow or run was changed.

## Remaining limits

- The two kills occurred at settled native Pause and settled ledger acceptance. They did not target the create-response gap, a resume in flight, network partition, lost acknowledgement, storage failure or simultaneous engine failure.
- Native create ambiguity fails closed and requires reconciliation. Resume recovery across an uncertain HTTP outcome and automatic durable recovery ownership remain unproved.
- The native fixture has one Pause, no loop and no external business side effect. Checking a Pause task-run identity before resume is not a general atomic compare-and-set against a later loop iteration. Native parent/child composition, cancellation races, deadline/budget propagation and graph repair were not added here; earlier parent/child evidence remains fixture-engine evidence.
- Network calls occur inside some SQLite ownership transactions to serialize this small supported local command path. This is acceptable spike code, not a throughput or availability design endorsement.
- Native flow publication is performed by the test setup. Agent-authorized publication policy, model choice/reasoning controls, automatic native-engine/database installation, upgrade packaging and a one-command product installer were not proved.
- The evaluator is deterministic and runs inside the same trusted process. No real reviewer independence, real-model quality, hostile code isolation, multi-host fencing, host-loss durability or production authorization/TLS is claimed.

## Sources

- [Pinned execution API controller](https://github.com/kestra-io/kestra/blob/v2.0.3/webserver/src/main/java/io/kestra/webserver/controllers/api/ExecutionController.java): native creation/revision and resume routes; creation mints its execution ID.
- [Pinned output API controller](https://github.com/kestra-io/kestra/blob/v2.0.3/webserver/src/main/java/io/kestra/webserver/controllers/api/OutputController.java): verified `/outputs/tasks/` route.
- [Kestra execution response migration](https://kestra.io/docs/migration-guide/v2.0.0/execution-api-response): dedicated output storage, with the route discrepancy described above.
- [Native Pause](https://kestra.io/plugins/core/flow/io.kestra.plugin.core.flow.pause): native pause and on-resume input contract.
- [Strands A2A](https://strandsagents.com/docs/user-guide/sdk/multi-agent/agent-to-agent/), [Strands plugins](https://strandsagents.com/docs/user-guide/sdk/plugins/): SDK integration surfaces; actual compatibility is bounded by the pins and live wire test above.
