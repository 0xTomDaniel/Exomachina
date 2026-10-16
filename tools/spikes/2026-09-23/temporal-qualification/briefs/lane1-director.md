# Lane 1 (`tw_director`): typed Director inputs and failed-child projection

Owned directory: `tools/spikes/2026-09-23/temporal-director-contract/`. Ports 41000–41999. State `/tmp/exo-tq-director-*`. Follow [`../COORDINATION.md`](../COORDINATION.md), including the safety boundary.

## Gate A: typed Director run inputs (primary, observed evidence expected)

The frozen Director A2A start facade builds the Workflow input itself and cannot forward the typed `outcome_mode` required by the mixed v4 graph (`temporal-fresh-composition/definitions/withheld-a2c-v4-mixed.json`). Close that gap:

1. Base on `temporal-recovery-scale/` (and the A2C trial driver/definitions in `temporal-fresh-composition/`). Keep the interpreter, validator and worker semantics unchanged unless required; record a precise source delta with hashes.
2. Let a published factory version declare a typed run-input schema (names, types, enums, required/optional, defaults) as part of its immutable, digested package. The Director's A2A `message/send` accepts those inputs through a structured `DataPart`, validates them against the **pinned** version's schema before starting anything, rejects unknown/invalid/missing inputs with an A2A error or `rejected`/`failed` Task without starting a Workflow, and persists the validated inputs (plus their digest) with the run record so later reads and the child see exactly the same values.
3. Observed runs through the real Director A2A endpoint: mixed v4 with `after_first_repair` (expect r2 acceptance + one release receipt) and with `never` (expect r3 exhaustion → `awaiting-director` → authorized abort, no acceptance/release), each under its own original Director A2A Task ID that ends in the correct terminal state; plus rejection of an invalid enum value and a missing required input with no Workflow started. These are ordinary input-validation and happy-path runs.
4. Consider authorization: who may set typed inputs, and whether an input may steer Quality/acceptance. At minimum ensure inputs cannot bypass Quality or release gates, and record the authorization question as open if not implemented.

## Gate B: failed native child → same original A2A Task `failed`, incident, no acceptance/release

`DirectorTaskStore.get` currently maps accepted/aborted/expired and has no explicit native Workflow-failure → A2A `failed` projection.

1. Implement the projection in source: when the parent observes an unhandled child Workflow failure (or the parent itself ends failed/terminated/timed out), record a durable incident (run ID, child ID, pinned digest, failure class, timestamp), guarantee no authoritative acceptance and no release call on that path, and project the **same original** Director A2A Task to `failed` with an A2A status message referencing the incident. Also handle the Director reading a closed-failed parent via Temporal describe/result.
2. Evidence permitted here: source review and **pure unit tests** of the mapping/incident/no-release decision functions with in-memory inputs (e.g. a status/exception → A2A state table; a function asserting acceptance/release are unreachable on the failure path). Optionally an offline `temporalio.worker.Replayer` check against histories from ordinary successful runs to show your changes remain replay-compatible.
3. **Do not** run a live test that injects or provokes a child Workflow failure, kills processes, or otherwise repeats the previously rejected failed-child fault-injection test in any form. The live failed-child projection therefore stays **unproved** unless the orchestrator later obtains a safe path; say so explicitly.

## Output

`result.md`, `observed.json`, tests, source delta. Verdict per gate: Gate A observed pass/fail; Gate B implemented + unit-level / unproved live. List exactly what the Director product code still lacks.
