# Temporal Director contract lane

23 September 2026. **Overall: partial. Gate A: observed pass on the pre-policy snapshot. Gate B: source and unit-level implementation, live failure projection unproved.** This is bounded spike evidence, not product qualification. The ordinary live trial used the real Director A2A `message/send` and `tasks/get`, PostgreSQL/Temporal membership ports 31100–31199, other ports 41000–41999, and `/tmp/exo-tq-director-*` state. No failure injection, process kill, or tampered Quality verdict was run. The policy addendum below changed the package and Director source after that trial; it has **unit-level and source-inference evidence only**. The saved four-history replay also predates the addendum.

## Gate A: typed Director inputs

The one immutable mixed v4 package declared required `outcome_mode: string` with enum `after_first_repair | never`. Both values went through `message/send` as structured `DataPart` inputs. The Director validated and persisted each canonical input object and digest before Workflow start; the same values and digest appeared in parent and child Workflow status. Both child runs used the same pinned definition digest. [`trial_observed.json`](trial_observed.json) contains original Task IDs, native IDs, input and package digests, Task/Workflow results, and ledger rows; [`observed.json`](observed.json) is the compact machine-readable verdict.

| Ordinary run | Original Director Task | Native outcome | Quality actions | Release rows |
| --- | --- | --- | ---: | ---: |
| `after_first_repair` | `76842bdb-36ae-4441-9c5e-6d90e00f9fd1` → `completed` | r2 accepted; one receipt | 2 | 1 |
| `never` | `d0c18396-de3d-438f-89f7-db45f1620614` → `input-required` at r3 Director wait, then `completed` after a Director A2A authorized abort | r3 exhausted; aborted; no acceptance | 3 | 0 |

Invalid enum and missing required input each returned a Director A2A error. For each rejected run, the Director SQLite `runs` query returned zero rows and Temporal `describe()` found no Workflow. These are **observed** checks from the same live command, not inferred validation behavior.

After the live processes and state stopped, [`replay_trial.py`](replay_trial.py) replayed the four saved ordinary parent/child histories against the final `FactoryRun` code with `workflow_failure_exception_types=[ValueError]`. All four passed without replay failure: 14/56 events for the acceptance parent/child, and 14/69 for the abort parent/child. [`replay_observed.json`](replay_observed.json) records each history digest and result; the histories are retained under [`histories/`](histories/). This is an **observed offline replay compatibility check for these histories**, not an upgrade or failure-path test.

## Gate B: failed child projection

| Claim | Level | Evidence and limit |
| --- | --- | --- |
| A caught `ChildWorkflowError` writes an incident to parent Workflow history and yields a `failed` public result. The parent does not copy child acceptance or call parent release on that branch. | Source inference | [`factory.py`](factory.py). A failed child was not run live under the safety boundary. A child could already have an authoritative effect before a later failure, so a global no-effect guarantee remains unproved. |
| The original Task reader uses Temporal `describe()` to map closed `FAILED`, `TERMINATED`, `TIMED_OUT`, and `CANCELED` parent statuses to A2A `failed`; it emits an incident message and persists the incident at read time. It detects known acceptance or receipt state as an `authority_conflict`. | Source inference + unit-level | [`director_server.py`](director_server.py), [`failure_projection.py`](failure_projection.py), [`unit-output.txt`](unit-output.txt). No live closed-failed parent was run. |
| Plain Workflow `ValueError` would fail/retry a Workflow Task in temporalio 1.33.0 unless configured as a Workflow failure. This copy configures `workflow_failure_exception_types=[ValueError]`; `ActivityError` and `ChildWorkflowError` are `FailureError` subclasses. | Source inference; ordinary replay observed | Pinned SDK `temporalio/worker/_workflow_instance.py:1959-1973`, `_worker.py:280-287`, `temporalio/exceptions.py:25,288,342`; local [`worker.py`](worker.py). Four successful/aborted histories replayed, but no failing history was used. |

The four pure tests in [`test_contract.py`](test_contract.py) pass. They cover schema/type/enum/default rejection, the mixed package's required schema and Quality binding, Task-state mapping, and incident authority-conflict decisions. This is **unit-level** evidence only for Gate B.

## Source delta and reproduction

The source base is `temporal-recovery-scale/`. [`source_delta.json`](source_delta.json) gives base and final SHA-256 per file. `adapter.py`, `long_client.py`, `normalize.py`, `server.template.yaml`, `slow_harness_server.py`, and `supervisor.py` remain byte-identical. The mixed template came from `temporal-fresh-composition/`. Required edits add package/input validation, Director run persistence and Task projection, parent/child input propagation and child incident, the worker `ValueError` failure policy, trial/replay drivers, and corrected ports. The prior trial fault profiles are not reachable through this Director start command.

From the repository root, reproduce in this order:

```sh
cd tools/spikes/2026-09-23/temporal-director-contract
/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest -v test_contract.py
cd /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina
tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B tools/spikes/2026-09-23/temporal-director-contract/director_trial.py
tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B tools/spikes/2026-09-23/temporal-director-contract/replay_trial.py
```

The live command reported `passed` and `cleanup: {errors: [], state_removed: true}`. The offline replayer verified no owned ports were open and no `/tmp/exo-tq-director-*` state remained before replay.

## Remaining gaps and requests to orchestrator

1. **Gate B remains unproved live.** The mandatory safety boundary bars the previously rejected child-failure injection. A child that fails after an authoritative effect needs a stronger no-effect/incident design; closed-parent incidents currently materialize only on Task read.
2. The fixture Bearer token authenticates the Director endpoint, but there is no caller-specific authorization for `outcome_mode`. This input steers a candidate's path to Quality; the validator preserves Quality and release gates, yet a product policy must decide who may provide it. Direct Temporal starts are outside this Director's access control.
3. The copied publisher pins JSON and approved service identities/URLs, but it does not pin capability contracts, Quality policy revisions, or interpreter/worker build. The version lane owns that wider closure. This remains spike code, not integrated Director product code.

Please carry these gaps into the integrated verdict. Gate A can now be reported as an **observed pass for the stated ordinary paths**; Gate B must remain **source and unit-level only**.

## Addendum: pinned typed-input authorization and post-acceptance failure

The published `run_inputs` schema now pins each input's `source` (`caller`, `director`, or `verified_artifact`) and `may_affect_acceptance` flag. Caller inputs must pin an `allowed_actors` list; artifact inputs must pin an `artifact_contract`. The mixed package marks `outcome_mode` as acceptance-affecting and caller-supplied, allowlisted to `fixture-operator`. Publication validation rejects a `from_run` graph if this flag is false. The Director's A2A middleware derives the fixture actor from the validated bearer token; the start path resolves inputs against the pinned policy before inserting a run or starting a Workflow. It writes `authorized_actor` and per-input authority alongside the value digest in the run row, and forwards them to parent/child status. External A2A data cannot provide a value declared Director-derived or artifact-derived. These are **source-inference** claims for the new server path; the pure policy checks passed **unit-level** in [`unit-output.txt`](unit-output.txt) (6/6). The HTTP-to-tool actor context and the new run-row/status fields have **not been observed live** after this change. Director-derived and verified-artifact values require trusted resolvers in product code; this fixture passes none, so a published package requiring either cannot start through its current A2A path.

For a child that closes failed after acceptance or release activity, [`decide_closed_failed_child`](failure_projection.py) defines a **pure decision**: the original public Task is `failed`, the authoritative acceptance and any confirmed receipt remain visible in the incident, and `may_submit_release` is always false. With a confirmed receipt the incident class is `post-acceptance-after-release`; with a participating receiver's submitted/unknown dispatch intent it is `post-acceptance-release-outcome-unknown` and the only permitted action is lookup by the original action ID. Opaque or inconsistent outcomes become manual incidents. Without an outcome journal, `post-acceptance-release-unaccounted` requires investigation and permits no automatic send. The function takes lane 4's [`a2a_outcome.py`](../temporal-quality-reconciliation/a2a_outcome.py) `OutcomeRecord` interface; its `may_submit=False` dispatch-intent rule is the no-second-release boundary. The in-memory cases in [`test_contract.py`](test_contract.py) pass **unit-level**, including confirmed, submitted, unknown, opaque, missing-journal, and binding-conflict decisions. This decision is **not wired to a live receiver journal**, and no failed child was run; the actual public Task and reconciliation behavior remain **unproved**.

This addendum supersedes the earlier open authorization question only at the schema/fixture policy level. Per-caller production authentication, trusted Director/artifact resolvers, a durable release outcome journal on this lane, and live behavior under the new policy remain open. The prior Gate A live and replay evidence remains valid for its exact recorded source snapshot and should not be presented as a pass of this addendum.
