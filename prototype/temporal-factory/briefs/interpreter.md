# Lane: interpreter merge (pane wG:p5)

Read `prototype/temporal-factory/INTERFACES.md` first; it is binding. Work only in `prototype/temporal-factory/`.

Goal: merge the four Temporal lane changes into ONE interpreter in `src/`. The files there are unmodified copies:
- `factory.py`, `definition.py`, `failure_projection.py` come from `tools/spikes/2026-09-23/temporal-director-contract/` (typed inputs, child-failure incident).
- `binding.py`, `worker.py` come from `temporal-version-binding/`. Also apply that lane's `factory.py`/`director_server.py` deltas (PINNED behavior, verify_closure before each Activity/child, closure in child input, QUEUE). Diff them against `temporal-recovery-scale/`.
- `adapter.py`, `a2a_outcome.py`, `quality_authority.py`, `incident_projection.py`, `long_client.py` come from `temporal-quality-reconciliation/`. Apply its `factory.py` delta (quality_action_id, Quality binding/assignment/attempt passed to review, incident results instead of holding forever).
- Remove every `faults`/barrier/drop_ack/trial path from the product code path.

Files you own: `src/{factory,adapter,binding,buildinfo(new),worker,definition,fixture,failure_projection,incident_projection,quality_authority,a2a_outcome,long_client,receiver_client}.py`, `tests/test_contract.py`, `tests/test_binding.py`, `tests/test_decisions.py`, and a new `tests/test_interpreter.py`. Do not touch `harness_server.py`, `runtime.py`, `supervisor.py`, `services/`, or `definitions/`.

Required:
1. Implement the interpreter contract in INTERFACES.md exactly: start-input keys, status fields, result statuses, `question` run input flowing into briefs, env names (`EXO_OUTCOME_DB`, `EXO_ACTIVITY_LOG`, `EXO_WORKER_BUILD_ID`, `EXO_WORKER_SOURCE_DIGEST`, `EXO_TEMPORAL_ADDRESS`), `buildinfo.py`, `binding.INTERPRETER_FILES`/`source_digest`/`build_id_for`, and `PublicationStore` with `label`, `get`, `list`.
2. Make the imports flat and local (no `tools/spikes` sys.path hacks). Keep Workflow determinism: passed-through imports only, no I/O in workflow code.
3. Port the three existing unit test files to the new layout and make them pass. Add `tests/test_interpreter.py` covering at least: validation of the materialized v1 template (inline the lane-1 `materialize` logic in the test, or read `definitions/v1-template.json` with fixture bindings); `verify_closure` rejecting changed definition/contract/policy/build; `project()` mapping for `incident`; `fixture.branch_brief` with a question; `source_digest` stability; and that `factory.py` has no reference to `faults`.
4. Check that the Workflow class passes Temporal's sandbox validation offline. Construct `temporalio.worker.Replayer(workflows=[FactoryRun], workflow_failure_exception_types=[ValueError])`, or use `temporalio.worker.workflow_sandbox.SandboxedWorkflowRunner` to prepare the workflow definition without a server. Report exactly what you ran.
5. No live Temporal server is needed in this lane. Don't start one.

Run: `PY=/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python; cd prototype/temporal-factory && $PY -B -m unittest discover -s tests -v`

Handoff: `handoff/interpreter.md`, with a summary of the merged semantics per source lane, the files changed, test output, and gaps.
