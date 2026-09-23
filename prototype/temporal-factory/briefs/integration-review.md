# Lane: read-only integration review (pane wG:p5)

Read `prototype/temporal-factory/INTERFACES.md`. This is a READ-ONLY review. Do not edit any file except your handoff.

Review the orchestrator-owned integration code against the merged interpreter and the lane modules:
`src/harness.py`, `src/admin.py`, `scenarios/common.py`, `scenarios/integrated.py`, plus the orchestrator's patch at the end of `src/factory.py` (the accepted `artifact` in the `complete` result).

Look for concrete correctness defects that would break the two happy paths or the stated guarantees:
- The factory-mode harness exposes only its normal A2A identity/contract, and callers can't choose a graph or version.
- The runner starts lazily: never at routine startup, only on first factory work or recovery of unfinished runs.
- Each run pins its closure (definition, bindings, contracts, Quality policy, build), and a waiting v1 keeps it after v2 is published.
- There is exactly one result/receipt on the original A2A Task. Abort continues the original Task (a2a-sdk 0.3.26 `DefaultRequestHandler` semantics for a message with `taskId` on an `input-required` Task: check `.venv/lib/python3.12/site-packages/a2a/server/request_handlers/default_request_handler.py` and `a2a/server/tasks/task_manager.py`).
- Mismatches with actual signatures in `src/runner.py`, `src/authoring.py`, `src/binding.py`, `services/testbed.py` (its CLI output format and file locations), and `src/factory.py` status/result keys.
- Temporal client API usage in `scenarios/integrated.py` `histories()` (temporalio 1.33.0: `describe().raw_description.workflow_execution_info.versioning_info` field names, `fetch_history().to_json()` shape), and the outcome-journal table name used there vs `src/a2a_outcome.py`.
- FastAPI `app.on_event("startup")` support in the installed version.

The venv is `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv`. You may run import checks and pure Python snippets, but don't start servers, Temporal, or PostgreSQL.

Handoff: `handoff/integration-review.md`, with findings ranked by severity. Each gives file:line, the defect, a concrete failure scenario, and a suggested fix. Say plainly if a checked item is fine.
