# Integration review handoff

Read-only review of the factory harness, operator CLI, integrated scenarios, and their interpreter, runner, testbed, and installed SDK dependencies. No services or databases were started. This handoff is the only file written by this lane.

## Findings, ranked by severity

### High — Publishing v2 fences the still-running harness

**Location:** `src/admin.py:18-20`, `src/harness.py:163-167`, `src/harness.py:275-277`.

`admin.py author` and `publish-template` construct a new `Director`. Its constructor increments the durable identity incarnation even though the live harness remains running. The live harness then fails `fence()` for every `perform()` call. In path 2, after publishing v2, a caller who starts a v2 run or answers v1's Director wait before the scripted restart receives an error Message instead of continuing the original Task. The scenario's restart at `scenarios/integrated.py:182-185` masks this defect. Keep publication commands from claiming a serving incarnation; reserve the epoch increment for a harness process taking ownership, or provide a separate read-only publication context that does not mutate `identity`.

### High — A retry can publish the same result and receipt on a second A2A Task

**Location:** `src/harness.py:282-305`, `src/harness.py:310-323`, `src/harness.py:413-418`.

`start` deduplicates by `action_id`/run ID but does not require a retry to use the recorded `runs.task_id`. It inserts a new alias for any new Task ID. If a caller retries an accepted `start` with the same action ID and inputs but without `taskId`, the SDK allocates a new Task; both Tasks project the accepted artifact and release receipt. This violates the one-result-on-the-original-Task guarantee. Return or require the original Task ID for a duplicate start, and never alias a second Task to an existing run.

### Medium — Parent child execution is exposed as a Director input request

**Location:** `src/harness.py:388-406`, `src/failure_projection.py:18-19`, `scenarios/integrated.py:165-169`.

The parent uses phase `awaiting-child` from the moment it starts the nested child. `project()` maps that phase to A2A `input-required` even while the child is still gathering, reviewing, or repairing. Path 2's `poll_task(..., {"input-required", ...})` can therefore return before any Director wait exists; a prompt abort then fails the child phase check in `_abort()`. Query the child phase for the public projection, and emit `input-required` only when the child actually reaches `awaiting-director`. Keep `awaiting-child` as `working` until then.

### Medium — The integrated outcome-journal evidence is always empty

**Location:** `scenarios/integrated.py:157-159`; actual schema at `src/a2a_outcome.py:131-132`.

The query selects `*` from `outcomes`, whose columns are only `action_id` and JSON `value`. Filtering those row dictionaries by `r.get("run_id", "")` therefore drops every row. Path 1 records `[]` even after successful assignment and Quality sends, so the scenario cannot establish the durable outcome evidence. Decode `value` with `json.loads()` and filter its `run_id`, retaining `action_id` in each evidence row.

### Medium — Worker readiness check accepts a registered version without a poller

**Location:** `src/harness.py:194-198`, `src/runner.py:213-235`.

`ensure_runner()` relies on `wait_worker()` before starting a run. `wait_worker()` returns as soon as `describe_worker_deployment_version` succeeds, without checking that `response` reports an active poller; its fallback checks only whether the build ID appears in a deployment description string. If a previously registered worker has died while the supervisor is restarting it, the harness can start a pinned run with no worker actually polling. Inspect the version's poller information and wait for a live poller for the requested queue/version; fail at the timeout otherwise.

## Checked items with no defect found

- The factory A2A card exposes the instance's `verified-research@1` skill; `start` rejects extra top-level fields and `authorize_run_inputs` bounds caller inputs. No caller graph or version selector is present.
- Publication snapshots the interpreter build without starting the runner. `Director.recover()` calls `ensure_runner()` only when `runs.closed=0` exists. The installed FastAPI 0.141.1 still supports `app.on_event("startup")` (deprecated but callable; hook registration succeeded).
- The run start includes the pinned package, definition, binding/contract/Quality closure, source build, inputs and authority. `PublicationStore.get()` preserves an earlier publication after the active pointer changes. The interpreter's `complete` result contains `artifact`, `acceptance`, and `receipt`, and `FactoryTaskStore._task()` maps those keys into one accepted Task artifact.
- The installed a2a-sdk 0.3.26 `DefaultRequestHandler` loads an existing nonterminal Task when `message.task_id` is supplied, keeps its Task ID, and accepts `input-required` continuation. `scenarios/common.py:a2a_send()` supplies both `taskId` and `contextId` for abort. The defect above is in when this application advertises `input-required`, not in the SDK continuation path.
- Actual signatures and data locations used by `src/admin.py` and `scenarios/integrated.py` match `Runner`, `AuthoringSession`, `PublicationStore`, and `services/testbed.py`. Testbed `up` returns `bindings`, `health`, and `pids`, and writes metadata under `$HOME/testbed`. Quality and capability action databases are at `$HOME/services/<name>/harness.sqlite3`; release uses `$HOME/services/release/release.sqlite3`.
- The temporalio 1.33.0 protobuf descriptors contain `workflow_execution_info.versioning_info.deployment_version` and `versioning_override.pinned.version` with the accessed deployment/build fields. A pure `WorkflowHistory.to_json()` check returned a JSON object with `events`, matching `histories()`.

## Commands and evidence

All commands below were read-only except writing this handoff. Paths are relative to the repository root. No trial state or service evidence path was created.

```text
cat prototype/temporal-factory/briefs/integration-review.md                         PASS
cat prototype/temporal-factory/INTERFACES.md                                        PASS
git status --short --branch                                                         PASS; branch feat/temporal-factory-prototype, existing other-lane changes
rg --files -g 'AGENTS.md' -g '!node_modules' -g '!vendor'                            exit 1; no applicable repository AGENTS.md
find .. -name AGENTS.md -print                                                      PASS; none in this repository outside SDK dependency
wc -l prototype/temporal-factory/{src/{harness,admin,factory,runner,authoring,binding,a2a_outcome}.py,scenarios/{common,integrated}.py,services/testbed.py} PASS
nl -ba prototype/temporal-factory/src/harness.py | sed -n '1,280p'                  PASS
nl -ba prototype/temporal-factory/src/harness.py | sed -n '281,560p'                PASS
nl -ba prototype/temporal-factory/src/harness.py | sed -n '275,365p'                PASS
nl -ba prototype/temporal-factory/src/admin.py                                      PASS
nl -ba prototype/temporal-factory/scenarios/common.py                               PASS
nl -ba prototype/temporal-factory/scenarios/integrated.py                           PASS
nl -ba prototype/temporal-factory/src/harness_server.py | sed -n '195,285p'        PASS
nl -ba prototype/temporal-factory/src/harness_server.py | sed -n '35,115p'         PASS
nl -ba prototype/temporal-factory/src/factory.py | sed -n '30,125p;285,373p'      PASS
nl -ba prototype/temporal-factory/src/factory.py | sed -n '124,235p'             PASS
nl -ba prototype/temporal-factory/src/factory.py | sed -n '235,285p'             PASS
nl -ba prototype/temporal-factory/src/binding.py | sed -n '1,220p'               PASS
nl -ba prototype/temporal-factory/src/runner.py | sed -n '125,275p'              PASS
nl -ba prototype/temporal-factory/src/runner.py | sed -n '340,445p'              PASS
nl -ba prototype/temporal-factory/src/authoring.py | sed -n '1,335p'            PASS
nl -ba prototype/temporal-factory/src/adapter.py | sed -n '1,260p'              PASS
nl -ba prototype/temporal-factory/src/worker.py | sed -n '1,170p'               PASS
nl -ba prototype/temporal-factory/src/failure_projection.py                      PASS
nl -ba prototype/temporal-factory/src/a2a_outcome.py | sed -n '120,168p'         PASS
nl -ba prototype/temporal-factory/services/testbed.py | sed -n '1,230p'         PASS
nl -ba prototype/temporal-factory/services/quality_server.py | sed -n '1,120p'  PASS
rg -n 'sqlite3|CREATE TABLE|actions|release.sqlite|quality.sqlite|harness.sqlite' prototype/temporal-factory/services/{quality_server,release_server}.py PASS
rg -n 'run_inputs|outcome_mode|question|resolved' prototype/temporal-factory/definitions/v1-template.json PASS
nl -ba tools/spikes/2026-09-22/arbitration/temporal/.venv/lib/python3.12/site-packages/a2a/server/request_handlers/default_request_handler.py | sed -n '195,365p' PASS
nl -ba tools/spikes/2026-09-22/arbitration/temporal/.venv/lib/python3.12/site-packages/a2a/server/request_handlers/default_request_handler.py | sed -n '105,195p' PASS
nl -ba tools/spikes/2026-09-22/arbitration/temporal/.venv/lib/python3.12/site-packages/a2a/server/tasks/task_manager.py | sed -n '160,275p' PASS
nl -ba tools/spikes/2026-09-22/arbitration/temporal/.venv/lib/python3.12/site-packages/a2a/server/tasks/result_aggregator.py | sed -n '1,270p' PASS
```

Additional `rg -n` symbol searches over the same files and the installed SDK passed. One search named a nonexistent `temporalio/client.py` and exited 2; the actual SDK module was then located by `rg --no-ignore` under `temporalio/client/_workflow.py`. No action was rejected. Pure Python `-B -c` checks passed: imports of `harness`, `admin`, `runner`, `authoring`, `binding`, and `factory`; FastAPI startup-hook registration (`0.141.1 True`, one hook); temporalio descriptor field names; and `WorkflowHistory.to_json()` shape (`str`, object with `events`). A final prewrite check showed this handoff path did not exist. Full integrated scenarios were deliberately not run because the brief prohibits starting servers, Temporal, or PostgreSQL.
