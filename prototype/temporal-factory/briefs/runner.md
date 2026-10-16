# Lane: lazy local runner (pane wG:p6)

Read `prototype/temporal-factory/INTERFACES.md` first; it is binding. Work only in `prototype/temporal-factory/`.

Goal: `src/runner.py` implements the `Runner` class and CLI in INTERFACES.md. It is the bundled PostgreSQL + Temporal + interpreter-worker runner, started lazily and shared by harness instances. Fold the needed parts of `src/runtime.py` and `src/supervisor.py` (package-ops lane: SCRAM secrets, config rendering, schema setup, namespace creation) into `runner.py`. Leave those two old files untouched; the orchestrator will retire them. The old supervisor started fixture services and a Director. The new runner must NOT: services and harness instances are separate.

Files you own: `src/runner.py`, `src/server.template.yaml` (edit if needed), `tests/test_runner.py`. Read-only for you: everything else. Another worker is concurrently rewriting `src/worker.py`/`binding.py`. Import `binding.INTERPRETER_FILES`, `binding.source_digest`, and `binding.build_id_for` lazily. If they don't exist yet, code to the INTERFACES.md contract and test with a stub build directory.

Required behavior:
1. `ensure_started(reason=...)` is idempotent attach-or-start. It takes an exclusive `runner.lock` in the detached `serve` process and writes `runner-ready.json` (pid, address, namespace, started_at, builds). A second caller attaches without starting anything. Record every start and attach, with its reason, in `runner-events.jsonl`.
2. `serve`: first-start init (secrets 0600, initdb SCRAM, temporal and visibility DBs, schema, namespace `exomachina` with 1-day retention). Then supervise PostgreSQL, Temporal, and one worker process per non-retired build dir. Restart a crashed child. Pick up new build dirs by polling. On SIGTERM, stop gracefully (PostgreSQL with `-m fast`).
3. `ensure_build(source_dir)` snapshots `binding.INTERPRETER_FILES` into `builds/<build_id>/` with `build.json` (`build_id`, `source_digest`, `files`). It is idempotent. If an existing build dir holds a different digest, raise. Never overwrite a build.
4. Launch a worker as `cd builds/<id> && $PY worker.py` with env `EXO_TEMPORAL_ADDRESS`, `EXO_WORKER_BUILD_ID`, `EXO_WORKER_SOURCE_DIGEST`, `EXO_OUTCOME_DB=$HOME/runner/outcomes.sqlite3`, and `EXO_ACTIVITY_LOG=$HOME/runner/activities.jsonl`. Each build gets a separate log.
5. `wait_worker(build_id)` uses Temporal's worker-deployment describe API (temporalio client `WorkflowService` `describe_worker_deployment` / `describe_worker_deployment_version`, deployment `exo-factory`). It returns once that version has pollers or is registered. Report which API worked.
6. `status()` reports running state, pids, builds, and `footprint`-free RSS via `ps`.
7. A build marked retired is simply not run. Don't implement automatic retirement; `binding.may_retire` exists for later.

Live smoke, required: use `EXO_HOME=/tmp/exo-proto-runner-<suffix>` with `port_base=46000` and `member_base=33600` only. Run it cold: `start` → `status` → a second `ensure_started` attaches → `ensure_build` of a stub or real build → the worker is observed polling (if the interpreter lane's worker isn't ready, use a stub build whose worker.py registers as `WorkerDeploymentVersion` with a trivial Workflow, and label it clearly as stub) → kill -TERM the Temporal server pid and observe the restart event → `stop` → `start` again (warm start, no re-init) → `stop`. Save the observed JSON to `evidence/runner-smoke.json`. Do not delete the state dir.

Unit tests in `tests/test_runner.py` cover pure parts: port mapping, config rendering, build snapshot immutability and conflict, and ready-file parsing.

Handoff: `handoff/runner.md`, with the commands, observed events, what was stub vs real, and gaps.
