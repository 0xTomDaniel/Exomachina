# Runner lane handoff

## Files changed

- `src/runner.py` — install-wide detached runner, PostgreSQL/Temporal lifecycle, immutable worker builds, worker deployment observation, CLI, status and RSS.
- `tests/test_runner.py` — four standard-library unit tests.
- `evidence/runner-smoke.json` — live observations and event log excerpts.
- `src/server.template.yaml` was read but not changed.

## Verification and commands

Interpreter: `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python`; working directory: repository root. The following are the executed test and smoke commands (long Python heredocs are described by their actual calls below; their full observed values are in the evidence JSON).

| Command | Result |
| --- | --- |
| `.../.venv/bin/python -B -m pytest -q prototype/temporal-factory/tests/test_runner.py` | FAIL: `No module named pytest`. No dependency installation attempted. |
| `.../.venv/bin/python -B -m py_compile prototype/temporal-factory/src/runner.py` | PASS, exit 0. This created ignored `src/__pycache__/runner.cpython-312.pyc`; left in place under the no-deletion rule. |
| `.../.venv/bin/python -B -m unittest -v prototype/temporal-factory/tests/test_runner.py` | PASS, four tests: port mapping, config rendering, build immutability/conflict, ready parsing. Repeated after final changes: `Ran 4 tests ... OK`. |
| `EXO_RUNNER_PORT_BASE=46000 EXO_RUNNER_MEMBER_BASE=33600 .../.venv/bin/python -B prototype/temporal-factory/src/runner.py start --home /tmp/exo-proto-runner-smoke-20260923 --reason smoke-cold` | FAIL: Temporal exited 1 because its `cluster_membership.rpc_port` was signed `SMALLINT` and rejected membership port 33602. |
| Same `start`, reason `smoke-cold-retry` | FAIL: PostgreSQL authentication failed for local user `tomdaniel` during migration; corrected command to use `-U temporal`. |
| Same `start`, reason `smoke-cold-retry-2` | PASS: runner 15272, PostgreSQL 15275, Temporal 15288. This attempt exposed that a failure after schema creation but before namespace creation needs a namespace existence check. |
| Same CLI `stop --home /tmp/exo-proto-runner-smoke-20260923` | PASS: `"running": false`. |
| Same CLI `start --home /tmp/exo-proto-runner-smoke-20260923 --reason smoke-start` | PASS: runner 15588, PostgreSQL 15591, Temporal 15616; namespace `exomachina` created. |
| `Runner.status(); Runner.ensure_started(reason='smoke-attach'); Runner.ensure_build(Path('prototype/temporal-factory/src'))` using the specified Python with `-B` and the same ports/home | PASS: attached to PID 15588; real interpreter snapshot `b-aff77432772e`, digest `aff77432772e05f880eaa46b115be41c0eb57f621471ed407c63418eb3f2b0ba`. |
| `Runner.wait_worker('b-aff77432772e', timeout=30)` using the same Python | PASS: `describe_worker_deployment_version`, registered `exo-factory.b-aff77432772e`, workflow and activity task queues `exo-factory`; worker PID 15851. |
| `kill -TERM 15616` | PASS; observed `temporal-restart` event for PID 16026. |
| Same CLI `stop`, then `start --reason smoke-warm`, then `Runner.wait_worker(...)`, then `stop` | PASS: warm runner PID 16639, real worker PID 16683, version still registered, final `"running": false`; no new `postgres-start` event had `first: true`. |
| `Runner.ensure_started(reason='verify-restart-ready')`, `os.kill(17691, SIGTERM)`, wait for status, `Runner.stop()` with specified Python and same ports/home | PASS: ready-file Temporal PID changed to 17767 with RSS 117472 KiB; final `"running": false`. |

## Evidence and state

- `evidence/runner-smoke.json`: exact retained runner events, build/version observation, port allocation, final ready state, and restart verification.
- `/tmp/exo-proto-runner-smoke-20260923/`: retained PostgreSQL, Temporal, worker logs, build snapshot, secrets, and event journal. No trial state was removed.
- Unit test directories are retained under `/tmp/exo-proto-runner-unit-*`.

## Known gaps

- The Temporal schema migration is required for the mandated membership ports above 32767. It widens `cluster_membership.rpc_port` to `INTEGER` idempotently when needed; future Temporal schema upgrades should be checked against this local adaptation.
- The initial cold attempt and the next retry failed as described above. The subsequent smoke and warm restart passed after fixes. The first failed attempt created the PostgreSQL schema, so the successful `smoke-start` run recovered from that partial initialization rather than starting from an untouched directory.
- `wait_worker` reports version registration through `describe_worker_deployment_version`; the observed response listed both task queue types. It does not count live pollers separately.
- The ignored Python bytecode file created by `py_compile` remains at `src/__pycache__/runner.cpython-312.pyc` because deletion was prohibited.
