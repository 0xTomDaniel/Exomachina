# Runner correction handoff

## Files changed

- `src/runner.py`: removed the `ALTER TABLE cluster_membership` schema widening; `port_map` now rejects invalid runner and membership port ranges before `Runner` can start anything. Default `member_base` remains 32400.
- `tests/test_runner.py`: checks valid and invalid boundaries, the default range, and absence of schema widening in runner source.
- `evidence/runner-smoke.json`: retained the prior high-port observations and added `superseded_note`.
- `evidence/runner-cold-smoke.json`: new clean first-start and warm-start observations, full event log, PIDs, and ports.
- `handoff/runner-fix.md`: this report.

## Commands and results

Working directory for test and smoke commands: `prototype/temporal-factory`. Python executable: `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python`.

| Command | Observed result |
| --- | --- |
| `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest -v tests/test_runner.py` | PASS, exit 0: six tests `ok`; `Ran 6 tests ... OK`. |
| `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B - <<'PY'` (preflight script) | PASS, exit 0: `/tmp/exo-proto-runner-cold-20260923-195413` did not exist; 32500–32504 and 46002–46006, 46011–46012 all bound successfully on `127.0.0.1` before release. |
| `EXO_RUNNER_PORT_BASE=46000 EXO_RUNNER_MEMBER_BASE=32500 /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B - <<'PY'` (single smoke script) | PASS, exit 0: rechecked the fresh home and ports, then ran the exact sequence below once. Full observations are in `evidence/runner-cold-smoke.json`. |
| `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B - <<'PY'` (evidence inspection script) | PASS, exit 0: result `PASS`, event kinds and PIDs present, PostgreSQL first flags `[True, False]`, final `running: false`, old evidence retained with `superseded_note`. |

The smoke script invoked these operations in order, with no retry: `python -B src/runner.py start --home /tmp/exo-proto-runner-cold-20260923-195413 --reason clean-cold` (exit 0), `python -B src/runner.py status --home /tmp/exo-proto-runner-cold-20260923-195413` (exit 0), `Runner.ensure_started(reason='clean-cold-attach')` (attached), `Runner.ensure_build(Path.cwd() / 'src')` (build `b-05b5760359e6`), `Runner.wait_worker('b-05b5760359e6', timeout=60)` (registered), `Runner.stop()` (stopped), `python -B src/runner.py start --home /tmp/exo-proto-runner-cold-20260923-195413 --reason clean-warm` (exit 0), and `Runner.stop()` (stopped).

## Evidence and preserved state

- `evidence/runner-cold-smoke.json`: clean start runner PID 20802, PostgreSQL PID 20819, Temporal PID 20854, worker PID 20897. Warm start runner PID 21029, PostgreSQL PID 21032, Temporal PID 21039. Its event log has one `postgres-start first=true`, one `namespace-create`, one cold `serve-ready`, then `postgres-start first=false` and a warm `serve-ready` without a second namespace creation. The final status is stopped.
- `evidence/runner-smoke.json`: prior high-port failure and subsequent recovered passes remain intact. The new note marks them as superseded for clean-cold proof.
- `/tmp/exo-proto-runner-cold-20260923-195413/`: preserved home containing PostgreSQL, Temporal, build, logs, and event journal. `/tmp/exo-proto-runner-smoke-20260923/` was left untouched.

## Known gaps

- There is no separate SQL command audit. The warm `postgres-start first=false` event and the `if first:` guard around `setup-schema` and `update-schema` show that warm startup did not run schema setup.
