# Lane: harness mode and Director tests (pane wG:p7)

Read `prototype/temporal-factory/INTERFACES.md` and `src/harness.py` (orchestrator-owned; do NOT edit it — report defects in your handoff instead). Work only in `prototype/temporal-factory/`.

File you own: `tests/test_harness_modes.py` (new). Nothing else.

Write focused tests (unittest, `src` on sys.path, `-B`) for the harness instance boundary. Use temp dirs under `/tmp/exo-proto-harness-test-*` (never delete them). Where the real runner would start PostgreSQL/Temporal, substitute a stub object with the same method names (`is_running`, `ensure_started`, `ensure_build`, `wait_worker`, `address`) and record calls. The real `src/runner.py` is being finished concurrently by another worker, so don't depend on its internals.
1. Identity and durable state: constructing `Director` twice on the same instance dir keeps `identity`, increments `incarnation`; `fence` rejects the stale incarnation.
2. Routine startup is lazy: with no unfinished runs, `Director.recover()` returns None and the stub runner's `ensure_started` is never called. With one `runs` row where `closed=0`, `recover()` calls `ensure_started` once with a reason starting `recover-unfinished`.
3. Black-box contract: `Director.perform` (set `harness.CURRENT_ACTOR` to `fixture-operator`) rejects a start containing `package_digest`, `graph`, or `version` fields, rejects unsupported ops, rejects `inspect`/`abort` on a task id that isn't bound to a run, and rejects an unauthenticated caller. `run_id_for(action_id)` is stable and prefixed by the instance identity.
4. Publication does not start the runner: provision a catalog from a fixture `approved_bindings.json`/`contracts.json`/`quality_policy.json` (same shapes as `services/testbed.py` writes; see `tests/test_testbed.py`), materialize `definitions/v1-template.json` with `authoring.materialize`, and call `FactoryModule.publish(package, label="v1", approval=...)` with an approved record. The publication must be active with the label, and `ensure_started` must not be called. The stub's `ensure_build` can delegate to a real snapshot if `runner.Runner(home).ensure_build(SRC)` works without starting services; otherwise return a fixed `{build_id, source_digest}` computed with `binding.source_digest(SRC)` and `binding.build_id_for`. Also assert that an unapproved approval record is rejected.
5. Agent mode: with `instance.json` mode `agent`, start `python src/harness.py serve --instance-dir ...` on port 45300 as a subprocess. Send an A2A `message/send` assign (see `src/long_client.py` `send`, command shape in `src/fixture.py` `assignment`), get a completed artifact, SIGTERM it, restart it, and check `/health` identity is unchanged and incarnation increased. Also assert its Agent Card has no factory skill and that no `runner/` dir was created under its home.

Run: `PY=/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python; cd prototype/temporal-factory && $PY -B -m unittest tests.test_harness_modes -v`

Handoff: `handoff/harness-tests.md`, with test output and any harness defects found (with file:line and a suggested fix).
