# Harness tests handoff

## Files changed

- `tests/test_harness_modes.py` (new): five unittest cases for durable identity and fencing, lazy recovery, public command rejection, publication without runner startup, and A2A agent mode across a restart.
- This handoff file.

## Commands and results

- `cat prototype/temporal-factory/INTERFACES.md prototype/temporal-factory/briefs/harness-tests.md` — PASS; read the shared contract and lane brief.
- `git status --short` — PASS; other lanes' in-progress files were present and untouched.
- `sed -n '1,260p' prototype/temporal-factory/src/harness.py`, `sed -n '261,560p' prototype/temporal-factory/src/harness.py`, `sed -n '320,410p' prototype/temporal-factory/src/harness.py`, and `nl -ba prototype/temporal-factory/src/harness.py | sed -n '277,330p'` — PASS; read Director and instance boundary code.
- `sed -n '1,260p' prototype/temporal-factory/tests/test_testbed.py`, `sed -n '1,240p' prototype/temporal-factory/src/long_client.py`, `sed -n '1,210p' prototype/temporal-factory/src/harness_server.py`, `sed -n '210,430p' prototype/temporal-factory/src/harness_server.py`, `sed -n '1,150p' prototype/temporal-factory/src/fixture.py`, `sed -n '1,110p' prototype/temporal-factory/src/fixture.py`, `sed -n '1,210p' prototype/temporal-factory/services/testbed.py`, `sed -n '1,100p' prototype/temporal-factory/services/testbed.py`, `sed -n '1,150p' prototype/temporal-factory/src/authoring.py`, `sed -n '1,160p' prototype/temporal-factory/tests/test_authoring.py`, `sed -n '100,220p' prototype/temporal-factory/src/binding.py`, and `sed -n '1,115p' prototype/temporal-factory/src/binding.py` — PASS; inspected fixture shapes and A2A paths.
- `rg -n 'def assignment|def run_id_for|class Director|class FactoryModule|def recover|def perform|def fence|def publish' prototype/temporal-factory/src` and `rg -n 'harness|agent.mode|Agent Card|runner' prototype/temporal-factory/tests` — PASS; located interfaces and existing tests.
- `ls -l prototype/temporal-factory/tests/test_harness_modes.py prototype/temporal-factory/handoff/harness-tests.md` — expected exit 1; both target files did not exist before this lane wrote them.
- `PY=/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python; cd prototype/temporal-factory && $PY -B -m unittest tests.test_harness_modes -v` — PASS, exit 0. Output:

  ```text
  test_a2a_agent_mode_survives_restart_without_runner (tests.test_harness_modes.AgentModeTests.test_a2a_agent_mode_survives_restart_without_runner) ... ok
  test_identity_persists_and_stale_incarnation_is_fenced (tests.test_harness_modes.DirectorBoundaryTests.test_identity_persists_and_stale_incarnation_is_fenced) ... ok
  test_public_commands_keep_graph_selection_inside_instance (tests.test_harness_modes.DirectorBoundaryTests.test_public_commands_keep_graph_selection_inside_instance) ... ok
  test_publication_activates_without_starting_runner (tests.test_harness_modes.DirectorBoundaryTests.test_publication_activates_without_starting_runner) ... ok
  test_recovery_starts_only_for_unfinished_runs (tests.test_harness_modes.DirectorBoundaryTests.test_recovery_starts_only_for_unfinished_runs) ... ok

  ----------------------------------------------------------------------
  Ran 5 tests in 2.389s

  OK
  ```

- `git diff --check -- prototype/temporal-factory/tests/test_harness_modes.py` — PASS, no output. The test file is untracked, so this command did not inspect its content.
- `git status --short -- prototype/temporal-factory/tests/test_harness_modes.py prototype/temporal-factory/handoff/harness-tests.md` — PASS; showed the new test file before this handoff was written.
- `ls -dt /tmp/exo-proto-harness-test-*` and `rg --files /tmp/exo-proto-harness-test-* | rg 'agent.log|instance.json|active-publication.json'` — PASS; located retained trial state.

## Evidence

- Agent mode process log and durable state: `/tmp/exo-proto-harness-test-z7g7pp2j/agent.log` and `/tmp/exo-proto-harness-test-z7g7pp2j/home/instances/agent/`.
- Active publication pointer: `/tmp/exo-proto-harness-test-z70tsgvz/home/instances/factory/catalog/active-publication.json`.
- Other retained unit state: `/tmp/exo-proto-harness-test-xn8jw039/`, `/tmp/exo-proto-harness-test-hop72nc5/`, and `/tmp/exo-proto-harness-test-ivgnvy3o/`.

## Harness defect found by code review

- `src/harness.py:278-316`: `Director.perform` reads an existing Task alias but does not reject a new `start` action whose computed run differs from that alias. `INSERT OR IGNORE` then leaves the Task bound to its old run while a new run starts. Suggested fix: before inserting a run, reject `start` when `alias is not None and alias["run_id"] != run_id`; also verify the existing run's Task and context binding before accepting an action replay.

## Limits

- The runner is a stub in unit tests; these tests do not verify a live PostgreSQL or Temporal startup.
- No command or approval was rejected. No files were deleted.
