# Contract questions

- The frozen binding record has only `role`, `url`, `identity`, and `approved`; it cannot itself state `report_synthesis@1`. I require the canonical `synthesizer` service in `definition.validate`, and require the exact agent capability for research, synthesis, and Quality in the pinned closure contracts in `binding.make_manifest` and `verify_closure`. The async activity also checks the served contract capability before dispatch/poll.
- The existing `admin provision` interface had `--wait-seconds` but no `director_model` option. This lane retained `--wait-seconds`; Lane D owns `harness.py` and its `director_model` configuration. Integration should confirm Lane D's provision path exposes its desired model selection. I did not edit Lane D files.
- The packet validator checks schema, size, IDs, references, and evidence citations. The source excerpts' verbatim match to commit `2d609e3` belongs to the Lane R packet creation and scenario audit; the pure interpreter has no committed-source text embedded.

# Files changed

- `src/definition.py`, `src/report_contract.py`, `src/factory.py`, `src/adapter.py`, `src/long_client.py`, `src/quality_authority.py`, `src/binding.py`: report graph vocabulary, packet/report/Quality contracts, async agent path, exact candidate verdict binding, closure capability checks, status `max_repairs` and last verdict.
- `src/authoring.py`, `src/admin.py`, `definitions/report-template.json`, `definitions/authoring-brief-report.md`, `broker/testing/mock-codex.mjs`: report authoring, pinned packet provision/materialization, reference graph/brief, and mock author/Director scripts.
- `tests/report_fixture.py`, `tests/test_report_async.py`, `tests/test_report_contract.py`: in-test independent async A2A agents and validation/incident tests.
- Updated legacy tests: `tests/test_authoring.py`, `tests/test_authoring_budget.py`, `tests/test_binding.py`, `tests/test_contract.py`, `tests/test_interpreter.py`, `tests/test_long_client_async.py`. These formerly encoded `outcome_mode`, the old branch types, or in-factory synthesis. No test file was deleted.

All changes are uncommitted. No Lane A, R, or D file was edited.

# Commands and results

Working directory for Python suite commands: `prototype/temporal-factory`; for focused tests: `prototype/temporal-factory/tests`. Python executable: `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python`.

1. `/usr/bin/lockf -k /tmp/exo-qual-suite.lock /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest discover -s tests` — **failed initially**: 110 tests ran, one broker budget fixture error because this worktree's offline Node dependencies were absent.
2. `npm --prefix prototype/temporal-factory/broker ci --offline --cache /tmp/exomachina-pi-strands-debate/npm-cache` — **passed**; 85 packages added, zero vulnerabilities.
3. The same locked Python suite command — **passed baseline**, 112 tests OK.
4. `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m compileall -q src` — **passed**.
5. `/usr/bin/lockf -k /tmp/exo-qual-suite.lock /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest test_report_contract test_report_async test_authoring test_authoring_budget test_binding test_contract test_interpreter test_long_client_async` — **passed final owned tests**, 49 OK.
6. `/usr/bin/lockf -k /tmp/exo-qual-suite.lock npm --prefix prototype/temporal-factory/broker test` — **passed**, 18 tests.
7. `/usr/bin/lockf -k /tmp/exo-qual-suite.lock /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest discover -s tests` — **failed after**, 118 tests run, exactly two errors in files owned by other lanes (details below); output `/tmp/exo-sf-interp-suite-final.log`.
8. `node --check prototype/temporal-factory/broker/testing/mock-codex.mjs` and `git diff --check` — **passed**.
9. `lsof -nP -iTCP:44520 -iTCP:32510 -iTCP:44872 -iTCP:45720-45739 -iTCP:46450-46469 -sTCP:LISTEN` — no listeners (exit 1 means no matches). `ps -axo pid=,command= | rg 'Exomachina-sf-interp|exo-sf-interp-unit'` showed only the inspection shell/rg. All processes started by tests are stopped.

# Test counts and gaps

- Python baseline: **112/112 pass** after offline dependency install. Final: **116 pass, 2 errors of 118**. The six added tests cover report schema/packet constraints and async A2A research, synthesis, repair, Quality acceptance, and incidents for wrong candidate revision, wrong sha256, mismatched reviewer, or reviewer equal to author. Owned focused tests: **49/49 pass**. Broker: **18/18 pass**.
- Cross-lane conflict: `tests/test_harness_modes.py::DirectorBoundaryTests.test_publication_activates_without_starting_runner` (Lane D) still calls `materialize(template, bindings)` with no required `evidence_packet`. `tests/test_testbed.py::TestbedTests.test_template_materializes_and_validates_with_generated_bindings` (Lane A) still validates a package built from the old template without `evidence_packet`. Their owners must update those tests to the report packet path. I stopped at this ownership boundary; a compatibility fallback would reaccept the removed product graph.
- No live broker or live agent was used. The async tests use in-process loopback A2A servers on 46452–46455 and journal/snapshot state under `/tmp/exo-sf-interp-unit-*`. Those directories are preserved. The release fixture and end-to-end Temporal scenario were not run by this lane; integrated scenario work belongs to Lane D and the orchestrator.
- The quality rubric digest is checked against `quality_policy.json` when present. The in-test stub uses a fixed synthetic rubric digest. Live role behavior and source-excerpt fidelity require integrated Lane R/A tests.

# Fix 2 — registered Director wait follow-up

- Updated `broker/testing/mock-codex.mjs` so the exact R3-d follow-up, including its case-insensitive `waiting for a Director decision` and `review the run and decide` wording, calls `inspect_run` before `decide_wait`. The existing `answer the director wait` and `abort the waiting run` matches remain. This changes only the synthetic mock.
- Added `broker/test/director-mock.test.mjs`. It sends the exact registered text through the director script on loopback port 46460, replays the `inspect_run` result, and asserts `decide_wait(action="abort", revision="r3", sha256=<inspected sha>)` with no `start_research` call.
- `node --check broker/testing/mock-codex.mjs` and `node --check broker/test/director-mock.test.mjs` passed. The focused Node test passed; `/usr/bin/lockf -k /tmp/exo-qual-suite.lock npm --prefix broker test` passed **19/19** tests.
- `/usr/bin/lockf -k /tmp/exo-qual-suite.lock /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest discover -s tests` ran **142 tests with one error**. The remaining Lane A `test_testbed.TestbedTests.test_template_materializes_and_validates_with_generated_bindings` fixture calls `validate` with a package missing the required `evidence_packet`; Lane D's earlier harness error is resolved in this integrated tree. No live broker was used.
