# Contract questions

- **Cross-lane mock conflict:** Lane I's `--script director` handles old wait wording (`answer the director wait` or `abort the waiting run`), while route 3 must send exactly `The factory is waiting for a Director decision on this request. Please review the run and decide.` The mock would call `start_research` on that follow-up. Lane I owns the mock; the synthetic scenario cannot pass R3-d until its script recognizes the registered text. The scenario starts `--script authoring` for publication, then restarts the loopback mock on the same port with `--script director`; the broker process remains the same.
- **Cross-lane import audit conflict after A1:** The amended SF-2 audit covers only running report service modules `model_agent.py`, `agent_roles.py`, and `release_server.py`, and records `testbed.py`'s `agent_binding` and `agent_roles` imports as the launcher exception. It now flags only `services/release_server.py` importing `fixture`. Lane A owns that file and A1 says it will inline those helpers. I left it untouched; the current integrated tree would fail SF-2.
- The Lane A/R contracts are now present. `sf_stimuli.json` has `route2` and `route3` keys, `/_test/stimulus-log` returns a list, `model_agent.py` uses `model-agent.sqlite3` with `created_at` and `updated_at`, and `usefulness_check(content, packet)` returns `{ok, reasons}`. The scenario uses these shapes.
- Amendment A1 was read from `8820d0f` without merging branches. R1-d now emits only a structural pass/fail with semantic status `pending-orchestrator-reading`; the scenario saves the report path and packet ids for the orchestrator's separate reading. Agent SQLite `tasks`, `model_calls`, and `stimulus_log` are copied read-only into evidence. Stimulus verdicts bind every `sha256_after` to the final Task artifact and require one row per Task/revision. Scripted Quality verdicts are labelled `scripted route control`.

# Files changed

- `src/director_agent.py`: `start_research(question)` only; wait prompt describes the pinned `repair_exhausted` abort and inspection first; elapsed turn time recorded; 4/4/90 limits retained.
- `src/harness.py`: `inspect_bound_run` returns only phase, revision/hash, repair count, pinned max repairs, last Quality findings and wait deadline. Accepted `verified_report@1` projects as one TextPart plus DataPart; aborted Tasks have no artifact; legacy fixture DataPart remains.
- `scenarios/single_factory.py`: new CLI, collector, cleanup, leak scan, history export and pure verdicts for SF-0..SF-3, R1-a..e, R2-a..d, R3-a..e, G-1..G-5 and G-7, updated for A1. G-6 is an orchestrator suite gate, outside the requested scenario checks.
- `tests/test_director_agent.py`: question-only tool and audit assertions.
- `tests/test_harness_modes.py`: report/abort and inspection projections, checker missing-evidence and decisive-record tests; test harness ports moved into lane D's block. Its publication test now uses Lane I's `report-template.json`, report bindings, and the committed packet with `materialize(..., evidence_packet=packet)`; it passed after the Lane I merge.

# Verification

Working directory for commands below: `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina-sf-director/prototype/temporal-factory`.

- Before: 10 test methods in the two owned modules at `HEAD` (5 + 5), established with `git show HEAD:prototype/temporal-factory/tests/test_director_agent.py | rg '^    def test_' -c` → `5` and the matching `test_harness_modes.py` command → `5`. The documented prior full suite count is 112; I did not run the full suite because other modules open ports outside lane D's block.
- `/usr/bin/lockf -k /tmp/exo-qual-suite.lock /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest tests.test_director_agent tests.test_harness_modes` → 11 tests OK after the first projection test; then 13 OK after pure checker tests; then 14 tests FAILED (one test fixture omitted the synthesizer SHA needed by R1-d); then 14 OK after correcting that fixture; after A1 checker changes: **15 tests OK** in 2.581 s, including the report publication test.
- `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B scenarios/single_factory.py --help` → exit 0; CLI options printed.
- `git diff --check` → exit 0, no output.
- `PYTHONPATH=scenarios /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -c 'from single_factory import _import_audit; print(_import_audit())'` → after A1, `product_clean: True`, `service_clean: False`, launcher exception allowed; the remaining cross-lane violation is `release_server.py` importing `fixture`.
- `PYTHONPATH=scenarios /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -c 'from single_factory import _listen; print(_listen([*range(44540,44553),*range(32520,32525),44874,44875,*range(45780,45800),*range(46510,46530)]))'` → `[]`; no lane D listeners remain. The agent-mode test's server was stopped by its cleanup.
- `git status --short` → only the five owned source/test files above were changed or added, plus this handoff; all changes remain uncommitted.

# Pre-registered verdicts and evidence

SF-0..SF-3, R1-a..R1-e, R2-a..R2-d, R3-a..R3-e, G-1..G-5 and G-7 are **not run**. They have one verdict object each in `check_evidence`, but no observed-real or observed-synthetic scenario result exists on this lane. R1-d's automated verdict is explicitly structural; semantic reading remains pending and is never emitted as a pass. The pure checker assertions are unit-tested in `tests/test_harness_modes.py`. The orchestrator has not requested the synthetic scenario run; the two cross-lane conflicts above would prevent an all-pass result.

# Gaps and trial state

- No scenario home or evidence attempt was created. Neither the synthetic nor the live provider was run. No broker login, refresh or logout was run, and no live broker request was made.
- The full Python suite and Node broker suite were left to integration under the shared lock because their older tests use ports outside lane D's block. There is no new Node test count to report.
- The collector was updated against the integrated Lane A/R schema but has not been exercised end to end. Any runtime mismatch must be preserved as a failed attempt before adjustment.
