# Review 2 checker fixes

## Fix: pure checker replay

24 September 2026. `check_evidence` now evaluates only fields in its argument. G-4 no longer builds a required scan path from the current worktree; it checks the recorded candidate manifest, scan inputs, scanner count, zero real hits, positive control, and (for new evidence) the collector's coverage fields. The collector still lists candidates at run time, hashes each file, scans them, and records the coverage. New repository paths in the manifest, scan inputs, report and history records, synthetic binding, and import audit are relative to the repo root. Preserved `scripted-1.json` through `scripted-7.json` were not changed.

R1-d now checks the report bytes SHA recorded when the collector writes the report; pre-schema evidence uses its saved Task text and report path. G-7 uses the collector's embedded synthetic record and SHA field, without opening the synthetic evidence file at check time. Neither check reads the present checkout, git state, or a report file. The Review 2 tests include a scripted-7 replay with its director-worktree prefix replaced in memory, while patched `Path` methods reject checker file reads.

The full Python suite under `/usr/bin/lockf -k /tmp/exo-qual-suite.lock` passed **163 tests** in this worktree. The focused checker suite passed **12 tests**. The first fresh synthetic attempt at `/tmp/exo-sf-syn-8` produced `scripted-8.json` with 24/24 checks passing, 239 candidate files, 2,600 files scanned, zero hits and a detected positive control. A post-run audit found eight absolute repo paths in its import-audit record. After normalizing that collector field, the preserved final attempt at `/tmp/exo-sf-syn-8-final` produced `scripted-8-final.json`: **24/24 checks pass**, 248 candidate files, 2,608 files scanned, zero hits, positive control detected, and no absolute director-worktree prefix in the evidence. The final evidence's checker SHA matches the current scenario file, and a disk-read-blocked replay reproduces every saved verdict and, after JSON normalization, the saved check records. No live provider was used.

24 September 2026. Fast-forwarded this worktree to integration `32834c2` before the new runs. Changed only `scenarios/single_factory.py`, `tests/test_harness_modes.py`, new `tests/test_sf_checks.py`, this handoff and new evidence. No `src/` edit, commit, push, branch switch or live provider run.

| Finding | Change | Regression probe |
| --- | --- | --- |
| F1 | `_quality_bound` binds a completed Quality Task and content/artifact SHA to its action, run, definition, pinned reviewer, confirmed journal Task, exact synthesis candidate, model decision and Task model calls. Used in R1-c/e, R2-b/d and every R3-b revision. R1-e requires exactly one accepted Quality verdict. | `test_f1_quality_candidate_task_and_journal` |
| F2 | SF-3 requires routes 1–3, unique caller/parent/child IDs, two exported histories each, one manifest/package/build, and the actual pinned enum. G-1 requires complete confirmed research, synthesis, Quality and release action counts with no incidents. G-7 hashes and reads the synthetic evidence file, checks its git commit, checker SHA, build, manifest and route inventory. | `test_f2_route_and_journal_completeness`, `test_f2_live_synthetic_evidence_binding` |
| F3 | Raw histories stay in each trial home's `evidence-raw/`. Exported histories digest every input, including decoded base64 start/child/update payloads, and actor/epoch/token/package/closure/bindings fields. Workflow summaries include raw/exported SHA-256 and redacted JSON paths. G-4 checks the Director token, read in process from its store, against evidence and candidates including decoded payloads; the token is never recorded. | `test_f3_base64_history_redaction_and_decoded_scan` |
| F4 | Lane I's factory text-only A2A and caller metadata fix arrived in integration `32834c2`; this lane did not edit product code. | Integration suite was reported at 151 OK before this work. |
| F5 | R1-d compares saved markdown bytes and original Task TextPart to the accepted synthesis markdown, and the release row digest and confirmed journal receipt to the synthesis content digest. A1 semantic reading stays `pending-orchestrator-reading`. | `test_f5_report_bytes_and_release_digest` |
| F6 | Structural forbidden-key audit covers caller messages, raw Temporal start/activity inputs, briefs, run inputs and Director arguments. R2-a/R3-a compare the whole stimulus log to route-2/r1 and route-3/every-revision Task/hash tuples, with no route-1 row. | `test_f6_structured_controls_and_whole_stimulus_log` |
| F7 | R3-d binds inspect and abort to the follow-up message, original Task/context, one turn and exact accepted sequence. R3-e requires turn counts 1/1/2 before the 4/4/90 limits. | `test_f7_follow_up_turn_binding_and_counts` |
| F8 | SF-2 pairs dispatch/poll events with preceding same-action pin verification. G-2/G-3 require the journal Task inventory in the right stores and bounded calls. Broker start/attach events and boundary PID samples show one process. Authoring and Director streams are counted in windows; live agent calls reconcile by session and count. | `test_f8_pin_task_and_broker_inventory` |
| F9 | G-4 records exact scan inputs, candidate path/SHA manifest, scanner file count and the separate positive-control scan. It requires all scan coverage and zero real hits. | `test_f9_scan_coverage` |
| F10 | Collector records each started service, harness, runner/worker and mock PID/PGID. G-5 requires each exited, valid stop results, an empty full port block and broker before/after PID equality. | `test_f10_cleanup_process_and_stop_results` |
| F11 | SF-1 checks author outcome model kind/provider/id/live and its broker stream count. | `test_f11_author_model_record` |

The 24 emitted checks are `SF-0..SF-3`, `R1-a..R1-e`, `R2-a..R2-d`, `R3-a..R3-e`, `G-1..G-5`, and `G-7`. **Every check passed in the saved evidence for each new attempt**; the collector was tightened between attempts, so attempt 7 is the final checker shape. G-6 remains the orchestrator suite gate. R1-d is structural only, with semantic reading pending under A1.

| Check | Attempt 5 | Attempt 6 | Attempt 7 |
| --- | --- | --- | --- |
| SF-0 | pass | pass | pass |
| SF-1 | pass | pass | pass |
| SF-2 | pass | pass | pass |
| SF-3 | pass | pass | pass |
| R1-a | pass | pass | pass |
| R1-b | pass | pass | pass |
| R1-c | pass | pass | pass |
| R1-d structural | pass | pass | pass |
| R1-e | pass | pass | pass |
| R2-a | pass | pass | pass |
| R2-b | pass | pass | pass |
| R2-c | pass | pass | pass |
| R2-d | pass | pass | pass |
| R3-a | pass | pass | pass |
| R3-b | pass | pass | pass |
| R3-c | pass | pass | pass |
| R3-d | pass | pass | pass |
| R3-e | pass | pass | pass |
| G-1 | pass | pass | pass |
| G-2 | pass | pass | pass |
| G-3 | pass | pass | pass |
| G-4 | pass | pass | pass |
| G-5 | pass | pass | pass |
| G-7 | pass | pass | pass |

These are each attempt's saved verdicts. Replaying attempts 5/6 under the final checker fails G-4 because those earlier evidence files did not retain the positive-control scan object; attempt 7 is the complete final evidence.

| Attempt | Preserved evidence and home | Saved per-check verdicts | Redaction, scan, cleanup |
| --- | --- | --- | --- |
| 5 | `evidence/single-factory/scripted-5.json`, `scripted-5-route{1,2,3}/`, route-1/2 reports; `/tmp/exo-sf-syn-5` | All 24 pass | Six histories redacted; decoded token hits 0; 12 recorded processes/groups exited; G-5 pass. |
| 6 | `evidence/single-factory/scripted-6.json`, `scripted-6-route{1,2,3}/`, route-1/2 reports; `/tmp/exo-sf-syn-6` | All 24 pass | Six histories redacted; decoded token hits 0; 12 recorded processes/groups exited; G-5 pass. |
| 7 | `evidence/single-factory/scripted-7.json`, `scripted-7-route{1,2,3}/`, route-1/2 reports; `/tmp/exo-sf-syn-7` | **All 24 pass on final checker** | Six histories redacted; decoded token hits 0; 12 recorded processes/groups exited; G-5 pass. |

Attempt 7 scanned 2,591 files, including a 230-file candidate manifest, with zero real credential hits. Its separate positive control detected both planted files. An independent decoded scan of all exported histories in attempts 5–7 also found zero Director-token hits. Use `--synthetic-evidence evidence/single-factory/scripted-7.json` for a later live run; the CLI default still points at old `scripted-1.json` and was not changed after the final run because G-7 binds the checker SHA.

Verification: `/usr/bin/lockf -k /tmp/exo-qual-suite.lock .../.venv/bin/python -B -m unittest tests.test_harness_modes tests.test_director_agent tests.test_sf_checks` → **26 OK**; `git diff --check` → clean. The Review 2 probes start from preserved attempt-7 observations and change one decisive field at a time.

## Cross-lane needs

No new product field is needed for these checkers. The release store persists the validated content digest rather than the HTTP body; F5 compares that digest, the confirmed journal receipt and the synthesis content bytes. A1's semantic R1-d reading remains with the orchestrator.

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

This section describes the state before the preflight assignment. The four scripted attempts and their per-check verdicts are recorded in **Synthetic preflight** below. R1-d's automated verdict is explicitly structural; semantic reading remains pending.

# Gaps and trial state

- Before this preflight assignment, no scenario home or evidence attempt had been created. The live provider remains unrun. No broker login, refresh or logout was run, and no live broker request was made.
- The full Python suite and Node broker suite were left to integration under the shared lock because their older tests use ports outside lane D's block. There is no new Node test count to report.
- The collector was first exercised end to end in the four scripted attempts below; the preserved raw failures precede each correction.

# Synthetic preflight

24 September 2026. Four scripted attempts were run, each with a fresh home. No live provider was used. Raw evidence is retained as written by each attempt; the fourth attempt is **not** an observed all-pass run. `G-6` is the separate orchestrator suite gate and is not emitted by this scenario.

| Attempt, integration | Preserved evidence and home | Raw per-check verdicts |
| --- | --- | --- |
| 1, `8023634` | `evidence/single-factory/scripted-1.json`; `/tmp/exo-sf-syn-1` | Pass: G-5. Fail: SF-0, SF-1, SF-2, SF-3, R1-a, R1-b, R1-c, R1-d, R1-e, R2-a, R2-b, R2-c, R2-d, R3-a, R3-b, R3-c, R3-d, R3-e, G-1, G-2, G-3, G-4, G-7. Preflight stopped before provisioning. |
| 2, `8023634` | `evidence/single-factory/scripted-2.json`, `scripted-2-route1/`; `/tmp/exo-sf-syn-2` | Pass: SF-0, R1-a, R3-e, G-5. Fail: SF-1, SF-2, SF-3, R1-b, R1-c, R1-d, R1-e, R2-a, R2-b, R2-c, R2-d, R3-a, R3-b, R3-c, R3-d, G-1, G-2, G-3, G-4, G-7. Route 1 completed; route 2 stopped at stimulus HTTP 401. |
| 3, `6b89117` | `evidence/single-factory/scripted-3.json`, `scripted-3-route1/`, `scripted-3-route2/`, `scripted-3-route3/`; `/tmp/exo-sf-syn-3` | Pass: SF-0, R1-a, R3-e, G-5. Fail: SF-1, SF-2, SF-3, R1-b, R1-c, R1-d, R1-e, R2-a, R2-b, R2-c, R2-d, R3-a, R3-b, R3-c, R3-d, G-1, G-2, G-3, G-4, G-7. Routes 1 and 2 completed; route 3's registered follow-up left the original Task waiting. The old collector blocked on `parent.result()` after its 180-second follow-up poll, so I sent SIGINT to the scenario process. Its `finally` stopped all services and wrote the failed evidence. Missing collection makes most raw check failures non-diagnostic. |
| 4, `172aa1a` | `evidence/single-factory/scripted-4.json`, `scripted-4-route1.md`, `scripted-4-route2.md`, `scripted-4-route1/`, `scripted-4-route2/`, `scripted-4-route3/`; `/tmp/exo-sf-syn-4` | Pass: SF-0, SF-3, R1-a, R1-c, R1-d (structural only; semantic reading pending), R1-e, R2-a, R2-b, R2-c, R2-d, R3-a, R3-b, R3-c, R3-d, R3-e, G-1, G-3, G-4, G-5. Fail: SF-1, SF-2, R1-b, G-2, G-7. All three routes completed with accepted, accepted, aborted outcomes and zero incidents. These five failures came from collector predicates and fields, below. |

Own-file fixes, in `scenarios/single_factory.py`:

- Before attempt 1, a CLI rejection exposed that `/tmp` resolves to `/private/tmp` on macOS. The fresh-home guard now compares resolved parents. Attempt 1 then stopped at broker status because this worktree lacked broker dependencies; `npm --prefix prototype/temporal-factory/broker ci --offline --cache /tmp/exomachina-pi-strands-debate/npm-cache` installed 85 packages before attempt 2.
- After attempt 2, the stimulus POST and log GET now send the service's fixture bearer header. The missing header caused the observed 401.
- After attempt 3, a nonterminal route-3 follow-up raises a bounded error and preserves the route snapshot; `parent.result()` is also bounded to 60 seconds. This prevents another indefinite wait.
- After attempt 4, SF-1 passes the actual synthetic provider to `authoring_acceptance`, counts authoring broker `stream` calls (4 in attempt 4), and records the published package's bindings. The original collector passed `codex-subscription`, set `model_calls: null`, and looked for bindings in catalog metadata rather than the package.
- After attempt 4, SF-2/R1-b pin verification permits one card/contract verification to precede both the send and the poll in one adapter iteration, while requiring another verification after each prior poll. Attempt 4 recorded matching identity, card and contract digests for every action; the old predicate incorrectly required as many verification log rows as send plus poll rows.
- After attempt 4, G-2 requires one nonempty broker PID throughout the run and the same before/after PID. Attempt 4 started and stopped the synthetic broker, so both before and after were `null` and all four during samples were PID `20130`. The old collector also lacked the authoring call count.

An in-memory pure `check_evidence` replay of **the preserved attempt-4 observations**, supplying only the four collector corrections above, returned 24/24 passing check predicates, including G-7. This is a derived checker result, **not a fifth scenario run** or an observed structural-pass evidence file. The 4-attempt limit prevents verifying the edited collector in a new home. R1-d semantic reading remains an orchestrator judgment under A1.

Cross-lane diagnoses:

- At `8023634`, `services/release_server.py:21` imported `fixture` from `src`, violating A1's running-service import audit. Observed: `service_clean: false`; expected: no `src` import except `model_broker` for running services. Lane A fixed this in integration `6b89117`; attempt 4 recorded `service_clean: true`.
- At `8023634` and `6b89117`, `broker/testing/mock-codex.mjs:205` matched only `answer the director wait|abort the waiting run`. Observed in attempt 3: registered follow-up produced a rejected `start_research` call and left the original Task `input-required`; expected: `inspect_run` followed by accepted `decide_wait(abort, r3, sha)`. Lane I fixed the matcher in `172aa1a`; attempt 4 passed R3-d. The harness inspection result uses the `current_revision` and `current_sha256` keys consumed by that mock.

Verification after the collector edits: `python -B -m unittest tests.test_director_agent tests.test_harness_modes` ran 15 tests, OK; `git diff --check` passed. The orchestrator reported the integrated full Python suite at 147 OK on `6b89117`; this lane did not rerun that suite. All four attempts recorded G-5 pass. A final independent scan of all 60 ports in lane D's runner, member, harness, service and mock blocks returned `listeners: []`; a process listing found no process with a path under `/tmp/exo-sf-syn-1` through `-4` (including their `/private/tmp` forms). No commit, push or branch switch was made.
