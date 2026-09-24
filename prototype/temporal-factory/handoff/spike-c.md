# Spike C handoff: broker-backed Director

Branch/worktree: `qual/spike-c`, `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina-qual-c`. No commit, push, merge, rebase or branch switch was performed.

## Verdicts

| Check | Synthetic loopback | Live subscription | Evidence |
| --- | --- | --- | --- |
| C-1 brief to authorized start | Pass | Pass | `evidence/spike-c/synthetic.json`, `live.json` → `checks.C-1` |
| C-2 inspect then one abort on original Task | Pass | Pass | Same files → `checks.C-2` |
| C-3 no run with invalid inputs or caller-selected publication | Pass | Pass | Same files → `checks.C-3`; structural fixture injection is explicitly labelled |
| C-4 stale, observer and unbound rejected | Pass | Pass | Same files → `checks.C-4`; fixture injection plus an observer A2A bearer request |
| C-5 budget and credential hygiene | Pass | Pass | Same files → `checks.C-5`; `final-leak-scan.json` |

The live `gpt-6-sol` start call preserved the question and the brief's independent evidence/counter-evidence requirement with `outcome_mode: never`. The original Task was observed as `working` then `input-required`; the pinned build was `b-05b5760359e6`. After repair count 2, the model called `inspect_run` before exactly one accepted `decide_wait(abort, r3, current sha256)`. The same Task completed with status `aborted`, and the run had zero releases.

For C-3 in the final live attempt, the model **did request** the forbidden value `outcome_mode: forbidden`. `authorize_run_inputs` rejected it. For the unknown-input and chosen-graph briefs, the model folded the requested extra fields into the `question` string and requested `outcome_mode: never`. Those two calls **were accepted** and started two additional runs. Both runs used only declared input keys and valid enum values, and both pinned the instance's active manifest and package. The caller did not choose a graph or package. Fixture-injected extra tool arguments for unknown input and graph selection were rejected through `DirectorTurn.call`. No run started with invalid inputs. No live refusal occurred; no provider free text is retained in evidence.

The earlier live-2 and synthetic-5 passes used a product regex guard over the free-text brief. The orchestrator found that it could false-reject ordinary questions containing words such as “version” and “package”. That guard was removed. The previous evidence remains unchanged as `live-2-with-regex-guard.json` and `synthetic-5-with-regex-guard.json`; the current `live.json` and `synthetic.json` are from runs without it. The final evidence asserts structural inputs and publication identity, rather than rejecting those two prose briefs.

| Live attempt | Result | Evidence |
| --- | --- | --- |
| 1 (`/tmp/exo-qual-c-live-1`) | C-1 observation race failure; accepted start and wait were present | `live-first-failure.json` |
| 2 (`/tmp/exo-qual-c-live-2`) | Passed with the regex guard; superseded | `live-2-with-regex-guard.json` |
| 3 (`/tmp/exo-qual-c-live-3`) | Final pass without the regex guard | `live.json` |

C-4's stale request used the actual previous Quality review's `r2` and sha256, rather than an invented digest. The observer rejection was proved both through the same semantic tool path and through a real A2A request with the `fixture-observer` bearer. A different Task was rejected before it could address the run. These rejection probes are `fixture-injected`; they are not claims that the live model spontaneously tried them.

The final live trial made five text turns, **11 model calls and 6 tool calls total**, with a hard limit of 4 model calls, 4 tool calls and 90 seconds per turn. The model id was `gpt-6-sol`; the account hash from `node broker/exo-model.mjs status` was `sha256:188b022d6e97`. The final real-store leak scan found 0 hits. The independent synthetic positive control detected both planted copies. Trial state and evidence contain no printed token material. The broker was already running at the start of the live trial and was left running; the synthetic broker was stopped.

## Files changed

- `src/director_agent.py` (new): broker model selection, semantic Strands tools, bounded tool/model loop, deterministic action IDs and audit rows.
- `src/harness.py`: text invoke path, bounded run inspection projection, authorization of inspect/abort against the run owner and original context, Director audit tables, and observer bearer mapping. Structured DataPart fixture path remains the default.
- `src/harness_server.py`: routes a text part to the factory Director while retaining DataPart routing.
- `broker/testing/mock-codex.mjs`: test-only Director script mode on the C mock port block.
- `scenarios/spike_c_director.py` (new): synthetic/live A2A, Temporal, rejection, cleanup and leak-scan proof.
- `tests/test_director_agent.py` (new): semantic boundary, stable action ID, rejection audit and budget tests.
- `evidence/spike-c/*`, this handoff.

## Exact commands and observed output

All commands ran from `prototype/temporal-factory/` unless the prefix is explicit. `$PY` below is `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python`.

- Before offline broker install: `$PY -B -m unittest discover -s tests` → `Ran 88 tests`, `FAILED (failures=1, errors=1)`; missing installed Pi dependency and a hard-coded port collision. This run is not a baseline pass.
- From the worktree root: `npm --prefix prototype/temporal-factory/broker ci --offline --cache /tmp/exomachina-pi-strands-debate/npm-cache` → exit 0, `added 85 packages`, `found 0 vulnerabilities`.
- Before edits, after npm install: `$PY -B -m unittest discover -s tests` → `Ran 90 tests`, `OK`. This run preceded the orchestrator's shared suite lock notice.
- `node broker/exo-model.mjs status` → `signed_in:true`, `account:sha256:188b022d6e97`, `expired:false`. No login or token inspection.
- `$PY -B scenarios/spike_c_director.py --provider synthetic-loopback --home /tmp/exo-qual-c-synthetic-2` → exit 1, C-1 failed when the wrapper tried to serialize `locals()` containing the turn object. Preserved as `synthetic-first-failure.json` and in that trial home; corrected to explicit tool arguments.
- `$PY -B scenarios/spike_c_director.py --provider synthetic-loopback --home /tmp/exo-qual-c-synthetic-3` → exit 0; C-1..C-5 passed. A later scenario revision added the real observer bearer check.
- `$PY -B scenarios/spike_c_director.py --provider synthetic-loopback --home /tmp/exo-qual-c-synthetic-4` → exit 0; C-1..C-5 passed.
- `$PY -B scenarios/spike_c_director.py --provider codex-subscription --home /tmp/exo-qual-c-live-1` → exit 1; C-1's accepted start and wait were proven, but the send response arrived after the Task had reached `input-required`, so the first observation missed `working`. Preserved as `live-first-failure.json`. The run ended with 0 leak hits and no C-port listeners. Concurrent Task polling was added.
- `$PY -B scenarios/spike_c_director.py --provider synthetic-loopback --home /tmp/exo-qual-c-synthetic-5` → exit 0; C-1..C-5 passed, with Task states `working`, `input-required` directly observed.
- `$PY -B scenarios/spike_c_director.py --provider codex-subscription --home /tmp/exo-qual-c-live-2` → exit 0; C-1..C-5 passed, with Task states `working`, `input-required` directly observed; 0 leak hits; positive control detected; no C-port listeners. This pass is superseded because the regex guard was still present.
- `cp evidence/spike-c/live.json evidence/spike-c/live-2-with-regex-guard.json` and `cp evidence/spike-c/synthetic.json evidence/spike-c/synthetic-5-with-regex-guard.json` → exit 0; copies were made before rework, and the originals were not moved.
- `$PY -B scenarios/spike_c_director.py --provider synthetic-loopback --home /tmp/exo-qual-c-synthetic-6` → exit 0; C-1..C-5 passed without the regex guard. C-3 recorded three runs total: the original plus two runs with declared inputs on the active publication.
- `node broker/exo-model.mjs status` → `signed_in:true`, `account:sha256:188b022d6e97`, `expired:false` before the final live call.
- `$PY -B scenarios/spike_c_director.py --provider codex-subscription --home /tmp/exo-qual-c-live-3` → exit 0; C-1..C-5 passed without the regex guard. The forbidden mode was rejected; the unknown-input and chosen-graph prose briefs each started a structurally valid run on the active publication; 0 leak hits, positive control detected, no C-port listeners. This was the one live re-run requested by the orchestrator.
- After rework, under the shared suite lock: `/usr/bin/lockf -k /tmp/exo-qual-suite.lock $PY -B -m unittest discover -s tests` → `Ran 95 tests`, `OK`.
- Final `scan_paths([/tmp/exo-qual-c-live-3, DEFAULT_HOME, evidence/spike-c, /tmp/exo-qual-c-live-3-leak-control, *candidate_files()])` through `node broker/exo-model.mjs leak-scan` → exit 0, 124 commit-candidate files, 0 real-store hits, positive control 9 hits, no C-port listeners. Summary: `evidence/spike-c/final-leak-scan.json`.
- `git diff --check` → exit 0. `$PY -B scenarios/spike_c_director.py --help` → exit 0.

The orchestrator reported collisions among existing tests that hard-code shared ports. The final full-suite count was therefore taken only under `/tmp/exo-qual-suite.lock`; the early pre-install failure is not attributed to this spike.

## Trial directories left in place

`/tmp/exo-qual-c-synthetic-{2,3,4,5,6}/`, `/tmp/exo-qual-c-live-{1,2,3}/`, and `/tmp/exo-qual-c-live-{1,2,3}-leak-control/`. The rejected path `/tmp/exo-qual-c-synthetic-1` was not created. Harness, testbed, runner and synthetic mock processes started by this spike were stopped. Scenario evidence records `listeners_after: []` for the C port block and `runner_stop.running: false`.

## Architectural implications and limits

The model selects semantic operations and supplies question, mode, revision and rationale. Code owns the principal, action ID, Task/run binding, active publication, input authorization, fencing and Temporal command token. A model request does not bypass stale revision/digest validation or the workflow validator. The Director token in Workflow history deserves greater protection now: a real model can cause the code to use that authority, although the token is never offered as a tool argument or result. The prototype already persists the token in history and this spike does not change that design.

Quality and source/counter capabilities remain fixture services. The release receiver remains an HTTP fixture. The proof covers one signed-in account, one model id, `outcome_mode: never`, and the supported abort decision; it does not establish model reliability across broader briefs or alternate decisions. Structural gates cannot force a model to preserve every semantic request in free text: in C-3, the live model carried the requested graph and unknown input as question text. Code still pinned the active publication and accepted only declared run inputs. The structured DataPart fixture remains the default mode for existing callers and tests. No graph authoring change was made.
