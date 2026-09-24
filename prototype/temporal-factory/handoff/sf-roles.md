# Lane R handoff

## Status

Complete for the bounded repair job. The role implementation and 12 focused tests pass; the full unit suite passes 124 tests. `packets/` and `scenarios/sf_stimuli.json` remain as committed in `21a4b9d`.

The previous attempt stopped after a patch containing both Delete File and Add File for `services/agent_roles.py` failed patch verification. Its interface stubs, packet and stimuli were committed as `21a4b9d`. This repair used Update File hunks for the existing file and proceeded under the clarified stop rule. During this repair, the first focused test invocation found a syntax error (`== not`); the next found an assertion failure because the research system prompt omitted the literal capability. Both were corrected before the passing runs.

## Files changed

- `services/agent_roles.py`: prompts, strict JSON parsers, Quality precheck, scripted research/synthesis/Quality replies, and structured usefulness checker. Frozen exported names and method signatures remain.
- `tests/test_agent_roles.py`: 12 tests for each result schema, invalid data, precheck, verdict consistency and binding, scripted route acceptance/rejection/repair, usefulness, and all 10 packet excerpts against the pinned commit.
- `handoff/sf-roles.md`: this record.

## Commands and results

- Earlier attempt: `git show 2d609e3:prototype/temporal-factory/QUALIFICATION.md` and packet generation passed (`packet bytes 6019 items 10 stimuli written`); the Delete File + Add File patch failed verification, with no modification from that patch.
- From the roles worktree, `git log -1 --oneline && git status --short && command -v lockf` — pass: `21a4b9d`, branch clean at start, `/usr/bin/lockf` present.
- From the roles worktree, `npm --prefix prototype/temporal-factory/broker ci --offline --cache /tmp/exomachina-pi-strands-debate/npm-cache` — pass: 85 packages added; 0 vulnerabilities.
- From `prototype/temporal-factory`, `/usr/bin/lockf -k /tmp/exo-qual-suite.lock /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest discover -s tests` — first attempt failed: 110 tests, 1 `setUpClass` error because the offline Node dependencies were not yet installed. After `npm ci`, baseline passed: 112 tests in 19.887s. Final run passed: 124 tests in 19.335s.
- From `prototype/temporal-factory`, `/usr/bin/lockf -k /tmp/exo-qual-suite.lock /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest discover -s tests -p test_agent_roles.py -v` — initial run failed to import due to syntax error; next run had 11 pass and 1 prompt assertion failure; final run passed all 12 tests in 0.054s.
- From the roles worktree, `git diff --check && git status --short && git diff --stat` — pass at the implementation stage; no whitespace errors. No commit, push, merge, rebase or branch switch was made.

## Tests

Before the new test file: 112 tests, pass (after the required offline broker install). After: 124 tests, pass. The focused role suite: 12 tests, pass. The excerpt test runs `git show 2d609e3:<source path>` and compares each cited line range byte-for-byte as text with every packet item's `text`.

## Contract questions

- `usefulness_check(content, packet)` returns `{"ok": bool, "reasons": [...]}`, not a boolean. Callers must use the `ok` field. This repairs the earlier stub's boolean annotation.
- `policy_digest` in the Quality brief is a pinned policy digest, separate from `RUBRIC_DIGEST`. The parser validates its digest format and writes the local rubric and rubric digest into the verdict. A model may reply with just `accepted` and `findings`; the parser binds the full `quality_verdict@1` envelope to the supplied candidate and reviewer. A full, correctly bound envelope is also accepted.

## Gaps

- These are unit-tested role behaviors. The independent agent service, integrated scripted scenario and live broker routes are owned by the other lanes and were not run here. No live model or credential was used.
- The usefulness checker verifies claim count, packet citations, headings and non-empty markdown. The human/model review still decides whether the prose actually answers the question and whether each claim is supported by its cited excerpt.
- No lane service, harness or runner process was started; no trial home was created.

## Fix 2 (review 1)

The worktree was at integration commit `8820d0f`. Review finding 1 is addressed by parsing the four required Markdown sections and requiring at least 25 prose words per section after stripping Markdown markup and code. Placeholder markers (`TBD`, `TODO`, `N/A`, `to be determined`, `placeholder`, `lorem ipsum`, and ellipsis-only bodies) produce reasons and fail the structural check. The checker still requires at least three claims with packet citations and non-empty Markdown. Its docstring states that semantic judgment is a separate labelled orchestrator reading, so `ok` alone is not R1-d semantic evidence. Scripted synthesis now emits sections long enough for this check. Tests cover the review's `TBD` example, short sections, fenced code, and a realistic report.

Review finding 4 remains exact-match synthetic route control. The `scripted_reply` docstring and Quality branch comment identify its scenario stimulus catalog read and state that it supports no independence or detection claim. The Quality system prompt now explicitly blocks a missing or placeholder-only required section; its JSON shape is unchanged.

Commands from `prototype/temporal-factory`, both under `/usr/bin/lockf -k /tmp/exo-qual-suite.lock` with `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B`:

- `-m unittest discover -s tests -p test_agent_roles.py -v`: 16 tests passed.
- `-m unittest discover -s tests`: 141 tests run, 2 errors, 139 passed. The errors are the known other-lane cases `test_harness_modes.DirectorBoundaryTests.test_publication_activates_without_starting_runner` (missing `materialize(..., evidence_packet=...)`) and `test_testbed.TestbedTests.test_template_materializes_and_validates_with_generated_bindings` (package missing `evidence_packet`). They were not changed here.

`git diff --check` passed. Only `services/agent_roles.py`, `tests/test_agent_roles.py`, and this handoff changed. No live broker, credentials, commit, push, or branch switch was used.
