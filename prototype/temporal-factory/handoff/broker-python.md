# Python broker and authoring budget handoff

Status: review findings #2–#7 and the two named test gaps are addressed. No commit or real OpenAI contact. Other workers' files were left alone.

## Files changed

- `src/model_broker.py`: absolute model home; daemon-thread broker startup await so the session deadline returns promptly; successful losing `serve` process keeps polling; ordered text/tool-result replay with explicit rejection of unsupported blocks; fixed-text structured subscription errors, non-retried quota/auth classes, and rate-limit throttling.
- `src/authoring.py`: provider failures return `failed` with structured `error`; budget aborts after throttling carry `last_provider_error`; post-round-cap tools allow only revalidation or submission of an accepted valid digest; outcome includes `selection_seconds`.
- `src/admin.py`: records provider selection time in top-level output and outcome; `failed` exits 2 without publication.
- `tests/test_model_broker.py`: replay order and rejection, error mapping and sanitization, exited-attacher race, absolute home, actual unset default provider with both API keys, and selection timing regressions.
- `tests/test_authoring_budget.py`: startup deadline, explicit socket `cancel` on deadline, quota/auth failure and no retry, throttling error retained on abort, round-cap vocabulary rejection, and admin failed/no-publication regressions.
- `handoff/broker-python.md`: this report.

## Verification

Commands ran from `prototype/temporal-factory/` with `PY=/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python`:

- `$PY -B -m unittest tests.test_model_broker tests.test_authoring_budget -q` — latest targeted run PASS, `Ran 29 tests in 8.571s`, `OK` (before the final admin regression was added; the final full suite includes it).
- `$PY -B -m unittest discover -s tests` — initial run FAIL, `Ran 79 tests in 8.794s`, one unrelated `test_harness_modes.AgentModeTests.test_a2a_agent_mode_survives_restart_without_runner` incarnation assertion. A repeat PASS, `Ran 79 tests in 11.078s`. Final run after all edits PASS, `Ran 80 tests in 11.151s`, `OK`.
- `git diff --check -- prototype/temporal-factory/src/model_broker.py prototype/temporal-factory/src/authoring.py prototype/temporal-factory/src/admin.py prototype/temporal-factory/tests/test_model_broker.py prototype/temporal-factory/tests/test_authoring_budget.py` — PASS, no output.

The Strands runtime prints `exception=<AuthoringBudgetExhausted> | event loop cycle failed` for expected budget terminations; tests and outcomes pass. The first startup-deadline regression exposed that `asyncio.to_thread` still delayed return during Strands event-loop shutdown; the daemon-thread startup await fixed it.

Retained synthetic mock evidence includes `/tmp/exo-proto-budget-r27pjvn0/mock-requests.jsonl`, `/tmp/exo-proto-budget-qkhydsuh/mock-requests.jsonl`, and `/tmp/exo-proto-budget-po3ihzrz/mock-requests.jsonl`. Additional fixture state remains under `/tmp/exo-proto-budget-*/` and `/tmp/exo-proto-pybroker-*/`.

Known gap: no real subscription request was made. Explicit Anthropic/OpenAI API constructors were not exercised; those provider paths were outside this review.
