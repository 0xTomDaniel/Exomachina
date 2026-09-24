# Python broker and authoring budget handoff

Status: complete for the assigned Python lane. No commit or push was made. No real provider was contacted.

Orchestrator review fixes: evaluated draft digests can be revalidated or submitted at the round cap while budgets remain; a new fifth digest aborts before evaluation, and an exhausted set with no valid draft aborts on any tool. `admin.py author` now accepts `--max-rounds`, `--max-model-calls`, `--max-tool-calls`, and `--deadline-seconds`, echoes them in top-level and outcome `limits`, and still exits 2 without publication on abort. Python broker event records use `caller_pid` for the Python process identity.

## Files changed

- `src/model_broker.py` (new): lazy install-wide Node broker attach/spawn, readiness and health, reason events, stop, Strands Pi adapter, strict content indices, reasoning replay, error mapping, cancel.
- `src/authoring.py`: broker-only default subscription selection, explicit API provider checks, fixture-only loopback selection, model record labels, hard authoring budget and structured abort.
- `src/admin.py`: `model-status`, selected provider and refusal reason in `author`, exit code 2 on budget abort before publication.
- `tests/test_model_broker.py` (new): Python Unix socket fake, model loop, replay, errors, cancel, attach, selection refusals with zero connections.
- `tests/test_authoring_budget.py` (new): Python runaway model for all four abort reasons; valid fourth-round submission and fifth-distinct-draft rejection; real Strands → PiBrokerModel → Node broker → runaway mock; two independent Python subprocesses sharing one broker with isolated concurrent sessions; CLI flags and abort/no-publication test.
- `tests/test_authoring.py`: updated model selection and round-cap expectations.

## Verification

Commands ran from `prototype/temporal-factory/` with `PY=/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python`:

- `$PY -B -m unittest tests.test_model_broker -v` — PASS, `Ran 10 tests in 0.049s`, `OK`.
- `$PY -B -m unittest tests.test_authoring_budget -v` — PASS, latest run `Ran 9 tests in 4.482s`, `OK`.
- `$PY -B -m unittest discover -s tests` — PASS, latest run `Ran 69 tests in 7.349s`, `OK`.
- `git diff --check -- prototype/temporal-factory/src/admin.py prototype/temporal-factory/src/authoring.py prototype/temporal-factory/tests/test_authoring.py` — PASS, no output.

Earlier development runs failed on a syntax error in the new socket request, a fake socket path mismatch, and Strands wrapping budget exceptions in `EventLoopException`. Those were fixed before the passing runs above. Expected Strands `event loop cycle failed` diagnostics remain visible when the budget exception ends a runaway loop; the outcomes are `aborted` with the required reason and counts.

Retained evidence: `/tmp/exo-proto-budget-8gi48ayz/mock-requests.jsonl` records six requests from the two-process race, three per distinct session. The latest four budget cases retained mock request logs with counts 2, 3, 3, and 1 under `/tmp/exo-proto-budget-255d_mg2/`, `/tmp/exo-proto-budget-o6y_veh2/`, `/tmp/exo-proto-budget-iq3589ll/`, and `/tmp/exo-proto-budget-2_udifqb/` respectively. Python fake socket state remains under `/tmp/exo-proto-pybroker-*/`.

Known gap: a real Codex subscription call was intentionally not made. Explicit Anthropic/OpenAI API constructors were not exercised because their packages are not installed in the assigned Python environment. The Node budget and race tests used fixture-marked stores and the synthetic loopback backend only.
