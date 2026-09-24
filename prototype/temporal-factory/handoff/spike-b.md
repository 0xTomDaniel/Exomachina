# Spike B handoff

Claim labels: process and A2A trial observations are **observed-real**; the Director model and testbed content are **fixture**. The two provisioning boundary unit tests are **unit-tested**. No live model or live external directory was used.

## Verdicts

| Check | Unmodified | Fixed | Evidence |
| --- | --- | --- | --- |
| B-1 cold concurrent requests | Pass | Pass | `evidence/spike-b/before-corrected.json`, `after-final.json` → `checks.B-1` |
| B-2 instance separation | Pass | Pass | same → `checks.B-2` |
| B-3 alpha down while beta runs | Pass | Pass | same → `checks.B-3` |
| B-4 alpha reattaches and aborts parked Task | Pass | Pass | same → `checks.B-4` |
| B-5 hard kill during active run | Pass | Pass | same → `checks.B-5` |
| B-6 same-home config and process rejection | **Fail** | Pass | same → `checks.B-6` |

Unmodified B-6 accepted provisioning gamma on alpha's port (`returncode=0`) and accepted a different runner config. A duplicate beta process failed to bind but had already raised the identity table's incarnation to 3 while the live beta still reported 2. The runner PID did not change. After the fix, the port and runner config attempts are rejected; the duplicate process exits with `instance already serving`, and both the identity table and live beta remain at incarnation 2. The runner PID remains unchanged.

The first unmodified run (`before.json`) recorded B-1 and B-5 as failures because the scenario compared release receipts to the parent run ID. The HTTP release fixture writes receipts for the child run. I corrected that checker, then reran the **unmodified product code** in a fresh home; `before-corrected.json` is the baseline above. Both original raw observations remain in evidence.

## Changes

- `src/runner.py`: pin `port_base` and `member_base` under `$H/runner-config.json` with a home lock. Existing homes use that config; explicit conflicting ports fail. This config file does not create `pgdata` or start the runner.
- `src/harness.py`: `init_instance` serializes same-home provisioning, rejects a port already assigned to another instance, and validates requested runner ports against the home. `instance.json` stores no runner port values. The normal `harness.py serve` entrypoint holds an instance lock before `create_app` can claim a new incarnation.
- `scenarios/spike_b_two_instances.py`: reproducible B-1 through B-6 process trial and cleanup.
- `tests/test_same_home.py`: two provisioning/config boundary tests.
- `evidence/spike-b/*`: raw trial JSON, unit output, cleanup record.

No `src/admin.py` change was needed: its existing `provision` call reaches the fixed `init_instance`. No commit, push, merge, rebase, branch switch, or deletion was performed.

## Commands and output

Working directory for the commands below was `prototype/temporal-factory/` in the B worktree. `PY` in this section is the absolute executable `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python`.

| Command | Output / exit |
| --- | --- |
| `npm --prefix prototype/temporal-factory/broker ci --offline --cache /tmp/exomachina-pi-strands-debate/npm-cache` (from worktree root) | Exit 0; added 85 packages, found 0 vulnerabilities. |
| `$PY -B -m unittest discover -s tests` before product edits | Exit 0; `Ran 90 tests ... OK`. |
| `EXO_RUNNER_PORT_BASE=44300 EXO_RUNNER_MEMBER_BASE=32460 $PY -B scenarios/spike_b_two_instances.py --home /tmp/exo-qual-b-before-20260923 --evidence evidence/spike-b/before.json` | Exit 0; B-1/B-5 checker failures and B-6 product failure recorded. |
| `EXO_RUNNER_PORT_BASE=44300 EXO_RUNNER_MEMBER_BASE=32460 $PY -B scenarios/spike_b_two_instances.py --home /tmp/exo-qual-b-before-corrected-20260923 --evidence evidence/spike-b/before-corrected.json` | Exit 0; B-1…B-5 pass, B-6 fail. |
| `$PY -B -m unittest discover -s tests` after first edit | Exit 1; `Ran 92 tests ... FAILED (failures=4, errors=1)`. One failure was the runner-directory laziness assertion, fixed by putting config at `$H` root. The other failures were broker mock tests on shared hard-coded ports. |
| `$PY -B -m unittest discover -s tests -p 'test_same_home.py'` and `$PY -B -m unittest discover -s tests -p 'test_harness_modes.py'` | Exit 0; 2 and 5 tests, respectively. |
| `EXO_RUNNER_PORT_BASE=44300 EXO_RUNNER_MEMBER_BASE=32460 $PY -B scenarios/spike_b_two_instances.py --home /tmp/exo-qual-b-after-20260923 --evidence evidence/spike-b/after.json` | Exit 0; all six pass. |
| `EXO_RUNNER_PORT_BASE=44300 EXO_RUNNER_MEMBER_BASE=32460 $PY -B scenarios/spike_b_two_instances.py --home /tmp/exo-qual-b-final-20260923 --evidence evidence/spike-b/after-final.json` | Exit 0; all six pass after the final home-config and checker refinements. |
| `$PY -B -m unittest discover -s tests` unlocked after final edit | One attempt passed 92; another failed 3 broker budget subtests. See `unit-after-attempt1.txt` and `unit-after-final.txt`; concurrent worktrees run those tests on the same fixed 46140 mock port. |
| `/usr/bin/lockf -k /tmp/exo-qual-suite.lock $PY -B -m unittest discover -s tests` | **Exit 0; `Ran 92 tests in 11.035s ... OK`.** Full output: `evidence/spike-b/unit-after-locked.txt`. This is the final suite result. |
| `git diff --check` | Exit 0; no whitespace errors. |
| `lsof -nP -iTCP -sTCP:LISTEN` filtered to B blocks | `listeners_in_B_port_block: 0`; `evidence/spike-b/cleanup.txt`. |

The scenario `finally` stopped both harnesses, the detached runner via `src/runner.py stop --home H`, and the six testbed services in every trial. It did not start a model broker. Its per-home cleanup results are in each JSON file.

## Architecture and limits

The runner ports belong to the install home, not an instance. The process-level instance lock must be acquired before Director incarnation claim and held for the serving process's lifetime; the normal CLI entrypoint now does this. Direct embedding through `create_app()` is not covered by this process lock and would need its own lifecycle guard if supported. More than two instances and load/stress behavior remain unproven.

Trial directories, left in place: `/tmp/exo-qual-b-before-20260923/`, `/tmp/exo-qual-b-before-corrected-20260923/`, `/tmp/exo-qual-b-after-20260923/`, `/tmp/exo-qual-b-final-20260923/`, plus `/tmp/exo-qual-b-unit-*/` from unit tests. Nothing requested remains unrun.
