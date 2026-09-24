# Replay lane handoff

## Files changed

- `scenarios/replay_check.py` (new): retains histories, identifies each pinned build from Temporal description metadata, replays offline in a subprocess per immutable build, runs an available second build as a negative control, and writes one JSON summary.
- `handoff/replay.md` (this file).

## Validation commands and output

- `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B prototype/temporal-factory/scenarios/replay_check.py --self-test` — exit 0; `{"checks": ["Temporal imports", "CLI arguments", "history names", "pinned build metadata"], "self_test": "pass"}`.
- `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B prototype/temporal-factory/scenarios/replay_check.py --help` — exit 0; showed `--home`, `--address`, `--out`, repeatable `--workflow-id`, and `--self-test`.
- `git status --short -- prototype/temporal-factory/scenarios/replay_check.py prototype/temporal-factory/handoff/replay.md` — exit 0 before handoff was written; showed `?? prototype/temporal-factory/scenarios/replay_check.py`.
- `git diff --no-index /dev/null prototype/temporal-factory/scenarios/replay_check.py` — exit 1 because it displayed the new-file diff; no command rejection. It showed only the new checker.

Read-only inspection commands (`cat`, `rg`, `sed`, `ls`, `git status`, and Python SDK introspection) exited 0. They confirmed the integrated scenario's versioning metadata path, runner `build.json` format, and installed SDK support for `WorkflowHistory.from_json` and `Replayer.replay_workflow`.

## Evidence and gaps

- Self-test output is recorded above. No trial state was created.
- Live Temporal replay was not run in this lane; the orchestrator will run it after the integrated scenario produces histories and build snapshots.
- No command or approval was rejected. No files were deleted. No branch switch, commit, push, or merge was performed.
