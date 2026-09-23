# Lane: offline history replay checker (pane wG:p8)

Read `prototype/temporal-factory/INTERFACES.md`. Work only in `prototype/temporal-factory/`.

File you own: `scenarios/replay_check.py` (new). Don't edit other files.

Build a CLI that retains and replays Temporal Workflow histories from an integrated run, against the exact immutable interpreter build each run was pinned to:
`python scenarios/replay_check.py --home <EXO_HOME> --address 127.0.0.1:<port> --out evidence/histories [--workflow-id ID ...]`
1. If no ids are given, list all workflows in namespace `exomachina` (`client.list_workflows()`), fetch each history (`handle.fetch_history()`), and save it as `<out>/<sanitized id>.json` (`history.to_json()`).
2. For each history, determine the pinned build id from `describe()` (`raw_description.workflow_execution_info.versioning_info`, the deployment version or pinned override build id). Import `FactoryRun` from `<home>/runner/builds/<build_id>/factory.py` in an isolated way: a subprocess per build, with `cwd` = that build dir and `sys.path[0]` = the build dir, and env `EXO_WORKER_BUILD_ID`/`EXO_WORKER_SOURCE_DIGEST` set from its `build.json`. Replay with `temporalio.worker.Replayer(workflows=[FactoryRun], workflow_failure_exception_types=[ValueError])`, and record pass/fail per history.
3. Negative control: if a second build dir exists, replay each history against a different build too, and record the result (expect failure or a nondeterminism error, but record whatever happens honestly). If there is only one build, record `negative_control: "not-applicable (single build)"`.
4. Print one JSON summary and write it to `<out>/replay-summary.json`.

No live server is needed to write this. To test it, write a small self-check mode (`--self-test`) that imports modules and validates argument handling without a server. The orchestrator will run the real replay after the integrated scenario. Don't start Temporal yourself. Never delete anything.

Python: `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B`.

Handoff: `handoff/replay.md`.
