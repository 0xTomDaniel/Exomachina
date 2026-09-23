# Lane: runner correction, no schema widening (pane wG:p6)

Read `prototype/temporal-factory/INTERFACES.md` and `handoff/runner.md` (the previous runner lane's report). Work only in `prototype/temporal-factory/`.

Owner's review finding: `src/runner.py` added `ALTER TABLE cluster_membership ... rpc_port TYPE INTEGER` only because the smoke chose `member_base=33600`, above Temporal's signed SMALLINT column. The earlier package trial solved this by using a lower isolated membership port instead. Required correction:

1. Remove the schema widening completely from `src/runner.py`. Do not modify Temporal's schema in any way beyond its shipped `setup-schema`/`update-schema`.
2. Validate ports: `Runner.__init__` (and `port_map`) must reject any `member_base` whose range `member_base..member_base+4` exceeds 32767 (and any port outside 1024..65535), with a clear error, before anything starts. Keep the default `member_base=32400`.
3. Update `tests/test_runner.py`: assert that the high member base is rejected, that the default range is at most 32767, and that the source no longer contains `ALTER TABLE` or `rpc_port` widening.
4. True first-start smoke in a **fresh** preserved home that has never been initialized: `EXO_HOME=/tmp/exo-proto-runner-cold-<suffix>`, `port_base=46000`, `member_base=32500` (range 32500–32504; the orchestrator's live integration is using 32400–32404, so don't touch those). Check that those ports are free first. Sequence: `start` (cold: initdb, schema, namespace, all in one clean pass with no retry) → `status` → a second `ensure_started` attaches → `ensure_build(src)` → `wait_worker` → `stop` → warm `start` (no re-init: no `postgres-start first:true`, no schema setup) → `stop`. If the cold start fails, stop and report; don't retry into the same home, and don't count a recovered partial init as clean-cold proof.
5. Evidence: write `evidence/runner-cold-smoke.json` with the event log, pids, and the ports used. In `evidence/runner-smoke.json`, keep the earlier high-port evidence and add a top-level `superseded_note`: the high-port attempt failed on SMALLINT, and its later passes recovered from a partial init, so they are not clean-cold proof. Do not delete anything, including the old `/tmp/exo-proto-runner-smoke-20260923` state.

Python: `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B`. Tests: `-m unittest -v tests/test_runner.py` from `prototype/temporal-factory`.

Handoff: `handoff/runner-fix.md`.
