# Effect Workflow + Cluster: live document publication countertrial

22 September 2026. **The bounded publication and recovery question passed.** One unchanged `FactoryRun` workflow interpreted both approved JSON documents. The same helper published v2 while v1 was suspended, started v2, then recovered both pinned runs after `SIGKILL` and restart. Two concurrent decisions for v1 returned success and the synthetic delivery Activity wrote one event. This is evidence for this one local sequence, not an exactly-once external delivery guarantee.

## Topology and pinned inputs

One Node.js 26.0.0 process on macOS arm64 hosted the fixed [helper](helper.mjs): `ClusterWorkflowEngine.layer`, `SingleRunner.layer({ runnerStorage: "sql" })`, and `@effect/sql-sqlite-node` over one local SQLite file. A Python 3.14.3 [driver](probe.py) sent local HTTP requests and killed/restarted the helper. There was no Docker service, separate engine, model call, A2A peer, or Strands harness. The helper's content-addressed catalog, run bindings, SQLite files, event log, and process log lived in a temporary directory outside the repository. [Observed data](observed.json) includes the PIDs, responses, events, and measurements.

The reused [package manifest](package.json) pins `effect` 3.22.2, `@effect/cluster` 0.60.2, `@effect/workflow` 0.19.1, `@effect/sql` 0.52.1, and `@effect/sql-sqlite-node` 0.53.0; the installed native `better-sqlite3` was 12.11.1. The [lockfile](package-lock.json) SHA-256 is `9000394befc5974c7a491ec930d4af5667e9de69a2758e3fdcbada6810bca187`. The inspected manifests and local license files for those five direct packages and `better-sqlite3` say MIT; this was not a full transitive or product-distribution legal review. This trial uses Effect v3, not the v4 release candidate.

The publisher accepts exactly the two constrained shapes represented by [v1](fixtures/v1.json) and [v2](fixtures/v2.json): two independent named capability steps (either order), join, optional v2 verify, separate review, Director wait, then delivery. It rejects unknown fields and steps and a review bypass. The helper writes a content-addressed definition and passes the complete JSON snapshot plus digest into the workflow payload; a separate run-binding file rejects the same run ID with a different digest. The catalog digests used by the run were:

| Definition | Published SHA-256 | Capability references |
| --- | --- | --- |
| v1 | `d0d9478cb1524df4f3dce974d087736a78bd11df2a98183e77a81dbed579102b` | `capability-a@v1`, `capability-b@v1` |
| v2 | `f3e72ee14e1efbb7804bc9eaae3d7bb52d5b5eac77b4b73945cfee20f9747e17` | `capability-a@v2`, `capability-b@v2` |

These hashes cover the compact JSON written by the publisher, so they differ from hashes of the formatted fixture files.

## Observed sequence

| Check | Evidence |
| --- | --- |
| V1 wait | `harness-v1` reached `Suspended` after one event each for both capabilities, join, review, and Director wait. |
| Live v2 publication | The helper remained PID 3618 when v2 was published and `harness-v2` reached `Suspended`. V2 recorded one `verify` event; v1 recorded none. No workflow registration or helper restart occurred at publication. |
| Publication and binding policy | A v2 document without review returned HTTP 400. Starting the v1 run ID with the v2 digest, and later approving it with that digest, each returned HTTP 409. |
| Crash and recovery | PID 3618 was killed with `SIGKILL` (exit code `-9`). PID 3621 opened the same SQL and catalog state. Both runs completed with `Success`, their original digests, versions, capability references, and step lists. V1 still had no verify step. |
| Duplicate decisions | Three simultaneous HTTP requests sent two v1 decisions and one v2 decision. All returned 200; the event log contained one `deliver` for each run. The Effect deferred accepted repeated signaling in this sample. |

The two capability calls used `Effect.all(..., { concurrency: 2 })`, then the interpreter joined their synthetic outputs. The Activities appended JSON lines to a local file; their near-instant work does not measure real concurrent remote execution.

## Cost and limits

The warm helper used **194,208 KiB RSS (189.7 MiB)**. With two suspended workflows it used **197,088 KiB (192.5 MiB)**; the restarted helper used **193,776 KiB (189.2 MiB)** before decisions. The installed production dependency tree occupied **85,052 KiB (83.1 MiB)**. The live temporary runtime directory occupied **1,140 KiB**; at measurement, SQLite had a 4,096-byte main file, 32,768-byte shared-memory file, and 1,104,192-byte WAL. These are orientation measurements on a busy development Mac and exclude a product harness, A2A server, provider clients, and packaging overhead. They should not be compared as whole-install memory against unlike candidate topologies.

The owned implementation is **189 lines** in `helper.mjs`: 51 lines for validation, publishing, and catalog reads (lines 24–74), 26 lines for run-binding reads/writes (76–101), 28 lines for fixed workflow and interpreter (110–137), plus runtime wiring, local API, and event recording. The **181-line** Python driver is trial orchestration, not runtime code. This is a useful small spike, but a production document language, policy checker, immutable publication store, and run identity boundary would be Exomachina-owned code.

The catalog and run binding are filesystem writes outside Effect's SQL transaction and have no `fsync` or atomic commit with workflow start. The trial killed the process after both waits, not during publication or an external side effect. A file append inside a durable Activity can duplicate if the process dies after the append but before its completion is recorded. No competing helpers, multiple hosts, A2A submission/reconciliation, arbitrary graph edits, unbounded cycles, real decision authorization, or complete Strands integration were tested. Further versions or new block types would require extending this deliberately narrow validator/interpreter. The observed one-delivery event count does not establish exactly-once external effects.

## Reproduce

From this directory on Node.js 26/macOS arm64 with Python 3.14:

```sh
npm ci --omit=dev
python3 probe.py
```

`npm ci` may warn that `msgpackr-extract`'s optional install script is blocked; the approved `better-sqlite3` native package worked in this run. The driver makes a fresh temporary runtime directory, writes [observed.json](observed.json), and stops its helper. Successful output has `"passed": true`, `"same_helper_at_v2_publish": true`, two `Complete/Success` results, and one `deliver` event per run.
