# Temporal production-topology countertrial

22 September 2026 · macOS 26.5.2 arm64 · **bounded result, not a product qualification**

## Decision-relevant result

A pinned, non-development **Temporal Server v1.32.0** binary started without Docker against an isolated **PostgreSQL 16.15** cluster. PostgreSQL held both Temporal persistence and advanced visibility. One **Zigflow v0.15.2** worker ran a waiting factory; a second pinned worker served v2 while the old v1 run remained bound to v1. Six overlapping fixture HTTP activities completed. Killing the Temporal server and immediately stopping PostgreSQL did not lose a waiting v2 execution: after both helpers restarted, the same execution accepted its signal and returned the v2 result.

This establishes a plausible Docker-free local helper topology, **not** a one-install distribution or a finished Strands factory. The decisive authoring limitation remains: adding a valid new Zigflow definition file while its worker kept running produced a Workflow Task failure. The assessed Zigflow release needs a worker refresh or a managed versioned worker rollout for new factory types. Temporal's execution engine is not itself the cause of this limitation. Retaining old pinned versions means retaining worker processes until their runs finish, or adopting a different maintained authoring layer. If Exomachina still requires publication without worker refresh, this Zigflow baseline does not pass that gate.

The server and PostgreSQL topology has a lower *server-only* RSS than Kestra's observed JVM RSS, but a server-only comparison would be misleading. The whole sampled Temporal set included PostgreSQL and Zigflow and used a different fixture and configuration. No product memory threshold exists, and Dagu and other countertrials ran concurrently on the host. Do not rank candidates by the exact numbers below without a serial, matched repeat.

## Exact topology and provenance

- [Temporal Server v1.32.0 macOS arm64 release](https://github.com/temporalio/temporal/releases/tag/v1.32.0), SHA-256 `f95748376241f5941327fa4c4e8e76641e8c4a9acabf77de9c86eb3d8238f4d7`, matched the release checksum. This is the `temporal-server start` binary, **not** `temporal server start-dev`; its logged `debug-mode` was `false`.
- Core, visibility and schema-management binaries came from the same release tarball. PostgreSQL 16.15 was copied from the earlier host-local [Kestra package trial](../../s0/kestra/package/result.md); this countertrial used a separate cluster and port. Temporal v1.32.0's matching PostgreSQL v12+ schema migrations were applied with `temporal-sql-tool` to separate `temporal` and `temporal_visibility` databases on that one PostgreSQL instance.
- [Zigflow v0.15.2 macOS arm64 release](https://github.com/zigflow/zigflow/releases/tag/v0.15.2), SHA-256 `3932220f7c56cff5cd71080192b5ffc7e39768a410f814b7bb9e0f0da1830b0a`, matched its release checksum. The existing Temporal CLI v1.9.1 binary was used **only as a test client/operator tool** and excluded from runtime sizing.
- One Temporal server process hosted its frontend, history, matching and system-worker services. One PostgreSQL parent spawned background and connection processes. One Zigflow worker served v1; a second was needed to serve v2 concurrently. Temporal UI, Strands harness, A2A service and artifact store were **not** running. The deterministic HTTP delay fixture used for six activities is excluded from the runtime figures.
- The server used the release's PostgreSQL sample configuration with isolated loopback ports and its stock `numHistoryShards: 4`, 20 default-store and two visibility-store maximum connections. The test passed `--allow-no-auth`; PostgreSQL used local trust authentication. That is a **trial-only insecure access boundary**, not a product configuration or authorization proof. No external vendor service, key, model call or Docker runtime was used.
- Pinned Temporal source is [MIT licensed](https://github.com/temporalio/temporal/blob/v1.32.0/LICENSE); pinned Zigflow source is [Apache 2.0](https://github.com/zigflow/zigflow/blob/v0.15.2/LICENSE). This bounded source check does not audit the complete assembled release, PostgreSQL bundle, corresponding notices or intended redistribution.

## Observed behavior

| Probe | Observation | Limit |
| --- | --- | --- |
| Non-development startup | `temporal-server start` connected to PostgreSQL persistence/visibility and registered a namespace. A pinned Zigflow worker connected. | Insecure local sample configuration; no clean-machine install or product supervisor. |
| Paused v1 and v2 publication | A waiting v1 run reported pinned deployment `v1`. After starting a second Zigflow worker and making `v2` current, a new run reported pinned `v2`. Signals completed the old run with `revision: 1.0.0` and the new with `revision: 2.0.0`. | Old worker was retained. This does not freeze external capability/profile revisions or prove nested factory bindings. |
| New definition without refresh | A valid `countertrial-added` file was added while the v2 worker ran. Its started execution recorded `EVENT_TYPE_WORKFLOW_TASK_FAILED`. The test then terminated that execution. | Confirms the release's worker-refresh gate on this non-development topology; no hot publication path was found or tested. |
| Small overlap | Six concurrent starts used a deterministic three-second HTTP Activity; all six completed. | Local fixture only; not a throughput, A2A, idempotency or load result. |
| Helper crash/restart | A waiting v2 execution survived `SIGKILL` of Temporal and immediate PostgreSQL stop. PostgreSQL and Temporal restarted; existing Zigflow worker listeners remained; the same execution returned `revision: 2.0.0` after signal. | One run, one host, no lost external acknowledgement, no process-manager or host-loss test. |
| Cleanup | The owned PostgreSQL, Temporal, Zigflow and fixture listeners were stopped; no owned processes remained by PID/port check. | Isolated test state remains under `/tmp/exomachina-countertrials/temporal` for local inspection. |

## Resource observations

macOS `ps` RSS values below are **summed across every listed runtime process** and can double-count PostgreSQL shared pages. The separate group figure is macOS `footprint --noCategories -f bytes` over those same PIDs; it is a kernel footprint metric, not RSS or portable PSS. It also should not be treated as a controlled whole-machine delta while other trials were active. The fixture process and Temporal CLI test commands are excluded.

| Phase | Runtime processes | Summed RSS | Group footprint | Notable process RSS |
| --- | ---: | ---: | ---: | --- |
| Warm idle, one worker | 48 | 786.2 MiB | 321.2 MiB | Temporal 198.6; PostgreSQL processes 528.5; Zigflow 59.1 MiB |
| One waiting v1 run | 48 | 797.3 MiB | 327.0 MiB | Temporal 204.4; PostgreSQL 532.6; Zigflow 60.4 MiB |
| Waiting v1 and v2, two workers | 49 | 871.5 MiB | 363.4 MiB | Two Zigflow workers together 121.9 MiB |
| Six overlapping HTTP activities, two workers | 49 | 889.0 MiB | 372.8 MiB | Temporal 213.6; PostgreSQL 551.5; workers 123.9 MiB |
| After helper crash/restart, two workers | 48 | 757.7 MiB | 319.0 MiB | Warm-up and connection counts differ from the earlier phases |

The sample PostgreSQL configuration created **46 PostgreSQL processes** during the first four phases, mostly idle connection backends. The server is one process, and Zigflow adds one process per retained worker version in this trial. The second worker coincided with an approximately 36 MiB higher reported group footprint between the two paused samples; workload/time effects make that a directional observation, not an isolated per-version cost.

The four necessary trial binaries/bundles occupied about **375 MiB unpacked**: Temporal server 136.7 MiB, `temporal-sql-tool` 38.0 MiB, Zigflow 75.6 MiB and copied PostgreSQL bundle 125.0 MiB. The complete extracted Temporal release, Zigflow and PostgreSQL together occupied about **511 MiB**, including admin/other tools that could be excluded from a product package. Initialized PostgreSQL state occupied about **74–77 MiB** during the samples. Downloads, Temporal CLI, Strands, UI, artifacts, installer/notarization and future state growth are excluded.

A **warm helper crash/restart** on this concurrently busy host took 1.305 seconds for PostgreSQL and 0.262 additional seconds until Temporal's namespace API answered, 1.567 seconds combined. This excludes first database initialization/schema migration, worker launch/readiness, first installation, backup, and cold-machine boot. It is a timing observation, not a startup SLO.

Machine-readable phase/process measurements are in [measurements.json](measurements.json); bounded outcomes are in [publication-results.json](publication-results.json), [publication-gate.json](publication-gate.json), [batch-results.json](batch-results.json) and [restart-results.json](restart-results.json). The retained [fixtures](fixtures/) and local [measurement](measure.py), [delay fixture](slow_fixture.py) and [restart probe](restart_probe.py) scripts show the exercised contract. No raw service logs, database state or credentials are committed.

## Remaining gates before selection

1. **Authoring contract:** decide whether a managed worker rollout per new factory is acceptable. Zigflow's `--watch` is documented for development and was not used. If rollout is unacceptable, a permitted maintained stable interpreter must be demonstrated; Exomachina owning one is a substantial architecture cost, not a minor Adapter.
2. **Whole-install fit:** repeat a matched Kestra/Temporal/Dagu measurement serially on an agreed target machine and resource envelope, including the Strands harness, any required UI/diagnostics, artifact storage and realistic waiting/active workloads. Establish a shared-helper owner for multiple harness instances. Lower server RSS alone cannot select Temporal.
3. **Product package and governance:** produce a clean-machine one-command install/start bundle with proper PostgreSQL auth, Temporal API authorization, complete dependency/notice review, port/state ownership, backups and upgrade path. [Temporal's self-hosting guide](https://docs.temporal.io/self-hosted-guide/production-checklist) calls out authorization/audit and maintenance work; [server upgrades](https://docs.temporal.io/self-hosted-guide/upgrade-server) require schema and sequential-version care.
4. **Factory semantics:** only after the publication gate passes, run the shared Strands A2A/exact-artifact/lost-acknowledgement slice. This countertrial did not exercise two harness clients, direct-API bypass prevention, quality independence, remote cancellation, nested factory contracts, Engineering, full E01–E10 or production concurrency.

**Selection effect:** Temporal remains a conditional challenger. Its non-development local topology and durable versioned worker behavior are real, but the assessed Zigflow publication mechanism does not meet no-refresh agent-authored factory publication, and the full operating cost remains unqualified. Kestra remains the provisional behavioral development baseline until the matched resource and product package gates decide otherwise.
