# Temporal stable-interpreter countertrial

22 September 2026 · macOS arm64 · bounded engine-choice evidence

## Result

**The publication gate passed.** One unchanged Python worker registered only `FactoryRun` and three generic Activities. It accepted a v1 document, paused at the Director decision, then accepted a structurally different v2 document **in the same worker process**. V2 added `verify@1`; v1 stayed bound to its original content digest. After `SIGKILL` of the worker and Temporal Server plus immediate PostgreSQL shutdown, the same two executions resumed at their saved decision waits. Two concurrent Update requests from separate client connections returned `accepted` and `already-decided`; each run produced one accepted delivery receipt. The direct `research → deliver` bypass fixture was rejected before publication.

This overturns the earlier **Zigflow-specific** no-refresh finding for this narrower, product-owned interpreter design. It does not erase the cost of owning the document language, validator, catalog, authorization and lifecycle. [Observed JSON](observed.json) holds the exact outcomes; [probe.py](probe.py) runs the sequence.

## Pin and topology

- [Temporal Server v1.32.0](https://github.com/temporalio/temporal/releases/tag/v1.32.0), non-development `temporal-server start` binary. The prior downloaded release archive had SHA-256 `f95748376241f5941327fa4c4e8e76641e8c4a9acabf77de9c86eb3d8238f4d7`; the extracted server binary observed here had SHA-256 `f1663788fd4d8d702576b659db212b4d45a8dbfc0909eb7f7a86d0d442de6a60`.
- PostgreSQL **16.15** for default and advanced-visibility stores, one isolated cluster. `temporal-sql-tool` and PostgreSQL schema migrations came from the same pinned Temporal release as the server. Temporal CLI 1.9.1 was used only to create the test namespace, not as a runtime process.
- Python **3.12.9** and `temporalio==1.33.0`, with transitive pins in [uv.lock](uv.lock). One Python SDK worker hosted all revisions. No Zigflow worker, Docker, JVM, model provider, Strands harness, real A2A service, or Temporal UI ran. Two client connections supplied `harness-a` and `harness-b` logical identities; they were not separate harness processes. They shared one Temporal namespace/task queue, and this trial did not enforce access to each other's runs.
- The fixture uses trust-authenticated loopback PostgreSQL and `--allow-no-auth` on Temporal. [server.template.yaml](server.template.yaml) is a trial configuration with four history shards and default connection pools, including 20 default-store connections. It is **not** a production security configuration.

## What was exercised

The accepted JSON vocabulary in [v1.json](fixtures/v1.json) and [v2.json](fixtures/v2.json) has two parallel assignments, an optional `verify` assignment, independent review, a Director decision, and one delivery. [factory.py](factory.py) interprets these blocks inside a single durable workflow. The publication helper validates their allowed order and pinned capability references, writes canonical content-addressed JSON, and passes the exact document and digest as the workflow start argument; Temporal history therefore holds each run's input snapshot. The sample review checks research artifacts, and the sample delivery Activity inserts a unique run receipt in an application SQLite table. These are synthetic effects, not A2A protocol operations or a full acceptance policy.

| Check | Observed |
| --- | --- |
| Invalid graph publication | `bypass.json` rejected with `unreviewed, unbounded, or unsupported factory path`. |
| v2 publication during v1 wait | Worker PID stayed alive and unchanged; both runs reached `awaiting-decision` with different digests. |
| Restart | Both queries after helper restart reported their original digests and prior completed steps. Neither pre-decision Activity was rerun in the fixture event log. |
| Version shape | V1 completed `parallel → review → decision → deliver`; v2 completed `parallel → verify@1 → review → decision → deliver`. |
| Concurrent duplicate decision | Two Update calls through distinct client connections returned `accepted` and `already-decided`. |
| Delivery | One `deliver-attempt` event and one unique SQLite receipt per run. No deliberate lost-acknowledgement fault was injected. |
| Cleanup | Temporal, worker and PostgreSQL processes stopped; owned frontend/PostgreSQL ports were closed. |

The `director-decision` block includes a one-second Temporal timer after acceptance so both Update calls can race before completion. This is only a test fixture. It is not an approval delay requirement.

## Reproduce on the prepared host

The prior [Temporal production-topology trial](../../countertrials/temporal/result.md) already fetched the verified release archive and extracted the matching schema/config under `/tmp/exomachina-countertrials/temporal/runtime`. This trial reuses that **Temporal** installation and uses the host's Homebrew PostgreSQL 16.15; the script falls back to the prior bundled PostgreSQL binaries if Homebrew is absent. It creates an isolated PostgreSQL cluster, schema, catalog, event log and delivery ledger in a fresh `/tmp/exomachina-temporal-round2-*` directory. Environment overrides `EXO_TEMPORAL_RUNTIME`, `EXO_PG_BIN` and `EXO_TEMPORAL_CLI` can point to equivalent pinned installations. On a new machine, fetch and verify the linked release, supply its matching schema directory and PostgreSQL 16.15 before running the same command.

```sh
cd /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/round-two/temporal
uv run --frozen python probe.py
```

The final invocation exited 0 with `status: passed` and `owned_ports_closed: true`. An initial run failed because macOS's long default temporary path exceeded PostgreSQL's Unix socket limit; the probe now uses `/tmp`. The next attempt exposed Temporal's PostgreSQL membership-port `smallint` limit when ports exceeded 32767; the member ports now stay below that bound. Those failures occurred before factory execution, and no owned processes remained after either run. The final successful run took roughly 11 seconds after the pinned dependencies were installed; that includes creating/migrating a fresh PostgreSQL cluster and should not be treated as a product startup time.

## Resource and ownership cost

At the point where both runs waited, `ps` reported **159,760 KiB RSS** for Temporal Server, **69,360 KiB** for the Python worker, and **479,792 KiB summed RSS** across **44 PostgreSQL processes**. PostgreSQL shared pages are double-counted in that sum. After restart the respective values were 152,656, 60,208 and 476,768 KiB. These snapshots exclude the test driver, CLI, future Strands harnesses, A2A services and artifact storage, and other trials were active on the same host. They are not a matched whole-product comparison. The extracted server binary occupied 140,008 KiB on disk, `temporal-sql-tool` 38,864 KiB, the prior PostgreSQL bundle about 127,984 KiB, and this SDK virtual environment 55,052 KiB; clean product packaging was not performed.

The trial owns **137 lines** in the Activity/interpreter module, **24 lines** in the worker launcher, and **about 335 lines** in the publication/driver/process harness, plus a 117-line server template. Much of the driver is one-off setup and measurement code. A product would still need a supported definition schema, semantic governance beyond this exact graph, immutable catalog with ACLs, actor authorization for Updates, durable artifact storage, real A2A task correlation and lost-acknowledgement reconciliation, helper supervision, backup/upgrade/security, and run inspection fit for the Director. This trial did not validate arbitrary graph layouts or long-running version compatibility after changing interpreter code.

Temporal Server's pinned source is [MIT licensed](https://github.com/temporalio/temporal/blob/v1.32.0/LICENSE); the installed Python SDK wheel declares MIT in its package metadata; [PostgreSQL uses the PostgreSQL License](https://www.postgresql.org/about/licence/). No commercial runtime feature was needed for this behavior. A distribution review of all bundled binaries, notices and dependencies remains a product packaging task. Temporal's own [self-hosting checklist](https://docs.temporal.io/self-hosted-guide/production-checklist) describes substantial operating work. Its workflow model also requires [deterministic replay and compatible code evolution](https://docs.temporal.io/workflow-definition); passing data documents to a stable interpreter reduces the number of code rollouts but does not remove that rule.

**Selection effect:** Temporal is now a stronger viable challenger than the Zigflow trial suggested. Its authoring burden is a real, bounded product-owned Module rather than a demonstrated need for a worker per factory version. The one-install footprint and helper operations remain material tradeoffs, especially beside lighter candidates, and the real A2A acceptance slice remains open.
