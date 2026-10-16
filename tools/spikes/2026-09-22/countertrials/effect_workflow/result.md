# Effect Workflow + Cluster: bounded SQLite countertrial

Date: 2026-09-22. This tests the v3 Effect packages, not the v4 release candidate. The runtime is a single Node.js process on macOS arm64 with `ClusterWorkflowEngine.layer`, `SingleRunner.layer({ runnerStorage: "sql" })`, and `@effect/sql-sqlite-node` connected to a local SQLite file. There is no Docker container, external engine, or separately managed database. The actual probe is [probe.mjs](probe.mjs), with exact dependencies in [package.json](package.json) and [package-lock.json](package-lock.json). [measurements.json](measurements.json) records raw observations.

## Observed results

| Test | Result |
| --- | --- |
| SQL-backed local boot | Pass. `cluster_messages`, `cluster_replies`, `cluster_runners`, and `cluster_locks` appeared in the SQLite file. The runner used SQL storage, not the in-memory test engine. |
| Activity + approval wait | Pass. `Workflow.make` registered a factory with a `prepare` Activity, `DurableDeferred.await` approval, and a `deliver` Activity. The run reported `Suspended`. |
| Process-kill recovery | Pass for this path. After the waiting process was killed with SIGKILL, a fresh process using the same SQLite file completed the deferred; the run reported `Complete/Success`. Its event log had one `prepare` and one `deliver`. |
| Duplicate approval | Narrow pass. Two concurrent fresh processes called `DurableDeferred.succeed` for one waiting run. Both observed `Complete/Success`; the sample delivery Activity logged once. This does not prove exactly-once external effects under every failure. |
| Dynamic factory publication / v1 binding | Not tested. The probe registers workflow code on process boot. It does not submit and validate a factory document at runtime, prove version snapshots, or update a live workflow handler. |
| A2A / real agents | Not tested. The Activities write to a local event file instead of invoking agents or A2A tasks. |

Warm idle used **189,328 KiB RSS (184.9 MiB)** and **160 MB macOS physical footprint** in one process. With one suspended workflow the process used **191,888 KiB RSS (187.4 MiB)** and **162 MB footprint**. The installed minimal production dependency tree took **85,052 KiB (83.1 MiB)**; the SQLite main file was **84 KiB** for these tiny examples, with transient WAL/SHM files while a process ran. No workload-specific harness, A2A endpoint, provider client, or multi-agent state is included. The Mac was concurrently running other candidate experiments, so these are orientation measurements, not a controlled cross-engine benchmark.

## Interpretation for Exomachina

Effect deserves consideration as an embedded outer runtime. This trial falsifies the idea that Effect Workflow needs a separate Java or Docker workflow server: a one-process SQL-backed local setup can suspend and recover an approval. Its main tradeoff is **authoring and publication**. The tested API defines `Workflow.make`, `Activity.make`, and handlers in application code. To let arbitrary agents create and publish versioned factory *documents* without code rollout, Exomachina would have to own a constrained definition language and interpreter or another code publication mechanism, plus version binding, policy validation, and recovery rules. That is a product architecture cost, not a failure of the durable runtime shown here.

The two approval processes sharing SQLite completed one sample delivery, but this does not establish safe multi-process placement of full harnesses, multi-host operation, idempotent A2A effects, or crash windows around an external side effect. Those require dedicated tests if Effect becomes a finalist. We did not test the upstream reports of [a dropped deferred-resume race](https://github.com/Effect-TS/effect/issues/6318) and [delayed second child resume](https://github.com/Effect-TS/effect/issues/6294) against these pinned versions. Those reports are relevant follow-up probes rather than observed failures here.

## Reproduce

On Node.js 26/macOS arm64, `better-sqlite3` needed its approved native install script and a rebuild in the scratch installation; the pinned package manifest includes that approval. Keep runtime files outside the repository:

```sh
trial=/tmp/exomachina-countertrials/effect_workflow/repro
mkdir -p "$trial"
cp probe.mjs package.json package-lock.json "$trial/"
cd "$trial"
npm ci
EFFECT_SPIKE_ROOT="$trial" node probe.mjs hold kill-trial
```

In another terminal, after `suspended` appears, `kill -9` the logged PID, then run:

```sh
EFFECT_SPIKE_ROOT="$trial" node probe.mjs approve kill-trial
```

The expected result is `approved` followed by `complete` with `exit: "Success"`; `events.jsonl` should contain one `prepare` and one `deliver` for that ID. For an idle footprint sample, run `node probe.mjs idle` and inspect its PID with `ps` or macOS `footprint`. Stop the process afterward. The probe also supports `start <id>` as a one-shot command.

Relevant upstream source: [Effect Workflow package and example](https://github.com/Effect-TS/effect/tree/v3/packages/workflow), [Effect Cluster SingleRunner](https://github.com/Effect-TS/effect/blob/v3/packages/cluster/src/SingleRunner.ts), and [Effect Cluster Workflow engine](https://github.com/Effect-TS/effect/blob/v3/packages/cluster/src/ClusterWorkflowEngine.ts).
