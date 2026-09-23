# Dagu Community local-runtime countertrial

22 September 2026 · **Dagu v2.17.0 is a credible lightweight challenger, not a selected product engine.**

This bounded trial ran a released Dagu binary as a separate, local helper with file-backed state. It established one-command server/scheduler startup, low measured idle memory on this Mac, durable nested-review waits, and an important transitive-version gap. It did not run a Strands harness, A2A service, product authorization boundary, or the complete D01–D08 screen.

## Pinned artifact and operating boundary

The official [`v2.17.0` macOS arm64 archive](https://github.com/dagucloud/dagu/releases/tag/v2.17.0) matched its release checksum: `da86f2a7278afcadfe8202b9af2b9613a5fc997ffc47daf1799b64d7fc5afeb1`. `dagu version` returned `2.17.0`. The archive was 45.9 MiB compressed and the unpacked executable 147.9 MiB. The [tagged licensing guidance](https://github.com/dagucloud/dagu/blob/v2.17.0/LICENSING.md) identifies GPL-3.0-or-later, permits commercial GPL-compliant use and distribution, and distinguishes a separately operated CLI/server from linking its experimental Go API into a proprietary combined binary. **Exomachina's exact bundle and source/notice obligations have not been cleared.** This trial did not import the Go API or assume paid controls.

The exact runtime was `dagu start-all`, binding HTTP to loopback with `DAGU_HOME` pointing at isolated `/tmp` state and `DAGU_COORDINATOR_ENABLED=false`. This documented local mode kept the web/API server and scheduler in **one process**; no external database, broker, JDK, Docker, vendor key, or coordinator/worker process was used. Active DAG runs spawned short-lived Dagu runner processes and their command/watcher children. `DAGU_AUTH_MODE=none` was used solely for a loopback-only test. It **does not prove a production access boundary**; every product publisher, operator and direct engine path still needs authorization.

## Measured resource envelope

The detailed process records and methods are in [measurements.json](measurements.json). Values below are point-in-time observations on the operator's Darwin arm64 Mac; other Exomachina countertrials ran concurrently, so machine-wide pressure and startup timing were not isolated candidate comparisons.

| State | Processes included | RSS | macOS physical footprint |
| --- | --- | ---: | ---: |
| Fresh server/scheduler after first-time setup | One Dagu process | 105.6 MiB | Not sampled then |
| Two nested parent runs waiting for review | One Dagu process; CLI runners exited | 118.2 MiB | 74 MB in a nearby paused sample |
| Warm after restart and small flows | One Dagu process | 120.8 MiB | 77 MB |
| Two overlapping six-second runs started through HTTP | Server, two Dagu runners, two commands, two watcher shells | 277.5 MiB summed RSS | 156 MB for all seven processes |

RSS summed across processes can double-count shared pages; `footprint` is the macOS process-set physical-footprint measure. The HTTP active sample is the relevant one: `POST /api/v1/dags/exo_active.yaml/start` caused the server to spawn two separate runner processes, each around 73 MiB RSS. Both runs completed successfully. The initialized isolated home occupied about 576 KiB after the small cases, including five examples Dagu created on first setup. A single restart with that state reached HTTP OpenAPI 200 in **0.219 seconds**, but that timing is not a controlled startup benchmark. CPU, peak memory, sustained load, clean-machine portability and the Strands/A2A process footprint were not measured. These numbers suggest a materially smaller helper than the prior ~650 MiB Kestra-JVM RSS observation; the complete installations and workloads have **not** been compared under a serial common fixture.

## Factory behavior observed

- A root parent executed a `dag.run` child, then entered native `approval` **Waiting**. Its runner exited while the server kept the run state. Two separately identified parents waited simultaneously. After stopping and restarting the server/scheduler against the same files, both were still `Waiting`; approving one resumed it to `Succeeded` with one downstream delivery step.
- In one concurrent two-request approval race, one request returned HTTP 200 and the other HTTP 400 because the step was already succeeded. The persisted child and delivery steps each had `doneCount=1`. This is a narrow race observation, not a general external-effect guarantee.
- A root processless `human.task` returned `alreadyCompleted: true` with HTTP 200 when the same validated input was submitted again after the run succeeded. A lost HTTP response was **not** injected. Dagu's [documented human-task completion semantics](https://docs.dagu.sh/writing-workflows/human-tasks) are relevant, but this trial proves only this retry.
- A v1 parent waiting after its child retained its original child target and downstream command when its YAML was edited to v2 and the helper restarted. A new parent used the new child and command. The run snapshot protected the parent definition in this case.
- **Transitive binding was not automatic.** A second parent paused at a root `human.task` *before* invoking `exo_child_mutable`. Changing that child file from v1 to v2 while the parent waited caused the resumed parent to invoke **v2**. An agent publisher must use immutable, versioned child names or another tested dependency-closure policy. A parent snapshot by itself does not freeze an uninvoked mutable child definition.

Dagu's separate [`approval` on `dag.run`](https://docs.dagu.sh/writing-workflows/approval) is a workable native nested-review mechanism; the processless typed `human.task` remains [root-only](https://docs.dagu.sh/writing-workflows/human-tasks). This test did not establish typed waits inside child DAGs or durable correlation, cancellation, deadlines and budgets across independent root factories. Those are the main questions for D02–D08 before treating Dagu as a full factory service runtime.

## Reproduction and remaining gates

All files and processes were confined to `/tmp/exomachina-countertrials/dagu` and this countertrial directory. The trial used the included [YAML fixtures](parent-review-v1.yaml) and [startup measurer](measure_startup.py). The runtime filename supplies Dagu's DAG name, so, for example, copy `parent-review-v1.yaml` to `<home>/dags/exo_parent_review.yaml` and `child-v1.yaml` to `<home>/dags/exo_child_v1.yaml` before starting a run. The core commands were:

```sh
DAGU_HOME=<isolated-home> DAGU_COORDINATOR_ENABLED=false DAGU_AUTH_MODE=none dagu start-all --host 127.0.0.1 --port <loopback-port>
DAGU_HOME=<isolated-home> DAGU_AUTH_MODE=none dagu validate <home>/dags/exo_parent_review.yaml
DAGU_HOME=<isolated-home> DAGU_AUTH_MODE=none dagu start --run-id exo-parent-001 <home>/dags/exo_parent_review.yaml
# After the run reaches Waiting, use POST /api/v1/dag-runs/exo_parent_review/exo-parent-001/steps/child/approve with JSON {}.
python3 measure_startup.py /path/to/dagu <isolated-home> 18117
```

The trial leaves **GPL-compliant product packaging, Community authorization and audit alternatives, independent factory-root linkage, exact-artifact acceptance, ambiguous remote submission, cancellation, multi-instance harness identity, load and upgrade behavior unproved**. The transitive child result makes versioned dependency publication a concrete requirement. The resource observation warrants comparing the complete Strands+Dagu installation against shared Kestra under one serial protocol; it does not clear Dagu for production or remove Kestra's stronger S1–S4 behavioral evidence.

All owned Dagu processes were stopped after the trial; the isolated binaries and state remain only under `/tmp/exomachina-countertrials/dagu` for reproducibility.
