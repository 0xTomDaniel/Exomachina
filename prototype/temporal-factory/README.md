# Integrated Temporal factory prototype

23 September 2026 · branch `feat/temporal-factory-prototype` · macOS arm64, Python 3.12.9, temporalio 1.33.0, Temporal Server 1.32.0, PostgreSQL 16.15, strands-agents 1.57.0, a2a-sdk 0.3.26 · **a lean, runnable integration candidate, not product qualification.**

This merges the four bounded Temporal lanes (`temporal-director-contract`, `temporal-version-binding`, `temporal-quality-reconciliation`, `temporal-package-ops`) into one candidate built around the corrected architecture. A customized Strands harness instance runs as an ordinary agent or as a factory. In factory mode, its Director agent and Factory Module sit inside the instance, behind that instance's normal A2A identity and `verified-research@1` capability contract. There is no separate factory endpoint, and callers never choose a graph, package or version. The shared contract between modules is in [`INTERFACES.md`](INTERFACES.md).

## Verdict

**Both happy paths pass end to end on one factory-mode instance (run r2, [`evidence/integrated-observed.json`](evidence/integrated-observed.json)).** Live agent authoring by a real model is **untested**: no provider credentials or provider packages exist on this machine. The authoring Interface ran with a real Strands `Agent` tool loop driven by a scripted `Model`, labelled `live: false` throughout. Failure paths remain unit-tested only, as before.

| Claim | Level | Evidence (r2) |
| --- | --- | --- |
| Routine harness startup does not start the runner | Observed | Before the first request: no `pgdata`, no `runner-ready.json`, and the only runner event was `build-added` from publication. `/health` reported `runner_running: false`. |
| First factory work lazily starts the shared runner | Observed | Runner events `postgres-start → temporal-start → namespace-create → serve-ready → worker-start → start(reason=factory-work:<run>)`. |
| Work arrives on the instance's normal A2A identity | Observed | The Agent Card has one skill, `verified-research@1`. `message/send` returned Task `34277fae…` `working`, and `tasks/get` then showed `completed`. |
| Independent A2A services are invoked in parallel | Observed | All 4 `assign` Activities were scheduled by one workflow task (`workflowTaskCompletedEventId` 4). All 4 remote assignments started before the first completed. |
| Review/repair and exact-revision acceptance | Observed | Quality rejected r1 and accepted r2. Acceptance, delivered report and release receipt all name r2 `af57a60b…`. The release receiver shows 1 effect and 1 attempt. All 7 outcome-journal actions are `confirmed`. |
| One result/receipt on the original Task | Observed | One artifact on Task `34277fae…`. A duplicate `start` with the same `action_id` on a new Task was rejected with `continue original Task 34277fae…`. |
| A Strands authoring agent revises a graph from validation feedback | Observed with a scripted model; **live model untested** | Round 1 was invalid (`route must cover each typed value`, missing case `requires_scope`). Round 2 was valid and submitted, then Director auto-approval within the bounded profile. v2 is a different composition: 3 parallel branches with a `clear` counter branch, and a repair bound of 1. |
| v2 is published and runs through the same instance | Observed | v2 (`b7b6b319…`) was published on interpreter build B2 `b-e3d01047763d` and ran in the same harness process (same pid, incarnation unchanged): accepted at r1 with 1 release and `interpreter_revision: "b2"`. |
| A waiting v1 keeps its original bindings | Observed | v1 run (`a746eb04…`, build B1 `b-05b5760359e6`) stayed `input-required` at its r3 Director wait through v2's publication, v2's run, and a graceful harness + runner stop and restart. The runner then ran both builds' workers. After an abort continued on its original Task (`90833571…`), v1 completed `aborted` on manifest `a746eb04…` and B1, with 3 Quality rejections and 0 releases. |
| Recovery of unfinished work starts the runner at startup | Observed | After a graceful stop of both, harness restart logged `start(reason=recover-unfinished:1)`. Identity was unchanged and the incarnation advanced. |
| Each run pinned to its build | Observed | `describe()` shows the parent and child runs pinned to their build (`PinnedVersioningOverride`). [Offline replay](evidence/histories-r2/replay-summary.json): 6/6 histories replay on their own build, and 6/6 are refused by the other build. The refusal comes from the closure guard (`wrong interpreter build`), which surfaces as nondeterminism against the recorded history. |
| Runner clean first start (ports ≤ 32767, no schema change) | Observed | [`runner-cold-smoke.json`](evidence/runner-cold-smoke.json): a fresh home passed in one pass, and a warm restart did no re-init. The earlier high-port attempt stays in [`runner-smoke.json`](evidence/runner-smoke.json), marked superseded and not counted as clean-cold proof. |

A measurement point, not an envelope: with both builds' workers running, the runner reported RSS of about 198 MiB for Temporal, 15 MiB for the PostgreSQL parent, and 72–74 MiB per worker. No memory-ceiling work was done.

## What is merged, and from where

- **Director contract:** typed, provenance-checked run inputs; Director owner claims and abort; child-failure incident projection.
- **Version binding:** a closure manifest pins definition, bindings, contracts, Quality policy and interpreter build, and is re-verified before every node, Activity and child. The workflow is `PINNED` on Worker Deployment `exo-factory`, and every start names its version explicitly. Builds are immutable snapshots, with build ID = `b-` + source digest.
- **Quality and reconciliation:** the `quality_authority` decision; the durable A2A/release outcome journal; unresolved or inconsistent outcomes become terminal `incident` results instead of a workflow waiting forever.
- **Package and operations:** SCRAM-authenticated PostgreSQL, rendered Temporal config, one supervisor with crash restart. It is now a lazy, install-wide runner that serves any number of harness instances.
- **Fixes from the qualification verdict:** `ValueError` fails the workflow instead of retrying forever; a failed parent projects as `failed`; test fault profiles are gone from the product path. The Director token still travels in Workflow history (a known gap).

Integration fixes found in this work: operator tooling no longer takes a new incarnation, which would have fenced the serving harness. A Task is closed only after Temporal reports the execution closed. A running child is `working`, not `input-required`. A second Task is never aliased to an existing run. `wait_worker` requires a live poller of the exact build.

## Layout

| Path | Role |
| --- | --- |
| `src/harness.py` | Harness instance (agent/factory mode), Director, Factory Module, Task projection |
| `src/admin.py` | Operator CLI: `provision`, `publish-template`, `author`, `publications` (never starts the runner) |
| `src/runner.py` | Lazy shared runner: PostgreSQL, Temporal, one worker per build |
| `src/factory.py`, `definition.py`, `binding.py`, `adapter.py`, `worker.py`, … | Bounded declarative interpreter and its pinned closure (`binding.INTERPRETER_FILES`) |
| `src/authoring.py` | Authoring Interface, Strands authoring agent, scripted model, approval |
| `services/` | Pinned test A2A services (the stand-in for the directory) |
| `scenarios/integrated.py`, `scenarios/replay_check.py` | Both happy paths; history retention and replay |
| `briefs/`, `handoff/` | Parallel worker assignments and their reports |
| `src/runtime.py`, `src/supervisor.py` | Unmodified lane baselines kept for diff reference; not imported |

## Reproduce

```sh
PY=/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python
cd prototype/temporal-factory
$PY -B -m unittest discover -s tests            # 50 tests
$PY -B scenarios/integrated.py --home /tmp/exo-proto-int-<fresh>
$PY -B src/runner.py start --home /tmp/exo-proto-int-<fresh> --reason replay
$PY -B "$PWD/scenarios/replay_check.py" --home /tmp/exo-proto-int-<fresh> --address 127.0.0.1:44002 --out "$PWD/evidence/histories-<run>"
$PY -B src/runner.py stop --home /tmp/exo-proto-int-<fresh>
```

The scenario uses ports 44000–44012, 32400–32404, 44800 and 45200–45205, and requires the pinned local binaries named in `INTERFACES.md`. All trial state is preserved: r1 `/tmp/exo-proto-int-r1`, r2 `/tmp/exo-proto-int-r2` (including earlier replay attempts under `replay-attempts/`), and the worker smokes under `/tmp/exo-proto-*`.

## Remaining gaps

- **Live model authoring is untested.** Set a provider in the environment (`model_from_environment`) and run `admin.py author` without `--allow-scripted`.
- **Failure paths are not observed live:** failed child, inconsistent Quality verdict, ambiguous A2A outcome. They remain implemented and unit-tested only.
- **Old-build retirement is not automated.** A retired marker exists, and `binding.may_retire` requires drain.
- **Contract and Quality-policy attestation is missing.** Contracts are fixture-authored (`attested: false`).
- **Operational hardening is out of scope and unbuilt:** Temporal frontend auth, relocatable/signed packaging, online backup, and the memory ceiling. The Director token still travels in Workflow history.
- **Operator CLI concurrency is only partially covered.** Operator tooling now shares the instance catalog with the serving harness through `PublicationStore` locks; concurrent publication was not stress-tested.
- **Some probes are fixture-only.** The release receiver is HTTP, not A2A. `outcome_mode` remains a declared, caller-allowlisted fixture input that steers a candidate toward repair.
- **Scale is one factory instance and one organization.** A second instance attaching to the same runner is designed but was not exercised.
