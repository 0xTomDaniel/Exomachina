# Temporal bounded qualification: worker coordination

23 September 2026. Orchestrator: Claude Opus 5.5 (Herdr `wG:p4`, agent `exo_temporal_orchestrator`). Workers: Codex gpt-6-sol xhigh in Herdr tab `wG:t5` "Temporal spike workers".

This is bounded qualification work for the **conditional** Temporal + stable Python interpreter direction chosen in [`docs/engine-reassessment-2026-09-23.md`](../../../../docs/engine-reassessment-2026-09-23.md). It is not product qualification. Read that document, its [brief](../../../../docs/engine-reassessment-brief-2026-09-23.md), [`CONTEXT.md`](../../../../CONTEXT.md), and the two Temporal results before working:

- [`temporal-fresh-composition/result.md`](../temporal-fresh-composition/result.md) (typed `outcome_mode` Director gap)
- [`temporal-recovery-scale/result.md`](../temporal-recovery-scale/result.md) (latest source base; failed-child projection untested)
- [`zigflow-reassessment/result.md`](../zigflow-reassessment/result.md) (activation routing race)

## Product boundary (fixed; do not relitigate)

- The factory lives in the customized Strands harness; its Director exposes an A2A endpoint. Each harness instance owns its identity and durable state. One organization per initial deployment.
- Workflow graph nodes are independent black-box A2A services. Factory publication must **never** deploy, restart, or inspect those services.
- One install/start command may manage bundled local Temporal, PostgreSQL and worker processes. No mandatory Docker. Automatic worker rollout is acceptable if active runs keep their exact definition and a compatible interpreter/worker build.
- Required: approved-block versioned factory documents, exact dependency closure (definition, capability contracts, Quality policy, interpreter/worker build), independent Quality authority, and original Director A2A Task continuity.

## Lanes and ownership

| Agent | Pane | Lane | Owned directory | Ports | Runtime state |
| --- | --- | --- | --- | --- | --- |
| `tw_director` | `wG:p5` | 1. Typed Director inputs; failed-child projection to the same original A2A Task | `temporal-director-contract/` | 41000–41999 | `/tmp/exo-tq-director-*` |
| `tw_version` | `wG:p6` | 2. Immutable run/worker/interpreter version binding across upgrade and publication activation | `temporal-version-binding/` | 42000–42999 | `/tmp/exo-tq-version-*` |
| `tw_package` | `wG:p7` | 3. Native one-command Temporal/PostgreSQL package, backup/restore/migration, endpoint protection, full-bundle resources, memory-ceiling account | `temporal-package-ops/` | 43000–43999 | `/tmp/exo-tq-package-*` |
| `tw_quality` | `wG:p8` | 4. Quality identity/verdict consistency; A2A unknown-outcome reconciliation | `temporal-quality-reconciliation/` | 44000–44999 | `/tmp/exo-tq-quality-*` |

**Port correction (10:22):** Temporal persists membership ports as PostgreSQL `smallint`, so membership and PostgreSQL ports must be below 32768. Each lane additionally owns a low range for them: `tw_director` 31100–31199, `tw_version` 31200–31299, `tw_package` 31300–31399, `tw_quality` 31400–31499. Keep frontend/HTTP/service ports in the 4x000 ranges above.

All directories are siblings under `tools/spikes/2026-09-23/`. Copy the base source from `temporal-recovery-scale/` into your own directory; never edit another lane's directory, prior spike directories, or `docs/`. Replace the hard-coded `runtime.PORTS` and fixture `SERVICE_PORTS` with values inside your range. Reuse the existing pinned binaries under `/tmp/exomachina-countertrials/temporal/runtime`, `/opt/homebrew/opt/postgresql@16/bin`, and the venv `tools/spikes/2026-09-22/arbitration/temporal/.venv` (Python 3.12.9, temporalio 1.33.0) rather than downloading duplicates. **The data volume has about 20 GiB free**; keep state small, avoid large downloads, and delete your own `/tmp/exo-tq-*` state after recording evidence. Stop every process you start.

Shared-file changes (`docs/`, `docs/exomachina.html`, this file, the integrated verdict) belong to the orchestrator. Put requested shared changes under "Requests to orchestrator" in your `result.md`.

## Safety boundary (mandatory)

Automatic approval review previously rejected the local Temporal **failed-child/upgrade fault-injection** test and the Effect **Quality verdict disagreement fault-injection** test as possible cybersecurity risk. Therefore:

1. Do **not** repeat, reword, rename, split up, or otherwise route around those rejected actions: no deliberately injected child Workflow failures, no crash/kill-while-waiting upgrade experiments, no forced nondeterminism against live runs, no tampered or disagreeing Quality verdicts sent through live services, no process kills.
2. Permitted evidence: source review; implementation in your own directory; pure unit tests of mapping/validation/decision functions with in-memory inputs; Temporal's offline `Replayer` against histories you produced from ordinary successful runs; ordinary happy-path and ordinary-operations runs (normal publication, graceful stop/start, backup and restore, schema version checks, normal typed-input runs).
3. If an action is blocked by approval review, **stop that action**, do not retry it in another form, and record the gap as *unproved*.
4. Never claim a pass from an unrun test. Label each claim **observed** (you ran it; cite the command and output file), **unit-level** (pure function test only), or **source inference** (reading code/docs).
5. No external push, release, production deployment, or changes outside the Exomachina repository (other than your own `/tmp/exo-tq-*` state).

## Deliverable per lane

In your owned directory: source, tests, machine-readable `observed.json`, and a concise `result.md` with (a) verdict for the lane's gate: *pass / fail / partial / unproved*, (b) evidence table with observed vs unit-level vs source inference, (c) reproduction commands, (d) remaining gaps, (e) requests to orchestrator. Report a short status line when you finish.
