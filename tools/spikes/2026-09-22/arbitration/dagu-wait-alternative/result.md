# Dagu processless nested-wait alternative

22 September 2026. **A separately rooted Dagu parent can wait at native `human.task` without a parent Python polling process, while its separately rooted child also waits.** A single shared product supervisor/reconciler completed the parent after the child had both an authorized durable accepted outcome and native `succeeded` status. The two roots kept their pinned run IDs across restart, and parent continuation ran once. This is a feasibility result for wait ownership, not a replacement for the frozen arbitration candidate or proof of its full A2A, Quality, release, and abort contract.

The frozen [`ops.py`](../dagu/ops.py) `nested()` starts a deterministic child root and polls product state every 250 ms in the parent command process. This scratch directory did not edit that file, its sibling frozen sources, or their manifests. The six frozen source SHA-256 values still match [`freeze.json`](../dagu/freeze.json), as recorded in [`freeze_check.json`](freeze_check.json). The official Dagu Community v2.17.0 Darwin arm64 binary was the same pinned artifact, SHA-256 `fd855996bab956835043ae8cbb00ac3ea5211a6be6992374727b7908d393aac3`.

## Native wait and one-shot bridge

[`trial.py`](trial.py) ran two independent native roots, `exo-wait-parent-001` and `exo-wait-child-001`. Both were `waiting` at their own `human.task` nodes with downstream steps `not_started`. The scoped wait sample contained **one Dagu `start-all` process and zero bridge/wait Python processes**. After a graceful Dagu restart, both native runs remained waiting with the same IDs. A bridge owner-epoch increment rejected the old child-completion command and old parent reconciliation; an invalid fixture token was also rejected. Valid acceptance completed the child. The parent stayed waiting until the bounded [`bridge.py`](bridge.py) command checked the exact child ID/status and completed its existing gate. Both native roots then succeeded, and repeating the command returned `already_completed` without posting a second gate completion. See [`observed.json`](observed.json). The probe driver itself was outside the runtime process count.

## Child-final-step callback failed under interruption

The separate [`auto_trial.py`](auto_trial.py) put `reconcile` in the child's final Dagu step with `retry_policy: {limit: 3, interval_sec: 2}`. It first recorded an authorized accepted child outcome. The driver then killed that bridge step **before** its parent-gate HTTP call and restarted Dagu. After 65 seconds, the parent was still waiting. Native `dagu status` marked `notify_parent` **failed** with `process terminated unexpectedly - stale local process detected`; it did not run the expected step retry after this interruption. [`auto_observed.json`](auto_observed.json) preserves the pre-kill native nodes, IDs, rejected wrong-child/unauthorized commands, source hashes, and timeout; [`auto_failure_status.json`](auto_failure_status.json) extracts the final native failed step and error. The full native status and logs remain in `/tmp/exomachina-dagu-wait-auto-001/home`. A three-retry, two-second policy is a short bounded step retry, not a guarantee that a Director wait lasting days can wake after an arbitrary process failure. The short restart already falsified that guarantee here, so a longer outage was unnecessary for this bounded trial. Dagu's [scheduling documentation](https://docs.dagu.sh/writing-workflows/scheduling) describes stale local run detection, and its [human-task documentation](https://docs.dagu.sh/writing-workflows/human-tasks) describes persisted processless waits and idempotent completion.

## Shared reconciler passed the bounded recovery cases

The safer [`shared_supervisor.py`](shared_supervisor.py) starts Dagu and, in the **same single Python supervisor process**, scans pending pinned bridge rows. It does not schedule graph steps. For an eligible row, it requires the exact authorized product child outcome, matching parent/child definition digests, native child `succeeded`, and the child's Director and outcome nodes `succeeded`. It then completes the parent's existing native `child_gate` and records that bridge as completed. SQLite `BEGIN IMMEDIATE` serializes a gate call with another scanner invocation; the one-command supervisor holds a local file lock. There is one scanner loop for all pending pairs, with O(pending pairs) row checks each cycle. No process is held per parent wait.

[`shared_trial.py`](shared_trial.py) proved two fresh cases in [`shared_observed.json`](shared_observed.json):

1. With the child natively `succeeded` and parent natively `waiting`, the scanner paused before parent-gate acknowledgement. The driver killed both supervisor and Dagu, then restarted the **same supervisor command**, without a driver reconciliation command. The replacement scanner completed the parent gate. A further injected failure immediately after Dagu accepted that gate, before product state was recorded, was recovered by observing the native completed gate on the next scan. Final parent and child were `succeeded`, parent continuation `doneCount=1`, product bridge `completed`, and the public-result stand-in contained the exact child ID and accepted outcome. The event log showed one `parent_gate_posted`, one injected `scan_error`, and one `bridge_completed`.
2. The child completed and reached native `succeeded` **before the parent native run started**. The scanner retained the pinned binding, found no parent run yet, then completed the parent's gate after it appeared. Both roots and the stand-in public result finished successfully.

The source hashes recorded before and after these fault cases match. The shared trial did not use the failing child-final-step callback. Its parent cannot continue merely because product `child_outcome='accepted'`; the scanner also requires the child native DAG to be `succeeded`. This resolves the ordering problem exposed by the callback trial, where the product outcome had been recorded while the child native DAG still ran and later failed.

## Wait footprint and cost boundary

[`scale_probe.py`](scale_probe.py) started 0, 2, then 10 separately rooted parent/child native wait pairs under the same minimal shared-supervisor topology. Every requested parent and child was observed in native `waiting` before its sample. Darwin `footprint -f bytes --noCategories` measured:

| Waiting parent/child pairs | Scoped PIDs | Footprint |
| ---: | ---: | ---: |
| 0 | 2 | 81,118,576 B |
| 2 | 2 | 84,673,952 B |
| 10 | 2 | 104,007,144 B |

The two PIDs were the Python supervisor/reconciler and Dagu. These are instantaneous de-duplicated footprint samples, not peaks. They include native Dagu file state and the shared scanner, but **exclude** the frozen candidate's Director A2A service, two capability servers, Quality, release receivers, and other product services. They are therefore not directly comparable to whole-bundle Dagu or Temporal footprints. The 2/10 scale pairs exercised only native waiting and correlated bridge rows; their hardcoded scratch continuation commands were not executed. See [`scale_observed.json`](scale_observed.json).

The scratch shared path contains **457 Python source lines** across `bridge.py` (153), `auto_bridge.py` (161), and `shared_supervisor.py` (143), plus **34 YAML template lines** for its parent and child DAGs. These source totals include fixture API/CLI plumbing and unused earlier manual-bridge operations; they are not a clean production line delta. Evidence drivers are separate. Durable scratch product state is one 8-column bridge binding row per pair (`parent/child IDs`, names, two definition hashes, owner epoch, bridge state) and one 4-column outcome/public-result row per pair. The frozen candidate already has run correlation, definition digests, owner epochs, Director commands, state, and public result in its product ledger, so a production integration should map to those records; that mapping and its exact code delta were not implemented here. There is no second durable graph-progress queue. One shared scanner process replaces the per-wait parent pollers, and its periodic checks are product-owned logic.

## Remaining integration gates

The scratch exercised **accepted child only**. It did not prove the arbitration exhaustion path: exact authorized current-r3 abort, parent `aborted` public result, and zero acceptance/release effect. The stand-in `public_result_json` carries child ID/outcome, not the frozen candidate's verified artifact, current revision/digest, receiver receipt, or A2A Task ID. The trial started roots through Dagu's loopback API rather than the frozen Director facade, so preserving the same factory A2A Task and full published closure remains to be integrated and tested. Fixture token authorization and sequential stale-owner rejection are not a concurrent multi-owner security proof; the shared scanner's local file lock was not tested against overlapping independent hosts.

The shared scanner observes an already completed parent gate after a lost product acknowledgement. It did not inject failure between Dagu persisting the human-task input and enqueueing continuation; the [documented recovery path](https://docs.dagu.sh/writing-workflows/human-tasks) may require resume or repeating the same completion input, and that branch remains unimplemented. It also does not repair a native child DAG that fails **before** reaching terminal product outcome. The existing frozen reconciler handles certain failed native steps; joining these responsibilities without duplicated policy is future integration work. The scratch Dagu endpoint used unauthenticated loopback only, and the prior GPL distribution review remains applicable.

To reproduce, use a fresh runtime path for each driver (the scripts reject reused `/tmp` state) and the pinned local Dagu binary. From the Exomachina checkout:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 tools/spikes/2026-09-22/arbitration/dagu-wait-alternative/trial.py
PYTHONDONTWRITEBYTECODE=1 python3 tools/spikes/2026-09-22/arbitration/dagu-wait-alternative/auto_trial.py
PYTHONDONTWRITEBYTECODE=1 python3 tools/spikes/2026-09-22/arbitration/dagu-wait-alternative/shared_trial.py
PYTHONDONTWRITEBYTECODE=1 python3 tools/spikes/2026-09-22/arbitration/dagu-wait-alternative/scale_probe.py
```
