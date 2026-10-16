# Dagu processless child bridge, integrated with the arbitration factory

23 September 2026. This new spike copied the frozen Dagu arbitration candidate into this directory, then replaced its long-lived `nested` command with a short child start and a native parent `human.task` gate. One scanner in the existing full-bundle supervisor observes the product ledger and native child, then completes the pinned parent gate. It does not schedule graph nodes. The earlier arbitration candidate and scratch bridge remain unchanged: all six frozen source hashes matched `freeze.json` before and after the end-to-end run. The two native v6 definitions have one closure digest, `cd7204c12c35ddbfb7ea39bb1ce5bc48ab4e5b33c62e349c6aaf02d0e08ad79e`.

The result is **a passing accepted path, a passing authorized abort path, and a material failure-path gap**. The same Director A2A Task IDs survived restart and eventually returned exact public results. A child that failed before a terminal product outcome left its parent and Director Task waiting through the observation period, with no failure propagation mechanism in this bridge. This is a bounded integration experiment, not a production-ready bridge.

## Director and recovery observations

[`trial.py`](trial.py) published v6, started each parent through the arbitration Director A2A `message/send` route, and inspected the *original* Task ID through `tasks/get`. [`observed.json`](observed.json) contains the Task snapshots, native node statuses and counts, ledger rows, definition bindings, service PIDs, and bridge events. The fixture's capability and Quality nodes are separate A2A services with their own persisted state; the factory only invokes them as graph nodes. Supervisor process restart and external agent-service restart are distinct concerns; this trial restarted both sets of local fixture processes together.

| Case | Observed result |
| --- | --- |
| Accepted, parent gate restart and lost gate acknowledgement | Child source/counter assignments, typed join, r1 rejection, r2 repair/positive Quality, one acceptance, and one release effect/receipt completed under the pinned child run ID. Its native child was `succeeded` while its parent was natively `waiting` at `child_gate`; the original Director Task was `input-required`. The supervisor and its Dagu/services were killed and restarted. The scanner then completed that existing gate. A fault after Dagu accepted the gate but before the scanner recorded success caused one scan error; its next scan found the gate already succeeded and recorded bridge completion. The same Task became `completed`, with the exact r2 artifact, accepted digest, child ID and release receipt. Both native roots succeeded, and `public_result.doneCount=1`. |
| Both roots waiting through restart, then authorized abort | A second parent was natively `waiting` at `child_gate` while its child was natively `waiting` at `director_wait` after three negative Quality verdicts. Its Director Task was `input-required`. After supervisor/Dagu/service restart, the **same parent and child IDs, native waits, closure digest, and original Task ID** remained. An exact current-r3, owner-epoch-bound Director A2A abort completed the child; the scanner required the delivered authorization, zero acceptance/release, and native child success before completing the parent gate. The original Task became `completed` with `state=aborted`, the exact child ID, no receipt, zero acceptance and zero release. Both native roots succeeded; parent `public_result.doneCount=1`. |
| Child failure before terminal product outcome | An injected r1 Quality failure exhausted the native step retries. The child became native `failed` while its product row remained `active`. Four seconds later the parent was still native `waiting` at `child_gate`, `public_result` had not run, and the original Director Task still reported `input-required`. The scanner did not incorrectly publish an accepted or aborted result. **It also did not propagate or repair this failure**, so parent/Task liveness is unresolved. |

The accepted fault tested a lost scanner acknowledgement **after the Dagu completion HTTP call returned**. It did not force Dagu to persist a human-task input and crash before enqueueing the downstream step. That earlier engine window remains untested. The restart at two simultaneous native waits tested state persistence, not a days-long hold or concurrent competing supervisors. The local supervisor file lock and SQLite transaction serialize one scanner; independent-host ownership was not exercised. Existing arbitration candidate issues remain: the Quality identity guard gap, the participating release no-commit liveness gap, and the Dagu GPL distribution review.

## Matched full-bundle wait scaling

[`scale_probe.py`](scale_probe.py) ran the same full arbitration service bundle and the same `footprint -f bytes --noCategories` PID-selection method as the [earlier wait-scaling trial](../../2026-09-22/arbitration/wait-scaling/result.md). Each parent was admitted by a separate Director A2A Task. At 2 and 10 pairs, every original Task was `input-required` and identified the same pinned child ID and closure digest as the native runs. Every parent was native `waiting` at `child_gate`, every child native `waiting` at `director_wait`, and all ten pairs were still so three seconds after the ten-pair sample. [`scale_observed.json`](scale_observed.json) records every Task ID, pair, PID, role, elapsed time and footprint command.

| Waiting parent/child pairs | Integrated Dagu footprint | PIDs | Earlier frozen Dagu | Earlier Temporal Director |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 549,737,632 B | 9 | 548,050,032 B / 9 | 756,746,768 B / 49 |
| 2 | 599,643,392 B | 9 | 714,760,616 B / 15 | 790,891,216 B / 49 |
| 10 | 678,827,408 B | 9 | 1,302,729,752 B / 39 | 849,300,224 B / 49 |

The second ten-pair sample was **685,741,456 B / 9 PIDs**. The nine roles at every sample were supervisor/shared scanner, Dagu, Director, the existing failure reconciler, two capability A2A fixtures, Quality A2A fixture, and participating/opaque release receivers. The processless parent gate therefore removed the frozen candidate's three retained processes per waiting pair in this tested range. Fresh supervisor ready time was 2.88 seconds; 2/10 admission-to-wait stages took 7.37/10.31 seconds including the two-second settle. The capability fixture restarted four times during the scale run because the v6 child deliberately retained the frozen `--drop-ack true` source assignment. The scanner lived inside the existing supervisor PID.

These are two serial experiments on the same macOS host using the same probe method, but the Temporal and frozen Dagu numbers came from 22 September and different fresh runtimes. The figures are instantaneous de-duplicated physical footprint, not peaks, a concurrency law, or an installation budget. The local A2A services stand in for black-box agent nodes; their future directory discovery and independent deployment were not spiked.

## Effect on the engine choice

This integration **reverses the measured nested-wait process and ten-pair footprint argument** that favored Temporal: the product-shaped Dagu bundle now held 9 PIDs and about 679 MB at ten waits, versus the earlier Temporal Director bundle's 49 PIDs and about 849 MB. Dagu also retains native declarative graph authoring, avoiding a product-owned Temporal graph interpreter. On those measured criteria, Dagu deserves to move ahead provisionally.

It does **not** establish an unconditional final engine reversal. The pre-terminal child failure currently strands the parent Task, and the persisted-human-input/before-enqueue recovery window has no injected proof. Those are precisely the cross-root recovery costs that motivated Temporal. A final ranking should depend on demonstrating bounded, authorized failure propagation or recovery for a failed child and recovery of the persisted-input window, while carrying forward the Quality identity, release liveness and distribution gates. The 0/2/10 result alone cannot waive them.

Reproduce with fresh `/tmp` state (both drivers create one automatically):

```sh
tools/spikes/2026-09-22/s2/.venv/bin/python -B tools/spikes/2026-09-23/dagu-bridge-integration/trial.py
tools/spikes/2026-09-22/s2/.venv/bin/python -B tools/spikes/2026-09-23/dagu-bridge-integration/scale_probe.py
```
