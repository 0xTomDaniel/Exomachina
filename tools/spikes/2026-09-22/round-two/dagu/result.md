# Dagu Community round-two runtime trial

22 September 2026. **Immutable, versioned DAG names kept a paused v1 root bound to its whole two-level child closure after v2 publication and a Dagu restart.** Dagu did not resolve an ambiguous remote submission itself: after the remote receiver accepted work and dropped the acknowledgement, Dagu marked the submit step failed. A manual same-run retry succeeded because the synthetic receiver retained an idempotency receipt. This is a bounded local result, not a product runtime selection or an exactly-once external-effect claim.

## Pinned runtime and topology

Reused the official Dagu Community `v2.17.0` Darwin arm64 archive from the [first Dagu countertrial](../../countertrials/dagu/result.md). Archive SHA-256 was `da86f2a7278afcadfe8202b9af2b9613a5fc997ffc47daf1799b64d7fc5afeb1` (matching the published release checksum in that trial); unpacked binary SHA-256 was `fd855996bab956835043ae8cbb00ac3ea5211a6be6992374727b7908d393aac3`. `dagu version` returned `2.17.0`. The [tagged licensing guidance](https://github.com/dagucloud/dagu/blob/v2.17.0/LICENSING.md) identifies GPL-3.0-or-later and distinguishes the separately operated CLI/server from linking its experimental Go API into a proprietary binary. Exact Exomachina bundle, source, and notice obligations remain uncleared. No paid Dagu feature or Go API was used.

One Dagu `start-all` process served HTTP and scheduling at `127.0.0.1:18417`, with `DAGU_COORDINATOR_ENABLED=false`, `DAGU_AUTH_MODE=none` for loopback only, and file state in `/tmp/exomachina-round-two-dagu/home`. Active DAGs spawned Dagu runner and command processes; no database, broker, coordinator, worker daemon, Docker, model provider, Strands harness, or A2A service was used. The independent synthetic remote receiver was a second Python process at `127.0.0.1:18418`, persisting attempts and receipts in `/tmp/exomachina-round-two-dagu/remote.sqlite`.

At a two-root waiting sample, Dagu PID 1271 had 124,736 KiB RSS. After the remote probe, a warm idle Dagu PID 1732 had 114,160 KiB RSS and the separate Python receiver PID 1733 had 24,192 KiB RSS. The Dagu home occupied 676 KiB at the final sample. These are instantaneous Darwin RSS values; active child peak, CPU, physical footprint, Strands/A2A, and installation-wide memory were not measured in this round. The [first trial](../../countertrials/dagu/result.md) has a separate active-process sample, which is not a common-workload comparison.

## Immutable closure and independent roots

The source fixtures in this directory use names supplied by their filenames. Each root has a processless `publication_gate` human task before invoking either capability. V1 names `exo_capability_{a,b}_v1`, and those child DAGs name `exo_leaf_{a,b}_v1`; v2 uses the corresponding `_v2` names and adds `verify` before `review`. Each root joins its two independent capabilities, performs review, waits at `director`, then records one synthetic delivery. This is two independent factory roots in one helper, not a parent/child root orchestrator or A2A correlation proof.

Sequence actually run:

1. Copied only `*_v1.yaml` files into the isolated DAG directory, validated them, and started `factory-v1-001` through HTTP. It reached `waiting` at `publication_gate`; neither child had started.
2. Copied and validated the five `*_v2.yaml` files **without restarting the server**. Started `factory-v2-001` through HTTP. Both roots were simultaneously `waiting` with their respective graph shapes; v2 alone had `verify`.
3. Gracefully stopped and restarted the Dagu process against the same file state. Both roots remained waiting. Completed each `publication_gate`. Both then executed their capabilities and review, and waited at `director`.
4. Inspected Dagu's persisted child run IDs using `dagu status --run-id <root-run> --sub-run-id <child-run> <root-name>`. V1's `capability_a` and `capability_b` invoked `exo_leaf_a_v1` and `exo_leaf_b_v1`; v2 invoked the two `_v2` leaves. The event file contained exactly one of each version's two capability markers, one v1 review, and one v2 verify and review before delivery.
5. Sent two genuinely concurrent, identical `director` completion requests to the v1 root. Both returned HTTP 200, one `alreadyCompleted:false` and the other `alreadyCompleted:true`; one response said `queued:false`, the other `queued:true`. Completed v2 once. Both roots finished `succeeded`; each node reported `doneCount=1`, and the event file had exactly one delivery marker per version. This is one request race, not a general concurrency guarantee.

The [final API run summary](final-run-summary.json) records root status, node state, child names, and persisted sub-run IDs.

The complete synthetic event file at the final sample was:

```text
v1:capability_a
v1:capability_b
v1:review
v2:capability_a
v2:capability_b
v2:verify
v2:review
v1:deliver
v2:deliver
remote:deliver
```

Fixture SHA-256 digests were:

| Definition | SHA-256 |
| --- | --- |
| `exo_factory_v1` | `585697aa9d61584ac015d6ba29d0381005e3ece999cfceb2e120e94d0c5f4aa6` |
| `exo_capability_a_v1` / `exo_capability_b_v1` | `6d696c5c5a778bebe7097941c764f999119cc397f14aed88d6850ad6e99eae64` / `c1854b1ee4c4c31b59c22887934f84b32adf7b1d2233bdcf4b4f575fa3cc720b` |
| `exo_leaf_a_v1` / `exo_leaf_b_v1` | `748a5403815b1801e96c54888b1b3c9573a9ea65c5554428f0625a5b5a78a7fd` / `83e09f96b1e2d3b9e94de03a6918c757029d53385584a5402d687746a962bc96` |
| `exo_factory_v2` | `263876514282f7bfbfe6ccc5df30c3b3106bee6dbaab5d0cd2ad83f39f59d208` |
| `exo_capability_a_v2` / `exo_capability_b_v2` | `a02f92875efdaba738d30ab994c87d20bb51ee44e1d129eefa231a9fbdcbd46b` / `062c82662054532bd8e5003151edf2b1a1de5e7f0d3d9fa0b2fd0a2cff9cbccc` |
| `exo_leaf_a_v2` / `exo_leaf_b_v2` | `0a15e8810af6f861a0cb751aef5ea4e14b8281c907d21927b4fbde0d662349ea` / `0e0ba8c43b3efff2734f6cb7890a0d6d5db2867545859e4004be5a51e52f4fc4` |
| `exo_remote_work` | `2c6f75d11d5fb96b91f5cf0e000c6fbbad929a9941bc3f14ffcd31c64c75b666` |

Those digests were measured from the published fixture files, but **Dagu did not persist the closure digest in each run**. The earlier trial showed that editing a referenced mutable child file while a parent waited caused that parent to use the new child. This round deliberately kept every published v1 file intact and used new v2 names. A product publisher must enforce no overwrite, resolve and hash the full transitive closure, reject missing or forbidden references, and store the closure identity with the run. Dagu's parent snapshot alone is insufficient. This trial did not implement that publisher or a direct-review-bypass validator.

## Remote accepted effect with lost acknowledgement

The separate [receiver](remote_stub.py) uses SQLite's unique `key` receipt and records every attempt. Its first `POST /submit` commits `{work: synthetic-render}` under `remote-work-001`, then closes the socket without an HTTP response. The [client](remote_client.py) therefore exits nonzero. Dagu recorded `remote-root-001` as `failed`: `submit` failed, while `director` and `deliver` were aborted. SQLite showed **one attempt and one accepted receipt**.

After restarting both Dagu and the receiver, `dagu retry --run-id remote-root-001 exo_remote_work` re-executed `submit`. The receiver returned the existing receipt (`alreadyAccepted:true`), and the same root reached its Director wait. SQLite then showed **two attempts but one accepted receipt**. Completing the Director task finished the root; the event file had one `remote:deliver`. The remote root was a third independent root with its own persisted run ID. There was no simultaneous same-run owner race and no real A2A endpoint. The single accepted effect came from the receiver's unique key; removing that receipt policy would leave duplicate acceptance possible after this failure.

## Reproduce the bounded sequence

The following commands show the exact runtime surfaces used, from the Exomachina checkout. The probe files are confined to this directory and the isolated `/tmp` path. Use the included YAML/Python sources; first copy only v1, then copy v2 while Dagu is running.

```sh
mkdir -p /tmp/exomachina-round-two-dagu/home/dags
cp tools/spikes/2026-09-22/round-two/dagu/*_v1.yaml /tmp/exomachina-round-two-dagu/home/dags/
for f in /tmp/exomachina-round-two-dagu/home/dags/*.yaml; do DAGU_HOME=/tmp/exomachina-round-two-dagu/home DAGU_AUTH_MODE=none /tmp/exomachina-countertrials/dagu/dagu validate "$f"; done
DAGU_HOME=/tmp/exomachina-round-two-dagu/home DAGU_COORDINATOR_ENABLED=false DAGU_AUTH_MODE=none /tmp/exomachina-countertrials/dagu/dagu start-all --host 127.0.0.1 --port 18417
curl -X POST http://127.0.0.1:18417/api/v1/dags/exo_factory_v1.yaml/start -H 'Content-Type: application/json' -d '{"dagRunId":"factory-v1-001"}'
cp tools/spikes/2026-09-22/round-two/dagu/*_v2.yaml /tmp/exomachina-round-two-dagu/home/dags/
curl -X POST http://127.0.0.1:18417/api/v1/dags/exo_factory_v2.yaml/start -H 'Content-Type: application/json' -d '{"dagRunId":"factory-v2-001"}'
# Stop/restart start-all against the same DAGU_HOME here.
curl -X POST http://127.0.0.1:18417/api/v1/dag-runs/exo_factory_v1/factory-v1-001/human-tasks/publication_gate/complete -H 'Content-Type: application/json' -d '{"accepted":true}'
curl -X POST http://127.0.0.1:18417/api/v1/dag-runs/exo_factory_v2/factory-v2-001/human-tasks/publication_gate/complete -H 'Content-Type: application/json' -d '{"accepted":true}'
```

For the separate lost-ack probe, copy `remote_client.py` and `remote_stub.py` to `/tmp/exomachina-round-two-dagu/`, copy `exo_remote_work.yaml` into its `home/dags`, and start `python3 /tmp/exomachina-round-two-dagu/remote_stub.py`. Then:

```sh
curl -X POST http://127.0.0.1:18417/api/v1/dags/exo_remote_work.yaml/start -H 'Content-Type: application/json' -d '{"dagRunId":"remote-root-001"}'
# First submission fails after the SQLite receipt commits. Restart both processes.
DAGU_HOME=/tmp/exomachina-round-two-dagu/home DAGU_AUTH_MODE=none /tmp/exomachina-countertrials/dagu/dagu retry --run-id remote-root-001 exo_remote_work
curl -X POST http://127.0.0.1:18417/api/v1/dag-runs/exo_remote_work/remote-root-001/human-tasks/director/complete -H 'Content-Type: application/json' -d '{"accepted":true}'
```

The owned code is 79 lines of factory-root YAML, 32 lines of child/leaf YAML, 17 lines of remote-root YAML, and 59 lines of Python client/receiver. Runtime logs, the SQLite database, and the executable stay under `/tmp`; no model or real customer effect was called. The remaining decision gates are product-owned immutable publication and acceptance validation, run-linked closure digests, real A2A submission and reconciliation, concurrent competing owner behavior, cancellation/deadline/budget handling, authorization, license packaging review, and an installation-wide resource comparison. Dagu remains a lightweight candidate, with these obligations visible rather than cleared.
