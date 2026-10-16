# Using the common arbitration fixture

Read [VOCABULARY.md](VOCABULARY.md) before implementation and
[CONTRACT.md](CONTRACT.md) for A1–A5 outcomes. The files in this directory are
shared test services and evidence checks. They are not candidate engine code.

The retained [observed.json](observed.json) is a sanitized, deterministic
summary of the common-service probe. Its oracle verdict is `verified: true`
for **service-fixture** only: three distinct Strands/A2A identities, five
Quality verdicts, one receiver effect after the assignment lost reply, and one
release effect after the release lost reply. It contains no credentials, ports,
process IDs, task IDs, logs, or database state. The exact reproduction and
retained-artifact check from the repository root are:

```sh
tools/spikes/2026-09-22/s2/.venv/bin/python \
  tools/spikes/2026-09-22/arbitration/common/service_probe.py \
  --summary-out tools/spikes/2026-09-22/arbitration/common/observed.json
python3 tools/spikes/2026-09-22/arbitration/common/oracle.py verify-summary \
  tools/spikes/2026-09-22/arbitration/common/observed.json
```

That command uses a fresh temporary state directory and removes its raw logs
and SQLite files after the probe. The summary includes SHA-256 hashes of the
shared service/client/oracle sources. Reproducing it with unchanged source
bytes yields the same JSON; the oracle rejects a stale source hash.

From the Exomachina repository root, use the pinned S2 environment (Strands
1.57.0, A2A SDK 0.3.26, wire protocol 0.3.0):

```sh
tools/spikes/2026-09-22/s2/.venv/bin/python \
  tools/spikes/2026-09-22/arbitration/common/service_probe.py \
  --base /tmp/exo-arbitration-common-YOUR-RUN
python3 tools/spikes/2026-09-22/arbitration/common/oracle.py verify-service \
  /tmp/exo-arbitration-common-YOUR-RUN/service-result.json
```

Choose a **fresh** base directory: the test deliberately persists action IDs,
then kills and restarts receivers. It writes logs and SQLite state under that
directory and stops its child processes. The output is labeled `fixture_only`;
it proves that the shared services behave as promised, not that any engine
passed A1–A5. No model provider, Docker, cloud infrastructure, or credential
is used.

For a candidate run, launch two copies of
`../../decision-round/common/harness_server.py --role capability`, with
separate state directories; launch `quality_server.py --state DIR --port PORT`;
launch `release_server.py --state DIR --port PORT --mode participating`. All
bind loopback only. The A2A calls use
`../../decision-round/common/client.py`'s `send`, `get_task`, and `reconcile`.
`fixture.assignment` makes stable actions; `fixture.typed_join` validates the
typed result content. Quality uses the same A2A `review` command shape as the
decision round. A release is HTTP POST `/release` with Bearer `fixture-token`
and the JSON fields `release_id`, `run_id`, `definition_digest`, `revision`,
`sha256`, `content`, and optional `drop_ack`. GET `/receipts/{release_id}`
retrieves a committed receipt. `receiver_client.py` wraps those calls. For
opaque uncertainty, launch a **separate** `--mode opaque` receiver, POST
`/submit` with the same artifact binding, and do not attempt a second submit
after an uncertain reply. It intentionally has no receipt lookup.

The two branch result types can each be instantiated under distinct action
IDs. `fixture.assignment(run, digest, instance, result_type=...)` names an
instance; `fixture.typed_join(..., declarations={instance: result_type})`
validates all declared results. `counter_evidence@1.scope_status` is the
validated `requires_scope|clear` enum; the join exposes its matching
`route_status` and `requires_scope` boolean. The visible A1 case uses exactly
one instance of each declared type. These helpers describe test data and
outputs; they do not publish or execute a factory.

`oracle.py verify-run success|exhaustion FILE` checks a normalized run
observation. The JSON must include `run_id`, `definition_digest`,
`quality_identity`, `branch_receipts` as returned by the shared A2A client,
`branch_intervals_ns` with `{start,end}` per branch, the exact `join`,
`revisions` containing `{artifact,quality}` pairs, and `repair_count`. A
success additionally includes exact `acceptance`, `release_receipt`, and
`public_child_result`/`public_parent_result` with status `accepted`; an
exhaustion has null acceptance/release, a persisted nested `director_wait`
with a current authorized abort command, and public results `aborted`. See
`verify_run` for the complete field checks. The oracle validates consistency;
retain native engine history and fault logs to establish that the events were
real and autonomous.

`oracle.py verify-candidate FILE` additionally checks the normalized A1
version evidence, six A3 publication negatives, two authority negatives, five
A4 fault points, A5 opaque uncertainty and separated counters, and the
product-owned code breakdown. Add `--require-a2` only after the withheld
composition is revealed and run. An oracle failure reports a missing or
inconsistent observation, not an engine verdict by itself. Record unrun cases
as `not tested` in the candidate report; do not fabricate a passing JSON.

Before the evaluator reveals A2, capture **all maintained implementation
source** and the visible v3 definitions, keeping new publishable definitions
in a separate path so a later valid document can be added without modifying
the frozen code inventory:

```sh
python3 tools/spikes/2026-09-22/arbitration/common/freeze.py capture \
  --out /tmp/exo-candidate-freeze.json PATH_TO_CODE PATH_TO_VISIBLE_V3_DEFINITION
python3 tools/spikes/2026-09-22/arbitration/common/freeze.py verify \
  /tmp/exo-candidate-freeze.json
```

Hand the manifest and `inventory_sha256` to the evaluator **before** A2 is
disclosed. The manifest detects changed, removed, and newly added files under
selected directories; it cannot independently attest when the evaluator
received it. Avoid selecting `node_modules`, `.venv`, runtime state, or logs.
