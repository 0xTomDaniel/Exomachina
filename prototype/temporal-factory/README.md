# Integrated Temporal factory prototype

23 September 2026 · branch `feat/temporal-factory-prototype` · macOS arm64, Python 3.12.9, Node 26.0.0, temporalio 1.33.0, Temporal Server 1.32.0, PostgreSQL 16.15, strands-agents 1.57.0, a2a-sdk 0.3.26, `@earendil-works/pi-ai` 0.87.1 · **a lean, runnable integration candidate, not product qualification.**

This merges the four bounded Temporal lanes (`temporal-director-contract`, `temporal-version-binding`, `temporal-quality-reconciliation`, `temporal-package-ops`) into one candidate built around the corrected architecture. A customized Strands harness instance runs as an ordinary agent or as a factory. In factory mode, its Director agent and Factory Module sit inside the instance, behind that instance's normal A2A identity and `verified-research@1` capability contract. There is no separate factory endpoint, and callers never choose a graph, package or version. The shared contract between modules is in [`INTERFACES.md`](INTERFACES.md).

## Verdict

**Both original happy paths pass end to end on one factory-mode instance** (run r2, [`evidence/integrated-observed.json`](evidence/integrated-observed.json)). The final-phase choice keeps the Python Strands harness and adds an install-wide Node/pi-ai broker for graph authoring only. A real ChatGPT/Codex subscription model authored and published v2 through the Strands tool loop; the harness A2A Task and pinned Temporal parent/child workflows completed ([live attempt 2](evidence/live-authoring-codex-subscription.json), [parent history](evidence/live-authoring-codex-subscription/parent.json), [child history](evidence/live-authoring-codex-subscription/child.json)). The valid first draft took 1 round, 4 model calls and 3 tool calls. The [live scan](evidence/live-authoring-codex-subscription-scan.json) covered 2,011 files, including 107 commit candidates, with 0 hits and a detected synthetic positive control. [Attempt 1](evidence/live-authoring-codex-subscription-attempt1.json) stopped on a scenario assumption that the model must revise an invalid draft: it had submitted a valid first draft in 1 round, 4 model calls and 3 tool calls, but never reached A2A or Temporal. Its [sidecar scan](evidence/live-authoring-codex-subscription-attempt1-scan.json) found 0 hits across 158 files, including 102 commit candidates. The revised provider-specific rule accepts a valid first draft. The [synthetic run](evidence/live-authoring-synthetic-loopback.json) proves the authoring repair loop with 2 rounds; the live run does not. The Director and release receiver remain fixtures, and the earlier non-broker failure paths remain unit-tested only.

Levels: **Observed-real** means real code and services, including Temporal, A2A or the harness; fixture decisions may still drive them. **Observed-synthetic** means real code against the loopback mock provider or fixture credentials. **Unit-tested** means an isolated test.

| Claim | Level | Evidence |
| --- | --- | --- |
| Routine harness startup does not start the runner | Observed-real | Before the first request: no `pgdata`, no `runner-ready.json`, and the only runner event was `build-added` from publication. `/health` reported `runner_running: false` (r2). |
| First factory work lazily starts the shared runner | Observed-real | Runner events `postgres-start → temporal-start → namespace-create → serve-ready → worker-start → start(reason=factory-work:<run>)` (r2). |
| Work arrives on the instance's normal A2A identity | Observed-real | The Agent Card has one skill, `verified-research@1`. `message/send` returned Task `34277fae…` `working`, and `tasks/get` then showed `completed` (r2). |
| Independent A2A services are invoked in parallel | Observed-real | All 4 `assign` Activities were scheduled by one workflow task (`workflowTaskCompletedEventId` 4). All 4 remote assignments started before the first completed (r2). |
| Review/repair and exact-revision acceptance | Observed-real | Quality rejected r1 and accepted r2. Acceptance, delivered report and release receipt all name r2 `af57a60b…`. The HTTP fixture release receiver shows 1 effect and 1 attempt. All 7 outcome-journal actions are `confirmed` (r2). |
| One result/receipt on the original Task | Observed-real | One artifact on Task `34277fae…`. A duplicate `start` with the same `action_id` on a new Task was rejected with `continue original Task 34277fae…` (r2). |
| A Strands authoring agent revises a graph from validation feedback | Observed-synthetic | The r2 tool loop used a scripted Strands `Model`: invalid first draft, valid second draft and bounded Director auto-approval. The broker-backed loopback scenario also revised an invalid draft and published v2. Neither used a live provider. |
| v2 is published and runs through the same instance | Observed-real | v2 (`b7b6b319…`) was published on interpreter build B2 `b-e3d01047763d` and ran in the same harness process (same pid, incarnation unchanged): accepted at r1 with 1 HTTP fixture release and `interpreter_revision: "b2"` (r2). |
| A waiting v1 keeps its original bindings | Observed-real | v1 run (`a746eb04…`, build B1 `b-05b5760359e6`) stayed `input-required` at its r3 Director wait through v2's publication, v2's run, and a graceful harness + runner stop and restart. The runner then ran both builds' workers. After an abort continued on its original Task (`90833571…`), v1 completed `aborted` on manifest `a746eb04…` and B1, with 3 Quality rejections and 0 releases (r2). |
| Recovery of unfinished work starts the runner at startup | Observed-real | After a graceful stop of both, harness restart logged `start(reason=recover-unfinished:1)`. Identity was unchanged and the incarnation advanced (r2). |
| Each run pinned to its build | Observed-real | `describe()` shows the parent and child runs pinned to their build (`PinnedVersioningOverride`). [Offline replay](evidence/histories-r2/replay-summary.json): 6/6 histories replay on their own build, and 6/6 are refused by the other build. The refusal comes from the closure guard (`wrong interpreter build`), which surfaces as nondeterminism against the recorded history. |
| Runner clean first start (ports ≤ 32767, no schema change) | Observed-real | [`runner-cold-smoke.json`](evidence/runner-cold-smoke.json): a fresh home passed in one pass, and a warm restart did no re-init. The earlier high-port attempt stays in [`runner-smoke.json`](evidence/runner-smoke.json), marked superseded and not counted as clean-cold proof. |
| Broker-backed synthetic authoring repairs and runs v2 | Observed-synthetic | [Final loopback run](evidence/live-authoring-synthetic-loopback.json) on commit `49b4c44`, trial `/tmp/exo-proto-live-syn-final-49b4c44-8174def4`: invalid then valid over 2 rounds, 4 broker model calls, 3 tool calls; v2 completed through the real harness A2A Task and pinned Temporal parent/child histories. The run scanned 104 commit-candidate files and 2,012 files overall with 0 hits, detected the synthetic positive control, and its [final sidecar scan](evidence/live-authoring-synthetic-loopback-scan.json) found 0 hits. Provider, Director decisions, Quality inputs and release receiver were fixtures. |
| Two processes share one broker and keep sessions separate | Observed-synthetic | [Node suite](evidence/broker-phase/node-broker-tests.txt): one PID and unchanged socket inode/ready PID, with distinct mock session histories. Python process racing and stream separation are covered by its broker tests. |
| Synthetic OAuth refresh and leak controls | Observed-synthetic | [Node suite](evidence/broker-phase/node-broker-tests.txt) rotated fixture credentials; the [loopback scenario](evidence/live-authoring-synthetic-loopback.json) scanned its artifacts and commit candidates with zero hits and detected a planted credential copy and bare token. |
| Fixture override, egress, error boundary, budget and no API-key fallback | Unit-tested | [Node regression](evidence/broker-phase/final-regression.txt): 18 passed. The Python count is 90: 80 in that regression plus 10 [provider-specific acceptance tests](tests/test_live_authoring_acceptance.py). Tests use fixture stores, mock OAuth and loopback HTTP. |
| ChatGPT/Codex subscription device sign-in and forced token refresh | Observed-real | [Refresh evidence](evidence/live-refresh-codex-subscription.json): signed-in account `sha256:188b022d6e97`; 1 forced refresh succeeded and expiry advanced 47,395 ms. [Live run](evidence/live-authoring-codex-subscription.json) used the same signed-in account. |
| ChatGPT/Codex acceptance of broker identity and originator | Observed-real | The device flow completed sign-in; [refresh evidence](evidence/live-refresh-codex-subscription.json) records successful token-endpoint refresh with broker originator `exomachina`, and [live authoring](evidence/live-authoring-codex-subscription.json) records SSE originator acceptance `success`. The browser authorize-URL rewrite was not live-tested. |
| Real model authors, publishes and runs a graph | Observed-real | [Attempt 2](evidence/live-authoring-codex-subscription.json): `gpt-6-sol`, first-pass valid, 1 round, 4 model calls, 3 tool calls; auto-approval published v2 and the A2A Task completed. [Parent](evidence/live-authoring-codex-subscription/parent.json) and [child](evidence/live-authoring-codex-subscription/child.json) Temporal histories are `COMPLETED` and pinned to `b-05b5760359e6` (14 and 44 events). The [final scan](evidence/live-authoring-codex-subscription-scan.json) found 0 hits across 2,011 files, including 107 commit candidates, with a synthetic positive control. Director and release remain fixtures. |

A measurement point, not an envelope: with both builds' workers running, the runner reported RSS of about 198 MiB for Temporal, 15 MiB for the PostgreSQL parent, and 72–74 MiB per worker. No memory-ceiling work was done.

## What is merged, and from where

- **Director contract:** typed, provenance-checked run inputs; Director owner claims and abort; child-failure incident projection.
- **Version binding:** a closure manifest pins definition, bindings, contracts, Quality policy and interpreter build, and is re-verified before every node, Activity and child. The workflow is `PINNED` on Worker Deployment `exo-factory`, and every start names its version explicitly. Builds are immutable snapshots, with build ID = `b-` + source digest.
- **Quality and reconciliation:** the `quality_authority` decision; the durable A2A/release outcome journal; unresolved or inconsistent outcomes become terminal `incident` results instead of a workflow waiting forever.
- **Package and operations:** SCRAM-authenticated PostgreSQL, rendered Temporal config, one supervisor with crash restart. It is now a lazy, install-wide runner that serves any number of harness instances.
- **Model broker:** a persistent, install-wide Node process runs published pi-ai 0.87.1 behind a Strands `Model` adapter. It powers graph authoring only. The embedded Pi SDK was the runner-up in the retained debate; it is not part of this prototype.
- **Fixes from the qualification verdict:** `ValueError` fails the workflow instead of retrying forever; a failed parent projects as `failed`; test fault profiles are gone from the product path. The Director token still travels in Workflow history (a known gap).

Integration fixes found in this work: operator tooling no longer takes a new incarnation, which would have fenced the serving harness. A Task is closed only after Temporal reports the execution closed. A running child is `working`, not `input-required`. A second Task is never aliased to an existing run. `wait_worker` requires a live poller of the exact build.

The broker accepts only its own ChatGPT/Codex subscription OAuth credential. Its `client_id` is pi-ai's hard-coded Codex CLI client (`app_EMoamEEZ73f0CkXaXp7hrann`), with no public override. Browser sign-in rewrites pi-ai's authorize URL from `originator=pi` to `originator=exomachina`; pi-ai's device-code request sends no originator natively, and the broker adds its identity headers to OAuth HTTP calls. Codex SSE uses `originator: exomachina` and a broker User-Agent. The device flow, token-endpoint refresh and SSE requests succeeded against the live service; the browser authorize-URL rewrite was checked only with intercepted OAuth. There is no API-billing fallback: an absent or rejected subscription credential does not select `OPENAI_API_KEY` or another provider.

The credential stays in the Node process and an owner-only install-wide store, separate from Temporal, A2A and evidence. Fixture base-URL overrides require a non-default marked store; login refuses that store. The broker allowlists egress, emits fixed error text with bounded status/code, scans for token leaks with a planted positive control, and redacts exact credential values from streams. The leak-scan positive control uses a fresh synthetic canary, never live tokens. Its threat model covers accidental exposure and other local OS users. A concurrent attacker with the same OS user can read the store or exploit the documented ancestor-swap and stale-socket check/use windows. The one observed ChatGPT Pro subscription run established service acceptance for that request; entitlement, applicable terms and the hard-coded `client_id` remain commercial uncertainties.

## Layout

| Path | Role |
| --- | --- |
| `src/harness.py` | Harness instance (agent/factory mode), Director, Factory Module, Task projection |
| `src/admin.py` | Operator CLI: `provision`, `publish-template`, `author`, `publications` (never starts the runner) |
| `src/runner.py` | Lazy shared runner: PostgreSQL, Temporal, one worker per build |
| `src/factory.py`, `definition.py`, `binding.py`, `adapter.py`, `worker.py`, … | Bounded declarative interpreter and its pinned closure (`binding.INTERPRETER_FILES`) |
| `src/authoring.py`, `src/model_broker.py` | Authoring Interface, Strands agent, scripted fixture model, broker adapter, hard authoring budget, approval |
| `broker/` | Node CLI and persistent pi-ai model broker; pinned package and offline tests |
| `broker/testing/` | Loopback Codex SSE and OAuth fixtures |
| `services/` | Pinned test A2A services (the stand-in for the directory) |
| `scenarios/integrated.py`, `scenarios/replay_check.py`, `scenarios/live_authoring.py` | Original happy paths, replay, synthetic and live-provider authoring scenario |
| `evidence/broker-phase/`, `evidence/live-refresh-codex-subscription.json`, `evidence/live-authoring-codex-subscription*.json`, `evidence/live-authoring-synthetic-loopback*.json` | Broker test transcripts, live refresh, live authoring attempts and scans, synthetic repair-loop run |
| `briefs/`, `handoff/` | Parallel worker assignments and their reports |
| `src/runtime.py`, `src/supervisor.py` | Unmodified lane baselines kept for diff reference; not imported |

## Reproduce

```sh
PY=/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python
cd prototype/temporal-factory
npm --prefix broker ci --offline --cache /tmp/exomachina-pi-strands-debate/npm-cache
node --test broker/test/*.test.mjs                # 18 passed in evidence/broker-phase/
$PY -B -m unittest discover -s tests              # 90 total: 80 in final regression plus 10 provider-specific acceptance tests
$PY -B scenarios/integrated.py --home /tmp/exo-proto-int-<fresh>
$PY -B src/runner.py start --home /tmp/exo-proto-int-<fresh> --reason replay
$PY -B "$PWD/scenarios/replay_check.py" --home /tmp/exo-proto-int-<fresh> --address 127.0.0.1:44002 --out "$PWD/evidence/histories-<run>"
$PY -B src/runner.py stop --home /tmp/exo-proto-int-<fresh>
$PY -B scenarios/live_authoring.py --home /tmp/exo-proto-live-syn-<fresh> --provider synthetic-loopback

# For a new own-store subscription sign-in, use device flow; do not set EXO_MODEL_HOME or EXO_CODEX_BASE_URL.
node broker/exo-model.mjs login --device
$PY -B scenarios/live_authoring.py --home /tmp/exo-proto-live-codex-<fresh> --provider codex-subscription
```

The integrated scenario uses ports 44000–44012, 32400–32404, 44800 and 45200–45205; live authoring uses runner ports 44100 onward, 32420 onward, harness 44830, testbed 45300–45305 and mock 46100–46149. Both require the pinned local binaries named in `INTERFACES.md`. Use a fresh `/tmp/exo-proto-live-*` home for each authoring run. The live-provider command passed once on attempt 2 at `/tmp/exo-proto-live-codex-49b4c44-attempt2-83f74a4e`; attempt 1 stopped before A2A on the scenario assumption described above. All trial state is preserved: r1 `/tmp/exo-proto-int-r1`, r2 `/tmp/exo-proto-int-r2` (including earlier replay attempts under `replay-attempts/`), and the worker smokes under `/tmp/exo-proto-*`.

## Qualification spikes

Three follow-up spikes tested the delayed external A2A agent, two instances in one home and the broker-backed Director. Their checks were fixed before any code change. Results, preserved failures and remaining limits are in [`QUALIFICATION.md`](QUALIFICATION.md). That file supersedes the Director, second-instance and fixture-only-A2A gaps below.

## Remaining gaps

- **Live scope is narrow.** One account and one completed live authoring run establish device sign-in, forced refresh, token-endpoint and SSE originator acceptance, and first-pass graph authoring through A2A/Temporal. Rate and quota behavior, entitlement and applicable terms remain unverified. The browser authorize-URL rewrite and callback were not live-tested; the hard-coded `client_id` remains a compatibility and commercial uncertainty.
- **Live authoring repair is unobserved.** The live model submitted a valid first draft. The synthetic run and unit tests prove repair from validation feedback; a live Director is also still untested.
- **Failure paths are not observed live:** failed child, inconsistent Quality verdict, ambiguous A2A outcome. They remain implemented and unit-tested only.
- **Old-build retirement is not automated.** A retired marker exists, and `binding.may_retire` requires drain.
- **Contract and Quality-policy attestation is missing.** Contracts are fixture-authored (`attested: false`).
- **Operational hardening is out of scope and unbuilt:** Temporal frontend auth, relocatable/signed packaging, online backup, and the memory ceiling. The Director token still travels in Workflow history.
- **Operator CLI concurrency is only partially covered.** Operator tooling now shares the instance catalog with the serving harness through `PublicationStore` locks; concurrent publication was not stress-tested.
- **Some probes are fixture-only.** The release receiver is an HTTP fixture, not A2A. The Director model and capability/Quality data are fixtures in both the live and synthetic runs; the loopback provider is a fixture only in the synthetic run. `outcome_mode` remains a declared, caller-allowlisted fixture input that steers a candidate toward repair.
- **Scale is one factory instance and one organization.** A second instance attaching to the same runner is designed but was not exercised. Broker singleton and session separation were exercised with two processes against a fixture store; two factory harness instances were not. The credential uses an owner-only file; Keychain storage was not tested.
