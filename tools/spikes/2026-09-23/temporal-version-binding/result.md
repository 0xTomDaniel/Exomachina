# Temporal version binding lane

23 September 2026. **Verdict: partial.** The final ordinary two-build trial passed publication, original Director A2A Task continuity, pinned parent/child routing, and B1 behavior through a B2 code rollout. Full contract/policy attestation and automated worker retirement remain unproved. This is bounded local evidence, not product qualification.

## Evidence

| Claim | Level | Evidence |
| --- | --- | --- |
| R1 began under B1; after B2 activation, R1 and its child remained pinned to `exo-tq-version-factory.b1`; R2 and its child pinned to B2. | **Observed** | [`observed.json`](observed.json), `runs.*_wait`; [`workflow_describes.json`](workflow_describes.json) records each parent's `versioningOverride`. Command: `python -B version_trial.py` below. |
| R1 and R2 each completed their original Director A2A Task after an ordinary authorized abort. R1's result kept the old shape; R2's result has `output_shape: 2`. | **Observed** | [`observed.json`](observed.json), `runs.*_done`; same command. B2 uses renamed `synthesize_v2` Activity in [`build_b2/factory.py`](build_b2/factory.py) and [`build_b2/adapter.py`](build_b2/adapter.py). The rename itself is **source inference**; the completed B2 path is observed. |
| B1 stayed polling while its wait was open. Publication did not restart or inspect the external A2A services; their identities and PIDs stayed the same. | **Observed** | [`observed.json`](observed.json), `retirement.b1_retained_while_open` and `independent_services_before/after`; same command. |
| A retained B2 worker added one process; `ps` RSS was 69,168 KiB for B2, while B1 was 69,152 KiB both alone and alongside B2. | **Observed** | [`observed.json`](observed.json), `retained_build_rss_kib`; same command. RSS is a local per-process snapshot, not incremental bundle memory or a ceiling. |
| Offline replay of the ordinary B1 child history passed on B1 and failed on B2 at the explicit `wrong interpreter build` guard. It did not separately demonstrate a Temporal nondeterminism error from the renamed Activity. | **Observed** | [`b1-child-history.json`](b1-child-history.json), [`observed.json`](observed.json), `replay`; `version_trial.py` invokes `replay_check.py` offline. |
| Manifest validation rejects changed package/document, service contract, Quality policy, source binding, worker build, duplicate build ID with different source, and premature retirement. | **Unit-level** | [`test_binding.py`](test_binding.py), [`unit.stderr.log`](unit.stderr.log): 5 pure tests passed. |
| Python SDK 1.33.0 exposes Pinned behavior, `WorkerDeploymentConfig`, and start `PinnedVersioningOverride`; the pinned override reaches the start RPC. The bundled CLI exposes set-current, set-ramping, describe, and describe-version; Server 1.32.0 accepted set-current and reported both pinned versions. | **Observed** API inspection and trial, with **source inference** for SDK plumbing | [`sdk_observed.json`](sdk_observed.json), [`observed.json`](observed.json); SDK [`common.py`](../../2026-09-22/arbitration/temporal/.venv/lib/python3.12/site-packages/temporalio/common.py), [`_worker.py`](../../2026-09-22/arbitration/temporal/.venv/lib/python3.12/site-packages/temporalio/worker/_worker.py), [`_impl.py`](../../2026-09-22/arbitration/temporal/.venv/lib/python3.12/site-packages/temporalio/client/_impl.py). |
| B1 was `draining` immediately after both runs closed and did not reach `drained` during the bounded 55-second observation. Production retirement was not exercised. | **Observed** status and **unproved** retirement | [`observed.json`](observed.json), `retirement`; [`binding.py`](binding.py) `may_retire` fails closed unless no pinned run is open and Temporal reports `drained`. Trial teardown was after run closure, not a production retirement event. |

The final command stopped all processes it started, closed its owned ports, and removed its `/tmp/exo-tq-version-*` state; see `observed.json.cleanup`. The initial high-membership-port startup failure is retained in [`initial_port_failure.json`](initial_port_failure.json). The orchestrator's low-port correction is applied in [`runtime.py`](runtime.py).

## Source audit and implementation

**Source inference:** The copied base validates and content-addresses the package, including child digests, in [`definition.py`](../temporal-recovery-scale/definition.py) lines 230–273. Its service binding fields are only role, URL, identity, and approval at lines 237–246; there are no separate capability-contract or Quality-policy digests. The original Director SQLite run row holds only `package_digest` and starts without a worker version override in [`director_server.py`](../temporal-recovery-scale/director_server.py) lines 56–57 and 123–146. The original [`worker.py`](../temporal-recovery-scale/worker.py) lines 1–21 is unversioned. The original [`factory.py`](../temporal-recovery-scale/factory.py) lines 23–49 and 309–319 retains package/definition digests in Workflow state but no interpreter build; the child receives the package at lines 273–285. The original [`supervisor.py`](../temporal-recovery-scale/supervisor.py) lines 83–85 restarts one worker from the local source path. Therefore an old waiting run had no enforced worker-code binding across an ordinary code replacement.

**Implemented:** [`binding.py`](binding.py) records the package/root/child, binding, capability-contract, Quality-policy, source-tree and Python/SDK digests/versions. The immutable publication and atomic active pointer are separate; a build ID cannot be republished with different source. [`director_server.py`](director_server.py) persists package, manifest, and build per run, embeds the closure in Temporal start history, and always supplies an explicit pinned version override. [`factory.py`](factory.py) and both versioned builds verify the closure and exact document at run start and each node/Activity/child boundary. [`worker.py`](worker.py) registers Pinned Worker Deployment Versioning and checks its source digest at startup. The B1/B2 workers ran from separate source directories.

The explicit start override is the activation protocol. The publication pointer changes only after target worker registration and source-digest matching; every Director start then names that version. It does not rely on the timing of `set-current-version`, which the earlier [Zigflow activation race](../zigflow-reassessment/activation-race.json) showed could lag a successful CLI response. [Temporal's Worker Versioning documentation](https://docs.temporal.io/worker-versioning) describes pinned Workflow/Activity routing and child inheritance; the observed parent and child pins here confirm that route in this one-server fixture.

## Reproduce

From the repository root:

```sh
cd tools/spikes/2026-09-23/temporal-version-binding
../../2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest -v test_binding.py
../../2026-09-22/arbitration/temporal/.venv/bin/python -B sdk_check.py
../../2026-09-22/arbitration/temporal/.venv/bin/python -B version_trial.py
```

The last command uses the pinned Temporal 1.32.0/PostgreSQL 16.15 binaries, starts only ports 42000–42999 plus assigned low ports 31200–31299, writes `observed.json` and the successful child history, and removes its temporary state. It is an ordinary operations run. No failed-child, process-crash, forced-nondeterminism, or Quality-disagreement action was run in this lane.

## Remaining gaps and requests to orchestrator

- **Unproved:** The fixture-authored contract and Quality-policy objects are pinned but are not attested as the external services' actual published contracts/policy. Integrating authoritative contract and policy publication is needed for exact product closure.
- **Unproved:** Automatic retention/restart of all open pinned builds and retirement after Temporal reports `drained` are not wired into the copied one-worker supervisor. The decision gate is unit-tested; the live version remained `draining` through the bounded wait.
- **Unproved:** Concurrent activation, multi-frontend propagation, rollback, a Director code upgrade, and long-lived waiting runs beyond this local two-build trial. The explicit override removes dependence on `set-current` timing for tested starts but does not prove those wider cases.
- **Request to orchestrator:** Carry the partial verdict and the retirement/attestation gaps into the integrated qualification result. The low-port correction resolved the initial startup block. No shared-file change is requested from this lane.
