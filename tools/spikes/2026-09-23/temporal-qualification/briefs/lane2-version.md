# Lane 2 (`tw_version`): immutable run/worker/interpreter version binding

Owned directory: `tools/spikes/2026-09-23/temporal-version-binding/`. Ports 42000–42999. State `/tmp/exo-tq-version-*`. Follow [`../COORDINATION.md`](../COORDINATION.md), including the safety boundary.

## Question

Can the stable Python factory interpreter on Temporal bind every active run to its exact closure (definition digest, capability-contract digests, Quality-policy digest, interpreter/worker build) across (a) new factory-document publication and (b) an interpreter/worker **code** upgrade, keep old waiting runs on promised behavior (including renamed Activities and old output shapes), and activate a new version without the routing race observed in Zigflow?

## Work

1. **Source audit** of `temporal-recovery-scale/` (`definition.py`, `factory.py`, `worker.py`, `runtime.py`, `supervisor.py`, `director_server.py`): what is pinned per run today, what is not (Quality policy? contract digests? worker build?), and where an old waiting run would silently adopt new code. Cite lines.
2. **Design + implement** in your directory a run-binding manifest and publication controller:
   - A content-addressed closure record per factory version and per run, stored at run start and verified at every Activity/child boundary.
   - Temporal [Worker Deployment Versioning](https://docs.temporal.io/worker-versioning) with **Pinned** behavior for factory workflows, one deployment version per interpreter build; check what temporalio 1.33.0 and Temporal Server 1.32.0 actually support (`VersioningBehavior`, `WorkerDeploymentConfig`, `VersioningOverride` on start, set-current/ramping APIs) and cite the SDK source.
   - An activation protocol that cannot reproduce the Zigflow race: e.g. every Director start carries an explicit pinned version override derived from the publication record, or the controller blocks starts until describe-deployment shows the target version current and a harmless probe confirms routing. State which you chose and why. Relate this directly to [`zigflow-reassessment/activation-race.json`](../zigflow-reassessment/activation-race.json).
   - Retention and retirement: old worker builds stay polling while any pinned run is open; retirement only after drain is observed.
3. **Evidence permitted**: ordinary, non-fault operations. For example: start run R1 pinned to interpreter build B1 which waits normally for a Director decision; publish a new factory document and roll out build B2 (a benign code change such as a renamed Activity and changed output shape) through the normal controller without killing anything; start R2 via the Director and confirm it pins to B2; confirm R1 is still pinned to B1 (describe), then give R1 its ordinary Director decision and observe it complete with B1 behavior and old output shape. Also run Temporal's offline `Replayer` for B1 histories against B1 and B2 code to show why pinning is needed (a replay incompatibility reported offline by the Replayer is acceptable evidence; do not push incompatible code at live runs). Unit tests of the manifest/closure verifier.
4. **Do not** kill workers or servers mid-run, deploy deliberately broken/nondeterministic code to live runs, inject failures, or repeat the previously rejected upgrade fault-injection test in any form. If approval review blocks the normal rollout test, stop it, do not retry reworded, and mark the live upgrade gate unproved with your source/unit/replayer evidence.

## Output

`result.md`, `observed.json`, tests, source. Verdict on: exact closure binding; code-upgrade behavior for waiting runs; race-free activation; retention/retirement. Mark observed vs unit-level vs source inference. Include the per-retained-build process/memory cost if you observe it.
