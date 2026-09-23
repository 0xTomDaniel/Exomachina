# Effect helper upgrade while an old factory run waits

23 September 2026. This follow-up tests the runtime-code version seam left open by the [Effect parity trial](../effect-parity/result.md). It uses a **new sibling directory** and does not modify that trial's frozen helper, bridge, validator, package files, or evidence. [`helper_v1.mjs`](helper_v1.mjs) is byte-identical to the frozen helper (SHA-256 `487cfe0e7aae1f2b00d448d8a3a1c15c94462411e873b3cd77cbdb4698a9db93`); the copied [`bridge.py`](bridge.py) and [`definition.py`](definition.py) also match their frozen originals. Full observations are in [observed.json](observed.json).

**Finding:** The published factory document stayed pinned, but an old waiting run resumed under the replacement helper's new interpreter behavior. Renaming one Activity caused its three earlier Quality bridge calls to execute again. A separately retained v1 helper, with its own Effect store, preserved the old behavior. Automatic worker rollout is therefore compatible with the architecture only if the runtime also preserves the behavior that each active run was promised; a document digest alone did not do that in this case.

## Exact change and matched run

The copied [`helper_v2.mjs`](helper_v2.mjs) made three bounded changes: it prefixed the `quality` Activity name with `v2:`, added a synthetic `interpreter_revision: 'v2'` field to the abort result, and logged calls entering its bridge. The Workflow name, payload, document validator, graph document, child closure, service bindings, and bridge implementation stayed the same. The marker is an observation aid, not a proposed product field.

[`upgrade_probe.py`](upgrade_probe.py) published the **same package digest** `4bd48fb4c79d5c1e70e6b43654abc1f69e96f48093f1f825b704b1214a49282a` into two isolated Effect stores. Each run reached r3 `awaiting-director` through separate real Strands/A2A capability services and an independent Quality service. Both child runs had the same definition digest `6f98bf6bcd4f7f813e81daad661ae96af3172cb9f87fb589362fe89157b7d9fe`; each had two assignment events, three Quality events, three synthesis events, and no acceptance or release. The two v1 helpers were stopped. One store restarted under v2 and the other under the retained v1 helper. Both child IDs, digests, r3 state, and waiting phases survived. The same exact Director decision was then sent to each helper.

| Old waiting run resumed by | Observed result |
| --- | --- |
| Replacement v2 helper | Child and parent completed an authorized abort with zero acceptance/release. The child result contained the new `interpreter_revision: 'v2'` marker **despite its old document digest**. The v2 bridge log showed three `review` calls for r1, r2, and r3 during replay. The unchanged assignment/join/synthesis Activities did not enter the bridge again. |
| Retained v1 helper | Child and parent completed the same authorized abort with zero acceptance/release. The child result had no v2 marker. Its old run stayed on the old helper code and old store. |

The product event ledger still held only the original three Quality events per run because event keys deduplicated. The Quality fixture held **three distinct action IDs and three submission attempts per child**, rather than six, because the bridge looked up stable A2A action IDs before any send. Thus the renamed Activity **did re-enter product bridge code and remote lookup**, but this fixture did not create duplicate remote Quality tasks. That safety came from the product bridge and participating service contract; the engine's Activity record alone was insufficient once its name changed.

At the waiting checkpoint, the two concurrently retained Node helpers were separate PIDs with sampled `ps` RSS of **199,622,656 B** (v2) and **200,114,176 B** (v1). Their Effect/product SQLite file totals after completion were **1,639,392 B** and **1,482,832 B**. RSS double-counts shared pages and these are neither de-duplicated physical footprint nor full-install measurements. The demonstrated pinning arrangement entails two helper processes, two stores, and routing each run to its assigned helper version. A single process carrying versioned workflow handlers could be another implementation, but was **not tested** here.

## Decision implication and limits

This is one changed Quality Activity name and one changed abort output on an r3 wait using `@effect/workflow` 0.19.1 and `@effect/cluster` 0.60.2. It does not establish behavior for all code edits, an in-flight release, a changed graph control-flow implementation, multi-host ownership, or a same-store rolling deployment. The probe used the Effect helper's Director HTTP command rather than the separate post-freeze Director A2A facade. It demonstrates the interpreter-version hazard in the engine run and an isolated old-helper workaround, not a complete production upgrade plan.

The tested product would need to bind each active run to an interpreter version as well as a factory document version, retain executable code for old runs until they finish, and ensure Activity names/side effects stay compatible on replay. The two-store arrangement is one concrete way to do that, with version-aware run routing and more process/store management. Automatic rollout of workers remains acceptable; it does not by itself preserve an active run's exact behavior.

Reproduce from the Exomachina root with the existing pinned S2 Python environment and Effect dependencies:

```sh
tools/spikes/2026-09-22/s2/.venv/bin/python -B tools/spikes/2026-09-23/effect-upgrade-stability/upgrade_probe.py
```
