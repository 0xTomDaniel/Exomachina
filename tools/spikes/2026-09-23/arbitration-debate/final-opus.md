# Final position — Claude Opus 5.5

23 September 2026.

**Final top pick: Temporal Server 1.32.0 with the stable Python factory interpreter.**
**Final runner-up: Effect Workflow + Cluster v3.** Dagu Community v2.17.0 is a close third.

This is a development direction, not production qualification. Two Temporal gates stay **open**: a failed child reaching the same Director Task as `failed`, and upgrading the interpreter while runs wait. Both have no passing result, because the local test was stopped by automatic approval review. I don't request or propose any further fault injection here. No selection documents were edited.

Evidence labels: **Observed** means a retained result or observation file. **Source** means I read the code. **Inference** means my reasoning, not yet tested.

Inputs:
- my [independent assessment](independent-opus.md), §§1–8;
- [Astra's verdict and direct response](independent-astra.md), which includes Astra's direct response;
- the final [Temporal A2C](../temporal-fresh-composition/result.md) result;
- the final [Temporal recovery-scale](../temporal-recovery-scale/result.md) result;
- all Dagu and Effect results linked in the [brief](../../../../docs/engine-reassessment-brief-2026-09-23.md).

## 1. What the final Temporal reports establish

**Observed:**
- **Fresh blind composition passed** ([A2C](../temporal-fresh-composition/result.md)).
  - The 21-file freeze receipt `77956a…` was verified before the reveal. The interpreter, validator, adapter, publisher, Director, supervisor, and dependencies stayed unchanged.
  - One immutable v4 package, `02713a…` with child `911e5a…`, ran a four-branch graph with a held `working` branch, routed before synthesis, and produced both outcomes: one-repair acceptance with one release, and r3 exhaustion followed by an authorized abort with no acceptance or release.
  - v5 (all-clear) completed through the original Director A2A Task. An old revision kept its wait through publication and restart.
- **Two crashes through the original Director Task recovered** ([recovery](../temporal-recovery-scale/result.md), `recovery_observed.json`). Temporal was killed and PostgreSQL stopped at each seam:
  - after the remote Quality r1 verdict committed but before its Activity recorded it;
  - after r2 acceptance was durable but before release.

  In both cases the same Task went from `input-required` to `completed`. Every remote action had one attempt, with one acceptance and one release effect.
- **Held A2A calls** (`held_assignment_observed.json`, final run): **760.6 / 789.8 / 836.8 MB, 50 PIDs** at 0/2/10 working source Tasks. That is **+76 MB and +0 PIDs** from 0 to 10, with calls blocked inside the existing worker.
  - **Correction:** my `independent-opus.md` §8 quoted 782/813/854 MB and 57 PIDs from an earlier write of that JSON, which the final run replaced. The conclusion is unchanged.

**Open, not proven:**
- **Failed-child public failure path.** Source: `../temporal-recovery-scale/director_server.py:221-235`. `get()` reports `completed` only for the `accepted`, `child-aborted`, and `child-expired` phases, and has no `failed` mapping.
  - Inference: a child that fails could leave the original Task stuck in `working` or `input-required`. That is the same kind of symptom Dagu's v6 bridge showed.
  - The fix looks local to the Director projection, because the engine already delivers child failure to the parent workflow. It is still unproven.
- **Changing the interpreter under waiting runs:** replay compatibility and pinning runs to a worker version. The Effect upgrade result shows this hazard is real for replay-based interpreters.
- **The Director facade doesn't carry typed run inputs.** The mixed A2C outcomes started through the direct Workflow API.
- **Remaining gaps:** full closure of Quality policy and service versions; Quality Task-versus-lookup consistency, which is missing by source in Temporal too (`../../2026-09-22/arbitration/temporal/adapter.py:76-80`); and clean-machine PostgreSQL packaging, backup, and migration.

## 2. Why Temporal is first

- The tested displacing conditions came out in its favor. It passed the fresh frozen composition without interpreter edits. It kept the original Task through engine and store crashes after remote commits. No old binding changed, no remote effect was duplicated, and no accepted run was stranded. The failed-child and upgrade conditions remain untested (§1, §5).
- Parent/child execution, waits, and the authority to accept all live in one Workflow history, behind Update validators.
- It is MIT-licensed.
- Under held-call load, it was the only complete bundle whose process count stayed flat.
- The cost is real: PostgreSQL lifecycle, a higher idle baseline (about 761 MB against 548–597 MB), 10.5 s from start to ready, and a graph interpreter we own and must evolve compatibly.

## 3. Engaging Astra's actual Effect-versus-Dagu argument

Astra's direct response was written against the owner's summary of my earlier Dagu-second position. Astra could not read my file because of a `herdr` protocol mismatch. My `independent-opus.md` §8 had already moved Effect to second after the fresh Temporal evidence. Point by point:

1. **Astra's best challenge: "which backup can we advance with fewer untested correctness assumptions today" favors Dagu.** I agree this is Dagu's strongest claim. I no longer treat it as decisive, for three reasons:
   - The runner-up is a *fallback from Temporal*. The Temporal evidence now shows the interpreter-over-durable-engine design passes blind composition and crash recovery.
   - Temporal's remaining risks include operating cost (PostgreSQL, packaging) and unproven behaviour and authority paths (failed-child projection, interpreter upgrades). Effect addresses the operating-cost risk while keeping our language, validator (already the same file), and interpreter design. It shares the upgrade risk.
   - Falling back to Dagu would discard that and take on native-YAML restrictions plus a separate-root protocol.
2. **"More evidence includes adverse evidence."** Accepted. Dagu's record includes a *demonstrated* Quality identity-guard failure and a participating-release liveness gap. The v7 bridge is also not one frozen, fully re-tested implementation. I had credited Dagu's evidence volume without discounting its adverse findings enough.
3. **"The bridge's fixture-specific step names are not proof of a generic module."** I confirmed a correction and an addition in source:
   - **Correction:** sealing is not tied to step names. The reconciler and bridge handle any failed step.
   - **Addition:** both act only when **exactly one** native step has failed (`../dagu-failure-recovery/reconciler.py:37-39`, `../dagu-failure-recovery/bridge.py:62-64`). A child whose parallel branches fail together is neither retried nor sealed. This is an untested stranding risk. It strengthens Astra's point that the cross-root protocol must be generalized across every allowed composition.
4. **"Effect's interpreter cost can be more predictable than Dagu's cross-root protocol; falsified if Effect's interpreter grows its own retry scheduler, ownership election, or duplicate continuation engine."** I adopt this as the correct test of the runner-up judgment. I add one condition I hold more firmly than Astra:
   - Effect's interpreter-version routing (separate helpers and stores per incompatible build, routed through the Director facade, with retirement) must stay a bounded catalog-and-routing function.
   - It must not become a second coordinator.
   - That work is engine-specific, in the same class Astra holds against Dagu's bridge.
5. **Held calls and MIT.** Accepted as observed advantages over Dagu, with Astra's own caveat: the six-role versus nine-role baseline means the whole-bundle comparison is not proven. Temporal's flat held-call result also suggests the per-call cost belongs to the adapter design; inference. Effect can plausibly reduce it inside the adapter, while Dagu needs a graph-level gate-and-scanner protocol that hasn't been built.
6. **Quality consistency is not asymmetric.** Agreed. In source it is missing for Effect and Temporal alike. Dagu has a separate, demonstrated identity-guard failure. It is a product-wide gate and not a ranking factor.

## 4. Consensus

**Final consensus with Astra: Temporal with the stable Python interpreter first, Effect second, Dagu the close third alternative.** [Astra's final position](final-astra.md) confirms this after reading my full position. We each reached it independently, and it is a conditional development direction: it closes none of the gates in §5.

I accept Astra's two narrowings:
- Not every condition that could displace Temporal has been resolved. The failed-child public failure path and interpreter upgrades remain unproven, and they are behaviour and authority risks, not only operating cost.
- Effect's total recovery surface is not demonstrably smaller than Dagu's.

The remaining difference is one of weight, not order. I weigh Dagu's demonstrated qualification and Effect's interpreter-version routing burden somewhat more than Astra does. Effect second is a product-fit judgment carrying significant qualification risk. Candidates below the top three were **not jointly ranked**; the order in §5 is mine alone.

## 5. Open gates and what would change this order

These are qualification gates, not requests for new fault injection.

**Temporal loses first place if:**
- the failed-child public failure path cannot be closed without a coordinator comparable to Dagu's bridge;
- interpreter-version upgrades cannot keep waiting runs on their promised behavior through Worker Deployment Versions or an equivalent retained worker;
- clean-machine PostgreSQL packaging, backup, and migration prove unworkable for one-install operation.

**Effect loses second place to Dagu if:**
- the matched acceptance-to-continuation recovery through the original Director A2A Task can't be made to work without a new substantial recovery coordinator (Astra's discriminator);
- durable interpreter-version routing through the integrated Director facade requires more than bounded catalog and routing logic;
- a complete Effect bundle with its lifecycle owner, retained versions, and opaque receiver loses the held-call advantage.

**Dagu regains second if** any Effect condition above fails, or if Dagu shows all of the following:
- a reusable bridge that handles multiple failed steps;
- recovery from a retry interrupted mid-flight;
- a short-submit, working-Task assignment path that holds processes near its wait-phase footprint;
- the Quality-guard and release-liveness defects fixed;
- a completed GPL-compliant distribution assembly.

**My own order below the top three (not jointly ranked with Astra):** Temporal+Zigflow fourth, Kestra fifth, Restate sixth, Conductor source variant seventh, then Strands Graph, Hatchet, and Argo. LangGraph, n8n Community, and distributed Windmill Community stay excluded. Reasons are in `independent-opus.md` §3.
