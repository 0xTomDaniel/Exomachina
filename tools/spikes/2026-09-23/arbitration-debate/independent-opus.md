# Independent engine assessment — Claude Opus 5.5

23 September 2026. Written for the fresh reassessment in the [brief](../../../../docs/engine-reassessment-brief-2026-09-23.md). I did not inherit the 22 September ranking, did not edit selection documents, and did not run any spikes. The Temporal fresh composition and recovery trials (`../temporal-fresh-composition/`, `../temporal-recovery-scale/`) had no result files when I wrote this, so they are excluded. **Every ranking below is provisional until those reports exist.**

Evidence labels: **Observed** means a retained trial result or observation file. **Source** means I read the code. **Inference** means my reasoning, not yet tested.

> **Revised later on 23 September, after reading Astra's full verdict, the final Temporal A2C result, and the Temporal recovery and held-call observations: see §8.** The current order is **Temporal first, Effect second, Dagu third**. Sections 1–7 keep my earlier independent position unchanged, for provenance.

## 1. Position

| | Current position | How it moved |
| --- | --- | --- |
| Top pick | **Temporal Server 1.32.0 + stable Python interpreter** | Unchanged throughout. |
| Runner-up | **Dagu Community v2.17.0** | My first provisional runner-up was Effect. I moved Dagu ahead after the [Dagu failure-recovery](../dagu-failure-recovery/result.md) result closed the stranded-child and saved-input seams. I also withdrew my assumption that Effect holds A2A calls in-process: as built, it starts one Python bridge process per call (`../effect-parity/helper.mjs:50`, `execFile`). The later [Effect in-flight](../effect-inflight-scale/result.md) and [Effect upgrade](../effect-upgrade-stability/result.md) results cut in opposite directions and did not change the order. |
| Third | **Effect Workflow/Cluster v3** | Close behind Dagu. Low confidence in that order. |

Confidence: **moderate** that Temporal is first, **low** that Dagu is second rather than Effect.

## 2. The product boundary I judged against

From the brief and `docs/exomachina.html`:
- Factory nodes are independently managed black-box A2A services. The factory routes and gates them, but never deploys, restarts, or inspects them.
- Agents publish flexible graphs built from an approved block vocabulary: assignment, parallel plus typed join, conditional route, bounded repair, Director wait, and nested factory.
- Active runs keep exact bindings. Automatic rollout of bundled orchestration workers is acceptable if active runs keep their behavior.
- One install and start command, one organization per deployment.
- Package and distribution rights, operating cost, and product-owned code matter alongside feature coverage.

## 3. Evidence summary by candidate

### Temporal + stable Python interpreter

**Observed:**
- **A1 (partial):** exact child, capability, and identity closure. Quality policy and service build are not bound. [Result](../../2026-09-22/arbitration/temporal/result.md).
- **A2 (partial):**
  - The frozen interpreter failed the blind hidden test: success and exhaustion needed different immutable packages.
  - A post-disclosure correction made one digest produce both outcomes. There was no replay test ([correction](../../2026-09-22/arbitration/temporal-input-correction/result.md)).
- **A3:** publication negatives rejected. The Update validator rejected a forged actor, a stale revision, and a wrong definition. Out-of-band forged and stale Quality reviews were ignored, but nothing was injected into the active Activity.
- **A4:**
  - Survived four separate stops of Temporal Server **and PostgreSQL**: during an active assignment after remote commit, after Quality committed remotely, after acceptance while a timer held release, and during a nested Director wait.
  - Stale-owner fencing held.
  - The crash that started from the original Director A2A Task happened **before** the remote commit. The crashes after remote commits used direct Workflow starts.
- **A5:** participating assignment and release lost replies reconciled with one effect each. The opaque receiver stayed durably unresolved.
- **Footprint:** Director-routed waits at 0/2/10 measured **757/791/849 MB, 49 PIDs** ([wait-scaling](../../2026-09-22/arbitration/wait-scaling/result.md)). Calls held in flight have **not been measured**.
- **Owned code:** 1,421 lines, counted by me with `wc -l`: validator and catalog 304, interpreter and worker 337, A2A adapter and Director 487, supervisor and runtime 293.

**Source:**
- Native child workflow at `../../2026-09-22/arbitration/temporal/factory.py:271`.
- Durable waits at `:105,228`.
- Update validators at `:66,79`.
- Blocking A2A calls run through `asyncio.to_thread` in one worker (`adapter.py:119,140,207`).

### Dagu Community

**Observed:**
- **A2:** passed the original hidden composition on first attempt with frozen code ([arbitration](../../2026-09-22/arbitration/dagu/result.md)).
- **A3:** all six publication negatives rejected. **The Quality identity guard failed** an isolated probe (`../../2026-09-22/arbitration/dagu/a3-identity-gap.json`).
- **A4:** passed the injected points.
- **A5:** participating lost replies reconciled; the opaque receiver stayed unresolved. The participating release still has a liveness gap when a send never arrives.
- **Nested waits without a held process** ([integration](../dagu-bridge-integration/result.md)):
  - Accept and authorized abort both worked through the original Director Task.
  - Waits measured **550/600/679 MB, 9 PIDs** at 0/2/10.
- **Failed child** ([follow-up](../dagu-failure-recovery/result.md), `failure_observed.json`):
  - Sealed as an incident: `failure_json`, parent `child_failed`, original Task `failed`, zero effects, all surviving restart.
  - A direct native retry was refused by **product code** (`ops.py get_run`: "failed product run is sealed"), not by Dagu.
- **Gate input saved but continuation not queued** (`queue_observed.json`):
  - Dagu returned 503, the gate showed succeeded, and the parent stayed waiting.
  - After a full restart, `human-tasks/resume` completed the same Task once, with one release.
- **Calls held in flight** ([scale](../dagu-inflight-scale/result.md)):
  - **548/703/1,278 MB, 9/15/39 PIDs** at 0/2/10 working source Tasks.
  - Each held call keeps a Dagu runner, a Python step adapter, and a shell watcher alive.
- **Owned code:** 1,636 lines frozen, **1,850 lines** in the v7 follow-up (`wc -l`).

**Source:** `ops.py` `assign()` sends synchronously, then polls every 0.25 s (`../dagu-bridge-integration/ops.py:181-296`).

**Remaining gaps:**
- GPL-3.0 needs Corresponding Source delivery and a review of how tightly our code couples to Dagu ([GPL review](../../2026-09-22/decision-round/dagu/gpl-bundle-review.md)).
- The router schedules descendants of a skipped target unless each descendant is listed explicitly.
- Run IDs can collide through a socket shared across the host.
- Automatic retries only scan a 24-hour window by default.
- No rule for recovering a retry that is interrupted mid-flight.
- No protocol for reopening a sealed child.
- Resume attempts stop after three tries.

### Effect Workflow/Cluster v3

**Observed:**
- **A2B:** a fresh withheld four-branch composition passed under a frozen interpreter; one package produced both repair-success and exhaustion-abort ([parity](../effect-parity/result.md)).
- **After the freeze:**
  - A Director facade carried the original A2A Task through helper and Director restart, authorized abort, and accepted release.
  - One participating release lost reply reconciled.
- **Waits:** 598/613/657 MB with 6 PIDs. This omits a supervisor, a reconciler, and an opaque receiver.
- **Calls held in flight** ([scale](../effect-inflight-scale/result.md)):
  - **597/641/804 MB, 6/8/16 PIDs.**
  - Ten held calls added **+207 MB and +10 Python bridges**, against Dagu's **+730 MB and +30 processes**.
- **Interpreter upgrade under a waiting run** ([upgrade](../effect-upgrade-stability/result.md)):
  - A replacement helper changed the old run's abort output despite the same document digest.
  - It re-ran three renamed Quality Activities through the bridge.
  - Duplicate remote work was avoided only by the product's lookup-before-send.
  - A retained v1 helper and store preserved the old behavior, at about 200 MB `ps` RSS per helper.
- **Owned code:** 1,030 lines: helper 333 in TypeScript, validator 273, bridge 165, Director 259. There is no supervisor or reconciler yet.

**Source:**
- For Quality, `../effect-parity/bridge.py:53-61` tolerates an artifact-ID mismatch and returns the verdict from the lookup, never from the Task payload. The fault-injection probe was rejected by automatic review, so this remains an **unresolved source-audit gap**.
- Pinned `@effect/workflow` 0.19.1 has no versioning, patching, or nondeterminism-detection API.

**Not yet run:**
- the A4 kill matrix on the current interpreter;
- A5 opaque-receiver uncertainty;
- two of the six A3 negatives (mutable closure and unapproved capability);
- recovery across the seam between Effect's store and the product's SQLite.

### Other routes

| Candidate | Observed and source basis | Main objection |
| --- | --- | --- |
| Temporal + Zigflow | Three pinned versions kept old waits. The unguarded activation raced, routing a v3 start to v2. Engine-only footprint was 303/360/405 MiB for 1/2/3 versions ([result](../zigflow-reassessment/result.md)). | One worker process for every *factory* version that still has waiting runs. Agents publish those versions frequently. The HTTP task is at-least-once, with no A2A adapter proven. No factory semantics tested. Pre-1.0. |
| Kestra OSS | Native four-way graph, route, repair/exhaustion, and old revision kept across restart of both Kestra and PostgreSQL. **Branches, Quality, and acceptance were placeholders.** Measured about 1.285/1.287/1.301 GB with an idle Director on a direct-API route ([result](../kestra-parity/result.md)). | No integrated A2A factory. The existing Director cannot govern its graph. The native publisher accepted a review bypass in S3. JVM plus PostgreSQL makes it the heaviest measured bundle. Basic Auth only. |
| Restate | One unchanged handler ran v1/v2 and survived kills ([result](../../2026-09-22/round-two/restate/result.md)). | The BUSL-1.1 server grant is conditional on our exact product and API. Narrow evidence: no A2A, no nesting. |
| Conductor source variant | A PostgreSQL-only build without the Orkes queue JAR completed a simple workflow across restart ([follow-up](../../../../docs/conductor-license-follow-up-2026-09-22.md)). | We would maintain a patched fork, and nothing product-shaped has been tested. The stock artifact is blocked on rights. |
| Strands Graph | Resumed pinned graphs; two unfenced owners both delivered ([round two](../../2026-09-22/round-two/strands_graph/result.md)). | Fencing, publication, and ledger/session consistency would all be product-built. |
| Hatchet embedded | Survived crash and restart; killing the owner orphaned PostgreSQL ([result](../../2026-09-22/round-two/hatchet/result.md)). | Upstream presents embedded mode for development and CI; operating it one-install is expensive. |
| Argo Workflows | Documentation only. | Requires Kubernetes. |
| LangGraph, n8n Community, Windmill distributed Community | Excluded in the earlier screen. | No concrete rights or operating path in the evidence, so I did not challenge the exclusions. |

## 4. What the engine supplies versus what we own

| | Engine supplies | We own |
| --- | --- | --- |
| Temporal | Child workflows, durable waits, history replay, Update validators, Worker Deployment Versions | Graph language, validator, catalog, interpreter, A2A reconciliation, Director facade, PostgreSQL/Temporal lifecycle, backup and migration |
| Dagu | Native YAML graph, per-root scheduling and retries, saved and resumable human-task gates | Publisher and closure pinning, state and receipt layer, separate-root parent/child bridge, failure sealing, resume retries, likely an assignment gate plus scanner, Director, supervisor, GPL release process |
| Effect | Durable workflows and Activities, child workflows, deferred waits, SQLite-backed runner | Graph language, validator, interpreter, product ledger across two stores, A2A bridge, Director, supervisor and reconciler (unbuilt), routing runs to the right interpreter version |

Owned by all three: authorization, a catalog with full closure binding (partial everywhere), Quality authority, and remote-effect uncertainty. **Quality authority is unproven for all three:**
- Dagu's identity guard failed.
- Effect's cross-check is missing.
- Temporal's active-Activity negatives were never injected.

## 5. Why Temporal is first

1. **Nesting and waits are engine features in Temporal.** Every Dagu wait longer than a step needs a product protocol. Each Dagu seam closed so far took another spike and added code: 1,636 → 1,850 lines.
2. **Authority lives in one history.** Acceptance and Director commands sit in Workflow history behind Update validators. Dagu and Effect keep authority in product SQLite beside a second engine store.
3. **Recovery breadth under engine plus store kills** is observed, although the crashes after remote commits weren't driven through the original A2A Task.
4. **Rights.** MIT plus the PostgreSQL License, with no Corresponding Source obligation.
5. **Interpreter evolution** (inference). The Effect upgrade result shows that replay-based designs need each run pinned to an interpreter version. Temporal supplies a supported pinning mechanism (Worker Deployment Versions, observed in the Zigflow trial with its activation race). It would likely also turn a renamed Activity type into a nondeterminism error rather than silent re-execution. A changed computed result, such as a new output field, would still be adopted silently unless the run is pinned. Nothing here is observed on our interpreter yet.

**Strongest counterargument against Temporal:**
- Temporal failed the only blind composition test.
- Its crashes after remote commits haven't gone through the original Director Task.
- Its in-flight footprint is unmeasured.
- It adds PostgreSQL, with 49 PIDs and about 850 MB at ten waits, to a one-install product.

Part of my preference rests on semantics I expect but haven't observed. The running Temporal A2C and recovery trials test exactly these points.

## 6. Response to Astra: Temporal first, Effect second

I could not open the shared `exo_astra` tab from this session: there is no tmux, no matching peer session, and no Astra file. This responds to the summary the owner relayed: Astra favors Effect as runner-up for its lower observed in-flight overhead and MIT packaging, and concedes Dagu's stronger A3–A5 evidence and Effect's gaps in A4, Quality, and upgrade.

### Strongest valid case for Effect as runner-up

1. **The active phase is the realistic workload.** Real agents work for minutes to hours across parallel branches. With ten held calls, Effect added **207 MB and 10 processes**; Dagu added **730 MB and 30 processes**. That result is observed, not inferred. Even after adding reasonable estimates for Effect's three missing roles, Effect's whole bundle is probably lighter at ten held calls (inference).
2. **MIT carries no recurring rights work.** Dagu needs a coupling review that must be redone for each release. Our DAGs call our adapter and the Director drives native runs, which is the "intimate exchange" pattern the GNU FAQ warns about.
3. **Nesting is native.** Effect child workflows survived helper and Director restart without a separate-root protocol. Dagu needed three spikes (bridge integration, failure seal, resume) to reach the same place, plus 214 more lines.
4. **A runner-up is a fallback, and Effect is the cheaper fallback from Temporal.** This is the strongest argument. Effect shares Temporal's architecture: an owned interpreter over a durable replay engine. The validator is already the same file, copied verbatim. If Temporal fails on *operating cost* (PostgreSQL, footprint, one-install), our language, validator, and pinning design move to Effect largely intact. Moving to Dagu means rebuilding around YAML, router-target wiring, and separate-root protocols.
5. **Less owned code:** 1,030 lines against 1,850, though Effect still lacks a supervisor and reconciler.
6. **Effect's gaps have short closure paths.** The Quality cross-check is a small bridge change. A4 can reuse the existing kill harness. Dagu's in-flight fix needs a new graph pattern plus a scanner protocol that hasn't been built.

### Strongest case for Dagu as runner-up

1. **It has the most complete evidence.** Dagu is the only candidate with observed passes on:
   - the original blind A2 test, on first attempt;
   - all six A3 publication negatives;
   - the injected A4 points;
   - A5 participating and opaque cases;
   - nested accept, abort, failure, and saved-input recovery through the original Director Task.

   The selection method says "do not rank incomplete profiles against complete ones" (`../../../../docs/exomachina-selection-judgment.json`, `method.unknownRule`). Effect has no A4 matrix, no A5 opaque test, two missing A3 negatives, and an unresolved Quality gap.
2. **Effect's unknowns sit exactly where durable systems fail.** Its product ledger is written beside Effect's store, and that seam between the two stores has never had a crash injected. Dagu's equivalent seams have been injected.
3. **The upgrade hazard is observed for Effect.**
   - An old run silently took new behavior and re-ran side-effecting bridge calls.
   - It stayed safe only because of product lookup, not the engine.
   - The demonstrated fix keeps one helper and store per interpreter version (about 200 MB RSS each) and needs product routing. That can erase the in-flight savings whenever several interpreter versions are live.
   - Dagu doesn't replay, so completed steps are never re-run. That is an inference; Dagu's own `ops.py` and binary-upgrade seam is untested.
4. **The in-flight comparison mostly measures unoptimized adapter plumbing, not engines.** Both trials hold a process per call. Effect's Python subprocess is a trial shortcut, and Dagu's can be removed with the gate-and-scanner mechanism already proven for child outcomes. Ranking on today's plumbing rewards whichever shortcut happened to be cheaper.
5. **Operating simplicity.** A single Go binary with file state, about 2.5–2.9 s from start to ready, and the lowest idle baseline.

### Where I disagree with Astra, and where I concede

- **I concede:** the in-flight increment is an observed and relevant difference, MIT is a real advantage, and the fallback-architecture argument is strong. Together they narrow Dagu's lead over Effect to near zero.
- **I disagree:** a lower in-flight increment from different adapter plumbing, in a topology missing three roles, should not outweigh complete A3–A5 contract evidence against missing A4 and A5 evidence. I also disagree that Effect's upgrade seam is a minor gap. It is an observed silent behavior change, and fixing it needs versioned helpers, versioned stores, and routing we would build ourselves.
- **Unresolved:** we weigh *observed completeness* differently from *structural fallback fit*. I keep Dagu second, with low confidence. I am not forcing agreement.

## 7. What would change my order

**Between Dagu and Effect:**
- **Effect moves above Dagu if** a frozen Effect candidate passes all of the following:
  - the five-point A4 kill matrix through the original Director Task, including a crash after product SQLite acceptance but before Effect's continuation;
  - A5 opaque-receiver uncertainty;
  - the two missing A3 negatives;
  - a Quality Task-versus-lookup consistency check, framed as a data-consistency test with two fixtures deliberately returning different verdicts;
  - a matched nine-role footprint.
- **Effect also moves above Dagu if** the Dagu GPL coupling review finds a combined-work risk that our distribution cannot meet.
- **Dagu's lead widens if** a gate-and-scanner assignment path is implemented and fault-tested and holds near 9 processes and about 700 MB at ten held calls, and the GPL release gate is cleared.

**Top pick.** The Temporal A2C and recovery reports now running decide it:
- **Temporal passes** blind composition, the crashes after remote commits through the original Director Task, failed-child propagation, upgrade parity with `effect-upgrade-stability` (unpinned and pinned), and a matched in-flight footprint: it stays first with higher confidence.
- **Temporal fails blind composition or an A4 test:** owning an interpreter is itself the risk, and Effect shares that risk. **Dagu becomes my top pick.**
- **Temporal passes the semantics tests but its in-flight or operating footprint is clearly heavier, with no practical upgrade advantage over Effect's two-store pinning:** the fallback argument applies, and **Effect overtakes Temporal**, provided Effect then passes the A3–A5 items above.

**Consensus remains provisional until the Temporal reports exist.**

## 8. Reconsideration after Astra's full verdict and the fresh Temporal evidence

This responds to [Astra's actual verdict](independent-astra.md), not the earlier summary. It also uses:
- the final [Temporal A2C result](../temporal-fresh-composition/result.md);
- the Temporal recovery-scale observations `../temporal-recovery-scale/recovery_observed.json` and `../temporal-recovery-scale/held_assignment_observed.json`. Their `result.md` was still being finalized. Both files report `status: passed`, with the original frozen inventory unchanged before and after.

Temporal's failed-child and interpreter-upgrade reports are pending. **No consensus is asserted.**

### 8.1 New Temporal evidence

**Observed, fresh blind composition** ([result](../temporal-fresh-composition/result.md), `a2c-observed.json`):
- The evaluator received the 21-file freeze receipt (`77956a…`) before the withheld four-branch A2C graph was revealed.
- One immutable v4 package (`02713a…`), with child `911e5a…`, produced both outcomes:
  - one-repair acceptance, with one release receipt;
  - r1/r2/r3 exhaustion, then an authorized abort with no acceptance and no release.
- The join waited for a held `working` counter Task, and routing happened before synthesis.
- v5 (all-clear) completed through the **original Director A2A Task**.
- An old revision kept its wait through publication and restart, and the worker PID was unchanged during publication.
- **Limit:** the frozen Director facade doesn't pass `outcome_mode`, so the two mixed-outcome runs started through the direct Workflow API. Carrying typed run inputs through the Director is an open integration gap.

**Observed, crashes through the original Director Task** (`recovery_observed.json`):
- **Quality-commit case, Task `8fed18a8…`:** Temporal and PostgreSQL were stopped, with both ports closed, after the remote r1 Quality verdict had committed and before the workflow recorded it.
- **Acceptance case, Task `dcc136a2…`:** stopped after r2 authoritative acceptance was in workflow history and before release.
- **Both cases:** the same Task went from `input-required` to `completed`. Every remote action had one attempt; there was one negative and one positive Quality verdict, one authoritative acceptance, and one release effect.
- **Not re-run here:** stale-owner fencing (it passed in the 22 September arbitration). Failed-child propagation is still pending.

**Observed, held A2A calls** (`held_assignment_observed.json`):
- The full bundle measured **781.5 / 813.1 / 853.6 MB, 57 PIDs throughout** at 0/2/10 held calls.
- Those 57 processes are 48 PostgreSQL processes plus supervisor, Temporal, worker, Director, two capability services, Quality, and participating and opaque receivers.
- All ten source Tasks stayed `working` before and after sampling, and all ten parents stayed `awaiting-child`.
- **Increase from 0 to 10: +72 MB, +0 PIDs.** Held calls run as threads inside the existing worker (`asyncio.to_thread`).
- Start to ready took 10.5 s.

Updated comparison, at 0 / 2 / 10 held calls:

| Bundle | MB | PIDs | Increase, 0→10 | Roles |
| --- | ---: | ---: | ---: | --- |
| Temporal | 782 / 813 / 854 | 57 / 57 / 57 | +72 MB, +0 | all nine roles plus PostgreSQL |
| Effect | 597 / 641 / 804 | 6 / 8 / 16 | +207 MB, +10 | missing supervisor, reconciler, opaque receiver |
| Dagu | 548 / 703 / 1,278 | 9 / 15 / 39 | +730 MB, +30 | all nine roles |

### 8.2 Responses to Astra's arguments

1. **Temporal first:** agreed, with higher confidence now. Both conditions I set in §7 for Dagu to take first place (a failed blind composition or a failed original-Task crash test) did not happen. The one-install concern shrinks to idle baseline, PostgreSQL lifecycle, and clean-machine packaging. Under held-call load, Temporal is now the lightest *complete* measured bundle at ten calls.
2. **"No Temporal Task-versus-lookup advantage":** **confirmed by source.** At `../../2026-09-22/arbitration/temporal/adapter.py:76-80`, a prior lookup record returns `_receipt(prior, …)` without fetching the A2A Task. The missing Quality consistency check is therefore common to Effect and Temporal. Dagu's `assign` also trusts the lookup it finds. I withdraw it as an Effect-specific objection, and it stays a product-wide gate.
3. **Effect second, for smaller local parent/child coordination obligations, MIT, and in-flight evidence:** **I now agree, conditionally.**
   - My §6 argument for Dagu was partly a hedge: if Temporal's owned interpreter failed, owning an interpreter would be the risk, and Effect shares it. The fresh A2C and original-Task crash results remove that hedge. An owned interpreter over a durable engine has now passed blind composition twice (Temporal A2C, Effect A2B).
   - The remaining ways Temporal could fail are operational: PostgreSQL, packaging, upgrades. For those, Effect is the better fallback, because it keeps our graph language, validator, and interpreter design.
   - Temporal's thread-in-worker call path also cost about 7 MB per held call. That suggests Effect could reduce its per-call cost by changing only the adapter; this is an inference, not implemented. Dagu's route to the same result needs a new graph-level gate and scanner protocol.
4. **Dagu's independent-root protocol burden:** **agreed, with one correction and one addition.**
   - **Correction:** the failure sealing isn't tied to named fixture steps. `reconciler.py` and `bridge.py` handle any failed step.
   - **Addition:** both functions act only when **exactly one** native step has failed (`../dagu-failure-recovery/reconciler.py:37-39`, `../dagu-failure-recovery/bridge.py:62-64`). A child whose parallel branches fail together would be neither retried nor sealed, which could strand the parent and the Director Task. This is a source finding, untested, and it is exactly the kind of generalization cost Astra describes.
5. **Upgrade obligations apply to all routes:** agreed. The Effect result shows the hazard; it proves nothing about Temporal or Dagu. The Temporal upgrade report is pending.
6. **Astra's decisive proof:** the Temporal half is now observed (the acceptance case above). **The Effect half is the single remaining test that decides the runner-up:** a crash after authoritative acceptance is durable in Effect's product SQLite and before release, through the original Director A2A Task, with independent services left running.

### 8.3 Where I still differ from Astra

- **Evidence completeness.** Dagu's observed A3–A5 record and its nested accept/abort/failure/saved-input recovery remain stronger than Effect's. I rank Effect second as a **product-fit judgment that still needs qualification**, not as the better-tested implementation. Astra frames it the same way.
- **Effect's upgrade seam.** I weigh this more heavily than Astra does. The only demonstrated way to keep old runs on old behavior is one helper and store per interpreter version, plus version-aware routing that we would own. That is engine-specific orchestration work in the same class Astra holds against Dagu's separate-root bridge.
- **Where the gap falls.** Astra favors a smaller durable recovery surface. Effect's separate product ledger is a second durable surface whose crash seams haven't been injected. Until that happens, I would not call Effect's recovery surface smaller than Dagu's, only different.

### 8.4 Revised ranking

| Rank | Candidate | Confidence | What would move it |
| ---: | --- | --- | --- |
| 1 | Temporal Server + stable Python interpreter | moderate–high | It falls if the pending failed-child or upgrade reports show silent divergence or a stranded Director Task, or if clean-machine PostgreSQL packaging proves unworkable. |
| 2 | Effect Workflow/Cluster | low–moderate | **Dagu retakes second if** Effect fails the acceptance-to-release crash through the original Director Task, or if any of these fails: the in-flight assignment crash after remote commit, stale owner, or the opaque-receiver case. |
| 3 | Dagu Community | low–moderate | It regains second if Effect fails the test above. It could contend higher only with a fault-tested assignment gate near its wait-phase footprint, multi-branch failure sealing, and a cleared GPL coupling review. |
| 4–10 | Temporal+Zigflow, Kestra, Restate, Conductor source, Strands Graph, Hatchet, Argo | — | Unchanged from §3. |

**Still open for Temporal:**
- the Director facade doesn't carry typed run inputs;
- failed-child propagation;
- interpreter-upgrade parity with `effect-upgrade-stability`, unpinned and pinned to a worker version;
- full closure of policy and service versions;
- Quality Task-versus-lookup consistency (common to all);
- clean-machine PostgreSQL packaging, backup, and migration.
