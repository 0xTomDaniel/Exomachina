# Factory engine reassessment: development direction

23 September 2026. This supersedes the **ranking**, not the observations, in the [22 September arbitration](engine-arbitration-2026-09-22.md). GPT-6 Astra xhigh and Claude Opus 5.5 high started in fresh Herdr threads, reviewed the [same evidence brief](engine-reassessment-brief-2026-09-23.md) and all still-eligible candidates, made independent choices, exchanged objections through their [retained debate files](../tools/spikes/2026-09-23/arbitration-debate/), and reconsidered the final Temporal trials. Both recommend **Temporal Server with Exomachina's stable Python factory interpreter first, Effect Workflow/Cluster second, and Dagu Community third** for the first development path. This is a conditional development choice, not production qualification.

| Stage | Astra | Opus |
| --- | --- | --- |
| Independent choice, before the final Temporal reports | Temporal first; Effect second | Temporal first; Dagu second, with low confidence in the runner-up order |
| After direct objections and the final reports | Temporal first; Effect second | Temporal first; Effect second |

The shared product boundary drove the judgment. A customized Strands harness contains the Factory Module and its Director; a factory version controls graph layout, routing, waits, budgets, Quality and acceptance gates. Its agent nodes are independently managed, black-box A2A services selected from a directory. The factory does not deploy or restart those services. One installation command may launch bundled local engine, database, and orchestration workers. Automatic worker rollouts are permitted if active runs keep compatible definition and executable versions. The first deployment serves one organization.

## Why Temporal leads

The [fresh frozen composition trial](../tools/spikes/2026-09-23/temporal-fresh-composition/result.md) resolved the old arbitration's most serious Temporal finding. After a 21-file freeze, the unchanged interpreter ran an unseen four-branch graph. One immutable mixed definition produced both r2 acceptance and r3 exhaustion followed by authorized abort; the all-clear branch accepted r1, and an old waiting run retained its definition through publication and restart. The mixed runs used direct Temporal starts because the frozen Director facade did not forward a required typed run input. That is an integration defect still to close, so this is a qualified frozen pass rather than an end-to-end Director pass for both outcomes.

The [original-Task recovery trial](../tools/spikes/2026-09-23/temporal-recovery-scale/result.md) stopped Temporal and PostgreSQL after a remote Quality verdict committed and, separately, after authoritative acceptance but before release. Both runs completed under the same original Director A2A Task and native parent/child IDs, with one acceptance and one participating release effect. At 0, 2, and 10 externally working A2A Tasks, its complete selected bundle measured 761, 790, and 837 MB with 50 processes throughout. Those are local snapshots; the within-bundle 76 MB increase and flat process count are more informative than absolute comparisons across unequal bundles.

Temporal gives the factory native durable parent/child execution, waits, and workflow history around acceptance while preserving a stable worker across new document publication. Exomachina still owns the bounded graph language, validation, catalog bindings, A2A reconciliation, policy, and one-command Temporal/PostgreSQL lifecycle. Its [MIT core license](https://github.com/temporalio/temporal/blob/main/LICENSE) permits a product path subject to the assembled dependency and notice review. [Worker Deployment Version pinning](https://docs.temporal.io/worker-versioning) offers an explicit upgrade mechanism; this exact factory interpreter's old-run behavior across a code upgrade remains untested.

## Why Effect is second, and why Dagu is close

Effect's [fresh frozen four-branch trial](../tools/spikes/2026-09-23/effect-parity/result.md) and post-freeze original Director Task trial show that a product-owned interpreter on its durable child-workflow substrate can express the approved graph vocabulary. Its [held-call trial](../tools/spikes/2026-09-23/effect-inflight-scale/result.md) added about 207 MB and ten processes across ten working A2A assignments in the tested six-process baseline. That topology omits roles present in the Dagu and Temporal bundles. Its [v3 MIT source](https://github.com/Effect-TS/effect/blob/v3/LICENSE) and SQLite-backed helper are attractive for a bundled installation. As a fallback from Temporal, it can retain the same language and validator design without adding PostgreSQL.

This is **fit, not qualification**. Effect's integrated path has not passed the full original-Task crash and opaque-effect matrix. Its [upgrade trial](../tools/spikes/2026-09-23/effect-upgrade-stability/result.md) showed an old run silently adopting changed helper behavior and re-entering renamed Quality Activities; separate old helper/store retention preserved old behavior, but version-aware Director routing and retirement are unbuilt. The product ledger and Effect store add a recovery seam. A matched crash after acceptance but before release through the original Director Task is the smallest discriminator against Dagu.

Dagu remains the best native YAML authoring option and has broader exercised [composition](../tools/spikes/2026-09-22/arbitration/dagu/result.md) and [failure/recovery](../tools/spikes/2026-09-23/dagu-failure-recovery/result.md) evidence. The integrated bridge now carries the original Director Task through accepted, aborted, and one failed-child outcome, plus persisted-input/enqueue recovery. That is substantive evidence, not a paper workaround. Its processless Director waits remain compact. But the tested [in-flight assignment path](../tools/spikes/2026-09-23/dagu-inflight-scale/result.md) reached 1,278 MB and 39 processes at ten working calls, adding three processes per assignment. Exomachina owns a separate-root parent/child completion and retry protocol; the current failure-sealing source handles exactly one failed native step at a time. The original Quality identity-guard failure and release-liveness gap are not erased by the newer bridge. Its [GPL-3.0-or-later licensing guidance](https://github.com/dagucloud/dagu/blob/main/LICENSING.md) distinguishes a separate CLI/server from embedding its Go API; the exact commercial bundle still needs a compliance review. These risks outweigh native YAML in the reviewers' current product judgment.

## Other options considered

The ordering below follows Opus's lower-tier ranking for scanning. Astra assessed every route but did not endorse a shared numerical order below the top three.

| Opus rank | Eligible route | Why it trails the top three |
| ---: | --- | --- |
| 4 | Temporal + Zigflow | A [pinned-version rollout](../tools/spikes/2026-09-23/zigflow-reassessment/result.md) kept old waits but retained a worker per active version and exposed an activation race requiring a barrier; full A2A factory behavior is unproved. |
| 5 | Kestra OSS | Native YAML and a [four-branch graph-shape proof](../tools/spikes/2026-09-23/kestra-parity/result.md) are strong; the proof used placeholder acceptance, and the selected JVM/PostgreSQL bundle was about 1.3 GB before a live Director path. |
| 6 | Restate | Attractive Rust/Python durable substrate; product-shaped A2A, nested factory and complete-bundle evidence remain narrower, and Exomachina would still own the document interpreter. |
| 7 | Conductor PostgreSQL source variant | Native documents, but an owned source build and exact distribution review precede full factory behavior and operating-cost proof. The assessed stock artifact remains uncleared. |
| 8 | Strands Graph | Harness fit is high; durable ownership, fencing, remote-effect recovery and publication would become substantial product responsibilities. |
| 9 | Hatchet embedded | Local operation is credible, but PostgreSQL, an owned interpreter and product-shaped integration remain, without a demonstrated advantage over Temporal or Effect. |
| 10 | Argo Workflows | Kubernetes operation adds disproportionate scope to the first one-install local product. |

The [independent assessments](../tools/spikes/2026-09-23/arbitration-debate/) give the full candidate-by-candidate arguments. LangGraph, n8n Community, and distributed Windmill Community remain excluded under the earlier product screen and remain visible in the feature matrix.

The existing [65-feature matrix](exomachina.html#feature-matrix) measures documented coverage, with Dagu and Effect not fully scored. It is not a hidden numerical tie-breaker for this choice. The [selection judgment](exomachina-selection-judgment.json) retains historical criterion ratings and this later decision separately.

## Hard-stop audit after the debate

On 23 September, Astra and Opus independently revisited the exact-route dealbreakers and cross-checked each other's conclusions. They agree that **a maintained Conductor PostgreSQL source build and Kestra's measured shared bundle are not established dealbreakers**. A hard stop requires a non-negotiable requirement, primary evidence that an exact artifact or use violates it, and no acceptable remedy within the agreed constraints. A resource ceiling must be agreed before using it to eliminate an option.

| Exact route | Current status | Boundary that matters |
| --- | --- | --- |
| LangGraph first-party production Agent Server | Excluded under the owner's no-paid-platform decision | Its licensed production server is distinct from the MIT core library, which could be hosted in an Exomachina-owned runtime but is not the preferred path. [Deployment documentation](https://docs.langchain.com/langsmith/deploy-standalone-server) |
| n8n Community for customer/agent-authored factories | Excluded for this product use | The [official license FAQ](https://github.com/n8n-io/n8n-docs/blob/main/docs/n8n-community-license/license-faq.md) restricts external users configuring workflow logic through a UI, API, MCP or an agent acting for them. Internal operator-authored automation is a different use. |
| Distributed Windmill Community binary wrapped into Exomachina | Excluded without a separate agreement | Its [distribution terms](https://github.com/windmill-labs/windmill/blob/main/LICENSE) restrict selling, modifying and wrapping that binary. A separately built AGPL source version remains an unqualified alternative. |
| Stock Conductor v3.32.4 server | Rights hold, not an engine-wide exclusion | The included Orkes queue dependency has [competitive-product restrictions](https://github.com/orkes-io/licenses/blob/14cd5d6b0619399c205a022cc7b512734ea51911/community/LICENSE.txt) that leave Exomachina development use and distribution uncleared. Do not use that artifact for product work without clearance. The [PostgreSQL source variant](conductor-license-follow-up-2026-09-22.md) removed the dependency and passed a simple restart trial; its ongoing patch, release, integration and footprint costs remain. |
| Shared Kestra OSS v2.0.3 helper | Eligible, with a measured operating disadvantage | The [native-wait bundle](../tools/spikes/2026-09-23/kestra-parity/result.md) measured 1.285 / 1.287 / 1.301 GB at 0 / 2 / 10 waits, with an idle Director and direct API starts. No whole-install ceiling or matched Director-routed measurement exists. One engine/database pair per harness remains rejected. |
| Zigflow on Temporal; Dagu Community separate server | Eligible but conditional | [Zigflow's authoring route](../tools/spikes/2026-09-23/zigflow-reassessment/result.md) needs safe version activation and a complete A2A factory trial. The tested [Dagu route](../tools/spikes/2026-09-23/dagu-failure-recovery/result.md) owns cross-root recovery and has remaining fault and distribution work; neither is a proven engine exclusion. |

The consensus supports starting bounded Temporal development **without another broad comparison spike**. It does not qualify Temporal for product release or justify ignoring eligible alternatives. The next decision-sensitive work is its original-Task failed-child projection, typed Director inputs, version-pinned interpreter upgrades with verified activation, and a clean-machine one-install package with backup, migration and an agreed resource envelope. A matched Kestra or Zigflow trial becomes necessary if a Temporal gate fails or its owned interpreter/operating cost proves unacceptable; Conductor source qualification matters if the project chooses to carry that release path forward.

## Bounded Temporal qualification follow-up

Later on 23 September, four parallel workers ran the development gates in bounded form. The [integrated verdict](../tools/spikes/2026-09-23/temporal-qualification/result.md) **keeps Temporal first; no gate failed in a way that changes the ranking, and none is product-qualified.**

- **Now observed on ordinary paths.**
  - *Typed Director inputs.* One immutable mixed package ran through the real Director A2A endpoint. One run accepted r2 with one release. The other exhausted at r3 and completed after an authorized abort, with no release. Invalid and missing inputs were rejected before any Workflow started. [Director contract](../tools/spikes/2026-09-23/temporal-director-contract/result.md)
  - *Version-pinned interpreter upgrade.* Every Director start named its Worker Deployment Version explicitly. A run waiting on build B1 stayed pinned to B1, with its child, through B2's publication (renamed Activity, changed output shape). It then completed with the old output shape, while new runs used B2. [Version binding](../tools/spikes/2026-09-23/temporal-version-binding/result.md)
  - *Quality and release journal.* An ordinary run passed the new Quality consistency check. Its journal recorded every assignment, Quality action and release. [Quality and reconciliation](../tools/spikes/2026-09-23/temporal-quality-reconciliation/result.md)
- **Observed failures as currently built.** The one-command package starts and stops everything, but 48 binaries reference Homebrew libraries, so it is not clean-machine relocatable. The Temporal frontend still accepts unauthenticated clients. [Package and operations](../tools/spikes/2026-09-23/temporal-package-ops/result.md)
- **Partial.**
  - A cold backup restored into a fresh prefix, and the same original Director Task then completed.
  - Schema updates at 1.32.0 are idempotent.
  - The full bundle measured 700 / 717 / 746 MB at 0 / 2 / 10 waiting A2A Tasks, with 40 PIDs throughout.
- **Unproved live, by the safety boundary.** A failed child, an inconsistent Quality verdict, or an ambiguous A2A outcome reaching the original Task as `failed`. Each is implemented and unit-tested only. Also unproved: retiring old builds after drain, contract and Quality-policy attestation, and online backup and upgrade paths.
- **Source findings in the baseline.**
  - Plain `ValueError`s in the interpreter loop as Workflow Task retries instead of failing.
  - A failed parent would read as `input-required`.
  - A test fault profile is reachable from the product start command.
  - The Director token is stored in Workflow history.
- **Owner decision.** The per-install memory ceiling is still unset. Below about 750 MB rejects the measured bundle; 1 GB has headroom but is unqualified.

No worker repeated the previously rejected fault-injection tests. One cleanup event is disclosed plainly in the integrated verdict. The Codex command policy rejected an `rm -rf` of a trial PostgreSQL data directory as an "rm -f style" command. A later, unapproved Python `shutil.rmtree` removed that same target.

## Development gates

The first Temporal slice should complete the Director's typed-run-input contract and map a failed native child to an incident and `failed` status on the original A2A Task, with no acceptance or release. It must pin the full definition, service-contract, Quality-policy, and interpreter/worker closure for every active run. Code upgrades must keep waiting runs on their promised behavior, including renamed Activities and old output shapes; the [official worker-versioning model](https://docs.temporal.io/worker-versioning) is a mechanism to implement, not proof that this candidate already uses it correctly. The installer must bundle Temporal and PostgreSQL under one start command, with owned backup, restart, and migration paths and a measured resource envelope. The common product gates remain authorization across direct engine paths, Quality identity and verdict consistency, ambiguous A2A action reconciliation, and independent release receipts.

The planned local failed-child/upgrade fault-injection follow-up has **no passing result**: automatic approval review stopped the test, and no claim is made from its partial setup. The later bounded follow-up above observed an ordinary pinned upgrade and typed inputs. It left failed-child projection unproved rather than retrying that test. The Effect Quality-verdict follow-up was also stopped; the source audit identifies a consistency gap, not an observed injected failure. These limits remain visible in the decision and should not be converted into implied passes or failures.
