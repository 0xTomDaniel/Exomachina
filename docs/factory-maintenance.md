# Factory maintenance and improvement

Architecture requirement · 21 September 2026 · Documentation only; no runtime implemented or engine selected.

## Problem statement

A factory must recover from ordinary incidents without repeatedly losing work or requiring a human to diagnose every interruption. It must also learn from recurring failures, unnecessary work, and weak results. Recovery of today's run and improvement of tomorrow's process need distinct authority: neither permits an agent to rewrite active work, bypass acceptance, or administer an opaque agent service without authorization.

## Solution

**Harness integration requirement · 22 September:** factory functionality is supplied by the customized Strands harness, whose agent acts as Director and can expose an A2A factory endpoint. One install/start command supplies it; automatically managed bundled local engine/storage helper processes are allowed, with no separately provisioned external orchestration service. Maintenance and improvement use this same factory functionality; they do not require a separate factory-service application per agent. Durable state survives conversation/process restart, and required Quality authority remains separately controlled. See the [planning assumptions and proposed spikes](factory-decision-spikes.md).

Each harness instance has its own stable identity and durable state, even when instances share local engine/storage processes. Start with trusted native local code and application-level authority checks; defer sandboxing until a concrete blocker. This initial scope preserves independent review and recovery ownership without claiming protection against hostile code running under the same OS account.

Every factory has four accountable responsibilities: a **Director**, with **Production**, **Quality**, and **Engineering**. Each function has a supervisor who directly coordinates specialist workers: Production operators, Quality reviewers/inspectors, and Engineering technicians/process engineers. There is no separate team-lead or foreman layer by default. Engineering combines incident recovery and continuous process improvement; optional autoresearch is an Engineering mode, not another department.

These responsibilities may use shared services and do not require a dedicated agent or process for every role. A supervisor may also perform specialist work in a small factory, while required review remains independent of the author. Engineering cannot approve its own improvements, and Production cannot bypass Quality's required acceptance. The workflow engine remains responsible for execution, and platform controls enforce budgets and permissions. Required maintenance coverage is an ongoing lifecycle responsibility alongside production; an endless maintenance branch must not prevent a successful production run from completing.

| Mode | Trigger and responsibility | Permitted result |
| --- | --- | --- |
| **Incident recovery — required** | An observed fault, stalled attempt, unavailable dependency, or repeated rejection needs attention now. | Reconcile evidence and apply a known, policy-approved recovery action; otherwise preserve the work and escalate. |
| **Continuous process engineering — required** | Completed-run traces, incident patterns, quality findings, and operating measurements reveal an improvement opportunity. | Propose, evaluate, and publish a new factory version or request a separately tested reusable implementation change. |
| **Proactive research — optional** | An enabled, budgeted campaign searches for improvements without an incident. | Run isolated experiments and submit candidates through the same publication requirements. The mode can remain disabled. |

The selected workflow engine remains the process authority for production and maintenance runs. Engineering observes execution evidence and submits authorized commands through existing lifecycle rules. A proposal, diagnostic report, or Director's conversation cannot itself change a run's state.

## Roles and user stories

| Role | Responsibility | Authority limit |
| --- | --- | --- |
| **Director** | Owns objectives, the customer brief, priorities, budget and operating policy, escalation decisions, and promotion/rollout policy within authorized scope. | Cannot waive protected acceptance or experiment constraints through an informal instruction. Changes to the Director's own protected limits, evaluator, permissions, or promotion authority require an independently controlled release; routine adjustments within existing delegation may remain automatic. |
| **Production** | Plans and schedules authorized demand, allocates capacity, sources and binds approved capabilities, supplies scoped inputs, and manages artifact logistics and delivery. | Coordinates through engine assignments and commands; does not maintain an independent dispatch queue, grant new service permissions, or bypass required acceptance. |
| **Quality** | Independently checks production artifacts and Engineering candidates, protects versioned evaluation criteria, records evaluation evidence, and applies required acceptance or release holds. Serves required improvement and optional research alike. | Cannot accept a candidate using the candidate author's authority, weaken protected criteria for a favorable result, or substitute evaluation success for authorized publication. Independence remains required when services are shared. |
| **Engineering** | Claims and diagnoses incidents, performs bounded approved recovery, verifies outcomes, escalates exhausted recovery, analyzes traces, and proposes improvements. Runs bounded autoresearch campaigns when enabled. | Cannot invent an unsafe retry, replace accepted evidence, alter active factory versions or an opaque service's private implementation, edit protected evaluators, or approve its own candidate. A kept trial updates the experimental incumbent; Quality evaluation and shared release policy still govern promotion. |

1. As a factory owner, I want maintenance coverage, so that ordinary incidents have an accountable response.
2. As Engineering, I want duplicate reports to converge on one incident, so that concurrent observers do not trigger competing recoveries.
3. As Engineering, I want current assignment and attempt evidence, so that I can distinguish reconnecting from retrying.
4. As a director, I want exhausted or ambiguous recovery escalated with preserved evidence, so that I can make an informed decision.
5. As a worker, I want in-flight artifacts and accepted revisions protected during maintenance, so that recovery does not destroy completed work.
6. As Engineering, I want successful and failed traces with their bindings, so that improvements address observed behavior.
7. As Quality, I want fixed evaluation criteria and independent evidence for production and improvement, including while research is disabled, so that a candidate cannot approve itself or improve its score by weakening the test.
8. As a factory author, I want to publish a tested native definition using existing capabilities, so that routine improvement needs no new orchestration deployment.
9. As an operator, I want controlled rollout and rollback of future admissions, so that regression does not silently change active runs.
10. As a service owner, I want administration requests to use my explicit contract, so that callers cannot reconfigure my private workers.
11. As an operator, I want engine failure detected independently, so that recovery does not depend entirely on the failed engine.
12. As a factory owner, I want proactive research to be optional and capacity-limited, so that experimentation cannot consume production's reserved resources.

## Implementation decisions

### One Maintenance Module, a small Interface

Place the Maintenance Module at the existing Seam between execution observations and authorized run/publication commands. Its Interface accepts a maintenance request and exposes its recorded status and evidence. A request identifies the factory/run scope, incident or campaign identity, observed state/version, evidence references, and applicable policy. Callers do not supply arbitrary executable recovery code.

Repeated requests return the same logical maintenance record. Responses distinguish recorded work, duplicate requests, stale expectations, denied actions, unresolved outcomes, and completed observations. Scheduling and potentially lengthy work are asynchronous under the engine's existing facilities. Policy supplies configured deadlines and limits; no recovery-time guarantee is implied before measurement.

The Implementation concentrates incident correlation, current ownership, policy checks, bounded recovery, coordination of independent Quality evaluation, and promotion evidence. It reuses the engine's history and existing acceptance/publication Interfaces. Engineering does not acquire Quality's acceptance authority by coordinating these calls. Engine-specific Adapters translate approved commands at the existing Seam. This provides Locality for maintenance rules without creating a universal workflow DSL or a parallel execution ledger.

### Incident identity and recovery

Define incident identity by factory/run scope, affected assignment or dependency, failure class, and incident generation. Repeated observations of the same unresolved condition coalesce; a later recurrence after closure remains linked but distinguishable. Claim and action-authorization updates must be atomic under concurrent requests. A superseded recovery owner or attempt loses authority to commit further state changes.

Recover only through the current policy's approved actions and preconditions. Preserve the same assignment across attempts; retain intent, remote identity, and all outcomes. A lost submission response is unknown until reconciled. Local completion fencing does not undo remote effects: the participating service must supply any promised action deduplication and outcome discovery.

Each recovery policy defines deadlines, attempt/restart limits, backoff, and a circuit-breaker condition. Repeated failure opens the breaker, stops the affected recovery/admission scope, and escalates. Resume requires a declared probe or authorized decision. Legitimate waits and intentional pauses are not stalled work. Incident closure requires observed recovery evidence, not merely a successful command submission.

### Preserve evidence and administration authority

Maintenance retains in-flight artifacts, accepted revisions, provenance, and delivery receipts until the relevant retention and recovery obligations permit cleanup. A repair creates a new revision and invalidates the declared dependent acceptance. A late recovery result cannot release an obsolete revision or bypass an independent review.

Factory maintenance and service administration are separate responsibilities. An opaque service owns its model, harness, credentials, deployment, and private memory. Engineering may invoke an explicitly authorized administration capability, but a factory command does not grant those permissions. New adapter or worker code requires a separate tested deployment and compatible version binding; it is not smuggled into a generated factory definition.

### Engine failure and the independent watcher

A minimal watcher outside the workflow engine observes its availability and can invoke only narrowly approved infrastructure recovery, such as a bounded restart, or notify the operator. Watcher instances coordinate ownership and deduplicate recovery actions. They preserve an outage/action receipt for reconciliation when the engine returns.

The watcher does not schedule assignments, reconstruct graph progress, publish versions, or declare artifacts accepted. Engine recovery resumes reconciliation from authoritative history. Failed restart or unavailable persistence exhausts the watcher's policy and escalates; repeated restarts are not a substitute for diagnosis.

### Process engineering and candidate promotion

Engineering compares successful and unsuccessful traces, retaining factory/service bindings, observations, corrections, and outcome evidence. A candidate records its hypothesis, parent version, native-definition changes, allowed mutable surface, and predicted quality/cost effect. Quality independently evaluates the candidate against protected criteria, whether or not proactive research is enabled. Rejected changes and their evaluation or structural-failure evidence remain discoverable so later proposals can account for prior failures; rejection is evidence, not a permanent ban on reconsideration.

The candidate follows: **draft → structural and semantic checks → controlled evaluation → authorized publication → observed rollout**. Native definitions are the default. Domain checks cover compatible contracts, approved capabilities and scope, bounded work, required independent acceptance, artifact lineage, and permitted effects. No average quality improvement compensates for a failed mandatory gate.

The evaluator, acceptance rules, held-out data, thresholds, and permitted mutation surface are fixed and versioned for an evaluation campaign. Enforce their immutability outside the candidate's permissions and record their digests; a prompt instruction alone is insufficient. An evaluator change starts a separately authorized campaign with a new baseline. Candidates cannot edit their own results, access protected answer material, weaken a gate, or expand permissions.

All engineering evaluations, including required process improvement while proactive research is disabled, use separate execution and credential/data scopes with protected production and recovery capacity. Evaluation admissions and reservations account for concurrent trials, model/tool usage, and evaluator overhead. Candidate permissions cannot authorize production effects. Optional research adds campaign-specific limits to these common requirements.

Use held-out quality and cost evaluation with repeated candidate/baseline runs under comparable pinned conditions. Keep training traces separate from acceptance examples and the final holdout. Account for stochastic variation, failure subsets, total model/tool spend, evaluation overhead, and latency. Record inconclusive results explicitly. Publication requires the campaign's declared evidence rule; a tie with a cached noisy score is not an automatic improvement.

Publication creates a new version for future runs. Promotion checks the expected current admission binding to prevent competing candidates from overwriting one another. Active runs retain their original definition and required dependency bindings. A limited initial rollout is observed against declared quality, failure, latency, and cost thresholds; regression can pause admission or restore the previous approved version for future starts. Existing runs are reconciled under explicit pause/cancellation policy. When a new version is required to recover work, an authorized policy may start a linked successor run using permitted retained artifacts and fresh acceptance checks where required. The authoritative transition transfers recovery ownership and fences the predecessor from further progress or publication commands. Preserve the logical identities of unresolved external actions whose intent is unchanged and reconcile their outcomes before successor dispatch; a new run ID does not authorize a duplicate action. A changed intended effect requires its own explicit authorization and handling of the prior outcome. If the service cannot establish an outstanding outcome, preserve unknown state and escalate. Late remote work may still complete; supersession cannot undo its effects. Rollback does not erase history, rewrite their bindings, or reverse completed external effects.

### Procedural guidance inside services

A service may retain structured procedures and retrieve relevant conditions, guidance, and pitfalls for its current assignment state. This is advisory context inside the service. The outer engine continues to enforce factory transitions and acceptance; ordinary workers still need only their assignment contract. Procedural learning remains distinct from the authoritative execution ledger.

Before adding a guidance-model call, compare the existing brief, an ordinary skill, raw local procedure context, and generated local guidance with the outer factory held fixed. Measure accepted artifact quality, repairs, latency, and total model/tool cost. Learned procedure changes are versioned through the service owner's permitted release path; factory changes remain native-definition candidates.

### Optional proactive research

Autoresearch is an optional Engineering mode. An enabled campaign specifies its hypothesis space, immutable evaluator, allowed edits, finite experiment count, wall-clock and spending budgets, concurrency limit, and stop conditions. It can change only its admitted native-definition fields or other explicitly owned experiment artifacts. A service-internal procedure is mutable only within that service owner's authorized campaign. Enabling research adds no management layer and does not transfer Quality's evaluation authority to Engineering.

Experiments use isolated contexts and controlled effects. Their separately capped capacity is subordinate to production's reserved capacity; production pressure defers new experiments. Reservations account for concurrent in-flight work and evaluation overhead. Unknown cost follows an explicit admission policy; delayed usage reports cannot prove a strict cap at an uncooperative provider.

Every trial records candidate, environment, evaluator, metrics, outcome, and rejection/crash reason. The first trial establishes the unchanged baseline. Validity status is separate from the objective value: a crash placeholder or fabricated candidate-reported score cannot win a minimization objective. Quality's independently controlled evaluation records the result. A promising result enters the ordinary promotion process; it does not immediately become production. Exhausted budget, evaluator mismatch, containment failure, repeated crashes, or an authorized stop prevents further experimental admissions and handles active trials through the declared cancellation policy.

## Testing decisions

Test observable behavior through the Maintenance Interface and existing engine, acceptance, publication, and service-administration Interfaces. Use the representative research factory and deterministic fault fixtures from the engine evaluation plan. Preserve actual invocation counts, concurrent ownership decisions, version bindings, protected artifact references, and external effect receipts. Fixture guarantees must not be credited to arbitrary real services.

All scenarios below are **unrun**. Concurrent cases require overlapping requests from at least two independent participants, followed by repeated contested runs; serialized calls are insufficient proof. Participants need not correspond to separate management roles.

| ID | Scenario | Required observable result |
| --- | --- | --- |
| **M01** | Two observers report one incident concurrently; its first recovery owner loses ownership and later submits a recovery result. | One logical incident and current authorized action owner; duplicate reports attach evidence; the stale result cannot advance state. A recurrence after closure is distinguishable. |
| **M02** | Inject repeated restart/retry failure, an intentional wait, and an ambiguous remote submission. | Recovery obeys limits and opens its breaker; the wait remains legitimate; ambiguous work is reconciled or escalated without unsafe resubmission. Evidence survives escalation. |
| **M03** | Stop the engine and race two watcher instances; then make the approved restart fail. | One authorized bounded infrastructure action sequence, no assignment scheduling or acceptance by the watcher, eventual escalation, and reconciled receipts when the engine returns. |
| **M04** | Recover a run with stored/in-flight artifacts; produce a repaired revision and deliver late acceptance for its predecessor. | Evidence remains retrievable; cleanup honors outstanding obligations; stale acceptance cannot release the repair; declared dependent gates are invalidated. |
| **M05** | Generate a native improvement using existing capabilities; submit malformed definitions and a valid graph that bypasses review. | Actionable validation rejects invalid or forbidden candidates. An eligible composition can publish without worker deployment. Rejections and their reasons remain available to later engineering. |
| **M06** | With proactive research disabled, run an engineering evaluation under production load and attempt to accept the candidate using Engineering's authority. Offer a better mean score with a failed semantic gate; try changing its evaluator or using production credentials; then evaluate a noisy candidate. | Quality independently evaluates required improvement while research is disabled; Engineering's self-approval is rejected. Production/recovery capacity and credential scopes remain protected. Mandatory failure and evaluator tampering prevent promotion. Repeated held-out comparisons use the fixed evaluator and declared quality/cost rule; inconclusive evidence remains inconclusive. |
| **M07** | Race two promotions while v1 runs remain active; observe a regression and roll back. Separately start a recovery successor while its predecessor has an ambiguous external action and completes late. | The expected-version check prevents a lost promotion update; active bindings stay intact; future admissions return to the approved prior version, with complete rollout and rollback evidence. Successor activation transfers authority; late predecessor commands are fenced, and outstanding action identities/outcomes are reconciled before redispatch. |
| **M08** | Engineering attempts unauthorized service reconfiguration and proposes an adapter code fix; the Director or Engineering attempts to weaken its own protected limits or release authority. | Administration is denied or routed to its authorized owner. The code fix requires its separate tests/deployment and compatibility evidence; no live factory or opaque service is silently changed. Neither responsibility can authorize its own protected-control change; independent release authority is required. |

| ID | Optional research scenario | Required observable result |
| --- | --- | --- |
| **R01** | No incident exists; research is disabled, then a bounded campaign is enabled. | No unsolicited experiment starts while disabled. Enabled trials carry campaign identity and declared constraints without fabricating an incident. |
| **R02** | A trial writes outside its permitted surface, modifies an evaluator, or attempts a production effect. | Enforcement rejects the operation; the trial cannot promote; retained evidence identifies the violated constraint. |
| **R03** | Concurrent trials contend for the last reservation as production pressure rises. | Admission respects aggregate campaign limits and production's reserved capacity. Unknown/delayed costs and in-flight reservations remain explicit; new experiments defer. |
| **R04** | A trial scores well once but regresses on repeated or held-out cases; another passes. | The first is rejected or inconclusive under the fixed rule. The second becomes a promotion candidate and still crosses ordinary semantic and authorization gates. |
| **R05** | Repeat a rejected hypothesis, trigger repeated crashes, exhaust a budget, and stop a campaign. | Prior evidence informs the proposal; every outcome remains recorded; stop conditions prevent new admissions and active trials follow the declared cancellation policy. |

## Out of scope

This change documents the required architecture and optional research mode. It does not implement agents, run experiments, deploy infrastructure, create tracker issues, select an engine, or set production thresholds. It does not authorize a second scheduler, arbitrary edits to active runs, unrestricted service administration, an unbounded research loop, automatic reversal of external effects, or a universal factory DSL.

## Further notes and sources

The [Procedural Graphs paper](https://arxiv.org/html/2609.09153v1) contributes an offline proposal/evaluation loop, fixed graphs within episodes, and retained rejection evidence. Its online guidance is advisory; its structural checks and measured validation gains do not establish Exomachina's authorization, acceptance, or durability guarantees. Those stronger requirements above are Exomachina design decisions.

[Karpathy's autoresearch](https://github.com/karpathy/autoresearch) illustrates a narrowly editable experiment surface and comparable trial budgets. Its [program](https://github.com/karpathy/autoresearch/blob/master/program.md) protects the evaluation implementation, records trial outcomes, and keeps or discards changes. Exomachina adopts those bounded experiment mechanics while requiring finite campaigns, immutable evaluator enforcement, production capacity protection, and governed publication.

Use the [domain glossary](../CONTEXT.md) for assignment, attempt, acceptance, and execution-ledger meanings. The [engine evaluation plan](engine-evaluation.md) supplies the existing fault-test approach; these scenarios add maintenance obligations without asserting that either candidate engine already implements them.
