# Factory decision spikes and assumption register

Planning baseline · 22 September 2026 · The owner approved a narrowed S0–S4 round, executed in parallel with isolated environments. Read the [observed results and remaining gates](spike-results-2026-09-22.md) before using this original plan. The 22 September [decision-round comparison](../tools/spikes/2026-09-22/decision-round/result.md) ranked Dagu first at that stage; the [23 September reassessment](engine-reassessment-2026-09-23.md) instead recommends Temporal's stable interpreter first, Effect second, and Dagu third for development. This file remains the original plan, not the current selection record. S5 and the broad S6 comparison remain deferred. Source inspection is evidence for a hypothesis, not an experimental result.

## Recommendation

The deployment gate is settled: **one install/start command must supply factory functionality without a separately provisioned external workflow service. Automatically managed bundled local helper processes are allowed.** The earlier required shared-Conductor-server assumption is superseded. Existing engine rankings describe the prior operating model and do not clear this new gate.

The original plan proposed seven bounded spikes, S0–S6, around one reusable reference factory. The approved first round narrowed this to the package gate, contested-state litmus, harness, native publication and remote recovery. Conductor-specific runtime work stopped at its stock artifact's unresolved license gate; Kestra cleared a local behavioral screen and was used for the bounded runtime probes. Its one-install product package remains unproved. The subsequent [resource decision and countertrials](runtime-resource-decision-2026-09-22.md) reject a Kestra/PostgreSQL pair per harness and keep one shared local pair conditional. Bounded Dagu, non-development Temporal and core Strands Graph trials now give package and publication evidence, but no complete common product topology or engine selection.

Passing these spikes would support an engine decision and implementation of the first product slice. It would not qualify a production deployment. Full E01–E10, M01–M08 and applicable R01–R05 scenarios remain the broader acceptance plan; a narrow probe does not mark a whole scenario passed. The existing Temporal/Zigflow probes remain valid for their recorded scope and need not be repeated.

The largest architecture risk is the assumption that a native workflow engine plus a small custom Module can enforce publication, remote recovery, exact-revision acceptance and Director authority. The spikes must measure that owned complexity, as well as whether the engine runs.

## Decisions and the updated harness architecture

- **Accepted:** initially one organization per deployment, preserving a path to multiple customers. Organization ownership belongs in durable records and public Interfaces. Shared-engine customer isolation is a future qualification; role authorization and scoped credentials are required initially.
- **Accepted:** factory functionality is built into the customized Strands harness. Any appropriately configured agent using that harness can operate as a factory service. Its agent acts as the factory Director. A separate factory-service application per agent is not required.
- **Accepted:** the harness cannot require an externally provisioned orchestration service. One install/start command supplies factory operation and can automatically manage bundled local engine/storage helper processes. A factory-mode harness exposes its configured capability through the instance's normal A2A interface; callers need not know a factory implements it. An in-process-only engine is not required; a separately managed shared server is not the baseline. Reuse of one locally managed engine across agents in an installation is a design option to evaluate, rather than launching an engine/database per agent by default.
- **Accepted:** every harness instance has its own stable identity and durable state, regardless of any future sandbox choice. Scope configuration, workspaces, artifacts and pending work by instance; restarting an instance preserves its identity and state while replacing its execution incarnation. Logical ownership does not require a separate database or engine process for every instance.
- **Accepted simplification:** start with native local processes and defer sandboxing until a concrete blocker warrants it. No sandbox comparison, implementation or whole-harness containment proof belongs in the initial spikes. Docker is not a baseline prerequisite; optional isolation must not drive the first design.
- **Evaluation preference:** Java/JVM is a deployment tradeoff, not an exclusion. Judge the actual installation, startup and idle footprint, helper lifecycle and upgrade burden. Do not introduce a replacement runtime or custom scheduler merely to avoid a language.
- **Preserved:** native factory definitions; publication of new compositions without harness/worker refresh; independent Quality; required Engineering; optional autoresearch; a reusable A2A Adapter; no second scheduler or universal workflow DSL. A new reusable capability implementation may still require deployment.
- **After the narrowed spikes and resource review:** Kestra OSS v2.0.3 is the observed behavioral development baseline, not a selected product runtime. Its ~650 MiB warm-idle JVM RSS excludes PostgreSQL and Strands. A separate Kestra/database pair per harness is rejected as the default; a shared local pair requires a measured whole-install envelope and independently owned lifecycle. Bounded Dagu, production-topology Temporal and core Strands Graph countertrials have run: Dagu is a credible light declarative challenger with transitive child-pinning work; Zigflow still requires worker refresh; Strands Graph resumes pinned checkpoints but needs fenced run ownership after duplicate delivery in a same-run race. Exact production Strands/A2A versions, storage package, workload targets and implementation budgets remain open.

The revised logical arrangement is:

```text
Customized Strands harness
  Director agent: understands the brief and requests authorized decisions
  Factory Module: publication, invocation, status, decisions, cancellation
  A2A Interface: exposes this harness's factory capability
  Adapter/reconciliation: connects durable engine work and remote tasks
  Durable workflow execution and storage supplied by the installation
    Embedded runtime or automatically managed bundled local helpers
    Native definitions, scheduling, waits, retries and execution history
       |
Agent capabilities, including other factory-enabled Strands harnesses

Durable product records + artifact storage are part of the runtime package.
Quality/Engineering are responsibilities, not mandatory extra processes.
```

The Factory Module should provide Depth through a small Interface: publishing an allowed composition, starting work, inspecting its evidence, submitting a scoped decision, and requesting cancellation. These are proposed operations, not an SDK specification. Implementation details such as engine task identities, callback delivery and reconciliation belong inside the Module. Engine-specific operations stay in an Adapter; do not build a general engine abstraction before the comparative slice shows an actual need.

The Director's conversation is not the execution ledger. Harness restart or conversation compaction must not create new work accidentally. A durable current-owner check must reject commands from a superseded Director instance. Routine deterministic progress should not require an LLM call at each graph edge. The Director is invoked for the decisions the factory policy assigns to it. Locally durable state must survive process restart; surviving loss of the entire host additionally requires backup/replication, which is an explicit production decision. Self-contained orchestration still permits configured model providers and remote A2A capabilities.

The initial trust assumption is trusted local harness code and configured capabilities. Application Interfaces still enforce scoped commands, current ownership and independent Quality acceptance. Separate identities and state prevent operational collisions; they do not establish protection against hostile code running as the same OS user or hard CPU/memory isolation. Those stronger boundaries are unqualified and deferred. S2/S4 check the supported application paths; they do not become sandbox or arbitrary-code containment projects. Revisit containment only when a concrete requirement or observed failure makes it necessary.

## New source evidence that changes the spike order

> **Follow-up, 22 September 2026:** the [Conductor package-rights investigation](conductor-license-follow-up-2026-09-22.md) subsequently built a PostgreSQL-only source variant without the direct restricted dependency and ran a simple workflow across restart. The stock artifact remains uncleared; reproducible product packaging, full notices and S1–S4 factory behavior remain open. The hypothesis below records the original spike-plan state.

**Artifact eligibility is also unresolved for a concrete reason.** The v3.32.4 server declares `io.orkes.queues:orkes-conductor-queues`, pinned to `2.0.0.rc3`. Its published POM declares the Orkes Community License, whose Article 3.3 contains third-party-service and competitive-product restrictions. This is not the Apache core license. The intended assembled product therefore needs explicit clearance or removal of that dependency; it is not established as a permitted baseline merely by building the repository. [Server declaration](https://github.com/conductor-oss/conductor/blob/v3.32.4/server/build.gradle#L106), [dependency version](https://github.com/conductor-oss/conductor/blob/v3.32.4/dependencies.gradle#L66), [published POM](https://repo.maven.apache.org/maven2/io/orkes/queues/orkes-conductor-queues/2.0.0.rc3/orkes-conductor-queues-2.0.0.rc3.pom), [pinned license](https://github.com/orkes-io/licenses/blob/14cd5d6b0619399c205a022cc7b512734ea51911/community/LICENSE.txt).

The dependency also appears through other modules. Selecting PostgreSQL at runtime does not remove it from the stock package. At planning time, a PostgreSQL-only assembly excluding the restricted dependency and dependent modules was an uncompiled hypothesis. The later follow-up established a narrow source-build and restart path, not a distributable or factory-qualified product. This artifact restriction and the newly required self-contained operation both qualify the previous Conductor recommendation; its feature score remains documentation-based technical coverage, now adjusted for the omitted workflow event listener.

Conductor's PostgreSQL documentation describes persistence, queues, indexing and locking, while explicitly advising workload-specific benchmarking. That establishes a supported configuration claim, not our reliability or capacity result. [PostgreSQL documentation](https://conductor-oss.github.io/conductor/documentation/advanced/postgresql.html).

The reviewed v3.32.4 lock source uses expiring database rows and thread-local holds. Its release/delete paths warrant tests involving lease expiry, failed acquisition and stale owners. The task-update path also warrants overlapping completion/cancellation tests: state checks and persistence are separate operations, and the ordinary update path does not itself demonstrate a compare-and-swap guard. **These are source-grounded race hypotheses, not reproduced failures or a claim that Conductor is unusable.** [Pinned lock source](https://github.com/conductor-oss/conductor/blob/v3.32.4/postgres-persistence/src/main/java/com/netflix/conductor/postgres/dao/PostgresLockDAO.java), [pinned execution source](https://github.com/conductor-oss/conductor/blob/v3.32.4/core/src/main/java/com/netflix/conductor/core/execution/WorkflowExecutorOps.java), [PostgreSQL execution persistence](https://github.com/conductor-oss/conductor/blob/v3.32.4/postgres-persistence/src/main/java/com/netflix/conductor/postgres/dao/PostgresExecutionDAO.java).

The independent review proposed ZooKeeper as a possible fallback. Treat that as **unverified for the pinned distribution**: the source review did not establish a bundled ZooKeeper implementation in v3.32.4. A guide mentioning it is insufficient. Verify an actual available implementation before proposing it as a remedy; changing lock storage would not automatically fix a separate task-update race. [Pinned deployment guide](https://github.com/conductor-oss/conductor/blob/v3.32.4/docs/devguide/running/deploy.md), [server dependencies](https://github.com/conductor-oss/conductor/blob/v3.32.4/server/build.gradle), [module inventory](https://github.com/conductor-oss/conductor/blob/v3.32.4/settings.gradle).

Strands documents plugins and A2A serving, supporting the direction of a harness extension. Its current A2A documentation also distinguishes per-context agent construction from a shared agent instance; documents caller-supplied context IDs as insufficient access protection; and identifies limitations in the high-level client for structured interrupt replies. Pin the actual SDK and test the exact public Interface. A stock text-oriented wrapper is not automatically the required factory task contract. [Plugins](https://strandsagents.com/docs/user-guide/sdk/plugins/), [A2A serving, contexts and interrupts](https://strandsagents.com/docs/user-guide/sdk/multi-agent/agent-to-agent/).

## Proposed spikes

### S0 — Establish a permitted, self-contained runtime package

**Question:** can we supply factory operation with the harness installation, with no separately provisioned workflow platform and no required restricted commercial dependency?

**Plan:** first screen the candidate against the accepted native installation with automatically managed bundled helpers. Record source commit, dependency inventory, artifact digest, included server/UI/modules, licenses, configuration and intended delivery model. For a Conductor trial, use v3.32.4 as the reviewed starting candidate, not an assertion that it is the latest release. Resolve the restricted queue dependency through every dependency path. Assess installation, managed local storage, process lifecycle, ports, startup/shutdown, recovery, update/backup ownership and actual startup/idle memory and disk footprint. Bundled helpers must come under the harness launcher rather than require a user's separate server setup. In-process execution is not required. Evaluate JVM overhead through these observations, with no language-based disqualification and no sandbox implementation.

PostgreSQL persistence/queues/indexing with explicit locking remains a conditional topology to assess, not a settled choice for the self-contained harness. Inspect actual loaded providers and disable accidental in-memory/development substitutes. After dependencies are available, test start, execution and restart without a remote orchestrator, vendor-network access or vendor key. Locally bundled infrastructure remains reachable. A Docker-only path does not satisfy the native baseline; no optional container or sandbox backend needs evaluation now.

**Pass:** a documented permitted artifact, self-contained startup within the agreed footprint, an observable native workflow, durable restart and identified providers; no required external orchestration platform or paid-only facility without an accepted alternative. If local helpers are shared, start two independently launched harness agents, stop/restart the first launcher, and verify process lifetime, port/store ownership and surviving work. One agent exiting must not silently destroy another's runtime. A successful offline boot proves behavior only, not license permission.

**Decision/stop:** unresolved distribution rights, incompatible process requirements or missing mandatory runtime capabilities block that artifact. Missing native A2A does not block it if the agreed Adapter can supply the required contract. Record the cost of every alternative. Do not turn this into an engine-embedding or dependency-remediation project. Re-screen lighter local runtimes if the existing server-oriented candidates fail this gate.

**Scope:** one pinned baseline; no deployment automation, load benchmark or production approval. Supports the entry conditions in the existing evaluation.

### S1 — Test the engine's contested state before building around it

**Question:** do locking, task updates and recovery converge correctly under the failures our product will create?

**Plan:** first use a tiny native workflow with deterministic tasks. Force overlapping updates, lock acquisition failure and lease overrun. Race completion against completion, timeout and cancellation. Kill/restart a process after persistence but before queue acknowledgement; interrupt storage access and restore it. Observe actual lock ownership, task history, downstream starts and reconciliation. If the proposed runtime permits multiple engine processes sharing a store, add the two-process lease case; otherwise prove exclusive store ownership and confine the first litmus to the supported local topology. Do not make distributed HA an accidental initial product requirement.

**Pass:** a coherent observable outcome under a race policy agreed before execution; no lost durable work, irreversible unauthorized progress, unexplained stuck join or corruption. Declare completion/cancellation/timeout precedence, the authoritative commit point and permitted downstream effects before observing results. At-least-once task delivery is allowed and must be measured; external effects still need their own idempotency. A duplicate delivery is not itself a failed engine test. A completion/cancellation race need not always choose cancellation, but an observed result cannot be used to invent its own passing policy.

**Decision/stop:** reproduce source suspicions before declaring defects. Separate configuration failure, lock-provider defect, engine-kernel defect and missing product fencing. Evaluate one bounded remedy on a verified available implementation. If reliable execution requires owning a substantial scheduler/kernel fork or serializing the entire platform externally, reconsider Conductor and move the same litmus to Kestra. A higher coverage score cannot excuse the failure.

**Scope:** correctness litmus, not HA certification. Run the early litmus immediately after S0; broader recovery tests can follow the harness fixture. Relates to E03/E05/E09 and M01/M03 without completing them.

### S2 — Make a Strands harness act as a durable factory service

**Question:** can factory functionality live in the customized harness while durable progress and authority survive that harness?

**Plan:** add the smallest Factory Module to a pinned Strands setup. Configure one ordinary capability agent and one factory Director using the same harness package. Give each instance its own stable identity, state namespace, configuration and workspace, with collision-free endpoints. Expose the latter through its normal A2A interface with a factory-result capability contract, start one run, wait for a Director decision and restart the harness. Resume under the same instance identity from durable evidence; confirm the other instance's state is unchanged. Replace the Director's execution incarnation and deliver a stale command from the old incarnation. Invoke a child factory hosted by another instance of the same harness; test parent cancellation and child result retrieval. Shared storage is allowed with explicit per-instance ownership; no sandbox is needed for this lifecycle proof.

**Pass:** one installation supplies the capability agent, factory Director, durable execution and A2A endpoint; no separately provisioned workflow service or bespoke factory application per agent. Reconnecting or repeating a tool call does not duplicate an assignment. The current Director can inspect/resume the same run, and stale or unauthorized commands are rejected. Factory completion reflects accepted output, not merely the Director returning a chat response. Reused harness code supports both capability and factory roles without bespoke graph code per factory.

**Decision/stop:** if this requires replacing Strands' agent loop, turning its session memory into a scheduler, or constructing a separate bespoke factory host for every agent, revise the integration design before selecting the stack. Any proposed engine must satisfy S0's process/dependency requirement first.

**Scope:** one parent, one child, one wait and one restart; no elaborate Director prompt or UI. Stub model decisions initially to isolate lifecycle behavior, then schedule real-agent validation under Q1. Supports E02/E03/E09 and the newly accepted harness requirement.

### S3 — Prove factory publication and complete immutable bindings

**Question:** can agents publish useful factory documents without harness refresh, while active work keeps its intended semantics?

**Accepted first language profile:** agents may compose flexible workflow graphs from approved node types: assignments, typed conditional routing, parallel work with defined joins, bounded loops including review/repair, Director waits and nested published factories. This is not a fixed template library. Reject arbitrary executable expressions, unbounded cycles, unsafe fan-out and paths that bypass required Quality acceptance or controlled delivery. A new composition of existing node types is data; adding a primitive requires a tested code release. A native-definition candidate must be restricted to the same permitted semantics; a code-first candidate must prove its document interpreter without claiming that the engine supplies one.

**Plan:** use the reference factory: two research branches, synthesis, independent review, one bounded repair path, Director wait and controlled delivery. Declare the supported node, task and predicate profile. Publish v1 and leave it waiting before a child/dynamic task starts; publish v2 and alter aliases or attempt same-version metadata mutation. Exercise child definitions, task definitions, retry settings, capability revisions, evaluation policy and dynamic selection. Try a schema-valid review bypass, an unapproved task type/destination and excessive fan-out.

**Pass:** new compositions run through the same harness/worker deployments. Accepted v1 dependency bindings stay fixed; v2 applies to new admissions. Mutation or undeclared resolution is rejected or made immutable by the publisher. Dangerous configurations cannot escape the supported profile, and errors are actionable. Emergency revocation may stop future dispatch without rewriting the recorded binding.

**Decision/stop:** explicit versioned task names, dependency manifests and restricted constructs are plausible costs. A bounded interpreter atop a code-first durable runtime is permitted, but its definition storage, graph routing, replay compatibility and diagnostics are product ownership and must be measured. Stop if the design grows into an independent scheduler, arbitrary-code safety prover or universal compiler. Do not require static proof of arbitrary expressions; exclude them from the first language and enforce effect permissions at runtime as needed.

**Scope:** one useful native vocabulary and one new composition. Conductor snapshots some static metadata; do not assume every dependency is late-bound, or that a parent snapshot freezes everything reached later. Inspect actual behavior. Supports E01/M05/M07. [Pinned metadata resolution](https://github.com/conductor-oss/conductor/blob/v3.32.4/core/src/main/java/com/netflix/conductor/core/metadata/MetadataMapperService.java).

### S4 — Prove remote recovery and acceptance across transaction gaps

**Question:** can the harness's Adapter and product records safely connect remote task state, acceptance decisions and engine transitions?

**Plan:** reuse the reference factory with deterministic fixtures for a pinned A2A version/binding. Persist intent before dispatch. Drop a response after remote acceptance, including before recording its task ID. Duplicate and reorder observations; restart during input-required and authorization-required waits; deliver a stale attempt after its replacement. Exercise both a cooperative deduplicating receiver and a receiver without discovery/deduplication. Crash between product-decision commit, engine update and acknowledgement. Reject r1, produce r2, and race stale/duplicate approvals and conflicting Director commands. Lose the final delivery receipt separately.

**Pass:** cooperative services yield one logical effect; unsupported guarantees yield an explicit unknown outcome and no unsafe automatic resubmission. One current attempt and exact eligible artifact revision can advance acceptance. Accepted artifacts survive delivery recovery. Durable command identity and replay/reconciliation recover partial commits without a second scheduler. Transient projection lag is allowed; permanent contradictory authority or double effects are not. All exposed task/control Interfaces enforce actor scope; raw engine administration is outside ordinary agent authority. This proves application authorization under the stated local trust assumption, not containment of hostile same-user code.

**Decision/stop:** a small durable intent/outbox/inbox and reconciliation implementation is plausible common product work, not a Conductor-specific defect. Sharing a PostgreSQL server does not make HTTP engine updates and product transactions atomic. If the design needs competing authoritative run state or a second dispatch engine, redesign it and reassess total ownership. A remote service without safe redispatch is a service-contract limitation; changing engines does not fix it.

**Scope:** selected failure windows with exact call counts, not every protocol transport. Verify artifacts are durable, readable and digest-bound before acceptance; the full storage/security qualification follows under Q1/Q3. Covers selected E03–E09 cases.

### S5 — Prove bounded authority, maintenance and improvement

**Question:** can the same harness functionality support our governance and Engineering model without privileged agent shortcuts?

**Plan:** contend for the final synthetic budget reservation across two branches and a nested factory. Duplicate/delay usage and cancel work whose remote outcome is unknown. Have two observers claim one incident, replace its owner, apply one approved remedy and exhaust its retry limit. With autoresearch disabled, submit one trace-derived improvement, independently evaluate it, and publish v2 through S3. Race promotions and attempt author self-approval, evaluator modification and use of unauthorized credentials. Plan one external-watcher restart/escalation check for an unavailable engine.

**Pass:** spend is attributed once at the agreed charge owner; unresolved remote exposure is retained rather than released just because a local lease expires. One current recovery owner acts within bounds and closes an incident only on observed recovery. Quality controls acceptance independently of the candidate author. Promotion checks the expected current version, preserves active bindings and can roll back future admissions. Disabled autoresearch leaves maintenance and ordinary improvement functional. The watcher does not schedule factory tasks.

**Decision/stop:** enforceable ceilings require cooperating services or defensible upper bounds. Fixture success cannot establish a strict cap at an arbitrary provider. If a supported Director command permits self-approval or protected evaluator changes, fix the application authority boundary before claiming governance. Arbitrary shell access under a trusted local account is outside this proof; do not expand it into sandbox work. If ordinary remediation requires continuous free-form replanning, the claimed small recovery Module needs reassessment.

**Scope and staging:** the engine-choice portion is limited to three small proofs through the existing reference slice: reject self-approval/evaluator mutation; fence one stale recovery owner; and admit one independently approved new version with research disabled. Give each an explicit effort/stop budget before execution. The budget-collision, full remediation, watcher, promotion-race and rollout/rollback extensions become Q2/Q3 implementation qualification unless an earlier result makes them decisive for engine choice. Reuse synthetic decisions; no autonomous campaign or statistical benchmark. Supports selected E10/M01–M08 obligations, without claiming full scenario passes or requiring the whole platform before choosing an engine.

### S6 — Give Kestra a fair chance to overturn the preference

**Question:** among candidates that meet self-contained deployment, does Conductor actually reduce total owned work compared with the closest viable alternative?

**Plan:** Kestra is the proposed countertrial only if it also clears S0. Otherwise choose a candidate with a credible accepted local deployment and update this plan together first. Preserve the harness's public factory contract, fixtures and acceptance policy. Express the reference factory in that runtime's native definition and assess parallel remote waits, one restart, nested invocation, v1/v2 publication and the failed-state diagnosis path. Credit a reusable Adapter equally. Test only equivalent slices already understood on Conductor; do not repeat the full audit of every engine.

**Authoring comparison:** proposed small sample of six matched tasks: create, add a parallel branch, add bounded repair, nest a factory, correct a mapping error, and reject a forbidden bypass. Use the same author model, tool budget, documentation access and three independent attempts per task/engine. Record semantic success, unsafe acceptance, repair turns, token/tool cost and human intervention. This is a directional comparison, not a benchmark proving that a DSL is universally best for agents. Paid model use and the actual budget remain to be agreed.

**Pass/decision:** both must satisfy the selected invariants; rank implementation burden and operation afterward. Choose Kestra if it provides comparable correct behavior with materially simpler integration or more usable authoring/diagnosis. Prefer Conductor if its task model clearly reduces that burden and S1's concerns are resolved. Record durable state, recovery paths, upgrades, custom plugins and deployment units, not just lines of code. A small inconclusive authoring sample does not overrule correctness evidence.

**Scope (updated 23 September):** the owner accepted automatic rollout of bundled orchestration workers when active runs retain their exact versions and external A2A agent services remain independent. Zigflow's refresh behavior is therefore an operating cost to measure, not a hard gate. Reassess its automatic rollout and version-retention path alongside the bounded product-owned interpreter; do not rerun the already established bare-file refresh failure. Dagu's integrated processless bridge and Effect/Kestra graph parity are also being tested. No excluded product distribution is silently reinstated.

## Complete planning assumption register

This register covers the material assumptions visible in the current architecture. **Accepted** means a product requirement, not verified implementation. **Documented** means source/documentation support, not local runtime proof. **Hypothesis** requires experimental evidence. **Open** requires a product or deployment choice. A spike cannot choose a product policy merely by producing a successful demo.

### Conductor and the proposed runtime

| ID | Assumption / status | Evidence needed or consequence | Plan |
| --- | --- | --- | --- |
| C01 | **Documented restriction:** Apache core includes a server dependency declaring Orkes Community licensing. | Clear/remove the actual restricted queue dependency and transitive paths; a permitted PostgreSQL-only build remains unproved. | S0 |
| C02 | **Hypothesis:** our required production facilities need no vendor key or network dependency. | Run the pinned artifact in the proposed production mode; identify every external enforcement alternative. | S0 |
| C03 | **Documented, untested here:** PostgreSQL can provide storage, queues, indexing and locks. | Verify providers and whether operating/bundling this topology fits the self-contained harness requirement. | S0/S1/Q3 |
| C04 | **Hypothesis:** concurrent engine operations converge under lock expiry, timeout and cancellation. | Source concerns require reachable, reproducible contention cases; do not infer safety from one happy-path run. | S1 |
| C05 | **Hypothesis:** engine work survives restart without silent loss or stuck progress. | Inspect tasks, queue state and recovered transitions; distinguish redelivery from duplicated logical effects. | S1/S4 |
| C06 | **Documented:** native documents can compose installed task implementations. | Demonstrate new factory publication without harness or worker deployment. | S3 |
| C07 | **Hypothesis:** publication policy can freeze all required dependencies. | Include task definitions, child bodies, dynamic choices, service guarantees and policy revisions. Version labels alone are inadequate. | S3 |
| C08 | **Hypothesis:** external task completion is a good fit for asynchronous assignments. | Measure timeout/heartbeat/polling behavior and Adapter state, including long input waits. | S2/S4 |
| C09 | **Accepted:** native A2A completeness is unnecessary. | Verify the actual reusable Adapter's protocol subset; do not credit fixtures with general interoperability. | S4/Q1 |
| C10 | **Hypothesis:** private engine access plus scoped product controls replaces required paid governance. | Cover publisher, worker, task update, restart/rerun, artifact, secret and administrative paths; login alone is insufficient. | S2/S4 |
| C11 | **Hypothesis:** native graph constructs represent our join/repair/nesting semantics. | Declare failed/skipped/canceled/late branches, stable dynamic item identities and bounded loops. | S3/S4 |
| C12 | **Hypothesis:** diagnosis and history retention are adequate without proprietary tooling. | Trace a run from harness command through engine task, remote task, artifact and acceptance; qualify retention/export later. | S6/Q3 |
| C13 | **Open:** production volume, latency, recovery objectives and maximum wait duration. | Ten-run fixtures are not production capacity targets. Establish workload before performance claims. | Q3 |
| C14 | **Unverified:** a different lock provider is a permitted practical fallback. | Confirm its implementation in the pinned artifact; ZooKeeper is not yet an established drop-in remedy. | S0/S1 |
| C15 | **Hypothesis:** engine upgrades and metadata evolution preserve old runs. | Test retained dependencies and migrations on the selected production version before deployment. | Q3 |

### Harness, service and execution model

| ID | Assumption / status | Evidence needed or consequence | Plan |
| --- | --- | --- | --- |
| H01 | **Accepted:** one customized Strands harness supplies factory functionality; its agent is Director. | Same harness package hosts ordinary capabilities and factory services; enablement does not grant unlimited authority. | S2 |
| H02 | **Accepted:** one install/start command; automatically managed bundled local helpers are allowed; no separately provisioned external orchestration service. | Requalify packaging, footprint and lifecycle; do not assume an engine/database per agent. | S0/S2 |
| H03 | **Accepted requirement; implementation unproved:** each instance owns a stable identity and durable state that outlive a harness session/process. | Restart with the same identity, a new execution incarnation and retained pending work; keep another instance's state unchanged. Namespaced shared storage is allowed. Host-loss recovery needs a backup/replication policy. | S2/Q3 |
| H04 | **Hypothesis:** Director authority and local runtime ownership remain coherent across harness instances. | Fence stale commands, isolate contexts and test two independent launchers sharing helpers; one exit cannot destroy the other's work. | S0/S2/S4 |
| H05 | **Hypothesis:** Strands extension/A2A facilities support the necessary lifecycle. | Pin Python/TypeScript packages; verify structured results, durable tasks, interruptions and cancellation rather than relying on text wrappers. | S2/Q1 |
| H06 | **Proposed:** the Director handles policy decisions while the engine advances ordinary transitions. | No model call required for each graph edge; define wakeups, decision expiry and unavailable-Director behavior. | S2/S3 |
| H07 | **Accepted:** capability and factory catalogs remain logically distinct. | A factory service advertises an input/result contract and binds a factory version; a catalog is not another scheduler. | S2/S3 |
| H08 | **Accepted:** ordinary workers may use other harnesses and need only scoped assignment context. | A factory-enabled harness can call opaque capabilities; no hidden dependency on shared Strands internals. | S4/Q1 |
| H09 | **Hypothesis:** nested factories remain opaque capabilities. | Both native subworkflow and factory-over-A2A paths preserve results, deadlines, cancellation, attribution and identities. | S2/S4/S5 |
| H10 | **Initial trust assumption:** trusted local harness code and configured capabilities; sandboxing deferred. | Enforce application permissions and independent review through supported Interfaces. Do not claim hostile same-user code containment or hard resource isolation. No sandbox qualification unless a concrete blocker reopens it. | S2/S4; containment deferred |

### Product state, evidence, contracts and cost

| ID | Assumption / status | Evidence needed or consequence | Plan |
| --- | --- | --- | --- |
| A01 | **Accepted:** engine owns graph progress; remote services own remote tasks; product Modules own their domain decisions. | The execution ledger is a joined authoritative record, not a second independent scheduler/database for every engine fact. | S4 |
| A02 | **Hypothesis:** partial commits across those owners are recoverable. | Durable command identity, intent and replay/reconciliation; a shared DB host does not establish one atomic transaction. | S4 |
| A03 | **Accepted:** assignment, attempt, remote task and artifact revision are distinct identities. | Retain mappings, lineage and current authority; reconnection alone does not create a new attempt. | S4 |
| A04 | **Hypothesis:** participating services provide sufficient duplicate handling and outcome discovery. | Send Message idempotency is optional; unknown outcomes remain unknown when guarantees are absent. | S4/Q1 |
| A05 | **Accepted:** cancel requested is distinct from cancel observed. | Account for refused cancellation, late completion and already-performed effects; supersession cannot undo an effect. | S4 |
| A06 | **Hypothesis:** the approved native profile is expressive and enforceable. | Reject arbitrary unsafe tasks/expressions and review bypasses; dynamic constructs need runtime policy too. | S3/S4 |
| A07 | **Accepted:** acceptance binds an exact revision, reviewer, policy and attempt. | Historical r1 acceptance remains evidence; repairing to r2 changes current release eligibility and dependent gates. | S4 |
| A08 | **Hypothesis:** independently controlled Quality can enforce that acceptance. | Define identity/credential separation and prohibit author self-approval; different prompts do not prove independence. | S4/S5 |
| A09 | **Hypothesis:** durable artifacts remain accessible through recovery and cleanup. | Digest-bound content, access checks, retention and retrieval; expired URLs and temporary workspaces are not durable evidence. | S4/Q1/Q3 |
| A10 | **Hypothesis:** explicit contexts and briefs isolate work while remaining sufficient. | Two unrelated real assignments, forbidden artifact canaries and fresh/continued-context checks; context IDs are not authorization. | S2/Q1 |
| A11 | **Open:** what an immutable service revision promises about models, skills, fallbacks and environment. | Record public guarantees and actual execution provenance; pinning does not make model output deterministic. | S3/Q1 |
| A12 | **Accepted:** new versions and rollback govern future admissions. | Active bindings remain historical; emergency permission revocation can halt dispatch, and linked successors must reconcile unresolved actions. | S3/S5 |
| A13 | **Hypothesis:** atomic reservations bound admitted work across branches/factories. | One charge owner, no double counting, delayed usage and retained exposure for unknown remote work. Strict caps require enforceable remote cooperation. | S5/Q1 |
| A14 | **Open:** overlap, catch-up, deadlines, admission and active-work cancellation policies. | Define queue/merge/skip behavior; paused admissions do not automatically cancel work. | S3/S5/Q3 |
| A15 | **Hypothesis:** the custom surface remains small enough to justify this engine. | Inventory durable state, failure paths, deployments and change fan-out; reject a disguised second interpreter/scheduler. | All; compare S6 |

### Organization, improvement and product operation

| ID | Assumption / status | Evidence needed or consequence | Plan |
| --- | --- | --- | --- |
| O01 | **Accepted:** Director, Production, Quality and Engineering are responsibilities with supervisors and direct specialists. | No mandatory foreman layer or process per role. Harness integration does not remove required separation of authority. | S2/S5 |
| O02 | **Accepted:** maintenance and continuous process improvement are required. | Coverage runs alongside production; it must not make completed runs wait forever for an endless maintenance branch. | S5/Q2 |
| O03 | **Hypothesis:** bounded approved remedies cover enough ordinary incidents to be useful. | One current incident owner, legitimate-wait classification, breaker, observed restoration and explicit escalation. | S5/Q2 |
| O04 | **Hypothesis:** infrastructure recovery is available outside the failed engine/harness. | Prefer existing supervisor/health mechanisms; bounded actions, ownership and outage receipts; no duplicate scheduler. | S1/S5/Q3 |
| O05 | **Accepted:** Engineering proposes changes; independent Quality and publication policy control promotion. | Protect evaluator, results, permissions and expected incumbent; Director cannot raise its own protected limits. | S3/S5 |
| O06 | **Hypothesis:** evaluation detects improvements without metric gaming or hidden cost regression. | Held-out evidence, repeated comparison, complete cost/latency and explicit invalid/inconclusive results. | S5/Q2 |
| O07 | **Accepted:** autoresearch is optional within Engineering. | Disabled means no proactive trials; enabling requires protected evaluator, permitted edits, isolated resources and finite campaign limits. | Q2; R01–R05 if enabled |
| O08 | **Accepted:** initial deployment serves one organization; future customers must remain possible. | Explicit organization scope now; defer shared-engine customer isolation tests until that deployment is chosen. | S2/S4/Q3 |
| O09 | **Open:** first useful factory, permitted effects, reviewer independence and automation scope. | The research example is a fixture, not a confirmed first customer use case. Product decisions determine realistic tests. | Before real-service Q1 |
| O10 | **Open:** hosting/redistribution packaging, operating owner and acceptable custom-work budget. | Decide intended delivery and infrastructure ownership; inventory permitted artifacts; cap experiment effort before execution. | S0/S6/Q3 |

## What should follow selection, rather than delay all development

**Q1 — Real-service and harness validation.** Run the chosen slice against at least one real Strands-hosted capability and an independently implemented A2A peer. Verify the pinned wire contract, meaningful outputs, structured continuation, scoped artifacts, model/provider provenance, context isolation and actual cost limits. Authorize provider usage and agree budget first. A deterministic fixture cannot prove model quality, real interoperability or remote spend enforcement. No complete claim of factory service compatibility precedes this work.

**Q2 — Full Engineering and optional research qualification.** Implement and validate the remaining required M01–M08 cases, including successor-run recovery and protected evaluation with research disabled. Agree quality metrics before claiming improvement. Qualify R01–R05 only before enabling autoresearch. Do not remove required maintenance/improvement from the product merely because the first decision spike exercises only one case.

**Q3 — Production operation.** Define workload/SLOs; qualify realistic throughput, long waits/outages, storage-failure handling, backup restoration, queue/history/artifact retention, upgrades, credential rotation and full authorization coverage. Database failover is required only if the selected topology promises it. Establish loss and recovery objectives and prove them on the actual topology. Complete full E01–E10 and relevant M coverage before corresponding production claims. Multi-customer shared-engine qualification is conditional on adding that deployment model.

The first implementation should grow from the reusable harness extension, native reference definition and evidence fixture. Defer a catalog marketplace, universal engine wrapper, elaborate board, arbitrary code-generation pipeline and autonomous optimization system. Sandbox research, sandbox backends and whole-harness containment are also deferred until a concrete blocker; they are not automatic follow-up gates for engine choice or first development. Per-instance identity/state, application authorization and durable recovery remain required. The factory's shared Interface and invariants come first.

## Original parallel plan and executed narrowing

The owner approved the necessary first round and explicitly deferred S5, broad S6 and sandbox work. Parallel agents ran isolated S0, S2 and S3 work while S1/S4 used their own candidate instances and product fixtures. The [result report](spike-results-2026-09-22.md) is the authority for what actually ran; the steps below retain the original planning logic and broader future scope.

1. Agree spike scope, unanswered product choices, pinned stack, pass/fail criteria, timeboxes and any model-spend budget. **Done for the narrowed S0–S4 development probes only; production budgets and thresholds remain open.**
2. Establish a shared test contract: identity scheme, synthetic capability/result schemas, independent-review policy, evidence format and fault-controller conventions. Publish the fixture revision before dependent agents use it.
3. **Wave A:** one subagent owns S0 then the early S1 litmus; another prepares S2's harness contract and fixture in isolation. Its engine integration depends on S0. A third may prepare S3 definitions/policy cases against the agreed fixture. Source/schema work can proceed before the runtime baseline; runtime conclusions require it. Stop candidate-specific investment if it fails the self-contained deployment gate.
4. **Wave B:** after the baseline and harness contract stabilize, run S3 publication, S4 lifecycle/transactions and S5 bounded authority in parallel. Give each its own databases, queues, ports, run IDs and output directory; share immutable fixtures, not live state. S4/S5 must agree the acceptance/ownership Interface before implementation splits.
5. **Wave C:** the broad S6 countertrial is deferred. Kestra became the observed candidate after the Conductor stock package gate stopped its runtime work. An independent evidence audit checked provenance and limits; reproduce decisive failures independently before an engine selection.
6. Review results together. Record an engine decision only after required choice gates pass and custom ownership is acceptable. An inconclusive or blocked experiment does not count as success. Preserve rejected alternatives and the conditions that would reopen them.

The concurrency plan describes logical lanes, not a requirement for a fixed number of agents. Dependencies stay sequential even when workers are available. Agents must not silently modify the shared requirements to make their implementation pass.

Every result should record candidate/source/image/SDK/fixture digests, enabled providers, configuration, test hypothesis, actual concurrency and repetitions, injected failure point, expected/observed events, retained evidence and remaining uncertainty. Proposed initial fixtures remain ten concurrent factory runs with two branches, and twenty deliberately overlapping duplicate requests. These numbers are reproducibility settings, not an accepted production capacity target. Agree repetition counts and stop budgets before running contested cases.

## Open questions to settle together

1. **Packaging envelope:** bundled local helpers and the simple native baseline are settled. A Kestra/database pair per harness is rejected as the default. What initial platforms, total warm-idle/active memory, startup, disk and operating effort are acceptable? Recommendation: no manual database/server setup; measure one local runtime shared by agents in an installation, including all required engine, store, worker and Strands processes. Docker/sandbox qualification is outside the current plan. No numerical limit has been set; Java/JVM remains eligible if a shared installation fits.
2. **First factory:** identify the first useful outcome and allowed effects. Recommendation: use verified research as the repeatable fixture, then substitute the actual first customer workflow before Q1.
3. **Quality independence:** define the required separation. Recommendation: separately authorized reviewer identity/context with protected evaluation and no author self-approval; different model/provider only where the quality requirement warrants it.
4. **Director automation:** define actions allowed automatically and escalation limits. Recommendation: routine commands inside a fixed delegated scope; protected permission/evaluator/budget-policy changes require independent authority.
5. **Cost promise:** strict provider-enforced ceiling or bounded admission with explicit uncertain exposure? Recommendation: hard limits only for cooperating providers; otherwise expose uncertainty and stop unsafe new admissions.
6. **Concrete stack and operations:** identify an existing customized harness, if any; choose SDK/language/protocol from its actual support; decide packaged/self-hosted delivery versus operated hosting. Recommendation: one initial stack and one organization's deployment, without promising future shared-engine isolation prematurely.
7. **Evaluation budget:** agree per-spike effort limits, model-call allowance, workload envelope and decision deadline. Recommendation: fail fast on S0/S1/S2, reuse one fixture, and expand comparative work only when it could change the choice.

## Evidence and retained comparisons

- [Current architecture and feature matrix](exomachina.html)
- [Existing engine scenarios and selection rules](engine-evaluation.md)
- [Maintenance and research requirements](factory-maintenance.md)
- [Independent engine judgment](independent-engine-review.md) — preserved as the original report; the lock-provider caution above qualifies its proposed fallback.
- [Observed Temporal/Zigflow probes](temporal-authoring-evaluation.md)
- [Production and product-use restriction register](exomachina-production-audit.json)
- [A2A specification](https://a2a-protocol.org/latest/specification/) — protocol correlation does not establish our acceptance or external-effect guarantees.
- [Kestra Pause](https://kestra.io/plugins/core/flow/io.kestra.plugin.core.flow.pause) — one candidate building block to assess, not proof of our complete parallel-remote lifecycle.
