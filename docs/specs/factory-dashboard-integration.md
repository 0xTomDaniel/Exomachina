# Factory Dashboard Integration and Agent Commerce

2 October 2026 · Updated 4 October 2026 · Canonical specification for the next Exomachina prototype.

Status: the operator approved a smaller **basic working prototype** milestone on
3 October 2026. Its gate below governs current execution. The dashboard
architecture, prototype model, commercial direction, and complete operational
smoke inventory remain agreed later milestones. AP2's role and qualification scope are the researched recommendation
recorded in this specification. This document states requirements; it does not
claim they are implemented, authorize production spending, or route tracker work.

Terms follow the [project glossary](../../CONTEXT.md). This specification extends
the [architecture principles](../architecture-principles.md) and preserves the
existing [Engineering and maintenance authority](../factory-maintenance.md).

## Problem Statement

The factory floor currently shows illustrative graphs, activity, decisions,
capacity, and spend. The Temporal prototype separately demonstrates real agent
work. An operator cannot yet submit useful work, watch its actual execution on
the floor, intervene through the Director, inspect accepted artifacts, or trust
the displayed operating and commercial figures.

Exomachina also needs an enduring demonstration without maintaining a second
dashboard that diverges from the real product. The basic prototype connects the
existing floor to authoritative execution and qualifies its supported core path.
The later roadmap qualifies every advertised operational capability and makes
independently priced agent services usable as suppliers to a factory. Managing
the owner's own agents remains a supported use.

## Solution

Use one dashboard application with shared graph and event schemas, state
reduction, rendering, inspectors, views, and command presentation. Live and demo
Adapters vary its observations and command execution at one dashboard-facing
Seam. The live dashboard follows current execution continuously. Demo mode
supports recorded runs and isolated interactive scenarios.

Start with the existing Verified Research factory: two independent research
assignments run in parallel; their results join; a synthesizer creates a report;
independent Quality assesses an exact artifact revision; accepted work is
delivered, rejected work is repaired, and exhausted work reaches a bounded
Director decision. Complete the basic prototype gate before extending the
runtime and contracts for the floor's other advertised capabilities. Demo
scenarios may illustrate deferred capabilities with explicit synthetic labels;
unsupported live controls stay unavailable and mockup data is never live proof.

The prototype's owned inference uses **GPT-6 Luna (`gpt-6-luna`) at `xhigh`**.
This is a requirement for the product being built, including its authoring,
Director, research, synthesis, and Quality model paths. It is not a request to
change the operator's Codex conversation model. Subscription access is historical
prototype evidence, not the intended commercial foundation.

The principal commercial user is a factory owner purchasing work from
independently priced agent services. Each supplier owns its commercial contract;
the factory accounts for its purchases. Usage, production cost, and customer
price remain distinguishable, with per-agent and per-attempt breakdowns.

For the basic prototype, expose per-agent usage and its completeness. Cost
estimates are optional and require explicit rates and allocation rules; unknown
cost is not zero. Usage-based pricing, hosting allocation, explicit basis-point
markup, alternative pricing bases, and payment triggers remain the commercial
roadmap. MPP, x402, and AP2 qualification follows the basic prototype and later
commercial integration; it is not a gate for the basic milestone.

## User Stories

1. As a factory owner, I want to submit a useful brief through the factory's normal identity, so that the floor shows work I actually requested.
2. As an operator, I want the floor to display the graph pinned to each run, so that a later publication does not misrepresent work already underway.
3. As an operator, I want to see parallel assignments, joins, queues, and active attempts, so that I understand where work is progressing or waiting.
4. As an operator, I want to inspect real Task identities and intermediate artifacts, so that each visual transition has supporting evidence.
5. As an operator, I want Quality findings tied to an exact revision and digest, so that I understand why work was accepted or rejected.
6. As an operator, I want repair history to retain prior revisions and findings, so that corrections are traceable.
7. As a Director, I want to grant a bounded extra repair or abort within my delegated authority, so that exhausted work has an accountable decision.
8. As a human responder, I want to receive an escalation, recommendation, permitted actions, and deadline, so that I can resolve work needing my authority.
9. As an operator, I want stale, duplicate, unauthorized, and expired commands to have explicit outcomes, so that concurrent interaction cannot silently change the wrong work.
10. As a caller, I want to open the delivered report and its acceptance and delivery evidence, so that completion corresponds to a usable result.
11. As an operator, I want to reconnect or refresh without losing run state or duplicating displayed events, so that interruption does not destroy visibility.
12. As an operator, I want a disconnected or stale dashboard to say so, so that it does not look like a healthy idle factory.
13. As an operator, I want the live floor to follow current execution without playback controls while a job is live, so that it behaves as an operating dashboard; a finished job may be replayed (operator request, 7 Oct 2026).
14. As a demonstrator, I want recorded and interactive scenarios in the same application, so that demonstrations remain available without real inference or payments.
15. As a maintainer, I want demo and live Adapters checked against the same Interface, so that demonstration changes cannot conceal product incompatibilities.
16. As a factory owner, I want actual capacity limits and admission queues, so that simultaneous work respects available resources.
17. As a factory owner, I want to see shared-agent occupancy and nested factory work, so that contention across factories is understandable. (Agent services hold no capacity queue; see "Work-in-progress limits" below.)
18. As a factory owner, I want atomic spend reservations and declared cost ceilings, so that concurrent purchases respect my budget.
19. As an agent owner, I want inference usage and hosting cost attributable to each assignment and attempt, so that I understand the cost of my own agents.
20. As a factory owner, I want provider costs, markup, service charges, and customer price distinguished, so that I understand my purchasing economics.
21. As an independent provider, I want to publish my billable units, rates, disclosures, and payment terms, so that purchasers can evaluate my offer without access to my private implementation.
22. As a purchaser, I want unknown or estimated costs labeled honestly, so that an incomplete record does not appear to be a zero charge.
23. As a purchaser, I want repairs, failures, cancellations, and credits handled by the agreed commercial contract, so that billing does not depend on an informal interpretation after work runs.
24. As a purchaser, I want prices and cost rules pinned when I buy work, so that future rate changes cannot silently reprice an assignment.
25. As a purchaser, I want purchase authority and payment receipts linked to the work they concern, so that authorization and settlement are auditable.
26. As an agent service, I want MPP and x402 purchases supported through qualified Adapters, so that I can sell work across payment ecosystems.
27. As a factory owner, I want bounded delegated purchase authority, including an AP2 mapping where appropriate, so that an agent cannot spend beyond its principal's permission.
28. As an operator, I want payment retries and uncertain settlement reconciled, so that an interrupted request does not produce a second charge or erase an outstanding obligation.
29. As Engineering, I want incidents, recovery, and candidate promotion visible with their actual evidence, so that program indicators describe real responsibilities.
30. As a maintainer, I want every original operational capability mapped to a real smoke case, so that visually convincing scenarios cannot substitute for runtime qualification.
31. As a demonstrator, I want scenario timing, artifact kinds, Quality verdicts, Director activity, and declared illustrative operating figures preserved, so that adopting the shared contract does not hollow out the existing demo.
32. As an operator, I want assignment and nested-factory stations to retain their distinct appearance and labels, so that the floor remains understandable across sources.
33. As an operator, I want declared, observed, illustrative, and unknown information distinguished, so that an attractive visualization does not overstate evidence or authority.
34. As an operator, I want to type a factory-scoped brief even when submission is unavailable, so that I can prepare work without bypassing readiness checks.
35. As an operator, I want an always-visible human inbox and an independently accessible incident view, so that I can inspect problems without relying on a healthy Director.
36. As a demonstrator, I want the default Recorded selection to offer a usable recording or an explicit evidence-only view, so that replay does not depend on a verifier injecting a different bundle.

## Implementation Decisions

### Architecture Intent and authority

Architecture pass: **needs-architecture-intent**. Correctness depends on preserving
execution authority, exposing a shared observation Interface, and keeping
commercial commitments separate from payment settlement and artifact acceptance.
The intent below is the authoritative design for this scope; no separate ADR is
required for this prototype specification. A later irreversible payment trust or
custody choice requires its own decision before production use.

- Keep Temporal as the durable execution Implementation and the customized
  Strands harness as the factory service's normal A2A identity. Callers submit
  briefs; they do not choose interpreter builds, graphs, bindings, or versions.
- The Factory Module and existing execution ledger own execution transitions.
  Quality owns required acceptance. Providers own their internal model/harness
  choices subject to promised contracts. Commercial enforcement owns spending
  checks; possession of a wallet or a model-generated instruction grants none.
- A Factory Observation Module projects authoritative graph, execution,
  commercial, and command-outcome records. It may maintain a rebuildable read
  projection with cursors; it must not become a second scheduler or invent a
  parallel execution ledger.
- Keep purchase accounting within a Commercial Module. Its Interface
  concentrates offer binding, metering attribution, reservations, accrued
  obligations, credits, and settlement reconciliation. Payment Adapters vary at
  the payment Seam; they do not define factory acceptance.
- Prefer existing Task, Director, acceptance, outcome-journal, publication, and
  recovery Interfaces. Add the smallest observation Interface needed by both
  actual dashboard Adapters. This gives Depth through a coherent view of a run,
  Leverage across modes and tests, and Locality for projection rules.

### One dashboard and two modes

- Live and demo use the same application, graph/event schema versions, reducer,
  renderer, views, inspection panels, and command presentation. Do not fork a
  second HTML application or maintain independent UI behavior implementations.
- The live Adapter uses actual observations and existing authorized commands.
  Graph topology, provider bindings, contracts, version identifiers, and allowed
  actions come from runtime records. Render ongoing attempts as ongoing; do not
  invent a completion time to fit the mockup's duration-based renderer.
- Demo sources are recorded execution bundles and an isolated deterministic
  simulator for interactive branches. Migrate the current mocked scenarios to
  the shared contract. Preserve them as clearly illustrative until replaced by
  real smoke recordings; synthetic simulation never gains a live evidence label.
- Demo can autoplay and optionally expose play, pause, speed, restart, and seek.
  **While the displayed Live job (or any job in the displayed Live scope) is
  live, Live mode has no play/pause button, replay-speed selector, timeline
  scrubber, jump-to-live control, or keyboard playback shortcuts.** Activity
  history is still inspectable without changing the displayed current execution
  state. Once the displayed Live scope has no live job, its observed facts may be
  replayed with the same transport; new facts return the view to the present
  unless the operator is replaying (operator request, 7 Oct 2026).
- Demo controls affect presentation or isolated scenario state. They cannot
  dispatch live assignments, use live credentials, invoke payments, or mutate
  real factories. Select and bind the Adapter before permitting commands.
- Live mode shows connection health and observation freshness. Losing the
  connection must not look like zero activity. Reconnection preserves the
  selected factory/job where those identities remain available.
- Keep the Factorio-style floor, board, job following, inspectors, and calm mode.
  Live/Recorded floor flow follows the A2A model in "Live A2A floor flow"
  below: Tasks occupy stations for their observed start-to-end, artifacts ride
  belts between a producing completion and the next observed start, and every
  item states whether its hand-off is evidenced or inferred.
- Complete Decisions, Outputs, Definition, and Agents navigation as useful views
  of actual records. A placeholder click notification is not integration proof.

### Shared dashboard reconciliation and fidelity

Operator-approved reconciliation direction, 4 October 2026. Keep distinct Live,
Recorded, and Demo Adapters feeding one versioned dashboard Interface, reducer,
and renderer. A shared reducer is not a source of execution truth, a simulator,
or a substitute for missing runtime behavior. Restore fidelity at the owning
Module rather than branching into independent mode-specific dashboards.

Use these presentation provenance categories; they are not new execution states:

- **Declared:** published graph, bindings, capabilities, and explicit operating
  policy, retaining their run/version scope. Declaration does not prove execution.
- **Observed:** recorded actual activity, measurements, or outcomes, retaining
  identity, time, completeness, and source. Observation does not grant authority.
- **Illustrative:** isolated Demo scenario facts, including simulated prices,
  budgets, occupancy, program indicators, faults, and communications. Preserve
  their usefulness without promoting them to runtime or provider evidence.
- **Unknown:** absent or incomplete evidence. Keep unknown distinct from zero,
  empty, not applicable, and explicitly unavailable. Presentation-only topology
  annotations remain separately labeled; they do not acquire execution meaning.

Extend versioned allowlisted schemas when legitimate source information cannot
cross the shared Interface. Do not bypass validation, silently discard required
scenario facts, expose private histories, or encode provenance only in a page-wide
badge when mixed facts need individual scope/completeness. Recorded facts retain
captured provenance and freshness; playback is not fresh Live execution.

| Surface | Reconciliation at the shared Seam | Responsibility outside reduction |
| --- | --- | --- |
| Station kinds and grouping | Preserve source graph kinds and derive consistent presentation kinds, boundaries, and groups | Renderer recognizes assignment and nested-factory kinds, icons, labels, instruments, and dimensions; pins remain unchanged |
| Scenario timing | Preserve declared start time, event ordering, durations, and an explicit clock basis | Demo Adapter supplies scenario timing even when a fixture has no job-created event; missing Live start time stays unknown |
| Artifacts and motion | Preserve artifact kind/revision/digest, spatial attribution, transfer and Quality facts where provided | Renderer draws common shapes, belts, verdicts, and releases; Live/Recorded movement follows the A2A floor flow (Tasks at stations from observed assignment/run-node facts, artifacts on belts with observed dwell, evidenced versus inferred hand-offs labelled), while Demo uses explicitly illustrative scenario events |
| Communication and inbox | Project declared routes, observed waits/incidents, original bindings, and permitted actions | Renderer supplies accessible wires/inbox navigation; runtime policy owns routing and actions, and a navigation link does not establish escalation |
| Capacity, economics, programs | Preserve declared or illustrative metadata and observed measurements with their provenance | Actual capacity enforcement (the factory's own WIP limit), commercial accounting, and Engineering behavior belong to their existing runtime Modules, not a Demo price or reducer calculation |
| Recorded graph | Render the authentic graph captured and pinned to that recording | Recording production must retain the graph; if absent, offer an honest evidence-only view or a complete provenance-backed default bundle, never another run's substitute graph |
| Draft interaction | Maintain isolated draft context and use one shared submission path | UI owns typing, focus, pointer interaction, keyboard access, and retention; an editable draft does not authorize Submit |
| Reconnect and readiness | Reduce transport/freshness outcomes and expose actual disabled reasons | Transport and runtime restore synchronization, authenticate callers, and enforce submission readiness; UI cannot declare a stale source fresh |
| Interactive Demo commands | Present commands and outcomes through the same contract and views | The isolated Demo Adapter owns bounded scenario transitions; reducer does not execute commands or emulate external-provider contracts |

The reducer must not infer acceptance or delivery from completion, a pending
human decision from an arbitrary error, direct-to-human authority for every agent,
zero usage/cost from absent values, a traversed belt from a graph edge alone
(a Live hand-off needs an observed producing completion; one that only follows
pinned graph order is labelled inferred), or Live
availability from Demo support. An agent may report a problem without choosing
its escalation route or authorizing a human action. Agent reports and runtime
faults need policy-bound handling; the incident view remains accessible without a
healthy Director. Do not fabricate a universal incident-to-Director relationship
when the current public contract has none.

Preserve the original illustrative factory profiles, including their artifact
registries and profile-specific main result kind, agent contracts, factory WIP
limits and factory queue occupancy (agents have no capacity of their own:
A2A decision 7, 8 Oct 2026), illustrative agent prices as quoted through the A2A
payment extension, program indicators, and simulated economic displays. These are Demo
fidelity requirements, not authorization to implement or qualify deferred Live
commerce, programs, or human workflows. Where a Demo command is unsupported,
state that limitation rather than showing a fabricated successful outcome.

### Observation Interface and protocols

The shared dashboard Interface exposes discovery of accessible factories/runs,
a pinned graph plus current snapshot, resumable observations, artifact inspection,
and submission of permitted commands with eventual outcomes. Artifact bytes may
be retrieved through authenticated HTTP; they need not be duplicated in every
stream event.

| Layer | Decision | Responsibility |
| --- | --- | --- |
| Agent assignments | A2A v1.0 only | Task lifecycle and artifact exchange through the mediating factory; no 0.3 interface ([decision, 7 Oct 2026](../a2a-v1-mediation-decision-2026-10-07.md)) |
| Dashboard transport | WebSockets | Persistent bidirectional observation and command transport |
| Observation envelope | CloudEvents | Event identity, source, type, time, subject, and schema reference |
| Factory event data | Versioned Exomachina schemas | Graph binding, assignments, revisions, decisions, operating state, and commercial facts |
| Interface documentation | AsyncAPI with payload schemas | WebSocket operations, message shapes, authorization, ordering, errors, and resumption |
| Director conversation | Existing authorized Director Interface | Model/tool interactions; an AG-UI Adapter can be evaluated later |

AG-UI has lifecycle, state, activity, subagent, and custom events and can use
WebSockets. The assessment is that the floor still needs its own factory and
commercial semantics, so AG-UI is not adopted as the floor's canonical protocol.

- Events identify factory, run, assignment, attempt, remote Task, artifact
  revision, and commercial record as applicable. Bind observations to the run's
  definition/manifest and provider contracts. Repeated attempts at one node need
  distinct identities. Factory-wide changes can have a factory subject without
  a run subject.
- Event identity is stable across retransmission. A factory observation stream
  has a durable ordering cursor; event time alone is not ordering. A run-filtered
  subscription carries an explicit continuation cursor so omitted unrelated
  events are not mistaken for missing events.
- Send a snapshot at an identified cursor and all subsequent relevant events
  without a race between snapshot capture and subscription. Reconnect from the
  last applied cursor, deduplicate stable event identities, and detect genuine
  gaps. If retention has removed the requested range, require a new snapshot.
- Include graph publication and discovery changes, admission/queue events,
  assignment start/progress/end, artifact lineage, Quality findings/verdicts,
  waits/decisions, delivery, incidents, capacity, usage, reservations, charges,
  and payment reconciliation. Started and completed work derive from durable
  facts, not an inference that a poll probably missed a short-lived state.
- Observation delivery can repeat events. Consumer reduction must remain
  idempotent. Slow consumers use bounded buffering with explicit resynchronization
  or disconnection; never silently drop authoritative transitions.
- Command messages carry a stable command identity and expected state/version.
  Separate received, validated or rejected, and applied or failed outcomes.
  Acknowledgement is not proof of application. Never optimistically paint an
  applied decision before authoritative confirmation.
- Mutations enforce actor, original Task/context, current revision/digest, and
  permitted action at the server. Concurrent commands from tabs or operators
  cannot independently resolve the same wait. Repeated identical commands return
  the original outcome; reuse with different intent is rejected.
- Emit only allowlisted observation fields. Do not expose raw Temporal payloads,
  reusable Director credentials, model credentials, payment signing material, or
  sensitive mandate disclosures. The historical Director-token-in-history gap
  requires correction before credential-bearing payment qualification; a
  sanitized dashboard projection alone does not correct that storage issue.

### Prototype model and real work

- Default every owned model-bearing prototype path to `gpt-6-luna` with `xhigh`:
  graph authoring, Director, research, synthesis, Quality, and any owned agents
  added for the qualified dashboard scenarios. Record actual model and reasoning
  configuration in safe evidence. Reject or surface unsupported settings instead
  of silently selecting a different model or effort.
- External black-box suppliers retain model choice unless their commercial or
  capability contract promises otherwise. Their internal model can be
  undisclosed; the factory may not infer it from a provider name.
- Qualify live inference and provider-returned usage against the actual selected
  access method. The inference purchasing/access method is still an environment
  decision. Existing subscription evidence cannot demonstrate API-billed dollars
  or MPP/x402 inference purchasing. Do not silently select subscription access as
  the target commercial default.
- The initial useful output is a report answering a genuine brief from the
  factory's permitted evidence/tools, delivered as accessible Markdown. Do not
  claim live web research if the run used only a pinned evidence packet.
- Report fixtures, induced defects, model decisions, and deterministic platform
  actions separately. Tests may deliberately cause a defect, delay, failure, or
  lost response; the resulting agent work and routing must actually run.

### Per-agent economics and commercial contracts

There are three distinct views: resource usage, production cost to the purchaser,
and customer price charged by a factory service. Per-agent details refine these
views rather than collapsing them into a single spend number.

| Record | Required meaning |
| --- | --- |
| Usage record | Resource counts and measurement source/completeness per assignment and attempt |
| Inference cost | Attributed model cost, rate schedule, currency, and evidence status |
| Hosting cost | Attributed infrastructure cost and declared metering/allocation rule |
| Markup | Commercial addition and the exact base to which it applies |
| Service charge | Supplier obligation under the pinned commercial contract |
| Spend reservation | Authorized amount held for pending/in-flight or unresolved work |
| Payment state | Separate authorized, accrued, settlement-pending, settled, credited/refunded, failed, or unresolved facts |
| Customer price | The factory's sale to its caller; independently contracted from supplier purchases |

- For owned agents, collect available input, output, cache, and other separately
  billable model usage; model calls; tools with costs; infrastructure units; and
  actual elapsed times. Attribute authoring, Director, Quality, failed attempts,
  and repairs as well as successful Production work. Shared preparation costs
  need a declared allocation or a separate overhead record.
- A usage count multiplied by a public model rate is a calculated cost basis,
  not evidence of an upstream invoice. Preserve whether amounts are measured or
  calculated from measured usage, provider-reported, estimated, or undisclosed;
  retain invoice/receipt references where available. Never turn missing usage
  into measured zero. Subscription allocations remain labeled estimates if used
  for historical comparisons.
- For external providers, the commercial contract defines disclosure. A fixed
  or otherwise opaque service charge does not require exposing private model
  costs. A cost-plus offer must disclose sufficient usage and rate/allocation
  evidence to reproduce the promised charge; unverifiable components remain
  explicitly provider-reported or estimated.
- The initial cost-plus profile charges inference plus hosting, multiplied by
  one plus markup basis points divided by 10,000. Thus 500 basis points adds 5%.
  This is an illustrative markup, not a chosen default. Separately disclose
  payment-network, tool, or platform fees and whether any receive markup.
- Hosting allocation states units, rates, minimums, and shared-cost treatment.
  Queue waiting is not active hosting consumption unless the contract explicitly
  sells reserved capacity. Hosting allocations must be labeled as allocations.
- Pin offer identity/version, cost/rate basis, billable units, markup, currency
  and asset/network where applicable, precision/rounding, maximum authorized
  charge, disclosure guarantees, expiry, and failure/repair/cancellation/credit
  rules before committing work. Rate changes affect future purchases.
- Use exact monetary arithmetic and declared atomic-unit conversions. A dollar
  display does not imply that every settlement asset has the same precision or
  market value. Currency conversion requires its own timestamped basis.
- Represent price basis separately from payment trigger. Price bases include
  metered usage, fixed assignment, fixed attempt, and accepted outcome. Triggers
  include upfront/admission, incremental use, attempt completion, and acceptance.
  Providers advertise only combinations actually implemented and qualified.
- Usage-based charging is the first implemented profile. The proposed initial
  policy bills authorized consumed usage on rejected attempts and cancellation,
  unless the pinned contract supplies included repairs or credits. This policy
  must be explicit in the offer before a live paid purchase; it is not permission
  to retrospectively charge failed work. Provider-caused faults and repair costs
  need declared treatment before payments are enabled.
- Show supplier charges, actual payment fees, and owner overhead as distinct
  contributions to factory production cost. Keep factory customer revenue and
  margin separate. Supplier-internal inference/hosting breakdowns explain an
  existing service charge; do not add them to that charge a second time. The same
  rule applies recursively to nested factories.
- Reserve budget atomically before admission and additional billable work.
  Concurrent reservations share the same limits across runs, processes, and
  payment Adapters. Reconcile reserved, accrued, settled, released, and credited
  amounts across restart and late usage reports.
- A hard budget guarantee requires an enforceable maximum or bounded cooperative
  metering. Unknown/unbounded provider cost cannot be presented as strictly
  capped. Additional authorization is required before a declared ceiling grows;
  an extra repair does not automatically increase spending authority.

### MPP, x402, and AP2

The commercial contract defines what is sold and charged. Payment protocols
execute authorized payment flows. Purchase authorization constrains who can buy
what. None establishes artifact quality or delivery by itself.

| Protocol | Intended role | Qualification scope |
| --- | --- | --- |
| MPP | Payment challenges, credentials, receipts, and metered sessions | Qualify one bounded session method/network and charge flow before making corresponding support claims |
| x402 | Payment challenges and settlement, including usage bounded by `upto` | Qualify selected schemes, facilitator/network support, limits, and recovery independently |
| AP2 | Verifiable purchase/payment authorization and evidence | Evaluate and qualify the mandate mapping at the authorization Seam; do not claim automatic nested delegation |

MPP sessions support repeated or streamed metering. x402 `upto` authorizes a
maximum and settles an actual amount for a request. Those are promising fits,
not interchangeable implementations. The documented MPP/x402 compatibility
guide covers specific `exact` flows; it does not establish compatibility for
all usage schemes. Pin protocol, SDK, scheme, method, and network versions for
each qualified profile.

AP2 is relevant because an agent needs verifiable purchase authority. Current
v0.2 uses Checkout and Payment Mandates; older sample/SDK vocabulary must not be
silently mixed with that version. Payment Mandate constraints include payees,
instruments, amount, budget, and execution timing. Treat these as externally
verifiable authorization evidence, with Exomachina enforcing its own scope and
concurrent spend accounting. The mapping is a researched recommendation to
qualify, not an assertion of present AP2 support.

The AP2 specification explicitly leaves agent-to-agent mandate delegation outside
its current scope. A nested factory therefore needs explicit local purchase
authority or a separately qualified delegation mechanism. AP2 evidence cannot
be claimed to supply automatic transitive authority. How mandate money precision
maps to sub-cent payment units also requires explicit qualification.

- Record a purchase's principal, service identity/payee, assignment/run scope,
  offer/contract digest, currency, ceiling, expiry, and authorized payment method.
  Preserve only necessary safe references to mandates and payment evidence in
  ordinary observations. Sensitive signing and presentation material stays out
  of the model context and dashboard stream.
- An AP2 Adapter, when qualified, verifies signatures, applicable trust,
  constraints, and purchase/payment binding before local authorization permits
  spending. Local revocation and ownership checks remain local enforcement; do
  not imply AP2 standardizes features beyond the pinned specification.
- Preserve logical purchase, usage, and payment identities through transport
  retries and restart. A payment retry is not a new assignment, and an extra
  repair is not a replay of already billed usage. Never infer unpaid state solely
  from a lost HTTP/WebSocket response.
- Reconcile uncertain settlement before authorizing a replacement payment for
  the same obligation. Keep reservations for unresolved liabilities. Once funds
  have settled, a local cancellation requires explicit refund/credit handling.
- x402's optional payment-identifier extension is useful, but its documented
  deduplication lifetime and server support do not establish durable factory-wide
  no-double-charge behavior. Local purchase accounting and participating provider
  guarantees must cover the promised concurrency and recovery window.
- Support account-limited test-network qualification first. Choose networks,
  assets, facilitators/relays, wallet custody, signature storage, funding ceilings,
  and provider endpoints explicitly before execution. This specification does
  not authorize wallet funding or real-money transfers.
- Outcome-based settlement is a later qualified profile. It binds payment to a
  specific accepted revision and independent Quality evidence, while also
  specifying delivery, cancellation, refund, and dispute rules. An HTTP payment
  receipt alone is not escrow or proof of an accepted outcome.

### Delivery sequence and milestone gates

**Current priority: finish the basic working prototype.** This operator-approved
milestone supersedes the earlier requirement to finish all operational scenarios
before delivering a usable prototype. The full inventory remains the later
qualification roadmap; unfinished rows do not block the basic gate or become
qualified merely because they are deferred.

Architecture pass for this scope refinement: **simple/localized**. Reuse the
existing Factory Module, Factory Observation Module, A2A/Director Interfaces,
and shared dashboard Seam with live/demo Adapters. Module ownership, protected
Quality authority, and the existing Architecture Intent remain unchanged. No
new Interface, second dashboard, or ADR is required by this milestone change.

#### Basic working prototype scope and completion gate

| ID | Required observable result | Relationship to the full inventory |
| --- | --- | --- |
| B01 | Submit a brief through the dashboard and normal factory identity; execute real `gpt-6-luna` at `xhigh`; retain original Task/context and pinned definition | Core S01–S03 |
| B02 | Display the actual graph, assignments, progress, Quality verdicts, and truthful terminal state in the existing views; preserve unknown historical facts without invented outcomes or timestamps | Core S02/S30/S32; fix the known Board terminal-run inconsistency |
| B03 | Complete two real dashboard-submitted workflows: first-pass acceptance, and live Quality rejection followed by bounded real repair and acceptance of a new exact revision | S04/S05; induced input defects are labeled, while Quality and repair use real inference |
| B04 | Download the accepted report locally; verify its relation to the accepted artifact digest and retain one distinct local delivery receipt | Core S22; local delivery suffices, remote delivery is deferred |
| B05 | Show provider-reported per-agent usage for the exercised owned model paths, including repair and Quality; identify unavailable categories or bindings explicitly; preserve historical gaps and unknown costs | Reduced S11; full legacy/backfill and complete commercial cost qualification are deferred |
| B06 | One local operator can perform one supported action, such as abort on its valid original Task; reject invalid or unauthorized actions clearly | Reduced S06/S10; runtime command proof may be deterministic and does not require a third paid workflow |
| B07 | Refresh and disconnect/reconnect one local dashboard during execution, then reconstruct matching current state without duplicate displayed items; show disconnected/stale state honestly | Reduced S28/S32; retention-eviction and multi-viewer stress qualification are deferred |
| B08 | Run live, recorded, and isolated interactive demo modes in the same application with shared schemas/reducer/renderers; demo commands cannot invoke live inference or payments; no playback controls while a live job is displayed (finished Live jobs may be replayed, 7 Oct 2026) | Core S31/S32; exhaustive parity of all illustrative scenarios is deferred |
| B09 | Enforce one active job on the local factory instance; clearly reject a second simultaneous submission; check/reject through the authoritative admission path rather than a dashboard-only button state | Reduced S15; capacity queues and cross-factory contention are deferred |
| B10 | Export allowlisted observations and usage only; keep credentials/raw private histories out of browser payloads and verify a synthetic leak positive control | Core S33 |

The gate is all B01–B10, demonstrated through the existing dashboard and public
runtime Interfaces. The bounded live-model scope is the two workflows in B03;
use existing approved inference access, explicit finite call/repair bounds, and
value-blind readiness checks. A retained recording alone does not establish
fresh dashboard submission or repair. Existing valid proof may be reused for
unchanged surfaces. New accounts, unrestricted inference retries, wallet funding,
and real-money transfers are not authorized by this milestone.

Use one pinned definition and update it between runs. Interrupted or failed work
must be reported honestly; seamless in-flight recovery is a later gate. Dollar
estimates may be shown only when explicit rates are supplied, with their source
and estimation status; tariff selection, hosting allocation, and payment-network
choices must not block usage visibility or the basic execution path.

Defer extra repair grants, human escalation and approval/send-back, concurrent
operator races, full commercial reservations/credits, capacity queues,
shared-agent contention, versions concurrently in flight, nested supplier fan-out
and uncertain remote outcomes, alternative factory profiles, automated
maintenance, improvement/research programs, and MPP/x402/AP2 settlement. Preserve
completed implementations and evidence, but stop expanding these capabilities
until the basic gate passes. Deferred live features stay explicitly unavailable;
their full requirements remain in S01–S33/P01–P10 below.

Current orchestration should assign only work that closes a B-row: live
submission/provider readiness and single-job control; truthful observation and
reconnect; shared dashboard/demo behavior and local delivery; usage attribution;
then bounded end-to-end verification. Fix observed product defects before
adding qualification machinery. Report runnable progress against B01–B10 and
continue integration until the gate is complete or a concrete external blocker
requires operator action. Do not stop merely because a worker checkpoint or the
full-inventory bookkeeping is finished.

#### Approved Floor presentation and work-discovery follow-up

Operator-approved 4 October 2026, after completion of the Basic milestone.
Restore factory entry/exit boundaries and floor groups in the existing shared
Demo/Recorded/Live application. This is a presentation and discoverability
follow-up; it does not reopen the paid two-workflow allowance or authorize a
third real submission, product inference, or payments.

- Every nonempty supported factory graph must have visible factory entry and
  exit presentation, including retained parent `nested_factory`/`complete`
  graphs and child graphs with `release` and no `intake`. Attach visual boundary
  stubs to existing nodes using declared kinds and graph topology. Preserve
  pinned node IDs, kinds, edges, and digests; do not manufacture an intake node,
  execution Task, event, assignment, or result to supply missing decoration.
  `complete` remains a terminal node. A factory exit is not evidence of accepted
  output or delivery; acceptance and delivery remain separate public facts.
  If the public graph has multiple source candidates or omits conditional-route
  edges, identify the selected entry attachment as visual presentation. Do not
  invent missing edges or assert that all source nodes are execution starts.
- Preserve explicit allowlisted node grouping metadata when available, including
  declared Demo fixture groups. Where group metadata is absent, the renderer may
  derive finite presentation-only groups from the existing node kinds. Label
  those groups as visual groups and do not present them as runtime departments,
  organizational authority, staffed capacity, or agent ownership. Grouping must
  not change routing, pins, reducer facts, or command authority.
- Place an obvious, keyboard-accessible **Send work** action at the factory
  entry on the Floor. It must open and focus a clear brief composer for the
  selected authorized factory, using the existing shared composer and normal
  A2A submission path. Opening the composer is not submission. Display mode,
  authentication, freshness, and provider-readiness disabled reasons visibly;
  Recorded remains read-only and Demo submissions remain local and isolated.
  Keep the existing Decisions path available without duplicating submission
  or authorization logic.
- Qualify this follow-up with focused regressions and bounded browser checks
  using retained actual parent/child graphs, Recorded evidence, and local Demo.
  Verify visible boundaries, grouping, composer discovery/focus, selected-factory
  binding, disabled reasons, and unchanged pinned graph semantics. Opening the
  composer must issue no A2A work submission, operator command, provider inference,
  or payment. Preserve the completed Basic evidence and earlier failures.

Runnable review uses the existing local dashboard: choose a source and factory,
open Floor, select a retained root or child graph, then choose **Send work** at
its entry to inspect the composer without submitting. Recorded's composer must
explain its read-only state. Use the same source picker for isolated Demo.

#### Approved communication, inbox, and draft interaction restoration

Operator-approved 4 October 2026, within the shared Floor presentation follow-up.
Live and Demo brief drafts must be editable and retain text across view changes,
source switches, and composer reopening, even when submission is unavailable.
Keep drafts isolated by source and selected factory in memory. Recorded stays
read-only. Disable only submission for authorization, freshness, transport, or
provider-readiness failures and display the actual reason. Opening, typing, or
pasting a draft must never submit work. Verify physical pointer focus, keyboard
entry, paste, and text retention through the existing local browser controls.

Show an always-visible Human inbox and an independent incident-view entry on
Floor. Empty means no observed current items; unavailable means the source cannot
establish freshness. Neither state proves that human escalation or recovery is
configured. Inbox navigation opens the existing Decisions/wait/incident projection.
Only observed waits with original Task/run bindings and permitted actions can
supply actionable items; absent policy or deferred controls must remain unavailable.
When a recording lacks a renderable graph, retain the Floor area and provide the
same inbox/incident navigation with an explicit unavailable label. Replay updates
must preserve focus on an opened inbox or incident heading.

Restore Director and human route references without changing pinned execution
nodes or edges. Distinguish work belts, graph-bound declared decision/control
references, and explicitly labeled presentation-only navigation links. A view link
is not a message, authorization, or escalation. Human routes require explicit
fixture policy, pinned policy metadata, or an observed human wait; do not invent
incident-to-Director ownership. The incident view is accessible independently of
Director availability. Preserve incident run/Task/owner attribution and show an
unreported origin when no public origin exists; highlight only an exact observed
origin. Do not draw fault wires to every execution node.

The brief composer must show submission progress, rejection/error and received
status inline, retain the draft, and prevent duplicate clicks while awaiting the
response. Disabled submission must display its actual blocking reason next to
the control; local Demo feedback must explicitly disclose that no real work starts.

Live reconnect must use the existing authenticated snapshot/resume Interface.
Startup discovery and snapshot failures must replace the centered Connecting
message with the actual failure and an operator retry path. Bound discovery
requests; a graph-layout worker that stalls must release the render queue and use
the existing presentation fallback. Fallback layout does not supply missing graph
nodes, source facts, authority, or freshness, and does not run a retry loop.
Serve the dashboard page/session bootstrap without caching and version the entire
JavaScript import graph together, including relative imports, when the shipped
contract changes. Failed/loading Live must not retain or receive Demo escalation
controls, job-version badges, or publication claims from the boot illustration.
A disconnected snapshot cannot be made fresh by UI decoration or by a catch-up
checkpoint alone. Scoped freshness may carry optional `scope` (`factory` or `run`),
`run_id`, `included_run_ids`, `factory_status`, and `unavailable_run_ids`.
Run ID lists are unique, allowlisted, and bounded to 256. A fresh run scope requires
successful reads for the exact selected run and all descendants established by
Temporal child-start facts; unrelated missing histories retain factory-wide
`disconnected` status and a visible warning. Other read failures fail closed.
Run selection and reconnect use the existing authenticated run-filtered snapshot;
no client may choose source endpoints or assert readiness. Missing terminal history does not establish current-factory unavailability.
New-work submission uses an authenticated, factory-bound current Runtime readiness
check (`GET /submission/readiness`), independently from historical run freshness.
Its strict response is `{schema_version:1,factory_id,observed_at,status,reason_code}`;
status is `ready` or `blocked`, ready has null reason, blocked has one allowlisted
reason code. Runtime freshly checks the pinned provider/owners/worker and durable
single-job state; busy, uncertain, unfinished, or unavailable current state blocks.
Clients cannot choose identity or source endpoints. The Adapter requires an open
factory connection and rechecks this no-store current Interface before each new
submission, with a bounded request and timestamp validation. The existing A2A
preflight and atomic single-job fence remain the sole start authority. A successful
readiness check is not inference proof or permission to bypass a busy slot.
Actions on an existing run continue to require that exact run’s fresh observation.
Expired or missing history remains disconnected/unknown and never becomes fresh
through current submission readiness. After accepting new work, the dashboard
refreshes the factory snapshot and follows the newly observed pinned run. Diagnose
the owned source when active-run reads fail, preserve fail-closed run controls,
and qualify run recovery with a real fresh snapshot. The current
public contract has no separate human-inbox object or incident-to-Director foreign
key; recovery is explicitly unavailable. This restoration adds no deferred human
workflow, new inference, paid browser call, third real workflow, or payment.

Review: select Floor, click Send work, type/paste a harmless draft without Submit;
open Human inbox and Incident view, then use the normal Live Reconnect control.
For the explicitly enabled loopback QA session adapter, opening `/floor` without
a valid session must lead to `/qa/login` and its explicit session button, rather
than a blank unauthenticated Floor. A server-declared local session link supports
recovery. This grants no non-loopback access and does not qualify production auth.
Use retained actual parent/child data, local Demo fixtures, and Recorded evidence
for bounded no-model checks. Document unrelated Demo conversion/default Recorded
gaps separately rather than asserting full historical scenario parity.

#### Additional regression inventory and reconciliation follow-up

The read-only audit on 4 October 2026 identified the baseline below. It records
observed defects and product gaps, not permanent current-state assertions or
claims that fixes have passed. Track closure against the fidelity checks below;
retain failed observations and do not erase them when code changes.

| Finding | Audited baseline | Required reconciliation |
| --- | --- | --- |
| Station kind mismatch | Assignment labels contained `undefined`; nested stations were projected as ordinary agents | Align renderer presentation kinds without changing pinned graph kinds; retain distinct assignment/nested appearance and instruments |
| Single-job Demo timing | Supplier Evaluation's event sequence extended to 118.4 seconds and Translation's to 32 seconds, but converted work timestamps collapsed to zero and Floor replay to about two seconds; completed Translation appeared active | Preserve scenario clock/start and terminal state; Supplier human wait remains a declared illustrative wait, not a new Live capability |
| Demo artifact/work fidelity | Original movement, most artifact spawns, verdicts, releases, flags, scraps, and readouts did not survive projection; artifact registry disappeared | Preserve legitimate illustrative events and identities through the shared contract; do not fabricate missing Live location or acceptance evidence |
| Demo communication/activity | Director turns, tool/activity summaries, alarms, escalation recommendations, and notes disappeared; restoring static wires alone cannot recover them | Preserve scenario communication/activity and render it through the same common views with explicit illustrative labels |
| Demo operating metadata | Declared agent bindings/capacity, simulated budgets/prices/shared occupancy, and program indicators were discarded; main result kind was forced to report for scorecard/translation profiles | Preserve profile metadata and result kinds; keep illustrative amounts/operations separate from unqualified Live figures |
| Default Recorded entry gap | Normal Recorded selection had an empty graph and no captured run graph, leaving the Floor unavailable; prior successful checks injected a different saved actual snapshot | Verify the shipped default selection, not only injected fixtures; render an authentic complete recording or keep useful evidence-only views when its graph is unavailable |

The default Recorded graph gap is a product-path limitation, not evidence that
refusing a graph substitution is a defect. Similarly, small-screen view navigation
was already hidden below the original layout breakpoint; record that as an
inherited usability limitation, not an integration regression. Do not invent a
replacement graph or qualify a new provider path to conceal either limitation.

The existing communication/inbox/draft follow-up and this inventory must stay
consistent. Static restoration, actual interaction usability, Demo fidelity, and
Live execution qualification are separate completion claims. Document unresolved
conversion/default-source gaps explicitly; passing the earlier Basic gate or a
suite of presence assertions does not mean all original scenarios retain fidelity.
Restoring illustrative metadata does not reopen the full S/P qualification loop.
No additional product inference, third real workflow, or payments are authorized
by documentation or regression checking; use retained actual observations and
isolated local scenarios.

#### Subsequent delivery milestones

| Milestone | Required result | Gate |
| --- | --- | --- |
| 1. Recorded-run integration | Render preserved real Verified Research observations and its pinned graph using the shared dashboard | Trace every displayed runtime fact to source evidence; retain honest fixture/induction labels |
| 2. Live connection | Stream actual execution over WebSockets with snapshots, resumable cursors, and current-state views | Prove refresh, disconnection, resumption, duplicate handling, and actual ongoing parallel work |
| 3. Basic working prototype | Complete B01–B10 through one shared dashboard and local factory | Two real dashboard workflows, one supported operator action, truthful usage and delivery, reconnect, single-job enforcement, and isolated demo |
| 3b. Extended Director interaction | Add exhaustion, extra repair, and human escalation/approval within existing authority | Qualify those additional decision paths independently after the basic gate |
| 4. Full operational qualification | Complete capacity, budgets, versions, nested work, recovery, output delivery, all views, and displayed Engineering programs | Every operational capability in the inventory has a passing real smoke case; publish safe recordings for demo |
| 5. Payment interoperability | Exercise bounded MPP and x402 payment profiles and AP2 authorization mapping | Preserve test-network verification/settlement receipts and negative/recovery evidence; mark any unqualified protocol profile explicitly |
| 6. Outcome settlement | Qualify accepted-outcome charging and delivery/credit behavior | Bind purchase, acceptance, delivered revision, and payment evidence; demonstrate cancellation and rejected-outcome rules |

Milestones 1 → 2 → 3 retain the operator-approved order. Runtime work for
milestones 3b–6 is deferred until the basic gate passes, except a dependency
actually necessary for a B-row. The basic prototype and full operational
qualification are separate completion claims. Existing commercial event shapes
and protocol declarations remain, but expanding or qualifying them is not basic
prototype work.

The smoke suite can use several representative graphs; one graph need not
artificially contain every capability. Preserve Verified Research, Supplier
Evaluation/nested research, and Document Translation as demo profiles. Qualify
their operational shapes with real work as the runtime grows. Generic visual
graph editing is not required to execute published definitions.

### Environment and evidence readiness

Before live qualification, record actual model/effort availability, permitted
inference access, non-secret runtime configuration, required binaries, ports,
persistence locations, and cleanup ownership. Record which processes are started
lazily and which state survives restart. Stable configuration belongs in validated
repository configuration; this spec does not add environment-variable defaults.
Any Octo orchestration configuration follows its existing TOML policy.

The operator's local factory stack keeps all durable state (runner, Postgres,
Temporal configuration, instance, agent service state, and the hand-off digest
key) under a durable home, by default `~/.exomachina/operator-stack/`. It never
lives under `/tmp`, whose nightly cleaner deleted a retained stack's database and
catalog on 8 October 2026. `scenarios/operator_stack.py up|status|down` is the
only supported way to start, inspect, and stop it.

Before payment qualification, additionally record provider activation, scheme and
network availability, test assets and amount limits, trust/signing setup, and
settlement/reconciliation readiness. If a method needs callbacks, webhooks,
hosted processes, or credentials, verify their actual materialization. Classify
each prerequisite as ready, an exact operator action before execution, or mocked;
secret existence alone is not readiness. Runtime artifacts and credentials remain
outside committed source. Documentation work does not run live authentication,
inference, payment, or secret-injection commands.

## Testing Decisions

### Test surfaces and evidence rules

Test externally observable behavior through the shared dashboard Interface,
existing A2A/Director Interfaces, Commercial Interface, and qualified payment
Adapters. The live and demo Adapters cross the same dashboard Seam. Reuse the
prototype's real-agent, version-binding, original-Task, outcome-journal,
restart/replay, and qualified-evidence patterns; historical counts are not a
substitute for new runs with the new model/configuration.

Use deterministic scenarios for targeted failures and low-cost contract checks,
then bounded live-model smoke runs for the claimed routes. Never fabricate a
Quality verdict to make a live acceptance case pass. Preserve all attempts,
including failures and inconclusive cases. Record configured bounds and actual
invocation counts. A live model that takes another valid route does not prove the
unvisited branch; report the missing case rather than relabeling it.

Each capability needs a recorded expected outcome, scenario identity, actual
model/effort, graph/binding/contract versions, relevant Task/attempt identities,
times, usage/cost basis, decisions, artifact digests, and receipts. Export only
safe projections and attest the final evidence bytes. A synthetic fixture proves
its declared role; it cannot establish external-provider compatibility or a real
payment. Test-network payment evidence is labeled separately from production
money movement.

### Shared dashboard fidelity and regression checks

Exercise the owned Adapters and reducer through the shared dashboard Interface,
and the renderer through actual browser interaction. Preserve source kinds, pins,
identity, times, provenance, and terminal outcomes; test what survives the full
Adapter → reducer → renderer path, not merely that a control or module exists.
Fixtures for illustrative scenarios remain illustrative, not external-provider
recordings. Use existing Modules rather than reproducing their projection or
policy in a test double. Broader architectural ownership remains unchanged.

| ID | Required fidelity proof |
| --- | --- |
| F01 | Assignment and nested-factory kinds produce correct distinct icons, labels, instruments, and dimensions; no undefined label; original node kinds, edges, and pins unchanged |
| F02 | Every preserved Demo profile retains ordered start/progress/end timing, including a fixture without a job-created event; Supplier and Translation no longer collapse to a two-second replay; completed work is not active |
| F03 | Declared Demo artifact kinds, revisions, movements, Quality outcomes, and release readouts survive full projection; Live unlocated artifacts stay unlocated and completion alone never implies acceptance/delivery |
| F04 | Original Demo Director activity, alarms, recommendations, waits, and notes remain visible at their appropriate scenario times; declared wires remain distinct from observed or illustrative communication activity |
| F05 | Demo agent bindings, factory WIP limits/queue occupancy, illustrative agent prices quoted through the A2A payment extension, simulated budget, program indicators, and profile-specific result kind survive; unknown Live values remain unknown and Demo labels cannot leak into Live facts |
| F06 | Always-visible human inbox and independent incident navigation show truthful empty/stale/unavailable states; supported items retain exact Task/run/origin/action bindings; absence of policy cannot enable an action |
| F07 | Physical click, keyboard typing and paste edit Live/Demo drafts even when Submit is disabled; view/source/factory switches and reopening retain the appropriate isolated draft; Recorded is read-only; no input event submits work |
| F08 | Ordinary shipped Recorded selection works without test-time bundle injection: authentic pinned graph is rendered or useful evidence-only views remain accessible with explicit graph-unavailable provenance; alternate/injected recordings are tested separately |
| F09 | Mode switches, reconnect, and duplicate event replay preserve identity and do not duplicate displayed items; freshness/auth/provider gates remain enforced, including partial run versus factory coverage |
| F10 | Browser checks cover real pointer interaction, relevant scenario timestamps, source isolation, and absence of product submissions, provider calls, or payments; failed and partial attempts remain distinct from final passes |

These checks close presentation/fidelity gaps, not deferred operational/payment
matrix rows. A faithful simulation does not qualify Live human escalation,
capacity enforcement, commercial accounting, or Engineering programs. Retained
recordings do not qualify new submission, synchronization recovery, or inference.
Static schema/source assertions support, but cannot replace, the browser and
Interface-level proof appropriate to each claim. Track per-row evidence and
remaining limits separately from aggregate test counts and the Basic gate.

### Complete operational smoke inventory

All operational capabilities represented by the original floor remain in scope
for **later full qualification**. They are not the basic prototype gate; use
B01–B10 above for current execution. The initial inventory below is a floor, not permission to
omit another discovered control or readout. Complete a control/readout inventory
before implementation and add any missing capability to this matrix. Hiding an
unsupported feature can keep an interim live view accurate, but cannot count as
passing its required smoke case. Visual conveniences such as pan, zoom, and calm
mode require UI verification rather than model inference.

| ID | Capability | Required observable proof |
| --- | --- | --- |
| S01 | Brief submission and identity | Submit through the normal factory identity; original Task/context bound to one run; actual permitted model/effort recorded |
| S02 | Pinned graph and readouts | Runtime definition and bindings match the displayed graph, version, contracts, and run facts |
| S03 | Parallel work and join | Two independent agent Tasks overlap; join waits for both actual results and passes them to synthesis |
| S04 | First-pass acceptance | Independent live Quality accepts the exact first revision; one actual accessible delivery |
| S05 | Rejection and repair | Live Quality rejects an induced defect; real repair receives findings; new revision needs new acceptance |
| S06 | Exhaustion and abort | Repair count reaches its bound; original Task enters a durable wait; permitted abort yields no delivery |
| S07 | Extra repair | Authorized Director grants a finite extra attempt; actual repair/review executes; spending authority is separately sufficient |
| S08 | Human escalation | Director escalates outside its authority; human sees context/recommendation and resolves the original wait |
| S09 | Wait expiry and stale response | A real configured deadline fires; late response cannot resolve or release superseded work |
| S10 | Decision lifecycle and concurrency | Observe requested/validated/applied or rejection; concurrent conflicting answers produce one permitted resolution |
| S11 | Inference attribution | Owned agent model usage and available billable categories attributed by agent, assignment, and attempt, including Quality/Director/repair |
| S12 | Hosting and markup | Measured or declared allocated hosting basis and chosen markup reproduce the commercial charge; estimates labeled |
| S13 | Reservations and budget limits | Simultaneous admissions compete for one remaining budget; atomic reservations admit only affordable work; restart preserves them |
| S14 | Usage charging and credits | Rejected/cancelled/failed work follows its pinned terms; late records and credits update obligations without rewriting usage |
| S15 | Capacity and admission queues | Submit more concurrent jobs than configured capacity; observe waiting, slot release, and admission without exceeding limits |
| S16 | Shared-agent contention | Two factory instances use one provider; occupancy separates own/other work. The former "respects the advertised shared capacity" clause is withdrawn (operator decision, 8 Oct 2026): agent services advertise no capacity |
| S17 | Versions in flight | Publish v2 with v1 active; new admissions use v2 while old runs retain original graph/contracts/rates where pinned |
| S18 | Nested factory and fan-out | Actual supplier-style fan-out invokes a nested factory through A2A; collect actual artifacts without exposing private internals |
| S19 | Nested outcome unknown | Drop a real response; preserve unknown state; reconcile the original remote Task rather than resubmitting opaque work |
| S20 | Approval and send-back | Actual scorecard/policy result reaches human approval; send-back produces a new revision, new required review, and another authorized decision |
| S21 | Small graph without a gate | Execute a real translation-style factory whose contract declares no independent gate; display acceptance as not applicable rather than inventing it |
| S22 | Artifact lineage and delivery | Inspect prior/current artifacts; final saved Markdown bytes match accepted digest and distinct delivery receipt; replace fixture delivery |
| S23 | Worker/harness restart | Interrupt an in-flight run; recover actual original work and its ledger without extra delivery or usage charge for replay alone |
| S24 | Incident, alarm, and acknowledgement | Produce an actual failure/stall/slow condition; show evidence and owner; acknowledgement does not itself resolve the incident |
| S25 | Maintenance | Claimed incident follows a permitted recovery or escalation; final evidence supports its outcome and preserves accepted work |
| S26 | Improvement | Engineering produces a bounded candidate; independent evaluation and authorized publication affect future runs, not active bindings |
| S27 | Optional research program | Disabled state causes no campaign admissions; an explicitly enabled bounded campaign records real trials and protected Quality/promotion limits |
| S28 | Snapshot, reconnect, and gaps | Refresh and disconnect during transitions; reconstruct matching state; duplicate events do not duplicate items; expired cursor requires resync |
| S29 | Multiple observers and slow consumer | Concurrent viewers see consistent ordered outcomes; a slow viewer resynchronizes without affecting execution or other viewers |
| S30 | All dashboard views | Floor/Board/Decisions/Outputs/Definition/Agents and inspectors show actual records; available controls perform their stated operation |
| S31 | Demo isolation and parity | Same application loads recordings and interactive simulations; commands affect only demo state; no live credentials/inference/payment calls |
| S32 | Live presentation | No playback controls/shortcuts while a live job is displayed (finished Live jobs may be replayed, 7 Oct 2026); actual ongoing states, freshness, pan/zoom/follow, calm mode, and accessible panels verified |
| S33 | Observation secrecy | Structural allowlist and synthetic leak positive control pass on final exports; credential-bearing raw histories are not browser payloads |

Research and Engineering requirements here qualify the capabilities advertised by
the floor and preserve the existing maintenance contract. A single controlled
campaign/promotion can prove a path; it does not establish generalized autonomous
improvement efficacy or market reliability.

### Work-in-progress limits (operator decision, 8 Oct 2026)

Work-in-progress limits are factory settings. Agent services have no capacity queue and are treated as infinitely scalable; an agent may someday have independent limits of its own, but that is its private business and nothing here models it. Agent-side execution capacity was removed on 8 Oct 2026 by operator decision. S15 covers the factory's own admission queue and limit. S16 keeps only factory-side observation of shared-provider occupancy; it no longer requires an agent-advertised shared capacity, and the agent's `--admission-db`/`--execution-capacity` options and `GET /admission/capacity` no longer exist.

### Payment and outcome smoke inventory

| ID | Capability | Required observable proof |
| --- | --- | --- |
| P01 | MPP metered session | On a declared test method/network, authorize bounded use, accrue actual units, settle, and reconcile unused funding according to method |
| P02 | x402 usage scheme | On a supported declared scheme/network, authorize a maximum and settle the actual charge no greater than it |
| P03 | Protocol-specific fixed charge | Qualify an advertised fixed-charge compatibility profile independently; do not infer usage compatibility from an exact-flow pass |
| P04 | Purchase binding | Offer, provider/payee, assignment, currency, amount, and payment references agree; altered or substituted purchase is rejected |
| P05 | AP2 authorization mapping | Verify a real signed mandate flow against pinned trust/schema; reject tampering, expiry, unauthorized payee, excessive amount, and wrong purchase binding |
| P06 | Nested authority | A child cannot spend a parent authorization transitively without explicit permitted delegation; local budgets constrain all purchases |
| P07 | Payment concurrency/retry | Concurrent identical requests and restart preserve one logical payment; conflicting reuse is rejected; retained identity outlives provider cache limits where promised |
| P08 | Settlement uncertainty | Lose settlement response; keep unresolved state/reservation; reconcile network/provider evidence before any replacement payment |
| P09 | Failure and refund/credit | Exercise declined authorization, interrupted work, cancellation, and refund/credit; distinguish obligations from actual funds movements |
| P10 | Outcome charging | Qualified later profile settles only under its acceptance/delivery rule; rejected or obsolete revision cannot earn an accepted-outcome payment |

### Explicit concurrency level

No-duplicate effects, no-double-charge, one wait resolution, capacity bounds,
publication preconditions, and budget safety apply to **inter-request concurrent
operation**, including multiple tabs/observers, simultaneous model commands,
multiple harness processes sharing qualified infrastructure, overlapping payment
Adapters, and crash/retry recovery. Sequential-only checks do not qualify these
claims. Test deliberate races at each promised scope and record the actual
concurrency. Do not claim a whole-provider limit from observing only one factory.

### Full roadmap qualification completion

The criteria below apply to full operational/commercial qualification after the
basic prototype. Completing B01–B10 does not assert these broader guarantees.

- Recorded, live, and demo paths conform to the shared Interface and versioned
  schemas. Live facts derive from runtime evidence and ongoing work is visible.
- Every operational matrix row and any added original-floor capability has real
  execution evidence, except presentation/isolation checks which have direct UI
  or effect evidence. A missing capability remains an explicit unmet requirement.
- All qualified model-bearing prototype paths use `gpt-6-luna` at `xhigh`.
- Supplier charges are reproducible under pinned contracts. Inference and hosting
  cost visibility is accurate for owned agents and honest about external
  disclosure. Concurrent spending and capacity enforcement pass.
- Actual artifact delivery replaces the historical fixture. Acceptance,
  payment, and delivery records remain distinct and correctly correlated.
- Qualified payment profiles have real test-network evidence; others remain
  unadvertised. Financial obligations survive uncertain settlement and restart.
- Safe recordings from qualified runs become maintained demo bundles. They pass
  the same observation contract checks as live data and retain provenance.

## Out of Scope

- A second dashboard implementation or a separate factory identity just for UI.
- General-purpose visual graph editing, a marketplace, arbitrary provider/model
  replacement during active work, or proof of all possible factory types.
- Production wallet funding, real-money purchases, global AP2 interoperability,
  every MPP payment method/network, or every x402 scheme/facilitator.
- Assuming AP2 provides automatic agent-to-agent spending delegation, payment
  proves Quality acceptance, or either payment protocol provides outcome escrow.
- Requiring an opaque provider to expose its private model costs without a
  disclosure promise; treating unavailable cost as zero.
- Subscription billing as the target agent economy. Existing subscription trial
  evidence remains historical and must not be rewritten or relabeled.
- Benchmark claims about model reliability, proof that improvement always wins,
  hostile-code isolation, production multitenancy, or a fully qualified release.
- Issue creation/routing, branch publication, commits, or deployment by this
  documentation request. Those actions retain their own authorization scope.

## Further Notes

### Current evidence and its limits

The [single-factory qualification](../../prototype/temporal-factory/SINGLE_FACTORY.md)
records real `gpt-6-sol` inference for authoring, research, synthesis, Quality, and
a caller-prompted Director abort. It demonstrates first-pass acceptance,
rejection/repair/acceptance, and exhaustion/abort. Defects on repair/exhaustion
routes were induced, and delivery was an HTTP fixture. It does not qualify Luna,
extra repair, human escalation, the current mock capacity/prices, or payments.

Amendment (8 Oct 2026): the release receiver is no longer an HTTP fixture. It is
an ordinary A2A v1 agent reached only through `SendMessage` and `GetTask`, whose
Task completes with a receipt artifact over the exact delivered bytes; one
delivery yields exactly one `delivery.receipt` fact. It remains a local fixture
destination, not a real external release target. The qualification above was
observed before this change.

The [existing prototype Interface description](../../prototype/temporal-factory/INTERFACES.md)
and [prototype overview](../../prototype/temporal-factory/README.md) describe the
integration to extend. The [factory floor](../design/exomachina-floor.html) is the
operational capability inventory and demonstration starting point, not evidence
that its supplier, translation, Engineering, economics, or load claims run live.

### Decisions still required before their execution

These details do not block recording the agreed direction. They must be selected
and recorded before the dependent smoke or payment profile runs:

- The authorized Luna inference access/purchasing method and verified `xhigh`
  support; rate sources and usage categories returned by that method.
- Concrete provider rates, markup basis points, hosting allocation, fee treatment,
  and disclosed failure/repair/cancellation credits. No default price was agreed.
- Exact scenario bounds, capacity values, deadlines, and evidence retention.
- Payment networks/assets, facilitator/relay, wallet custody, test funding limits,
  settlement timing, and the AP2 trust/mandate mapping.
- Outcome escrow or conditional-settlement mechanism if required, and commercial
  dispute rules. Those remain separate from required independent Quality.

### Primary protocol references

Protocol observations were checked on 2 October 2026. The layer assignments and
milestones are Exomachina design decisions, not guarantees supplied by the sources.

- [A2A v1.0.0 specification](https://a2a-protocol.org/v1.0.0/specification/): assignment/Task updates and artifact exchange; the only supported version from 7 Oct 2026.
- [CloudEvents 1.0.2 specification](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md): event envelope and identity.
- [AsyncAPI WebSocket binding](https://www.asyncapi.com/docs/reference/bindings/websockets): documenting transport-specific Interface details.
- [AG-UI events](https://docs.ag-ui.com/concepts/events) and [transport description](https://github.com/ag-ui-protocol/ag-ui/blob/main/README.md): evaluated optional Director-conversation Adapter.
- [MPP session intent](https://mpp.dev/intents/session): metered payment state and incremental authorization.
- [MPP and x402 integration guide](https://mpp.dev/guides/use-mpp-with-x402): specific compatibility profiles.
- [x402 upto scheme](https://docs.x402.org/schemes/upto): maximum authorization and actual-usage settlement.
- [x402 payment-identifier extension](https://docs.x402.org/extensions/payment-identifier): optional retry deduplication and cache lifetime limits.
- [AP2 v0.2 specification](https://ap2-protocol.org/ap2/specification/): Checkout/Payment Mandates; agent-to-agent delegation scope limit.
- [AP2 Payment Mandate](https://ap2-protocol.org/ap2/payment_mandate/): payee, amount, budget, and timing constraints.
- [AP2 Agent Authorization](https://ap2-protocol.org/ap2/agent_authorization/): trusted delegation and verification evidence.
- [A2A x402 extension](https://github.com/google-agentic-commerce/a2a-x402): candidate transport integration to qualify, not adopted compatibility proof.


#### Following accepted live work and preserving assignment progress (2026-10-05)

A new submission that returns an original A2A Task, context, and explicit `metadata.run_id` must follow that exact run through authenticated Observation. It must not depend on an unrelated factory-wide history refresh, infer IDs from opaque strings, or keep an older Floor visible after the selected run fails to load. On initial restore/reconnect, an expired saved run may yield to a readable observed run; the missing-history warning stays visible. Explicit historical selection remains available.

The Floor must display the selected observed job state/phase and Quality verdict in the existing right-hand job inspector, with links to work and outputs. These are persistent selected-job details, not a floating notification. They must never overlay the Floor composer or intercept its input or submit controls; hiding the inspector hides the details. Completed/accepted is separate from delivery. An idle Floor after completion must not obscure the outcome. Execution duration is anchored to the observed run start, rather than the first spatially renderable event. No illustrative movement substitutes for missing runtime facts.

Assignment materialization preserves earlier observed start, provider, and node within the same exact assignment/attempt when a later fact omits them. Another attempt never inherits those facts. A nonterminal/unknown correction without an end must remove a previously projected end. Raw append-only records remain immutable. Spatial placement requires an explicit workflow activity-input node present in the pinned graph; missing or invalid links stay unlocated, with work facts still accessible in Board. Historical node linkage is added by deterministic projection corrections, not by rewriting original events.


Run-scoped public snapshots retain the exact selected run's graph and facts; root scope does not imply that child records are included in its state. After first following the accepted root, the dashboard may make one optional authenticated factory snapshot read to discover a more detailed run with the same explicit original Task/context. It follows that run using its own pinned graph. Missing history or a failed discovery keeps the already readable root; a failed child read returns to root. No opaque ID parsing, inferred child identities, mixed graph pins, workflow retry, or model call is permitted by this presentation step. A concurrent user selection wins over a delayed discovery response. Root-only Quality remains unreported when its scope contains no verdict.

Cursor-only checkpoints must not replace unchanged evidence-view DOM or close artifact inspection. Actual changed view data or authority still updates the view. Expanded detail persistence across changed data remains a separate fidelity case; passing a static/unchanged-checkpoint check is not proof of all reconciliation cases.

#### Live station activity and selected-job rendering (2026-10-05)

Accepted new work must reveal the existing right-hand job card and select the new original Task/run even if the previous card was hidden or the operator was viewing a different screen. Same-graph updates must prefer the explicit selected run over the old renderer run or the factory aggregate. Child discovery may make at most three authenticated factory reads, separated by one second, then retain the readable root; a user selection or source switch cancels that discovery. No submission retry is implied.

Live station illumination and directional belt chevrons may show current observed activity at explicit pinned nodes. This is a presentation cue on declared inbound routes, not an artifact-transfer receipt or an observed physical transit duration. Do not manufacture moving artifact events or choose a successor branch without facts. Completed runs are static. Paused/unavailable source freshness disables live activity cues. Preserve Demo's existing illustrative item movement and Recorded's read-only replay. Run/assignment node linkage must come from actual activity input and a matching pinned graph; Temporal activity IDs are not graph-node identifiers. Keep raw source records immutable and append deterministic, versioned projection corrections.

Superseded in part on 7 Oct 2026 by "Live A2A floor flow" below: Live items now
follow observed assignment and run-node facts; belt chevrons remain an activity
cue for observed active stations.

#### Live A2A floor flow, branch agent pods, and all-jobs scope (operator request, 7 Oct 2026)

The operator reported that a real Live job lit a station and moved belts but no
work item travelled, only two agents looked active, and a new submission replaced
the previous job. Live and Recorded floors therefore follow A2A semantics,
derived only from Observation facts (assignment, run, artifact, quality, and
delivery records), never from A2A wire shapes, so the model is unaffected by the
A2A protocol version:

- **Tasks are work at a station.** An observed assignment attempt occupies its
  pinned node (or its branch agent pod, below) from `started_at` to `ended_at`;
  an active attempt stays open on the live edge. At a node without a declared
  capability and that does not fan out, an observed run node transition occupies
  that node until the run moves to another node or that node's completion fact
  (artifact revision, Quality verdict, or delivery receipt) is written. Nodes that
  take assignments use assignment facts only. A snapshot's current run node with
  no transition history is shown as current, with its arrival time not recorded.
- **Artifacts ride the belts.** An item appears on the outgoing belt when its
  producing station completes and stays on the belt until the next station's
  observed work starts, so belt dwell is the observed gap. The only presentation
  adjustment is a minimum visible hop of 0.8 s when that gap is shorter; exact
  observed times stay in the item and station inspectors.
- **Evidence levels are labelled.** A report artifact is evidenced by revision
  and sha256 (`artifact.revised` → `quality.verdict` with the same sha256 →
  `delivery.receipt` with the same sha256). Its item carries the revision label
  (R1, R2, …) through Quality and delivery; a rejected revision follows the
  repair route and is consumed when the next revision's synthesis starts. A
  release to the declared terminal requires a verified receipt; conflicting
  receipts show "delivery unverified" and no delivered exit. Assignment outputs
  are not observed as artifacts (Observation records the Task but not its output
  and has no consumed-input link), so their items read "Task output · artifact
  not recorded" and their hand-off to the next station is inferred from pinned
  graph order. Route-node cases are not declared as pinned edges; a hop from a
  route node to its declared control target (repair, director wait, release,
  terminal) is drawn and labelled as undeclared.
- **Location and time.** Artifact, verdict, and receipt facts are located by the
  run node transition open at their time, otherwise by the only pinned node of
  the producing type (synthesize, quality, release), otherwise left unlocated.
  Untimed snapshot rows never receive a time; their evidence is shown only on an
  item that has a timed fact. Snapshots carry no run-node history and no artifact
  or verdict times, so a retained run replays its observed assignments, outputs
  waiting on the belt, and its timed receipt/run end, and does not show
  occupancy for stations it did not record.
- **Branch agent pods.** When a fan-out node with one declared successor has
  observed assignments naming two or more distinct capabilities, the renderer
  draws one presentation-only pod per capability on that pinned route, named
  from the pinned binding whose identity matches the assignment's provider
  (for example "research_findings · packet_findings@1"). Pods light while their
  assignment is active, hold its Task item, count as agent stations in focus
  mode, and are labelled as derived from observed assignments. Pinned graph
  nodes, edges, and pins in reducer state and in the floor model are unchanged.
- **All jobs by default.** Live opens on all jobs whose manifest and definition
  pins equal the followed run's pins (its shared pinned graph). Runs other than
  the subscribed run come from a display-only retained factory snapshot that
  never selects, binds, or commands a run; the subscribed run's own facts win on
  overlap. A new submission keeps this scope; the new job joins the job rail,
  and the floor follows it once its exact Task/context/run binding is observed.
  While a submission is pending, retained jobs stay visible and none is
  followed. The run picker still narrows to one run. Finished jobs stay listed
  in a Live/Recorded rail. Submission safety rules (no resubmission, no binding
  to an unrelated older run, exact pending identity) are unchanged.

Superseded in part on 7 Oct 2026 by "Hand-off carriers, gems, and material and
control edges" below: once hand-off records exist for a run, its items are
recorded carriers, not inferred "Task output · artifact not recorded" items.

#### Hand-off carriers, gems, and material and control edges (operator decision, 7 Oct 2026)

The [A2A v1 baseline and factory mediation decision](../a2a-v1-mediation-decision-2026-10-07.md)
is authoritative; this section states its dashboard consequences.

- **The belt item is the factory's hand-off (the carrier).** It is derived from
  the factory's content-free hand-off Observation facts (produced, consumed,
  and item ready), never from A2A wire shapes or agent-supplied content. A
  carrier never travels empty: strict nodes fail their contract at the station
  when no artifact is returned, `message`-mode nodes carry one message item,
  and side-effect nodes produce no carrier.
- **Shape and gems.** Carrier shape is the producing node's declared output kind
  from the pinned definition, in Live as in Demo. Its items are gems set in rim
  sockets:
  - sapphire, round cut: text;
  - emerald, square cut: data;
  - amethyst, marquise cut: binary;
  - topaz, triangle cut: link;
  - clear diamond: message output.

  Cut carries the meaning without colour. More than four items shows four gems
  and "+N"; at low zoom the gems collapse to the badge.
- **Evidence is gem quality.** A flawless gem means a recorded hand-off whose
  digests match from producer to consumer. A chipped, cloudy gem means it was
  inferred from graph order; this applies only to runs without records.
  Existing rings keep their meanings: rejected, exhausted, exit, progress, and
  selection.
- **Timing.** A carrier fills in its station: empty sockets fill at each item's
  observed ready time. It leaves when the hand-off is produced and rides the
  belt until its observed consumption, with the 0.8 s minimum visible hop as
  the only adjustment. Carriers merge at joins. A gate verdict (Quality) is a
  seal on the forwarded carrier, not a new carrier. Revision labels (R1, R2, …)
  remain.
- **Material and control edges.**
  - Only material edges are belts with items.
  - Control edges, including route-node transitions to repair, Director waits,
    release, and terminals, are thin lines with no items. They replace the
    "undeclared" hop label.
  - Side-effect nodes (`output: none`) have no outgoing belt. Material that
    bypasses one is drawn on its own belt from the producer.
- **Inspection** lists each item's source, part kinds, `mediaType`, size, short
  keyed digest, and ready time, never content.
- **Qualification.** The hand-off facts widen the Observation allowlist, so S33
  is requalified when they land. Live-submission B-rows are requalified on A2A
  v1. Demo keeps its illustrative movement and gains illustrative gems labelled
  as such.

Implementation notes (dashboard presentation, 7 Oct 2026). Built and proved
over synthetic, test-only hand-off streams; runtime emission is a later phase.
These settle details the decision left open:

- **Where edge kinds arrive.** The pinned public graph (`graph.edges[].kind`,
  `graph.nodes[].output`) carries both. The event-stream `graph_nodes` form
  (`publication.activated`, `run.created`) gains `output` only; its `next`
  lists cannot express an edge kind, so those edges default to material until
  the runtime phase decides how control edges are announced there.
- **Carriers ride material edges only.** A consumer reached from the carrier's
  position by material edges receives it there. Otherwise the carrier is
  retired where it waits at that consumption (for example, a rejected R1 at
  Quality when the repaired draft consumes it); it does not travel the repair
  route over control edges. A later consumer that is reachable from the
  producer by a material bypass gets its own carrier on that belt, starting at
  production. Inferred items in runs without records still follow route cases;
  a hop over a declared control edge is labelled control, not undeclared.
- **Release.** A carrier consumed by a side-effect release node exits there on
  a verified receipt for its report item; it never travels the release node's
  control edge to the terminal.
- **Live filling.** Before production the total item count is unknown, so a
  filling carrier shows its ready items plus one empty socket.
- **Quality states.** Besides flawless (recorded, digests match) and cloudy
  (inferred: one rough-cut "contents not recorded" socket), a consumer naming
  digests that match no produced revision chips the gems ("digest mismatch").
- **Shape and merge.** In Live, carrier shape follows the producing node's
  pinned kind (synthesize square, parallel or branch pod capsule, join
  hexagon, nested factory pentagon, otherwise capsule). A merged carrier
  starts at the join, labelled J, holding every input item.
- **Field details.** `media_type` may be null (A2A `mediaType` is optional);
  `byte_length` may be null only when the part kinds include `url`;
  `handoff_revision` is a positive integer; digests are 64 lowercase hex; a
  `message` hand-off holds exactly one item; consumed inputs name each
  hand-off once.
- **Snapshots.** Snapshot run rows do not yet carry hand-off records, so a
  refreshed or retained run shows inferred carriers until the runtime phase
  adds them to the snapshot contract. *Superseded by the runtime phase (7 Oct
  2026):* snapshot run rows carry `handoffs: {produced, consumed, ready}`
  (deduplicated, at most 256 per list), so a refreshed run replays the same
  recorded carriers. Snapshot quality rows have no verdict time, so a retained
  carrier is sealed when it arrives at the gate and its inspector says
  "verdict time not recorded". A finished run replays until its last belt hop
  settles, because hops are drawn at 0.8 s or more and can outlast a sub-second
  run.

#### Agent-reported usage and delivery evidence (operator decision, 8 Oct 2026)

Following decisions 7–9 of the
[A2A v1 decision](../a2a-v1-mediation-decision-2026-10-07.md), the factory
reaches agents through A2A alone:

- **Usage.**
  - Per-agent usage comes from each agent's budget-extension report on its
    terminal Task. The dashboard labels it *agent-reported*.
  - Token categories an agent did not report stay unavailable, never zero.
  - The factory cannot see an agent's model, provider, or individual model
    calls. The dashboard must not show or imply them for agents; they stay
    visible only for the factory's own Director calls.
  - Money appears only when a reported cost or a payment record exists. A token
    count is never a cost.
- **Work-in-progress limits are factory settings.** The dashboard shows the
  factory's own admission and capacity only; it shows no agent-side capacity or
  occupancy.
- **Delivery.**
  - A delivered job shows exactly one receipt.
  - Delivery is *verified* only when the receipt's sha256 equals the accepted
    revision's `artifact_sha256`.
  - The floor shows the delivered carrier as the same item Quality sealed, not
    a re-encoded copy.
  - A mismatch, or more than one receipt per delivery, shows as a delivery
    fault, not as delivered.
