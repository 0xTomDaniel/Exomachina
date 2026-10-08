# Dashboard and commerce qualification

3 October 2026 · Qualification lane record

## Current verdict

**S01–S33: 8/33 qualified (S01–S04, S24–S25, and S33 observed-real; S09 observed-synthetic
runtime behavior). P01–P10: 0/10 qualified.** S16, S22, S23, S28, S29, and S30 are
partial and do not count as qualified. The S/P matrices below
are the requirements from `docs/specs/factory-dashboard-integration.md`; the
status column records qualification against the new Interface and smoke
requirements. A historical partial or illustrative row remains unqualified.
Demo, fixture, synthetic, and unit evidence never becomes live evidence.

Some interface artifacts have landed; S01–S04 are qualified from observed-real
factory route, public export, and local receipt evidence; S24–S25 are qualified
for a captured observation fault, acknowledgement/claim, and permitted escalation;
S33 is qualified for the current public snapshot and usage payload allowlists with
copied positive controls; S09 is qualified for deterministic runtime behavior. S16, S22, S23, S28, S29, and S30 are partial;
other rows remain unqualified. The
Observation projection, WebSocket transport, AsyncAPI, and JSON Schemas exist,
and Runtime has mounted the transport in `src/harness.py`. Runtime-focused
synthetic tests are reported passing; they do not establish live or dashboard
qualification. The local-delivery Observation projection has 14 focused Runtime
and 41 Observation tests plus actual receipt-browser evidence; the later
incarnation-14 Operations smoke exposed a separate closed-workflow query
regression recorded below.
The dashboard contract, reducer, and recorded/demo/live adapter
modules exist. The floor page source now imports all three adapters and the
shared reducer, and `dashboard/test/dashboard.test.mjs` is present. Those are
implementation artifacts; a limited public Observation smoke returned an
authenticated empty snapshot, and constrained browser evidence covers the six
views over Demo and historical Recorded data, camera interactions, and
fixture-backed Live UI. A separate Jev browser scenario made three native
Decisions calls; this is not product factory inference. The two authorized
real Luna `xhigh` Basic Dashboard workflows are now complete; their Tasks,
Quality outcomes, graph pins, per-call usage, and local receipt/download proofs
are summarized below. Final read-only browser validation made zero new starts,
model calls, or payments. The two-run allowance is consumed. The captured public
JSON was validated after parsing/canonicalization, not as original wire
whitespace; usage coverage remains partial and cost is unknown. This does not
alter the S/P matrix verdicts. A fresh no-model browser pass after the latest
run-picker/role/Demo source changes is recorded below. The
Node test source includes synthetic/demo and historical-recorded checks,
including an unauthenticated live-adapter check, but its presence does not
qualify an operational row. `src/commercial.py` exposes the
Commerce-owned Payment Adapter seam and MPP/x402/AP2 v0.2 declarations. Those
profiles remain unconfigured and unqualified, and no payment network readiness
is asserted. `INTERFACES.md` intentionally defines no separate Payment lane.
These source artifacts are implementation evidence only, not live smoke
evidence. The assigned files and suite locks for this lane are:

- `scenarios/dashboard_qualification.py` — no-network preflight and safe
  evidence candidate validation; it does not dispatch live commands.
- `tests/test_dashboard_qualification.py` — owned deterministic contract and
  safety tests.
- This file — row verdicts, evidence references, prerequisites, and remaining
  qualification work.

The runner never reads internal Temporal history or runtime databases. It
checks public A2A files, the Observation source/schema set and runtime mount,
dashboard adapter modules and references from the served page, the declared
Node adapter test path, the Commerce module, and Commerce's public Payment
Adapter seam/declarations. It also checks runtime prerequisites and current
port listeners, and validates safe evidence candidates. It does not make
network calls, dispatch Interface operations, configure payment profiles, or
infer readiness from source declarations. It records `unqualified` until a
reviewed, safe, Interface-level smoke record supports a row. Its evidence
candidate validator can return only `candidate-for-review`; qualification
remains a human-reviewed matrix change.

## BASIC milestone checkpoint

BASIC replaces the deferred S01–S33/P01–P10 completion loop as the active
acceptance target. The historical matrices and their row verdicts above remain
preserved; this checklist does not change their counts. The canonical B01–B10
gate supersedes the older paired-run note below.

**BASIC gate: passed for the two authorized Dashboard workflows and the bounded
deterministic B06 operator action, with the explicit evidence limits in B02,
B05, and B10.** This is not full lifecycle qualification and does not change
S01–S33 (8/33) or P01–P10 (0/10). Do not submit another live brief under this
allowance.

| BASIC row | Current evidence and status |
| --- | --- |
| B01 — normal Dashboard submission, real Luna `xhigh`, original Task/context and pinned definition | **Observed for both authorized workflows.** First workflow: original Task `07c6b0e8-9cf7-4b79-a3eb-353a9730424e`, context `db247349-0b65-49f9-9579-c701fda3a86a`, run `a40ec20b-2787-438a-8d11-11dd9ccecc16.8cf5a833c997f10b6483`, real child Quality accepted r1. Second workflow: Task `e7244abe-7b3d-4260-b520-314978408842`, context `7e244d50-aa58-4d4d-b46e-25a2bdbdebb8`, run `a40ec20b-2787-438a-8d11-11dd9ccecc16.d850a72cc51b1ddd1720`; real Luna `xhigh` work reached r1 rejection then r2 acceptance. Child pins are definition `05c74cddea24629a6aa777d5ae138fcabdf974ff766ee3d5a8dbf70a9407608c`, manifest `c000aa60983e071688157e794088fc16d161f44a108462c1aa1369e273d69e21`, package `d37d76d002e6918ebb62730699c636d9ac0fd0daff55676985c3f1a7c58e98b0`, build `b-84a8c06e465b`. Preserve the initial start failure and same-intent continuation. Evidence: first workflow `/private/tmp/exo-jev-qa-20261003/basic-first-resume-ui-evidence-attempt2.json`; second `/private/tmp/exo-jev-qa-20261003/basic-second-local-proof-evidence.json` (SHA-256 `3680b0eefad700ace7e3d90d8bbd7c4f2d07e1bc1dc206f24b6ceb3acb1b789e`). |
| B02 — truthful graph, assignments, progress, Quality, terminal state | **Observed for the actual child graphs, pins, Quality, and terminal states; receipt-conflict UI assertion remains separate.** First workflow attempt 3 verifies graph/pins, completed original Task/context, and child Quality acceptance at r1. Second workflow's 18/18 browser proof verifies the actual updated `compose_report` child graph, child pins, same completed Task/context, accepted r2 envelope, and no duplicate child. The Floor source implements `Receipt conflict; delivery unverified`, but the real runs did not induce a conflicting receipt and the browser did not assert this label. The synthetic `snapshot-output-parity.test.mjs` regression remains 4/4 only. Evidence: `/private/tmp/exo-jev-qa-20261003/basic-first-local-proof-evidence-attempt3.json`, `/private/tmp/exo-jev-qa-20261003/basic-second-local-proof-evidence.json` (18/18). |
| B03 — two real Dashboard workflows: first-pass acceptance, then real Quality rejection, bounded repair, and new-revision acceptance | **Passed for the authorized pair, with the second r1 candidate explicitly stimulus-modified.** First workflow ended with real Quality accepting r1. Second ended with real Quality rejecting r1 and accepting r2 at new accepted envelope SHA `d8c93b39cc59801f3949b20bff283e8e14f200d004f7e4ba115ae60d5cc28737`; the r1 route-2 stimulus applied once to fixture-modified candidate content, so r1 is not represented as a pure model result. Sanitized application proof joins the stimulus log to one actual synthesizer usage row via service Task `6481365a-0873-4b5b-aab6-2b7dd58b0fff`, explicit assignment/attempt IDs, and child run `a40ec20b-2787-438a-8d11-11dd9ccecc16.d850a72cc51b1ddd1720:child:05c74cddea24`; Quality rejection and repair were real. The authoritative busy reply rejected the concurrent brief without creating another Task/context. Preserve the second-workflow helper attempt 1 visual-run/null failure (`submissions=0`) and the earlier empty HTTP 500 usage-capture failure; both are superseded by later successful evidence, not erased. Both authorized workflows are consumed; no further live submission is implied. Stimulus application proof: `/private/tmp/exo-jev-qa-20261003/runtime-basic-second-stimulus-applied.json` (one prior public owner GET, zero additional reads, commands, provider calls, or payments). Workflow evidence: `/private/tmp/exo-jev-qa-20261003/basic-first-local-proof-evidence-attempt3.json`, `/private/tmp/exo-jev-qa-20261003/basic-second-local-proof-evidence.json` (SHA-256 `3680b0eefad700ace7e3d90d8bbd7c4f2d07e1bc1dc206f24b6ceb3acb1b789e`). |
| B04 — accepted report download and distinct local receipt | **Observed for both workflows.** First workflow: receipt bound to r1 accepted envelope `501946f1db1e8dc29c003bb5243fccf771ab892bd9ed92120f24208f19922cea`; downloaded Markdown was 1,956 bytes, SHA-256 `139c610acfc9945bf84d1c2bf23b500d163c0c77f6911350b9fd08ba02ae8b8a`. Second workflow: one local r2 receipt on the original Task/context, accepted JSON envelope SHA `d8c93b39cc59801f3949b20bff283e8e14f200d004f7e4ba115ae60d5cc28737`, downloaded Markdown 1,888 bytes, SHA-256 `a0ed438087751577441ad154fa94a29e51568fcb19d882804007ed069f18ab1b`. Each envelope digest is distinct from the Markdown byte digest. Evidence: `/private/tmp/exo-jev-qa-20261003/basic-first-local-proof-evidence-attempt3.json`, `/private/tmp/exo-jev-qa-20261003/basic-second-local-proof-evidence.json` (18/18). |
| B05 — provider-reported per-agent usage for exercised paths, including repair and Quality; explicit gaps and unknown cost | **Observed for Basic; aggregate coverage partial and cost unknown.** The current `/private/tmp/exo-jev-qa-20261003/basic-second-usage-wire.json`, independently reviewed in `/private/tmp/exo-basic-usage-review.md`, contains 8 scoped first-workflow rows (4 Director, 4 assignment) and 11 scoped second-workflow rows (4 Director, 7 assignment). Each workflow has one pre-run Director row with null `run_id` and `definition_digest`; preserve those nulls. All assignment rows have explicit assignment/attempt IDs. The seven second-workflow assignment `task_id`s are service A2A Task IDs, distinct from the original factory Task, and map by exact child `run_id` prefix. The 23-row whole response includes four older historical Director rows; it is not 23 pair calls and does not support a 13/13 claim. Pinned-service coverage is partial (16/24 owners responded, 8 owner-resolution failures; fixture-bearer-only authentication), and `commercial_costs=not_included` means cost unknown, not zero. Preserve the earlier first-attempt 7-row list and HTTP 500/empty-body capture as historical; do not use them for the current counts. Usage response SHA-256: `439eb4c07314ce20bc242adf13af3a2f849fc8b3dc79d6134a9e28210eef42f5`. |
| B06 — one supported local operator action on its valid original Task | **Observed for bounded deterministic operator action, terminal abort, and no delivery.** On the original input-required Task, public command outcomes show invalid pin rejected, observer abort rejected, and owner abort applied as `abort-recorded`; a later read-only same-Task snapshot confirms root and child completed with phase `aborted`, zero A2A result artifacts, and zero deliveries. The child retains three candidate artifact facts as history; no candidate bytes were verified and no accepted-output claim is made. Preserve the earlier public-wait assertion failure as an intermediate failed observation; the cause of the completion lag is unknown. This evidence does not establish paid-inference behavior, browser reconnect, or seamless recovery. No further B06 case is required for this gate. Sanitized evidence: `/tmp/exo-b06-supported-action-evidence-1f7e1a420fda.json` (SHA-256 `65eb65eceb514a7c4664562e414bb7685d8f8a0fa3f8b6be9a77f5814d22ea6d`); raw saved components remain separately preserved at `/tmp/exo-b06-result-1f7e1a420fda.json` and `/tmp/exo-b06-readonly-observation-1f7e1a420fda.json`. |
| B07 — refresh/disconnect/reconnect during execution; reconstruct current state without duplicate items | **Observed-real active refresh, offline readiness, and reconnect selection restoration.** The saved attempt-2 evidence independently asserts `active_refresh_selection_restored=true`, `offline_not_ready=true`, and `manual_reconnect_selection_restored=true`; its saved post-reconnect terminal view has 6 unique run IDs and 15 unique output identities (run plus artifact revision/SHA or receipt ID), independently checked offline. Terminal output de-duplication also has a separate synthetic source assertion, `replaying a known output after snapshot does not add a displayed row`, in `dashboard/test/snapshot-output-parity.test.mjs` (4/4). Retention eviction and service-process restart were not induced. Evidence: `/private/tmp/exo-jev-qa-20261003/basic-second-dashboard-evidence-attempt2.json` (SHA-256 `58461dc5b2999ea56c80555ab074cb7b059b96f2e5e9181eeccba8216d4f626f`); retained Runtime resume/replay remains separate: `/private/tmp/exo-jev-qa-20261003/actual-ws-evidence.json`, `/private/tmp/exo-jev-qa-20261003/runtime-observation-corrected-replay.json`. |
| B08 — Live, Recorded, isolated Demo in one application; local-only Demo commands and no Live playback | **Observed for retained UI isolation and Live playback boundary; no live inference claim.** Corrected no-model same-app browser evidence passed all nine recorded checks: Demo and Recorded controls/rows are present, their rows do not overlap, and the Demo window issued zero requests, POSTs, provider calls, or payment calls. The check captured 165 Demo rows and 3 Recorded rows. The reviewed six-view Live checkpoint separately confirms Live playback controls are absent. The earlier `StalePage` failure is preserved as historical, not overwritten: original `/private/tmp/exo-jev-qa-20261003/b08-demo-recorded-isolation-evidence.json` SHA-256 `8c438bb09f70148cc5465d57f72c6788cdffe9f86020dc75731fb8fadf79483d`; corrected attempt 2 is separate at `/private/tmp/exo-jev-qa-20261003/b08-demo-recorded-isolation-attempt2.json` SHA-256 `b982b7214e23f6e644a2aeab37f5fa937dbd017a2d8fcab85c039d11deb06d07`. Evidence: `/private/tmp/exo-jev-qa-20261003/actual-six-view-runtime-checkpoint-reviewed.json`. |
| B09 — authoritative single-active-job gate rejects a simultaneous second brief before inference | **Observed-real bounded busy rejection with no second Task/context.** The saved attempt-2 record shows an authoritative A2A busy rejection, `no_second_original_task=true`, and the rejection occurring while the first actual Task was active. The browser helper did not observe server-side inference counters, so no direct provider-call-count claim is made. The 30/30 synthetic gate/harness run remains separate supporting source evidence; preserve earlier 9-test and helper-failure history. Evidence: `/private/tmp/exo-jev-qa-20261003/basic-second-dashboard-evidence-attempt2.json` (SHA-256 `58461dc5b2999ea56c80555ab074cb7b059b96f2e5e9181eeccba8216d4f626f`). |
| B10 — allowlisted observation/usage only, no secrets/private histories in browser payloads, leak positive control | **Observed for captured, parsed public UI payloads; original wire whitespace was not retained.** Second-workflow proof validated the snapshot and usage payloads, rejected copied prompt/credential canaries, and confirms no cost/private fields. The usage payload contains 23 rows, including four older historical Director rows. Captured files are canonicalized parsed JSON, not original HTTP wire bytes/whitespace: snapshot 47,751 bytes, SHA-256 `0501b9474382b1f0cadd5a548203bc26157e5374bcdc34ba8497ab8004fc618f`; usage 34,211 bytes, SHA-256 `439eb4c07314ce20bc242adf13af3a2f849fc8b3dc79d6134a9e28210eef42f5`. The saved second snapshot also passed Draft 2020-12 JSON Schema validation with local refs: `/private/tmp/exo-jev-qa-20261003/basic-second-snapshot-jsonschema-evidence.json` (snapshot SHA `0501b9474382b1f0cadd5a548203bc26157e5374bcdc34ba8497ab8004fc618f`, schema SHA `24bdf9f4c68a8314ecc6ba3890711204da11fcfe69523dc4b38de4d83c32a5af`; zero network/model/payment calls). No raw histories were exported; final validation made zero new starts, model calls, or payments. Evidence: `/private/tmp/exo-jev-qa-20261003/basic-second-local-proof-evidence.json` (18/18). |

### Runnable local review path

The retained app is `http://127.0.0.1:47053/qa/login` (PID 42476,
incarnation 21 at this checkpoint). Use its loopback session, then choose Live
and the authorized report factory. Select either completed root or its child
in the Floor run picker; graph selection is independent of the factory-wide
subscription. Review Definition, Board, Decisions/Quality, and Outputs. In
Outputs, select the accepted artifact, **Save to local destination**, reopen
the artifact row, and **Download accepted Markdown**. Agents shows reported
tokens, nullable bindings, partial aggregate coverage, and unknown cost.
Reconnect restores the selected factory/run. Recorded and isolated Demo are
available through the same source picker; Live has no playback controls.

The normal Decisions brief form starts real Luna `xhigh` work. Both workflows
authorized for this milestone have already been executed; this review path
does not authorize a third live submission. The QA session is loopback-only;
production authentication and payment-network readiness are not qualified.

### Floor boundary/group and Send work followup

**Passed the approved presentation/discovery scope, 4 October 2026.** The
completed Basic milestone and historical S01–S33 **8/33**, P01–P10 **0/10**
assessment remain unchanged. No third workflow or new submission was made.

The shared reducer preserves pinned graph IDs/kinds/edges and derives renderer
annotations only: explicit safe Demo `dept` metadata survives; missing grouping
uses named Work/Review/Completion **visual groups**. Parent entry attaches to
`invoke_child`, child entry to `gather`. Declared terminals supply exits; a
release directly connected to a terminal stays inside the floor, avoiding an
exit stub crossing that terminal. Standalone Demo release exits remain present.
Entry attachments do not assert missing conditional edges or execution starts.
Exit is separate from acceptance and delivery. Unlocated artifact facts are
kept in Outputs, and conflicting receipts do not produce a delivered HUD claim.

**Send work** at entry opens the same `appendBriefForm`/`submitBrief` path used
by Decisions, focuses the brief, and shows current-factory context. Recorded
keeps a focusable read-only input, disabled Submit and a visible reason. Live
checks authenticated/current Observation and the existing same-origin
`/health` `readiness.submission_ready` value; failure stays disabled. Source,
factory or view changes close the Floor composer. UI controls are separated
from canvas panning. Opening does not submit. Demo work remains local.

Final proof, serialized with `/tmp/exo-qual-suite.lock`:

- Dashboard Node suite: **86/86 PASS**, zero failures/skips, 418.943333 ms.
  Exact command: `/usr/bin/lockf -k -t 1 /tmp/exo-qual-suite.lock node --test prototype/temporal-factory/dashboard/test/*.test.mjs`.
  Log: `/private/tmp/exo-floor-followup-node-final.log`. These are local
  synthetic regressions, including 8 boundary/model and 3 composer/Demo tests.
  Earlier 85/85 and final focused 11/11 results are preserved separately.
- Browser: **43/43 assertions PASS** against retained Live parent/child,
  Recorded presentation of their saved actual snapshot, and isolated Demo.
  Exact command: `/usr/bin/lockf -k -t 1 /tmp/exo-qual-suite.lock /private/tmp/exo-jev-qa-20261003/.venv/bin/python /private/tmp/exo-jev-qa-20261003/check_floor_followup_final.py`.
  Evidence: `/private/tmp/exo-jev-qa-20261003/floor-followup-browser-final.json`,
  SHA-256 `6f2b383390b8ad5d5bc834d3101a22e7aa4cfea413d676d70a9c79291528d4bc`.
  All four final screenshots were independently reviewed by the lead.
- Source/log/evidence digests and exact commands:
  `/private/tmp/exo-jev-qa-20261003/floor-followup-summary.json`, SHA-256
  `487e526c5dc6ee2a3fe6c492dd0bf6b933158c371babd6012b7f94fe4418d89b`.

The browser used the retained loopback app `127.0.0.1:47053/floor`, isolated
local CDP `127.0.0.1:47832`, and existing pinned Jev/browser-harness direct
no-model controls. No hosted Jev calls were made. Cache was disabled; the
static HTML/modules loaded without a runtime restart. The exact second
workflow Task/context and root/child IDs were matched. Recorded used
`basic-second-snapshot-wire.json` through the existing shared-schema bootstrap
with `frames: []` and snapshot-only provenance; the default historical recording
is not asserted to contain that pair. Its saved canonical public snapshot SHA
is `0501b9474382b1f0cadd5a548203bc26157e5374bcdc34ba8497ab8004fc618f`.

Final outbound audit: **0** A2A message/send, operator commands, workflow-start
capable requests, provider/payment requests, form submit events, runtime
exceptions, console errors, or resource failures. One local `/qa/session` auth
POST and two same-origin `/health` reads are counted separately. No cookie,
credential, request body, raw trace, or screenshot is committed.

Limits: this proves presentation and composer discovery, not another paid
submission or new provider/payment qualification. The current retained Live
session displayed `error` / waiting for a fresh snapshot after resynchronization;
its composer correctly exposed the freshness reason and disabled submission.
That stream status is not reclassified as a new Basic pass. Costs remain
unknown. Production authentication and deferred payment/operations readiness
remain outside this followup.

Failures remain intact: `floor-boundary-composer-syntax-failure.log` (helper
indentation, no browser opened), `floor-boundary-composer-evidence-20261004.json`
(unguarded Alpine evaluation on the login page), and
`floor-boundary-composer-evidence-20261004-attempt2.json` (Demo click timeout).
`floor-demo-opener-diagnostic.json` and `floor-demo-opener-after-fix.json` are
diagnostics, not passes: pointer events hit canvas during Fit. Source inspection
found the Fit transition lasts 0.8 seconds while the helper waited 0.4 seconds.
The bounded Demo-only check with a 1.0-second wait passed in
`floor-demo-opener-settled-fit.json`, before the final full browser pass. All
these evidence files remain outside product source under the same temporary
QA directory; no earlier failure was overwritten or converted into a pass.

Runnable review: open `/qa/login` on the retained local app, then `/floor`;
choose Live, the authorized factory, and either pinned parent/child run. Select
Floor and Fit, then **Send work** to inspect the focused composer and reason.
Do not submit another real brief: the pair allowance is consumed. The normal
source picker provides isolated Demo and read-only Recorded; the exact actual
Recorded snapshot review is reproduced by the bounded helper above.

### Bounded pair preflight and acceptance checklist

- Use the existing Live Dashboard `submit(factory_identity, capability, brief,
  caller_context)` path to the normal `verified-research@1` identity. Do not run
  `single_factory.py`, the historical collector, a new route, or a paid call
  from the Verify lane. The operator has approved the two BASIC workflows; the
  former allowance blocker is obsolete unless a separate exact-source
  operator/provider limit is identified. The per-Task model/deadline ceilings
  below are execution bounds, not an allowance prerequisite.
- Use one pinned published definition for the pair, updating only as directed
  by the Basic gate. Run one is unmodified and must complete real Luna `xhigh`
  work with first-pass real Quality acceptance. Run two uses the existing
  `scenarios/sf_stimuli.json` `route2` case: append its stated unsupported claim
  with `E7` to revision `r1` only. Dashboard submission stays normal; only the
  candidate mutation uses the existing `/_test/stimulus` control on an isolated
  service instance. Label the Quality input as stimulus-modified, not a pure
  model result. The control applies the claim after model output and is
  available only with `test_controls=True` (default `False`, endpoint absent
  otherwise); existing tests cover next-run and revision scoping. Runtime must
  confirm isolated one-shot activation for run two. Quality rejection, synthesis
  repair using its findings, and acceptance
  of the new exact revision must all come from real Luna `xhigh` service calls;
  no forced/scripted Quality verdict.
- Preserve the finite existing bounds: each model-agent Task has at most 3
  calls and a 240-second deadline; the Director has 4 model calls, 4 tool calls,
  and a 90-second deadline; the selected template allows at most 2 repairs.
  Stop at terminal state or deadline. Do not automatically rerun failed work.
- For both Tasks, retain the actual Task/context, run, pinned definition, model
  and effort, assignment/attempt bindings, revision/digest, Quality result,
  provider-reported usage, and safe artifact/evidence references. Verify B04
  receipt against the accepted artifact and downloaded bytes using the correct
  digest field; do not equate a JSON envelope digest with Markdown bytes.
  Preserve unavailable usage fields and unknown costs as unknown.
- During one of the two executions, use the existing authenticated Observation
  snapshot/`observe` cursor path for Dashboard refresh and reconnect; compare
  reconstructed current state and ensure no duplicate visible items. Exercise
  B06 on a valid original Task with one supported operator action. While a job
  is active, submit the simultaneous second brief and confirm the authoritative
  admission rejection occurred before any second model invocation. Capture only
  safe public snapshots/usage and run the existing allowlist checks.
- Coherently load the current page and reducer with browser cache fresh before
  the Dashboard check. Reuse existing Demo/Recorded/Live adapters and shared
  reducer/renderers; verify Demo isolation and no Live playback controls.
  **Historical readiness note (incarnation 19; superseded by the completed
  Basic gate above):** The retained six-view checkpoint is reviewed for terminal-state display; it
  does not clear the B07 reconnect/selection gap. The existing broker refresh
  succeeded with `returncode=0`, `refreshed=true`, `signed_in=true`,
  `expired=false`, and zero model/payment calls, so no broker, auth, or operator
  prerequisite remains. Runtime now reports `submission_ready=true`, 4/4 pins
  ready, and its deterministic active publication at PID 88424/incarnation 19.
  Dashboard must capture the two real workflow results and run-scoped usage;
  isolated one-shot stimulus activation still needs confirmation for run two.
  Evidence: `/private/tmp/exo-jev-qa-20261003/runtime-broker-refresh-status.json`,
  `/private/tmp/exo-jev-qa-20261003/runtime-basic-submission-readiness.json`.
  Verify runs no tests, probes, or paid calls.

**Historical pre-pair blockers (superseded by the Basic evidence above):**
B03 is 1/2 complete. The first workflow has completed on
the original accepted Task with child Quality acceptance at r1; the initial
startup failure is preserved, and continuation added no new start. Workflow 2's
real rejection/repair/new-revision acceptance remains pending, as does complete
run-scoped per-call service usage. B04 local receipt/download proof is still
pending for this workflow: current records say `fixture-received`, not a local
receipt. The route-2 stimulus remains unarmed; read pending owner intent
publicly, then arm only after workflow 1 is terminal. B05's normal Live
caller/display is present, but its usage binding/coverage gaps remain. B06's
same-Task owner abort was rejected and not applied. B07 active-run Dashboard
refresh/disconnect/reconnect checks were skipped; an actual active-run
second-submit rejection for B09 remains unproven. The operator has approved the
two workflows; no separate exact-source external/provider allowance limit was
identified. Existing per-Task call/deadline caps remain execution bounds.

**Current Basic blockers:** none for the demonstrated local scope. Aggregate
usage coverage remains partial and cost unknown. Production authentication,
tariffs/markup, hosting allocation, and payment-network/test environment choices
remain unresolved for later qualification. The first startup failure, initial
waiting snapshot, and usage HTTP 500 remain preserved historical failures;
seamless recovery and the full S/P roadmap are not established by this gate.

## Evidence rules and existing runnable cases

The existing `scenarios/single_factory.py` and
`evidence/single-factory/codex-subscription-3.json` record a historical live
`gpt-6-sol` run. `SINGLE_FACTORY.md` records 24/24 checks and a passing final
byte attestation for that attempt. It demonstrates the old runtime’s live
research, synthesis, Quality, repair, exhaustion, and caller-prompted abort
routes. Its defects were induced; delivery used the HTTP fixture. It does not
qualify the new dashboard or the required Luna `xhigh` configuration. The
historical `observed-real` label remains unchanged and scoped to that run.

`scenarios/integrated.py`, `scenarios/replay_check.py`, and
`evidence/integrated-observed.json` provide earlier runtime/replay evidence.
`scenarios/spike_a_delayed.py` and `scenarios/spike_b_two_instances.py` provide
fixture-backed delayed-agent and same-home recovery evidence, recorded in
`QUALIFICATION.md`. These records partially inform S17 and S23; they do not
prove dashboard observation, commercial enforcement, or current model settings.
`scenarios/sf_attest.py` provides safe evidence-file scanning and a synthetic
positive control. It does not establish the current public payload allowlist;
that bounded S33 assertion is recorded in the operational matrix below.

`docs/design/exomachina-floor.html` still contains deterministic seeded demo
scenarios for graphs, jobs, capacity, queue, price, spend, supplier,
nested-factory, translation, approval, and repair. Its source now imports the
shared dashboard contract/reducer and recorded/demo/live adapters; the Node
test source includes checks for those references, synthetic demo/recorded
adapter behavior, and the floor disabling playback in live mode. Demo data is synthetic. The
recorded bundle is historical `gpt-6-sol` evidence with a fixture release and
partial graph coverage. The browser pass verifies three historical recorded
routes and Demo isolation only; it does not prove live parity or factory
operation. `docs/factory-maintenance.md` explicitly
records no maintenance runtime; its M01–M08 and R01–R05 scenarios are unrun.

The current combined Python regression run passed 248 tests in 45.239 seconds
with `OK`, under `/tmp/exo-qual-suite.lock`, from
`prototype/temporal-factory` with `PYTHONDONTWRITEBYTECODE=1` and the pinned
runtime Python. The handoff gave this command with the pinned executable path
elided:

```sh
/usr/bin/lockf -k /tmp/exo-qual-suite.lock env PYTHONDONTWRITEBYTECODE=1 [pinned runtime python] -B -m unittest discover -s tests -v
```

The earlier combined Dashboard and broker Node regression passed 31 tests
(12 Dashboard and 19 broker), 0 failures, in 13.217 seconds under the shared
lock:

```sh
/usr/bin/lockf -k /tmp/exo-qual-suite.lock node --test dashboard/test/dashboard.test.mjs broker/test/broker.test.mjs
```

The previous combined Dashboard Node suite passed 49/49 under the shared lock
after receipt/usage/incident parity edits. One stale static expectation
requiring Live `selectRun(null)` rejection was replaced with assertions for the
selected adapter's scoped public `snapshot(factory, runId)` and
`observe(factory, cursor, runId)` calls; playback guards remain covered.
The latest full changed-surface Dashboard-only suite passed 75/75, zero
failures, in 380.925041 ms under the shared lock. Exact command:
`/usr/bin/lockf -k -t1 /tmp/exo-qual-suite.lock node --test prototype/temporal-factory/dashboard/test/*.test.mjs`.
This includes the final receipt-conflict parity test and Board/readiness checks;
it is synthetic unit-regression evidence only, with no browser, model, or
payment run. Evidence: `/private/tmp/exo-jev-qa-20261003/basic-dashboard-node-75-evidence.json`.
The earlier 74/74 run in 362.57 ms predates the additive receipt-conflict flag
and remains historical, as do the focused 4/4 parity-file result, 65/65, and
90/91 suite checkpoints.

The historical coherent combined suite passed 65/65, exit 0, under the shared
lock before the later reducer historical-correction clock change. It includes
wait/snapshot parity, coverage counters, wait-command binding, both stale-owner
regressions, and the two Dashboard incident tests. This remains historical
suite evidence. Sanitized lead checkpoint with source digests:
`/private/tmp/exo-jev-qa-20261003/dashboard-node-checkpoint.json`.
Exact command, from repository root:

```sh
/usr/bin/lockf -k -t 1 /tmp/exo-qual-suite.lock node --test prototype/temporal-factory/dashboard/test/*.test.mjs
```

Preserve the earlier 58-test coherent pass as historical. The interim 63/65
run failed because its QA fixture lacked required `factory.name`; that fixture
was corrected before the final run. The incident reducer's complete-row
replacement/current-snapshot normalization had three focused passing tests.

The normal Outputs Save UI browser check was rerun after the guard changes and
again passed ten assertions using the same evidence path below; it reused the
actual receipt and reported no resource errors, model calls, or payments.

The actual per-agent usage browser evidence passes and verifies all eight
provider rows, their per-call labels, five service identities, retained null
bindings, unknown cost, and bound receipt/artifact bytes. The operator summary
calls it 21 checks; its JSON contains 22 check entries, all true. Attempt 1 is
preserved as a QA-only failure for a missing safe-label prefix; the final
assertion checks the actual label suffix, with no product change. The final
browser pass reports zero console/resource failures, model calls, or payments.
Evidence: `/private/tmp/exo-jev-qa-20261003/actual-per-agent-usage-browser-evidence.json`.
Exact command:

```sh
/usr/bin/lockf -k -t1 /tmp/exo-qual-suite.lock /private/tmp/exo-jev-qa-20261003/.venv/bin/python -B /private/tmp/exo-jev-qa-20261003/check_actual_per_agent_usage.py
```

Both commands ran from `prototype/temporal-factory`. The lead reports
`git diff --check` was clean at the Node regression handoff. These results are unit, synthetic transport, and
historical-replay evidence only; no fresh model calls were made. Earlier counts
are historical: full Python suite 233; standalone Dashboard Node 10/10 and
broker Node 19/19; and this lane's earlier focused qualification suite 11. None
qualifies operational rows. Static review had found the recorded artifact
manifest accepted task_id and context_id outside the shared manifest allowlist,
and the Store bridge routed Demo decide/sendChat through live Task handlers
even though Demo snapshots have no run Task. The page lead reports final source
fixes: Demo uses `submit({text})` and displays an actual local-only result, Demo
decisions have a separate local gate, and WebSocket decision commands no longer
require an A2A submission endpoint. Source switching no longer leaves stale
Demo visibility; rerenders preserve brief draft and focus; text-editing keys
work while Live playback shortcuts remain blocked. Page syntax checks pass. The
31-test combined Node regression predates these final changes; an earlier
Dashboard-only locked run passed 13 tests after the Demo assignment fix. The
prior locked Dashboard Node suite passed 33/33 tests after reducer aggregation,
pin-guard, and outcome-regression changes. It used no sockets. The known Demo
multi-job parity gap remains open; no broader parity or live claim follows from
that suite.
The fresh no-model browser revalidation passed as described below; it does not
exercise actual-run export or the pinned-run picker.
External Chromium
(PID 67488, CDP 47832) and static floor port 47830 are now available. The
Dashboard browser checks and native Jev browser scenario have passed in their
narrow recorded-data scope, detailed below. The factory route below ran once;
the browser checks did not reuse or represent that inference.

Commerce previously reported a focused 17/17 suite under the shared lock. Its
two-process local acceptance case admitted one reservation and rejected the
competing reservation, then reconciled late usage and a credit after restart
under synthetic local terms; it reported zero external payment calls. This
demonstrates local ledger behavior only. Runtime and Observation focused tests
are reported as synthetic TestClient/tracer coverage; they do not establish
full operational qualification. The separate limited loopback Observation
smoke is recorded below. The owned qualification suite's previous 11 passing
tests are deterministic unit evidence, not row qualification.

The latest Commerce handoff reports 33 passing locked focused synthetic tests
across the usage journal, Director, model agent, and broker. Journal records
retain provider-reported usage categories plus call, Task, message, model, and
effort attribution. Run and definition bindings remain null until actual IDs
exist; subsequent calls bind only to actual run IDs. The authenticated GET
/usage/measurements endpoint supports run_id, task_id, and action_id filters.
These tests exercise synthetic attribution, retrieval, persistence/replay, and
unavailable-versus-zero usage. They do not establish a fresh Luna inference or
live cost proof; cost remains undisclosed and is not visible. An earlier
Commerce handoff recorded this model-suite command from
prototype/temporal-factory:

    /usr/bin/lockf -k /tmp/exo-qual-suite.lock
      /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python
      -B -m unittest discover -s tests -p 'test_model*.py' -v
They do not establish live Luna usage or qualify S11.

## Fresh-home operational smoke attempt — 2026-10-03

This bounded attempt targeted the S09 Director deadline/stale-response path and
S23 restart path using deterministic scripted services only. It made no
inference/provider calls or payments and did not reach a Temporal workflow, a
deadline assertion, or a restart assertion. The runner was invoked under the
shared suite lock with the pinned Python environment; the initial loopback
WebSocket handshake stopped because Uvicorn had no WebSocket backend. A
temporary install of `wsproto==1.3.2` and `h11==0.16.0` under
`/private/tmp/exo-operational-smoke-wsproto-20261003` enabled the real loopback
handshake and initial snapshot. No repository dependency was changed.

Three subsequent deterministic start attempts, also serialized under the
shared lock, failed before Temporal workflow creation. Preserved supervisor
logs show PostgreSQL `initdb` exited 1 with `FATAL: could not create shared
memory segment: Operation not permitted`. The harness then reported
`JSONDecodeError` at `harness.py:451` because the fixture tried to parse the
plain-text tool failure as JSON; this parse error masked the PostgreSQL failure
and was not the root cause. The lead reports a fixture fix that returns a JSON
failure envelope for `ToolResult` status `error`; its two focused tests passed.
That fix improves failure reporting but does not resolve sandbox PostgreSQL
shared-memory denial at that time; no further starts were run in that attempt.
Runtime later reports that the PostgreSQL permission blocker is resolved and
the exact v1.32.0 SQL schemas installed from the official pinned source.

A subsequent fresh-home deterministic retry passed as `observed-synthetic`,
recorded in `/private/tmp/exo-operational-smoke-result-retry-a6dcc8006701.json`.
It observed Temporal timer expiry, original parent/child Task bindings (two
run rows), original wait recovery across engine/harness restart with advanced
incarnation, public WebSocket snapshot/resume, and a late Director response
received then rejected on the original Task. No release or delivery occurred.
Temporal server and CLI versions/digest were verified and ports were checked
ephemerally. The smoke made no inference or payment calls. The earlier timeout
failure remains preserved as `be1c3b1a9f89`; the retry raised the temporary RPC
bound from 15 to 150 seconds for lazy startup. This earlier failed attempt
remains non-qualifying. The subsequent observed-synthetic retry evidence
qualifies S09's deterministic runtime deadline/stale-response behavior; S23 is
partial pending its replay usage/charge assertion.

In the earlier failed attempt, the Director SQLite store had one committed open
start-intent row written before runner startup; that row alone was not evidence
of a Temporal run, and public Observation exposed no Task binding. Harness ports
were closed after that cleanup. Fresh synthetic homes and logs remain under
`/private/tmp/exo-operational-smoke-*` for diagnosis. The Observation regression
suite previously passed 28 tests in 5.441 seconds after its SQLite migration fix, including
12 transport tests and populated-projection reopen/cursor coverage. These are
synthetic tests, not S09/S23 qualification.

## Runtime/Observation public-interface smoke — 2026-10-03

After the Runtime config-mtime and Observation empty-graph fixes, QA Uvicorn
restarted at `http://127.0.0.1:47831/floor` and remains intentionally running
(PID 64071 at handoff). The reported public-interface checks returned same-origin
session `POST 200`, authenticated `/discover 200` with one factory, accepted
WebSocket handshake, and schema-v1 snapshot with `active_publication: null`,
empty graph, zero runs, and cursor prefix `c1`. Unauthenticated `/discover`
returned 401; session issuance with `X-Forwarded-For` returned 403; public
`/dashboard-assets/contract.mjs` returned 200 (40,147 bytes). This earlier
empty-factory smoke verifies the empty snapshot while no publication is active;
it did not verify reconnect or cursor-gap recovery. The later retained-instance
WS smoke now supplies partial S28/S29 evidence (see the matrix). This Runtime
smoke did not verify browser presentation or live factory execution. No
inference/provider call, payment, or browser interaction occurred in this
Runtime smoke. The scripted check is
`/private/tmp/exo-runtime-public-smoke.py`, sanitized evidence is
`/private/tmp/exo-runtime-public-smoke-evidence.json`, and the detailed runtime
handoff is `/private/tmp/exo-v2-runtime-qa-20261003/runtime-handoff.md`.

Under the shared lock, the follow-up suite passed 25 tests: 17 Observation and
8 Runtime/Observation tests, including empty-publication snapshot, populated DB
reopen, config-touch restart, and command replay cases. These remain synthetic
test evidence, not full S28 qualification; the earlier standalone 28-test
Observation result is historical. At handoff, the QA instance reported
Temporal paths configured, runner stopped, no unfinished runs, author provider
unconfigured, verification unchecked, live inference false, commercial
reporting unreported, costs unknown, and payment adapter unconfigured. Runtime
traced the previous snapshot failure to `SourceContractError: pinned publication
graph facts are unavailable` in `observation.py:531`; Observation now treats
absent graph facts as an empty graph, and the existing factory discovery record
remains valid and unchanged.

### Runtime Operations smoke attempt — incarnation 14 (historical failure)

The later retained instance on `127.0.0.1:47053` (PID 77565, factory
`a40ec20b-2787-438a-8d11-11dd9ccecc16`, incarnation 14) was only partially
healthy. Health, authenticated capabilities, and the empty incident list
returned 200; unauthenticated Operations endpoints returned 401, and unknown or
foreign incident reads returned 404. Authenticated `GET /deliveries` returned
500 and its WebSocket returned `observation_unavailable`, so no fresh snapshot
was captured. The retained log attributes the failure to a closed-workflow
query against a drained worker deployment with no pollers (`harness.py:1042`).
The closed-bound-run guard is present on disk and two focused guard/principal
scope tests passed, but this serving PID predates that edit and was not
restarted for this evidence. Runtime reported restoring port 47053; this
record is the earlier failed attempt, not current readiness. No model, payment,
or incident POST occurred. The result is preserved at
`/private/tmp/exo-jev-qa-20261003/runtime-observation-failure-incarnation14.json`
and `/private/tmp/exo-jev-qa-20261003/runtime-operations-public-smoke.json`;
the contemporaneous status is `/private/tmp/exo-runtime-current.md`. That smoke
did not itself create an incident. Subsequent public evidence captured this
actual observation fault, displayed the report/owner acknowledgement and claim,
and exercised permitted escalation; those later assertions and the earlier
immediate-snapshot failure are detailed in S24/S25 below.

Runtime has since restored QA at PID 93364, incarnation 16. The bounded
publication-context reader used one `PublicationStore.active()` read and
`verify_closure` to return the current manifest and Quality-policy digests;
the record reports zero writes, model calls, and payment calls. This is reader
evidence only, not an Improvement-flow qualification. Evidence:
`/private/tmp/exo-jev-qa-20261003/runtime-publication-context-read.json`.

A separate narrow HTTP artifact-integrity record is available at
/private/tmp/exo-jev-qa-20261003/recorded-http-evidence.json. Local HTTP on
127.0.0.1:47830 returned a 72-frame recorded bundle and both referenced
artifacts with HTTP 200. The received artifacts were 1536 and 1639 bytes and
matched their exact SHA-256 manifest values:
6640e407b95d3487f4def51b2ee29c63ecb28b861a00de186deb4153be7666a3 and
2ff2d9e050d378f0a85473db2128551b8a42135f792f579d147cb8f98653f670. This is
recorded HTTP artifact-integrity evidence only; it is not browser QA, new
factory inference, or a distinct destination delivery receipt.

## Test-driver provider override and bounded QA

The operator-approved Jev test-driver override uses the native OpenRouter
Decisions endpoint https://openrouter.ai/api/alpha/decisions with requested
model typesafe/jev-1.13. A TypeSafe endpoint or TypeSafe credential is not a
prerequisite. This override applies only to the temporary browser-harness
decision agent; product factory inference remains gpt-6-luna at xhigh, and no
product inference was run. The operator reports OPENROUTER_API_KEY is available
through Phase; this qualification record contains no credential value.

The temporary tooling checkout is pinned to
1231850a0bf1a0c0341fe408ef1668dbbfdfac46, with browser-harness 0.1.13. The
reviewed provider patch SHA-256 is
035455eb91dbcebd5593d8eeca58b9f1e1a16e78af35cc423db6cbf875ac0a9f. Its
request fixes the endpoint and model and sets provider.allow_fallbacks=false.
The response model is checked against an exact allowlist containing the
requested ID and the operator-approved dated ID
typesafe/jev-1.13-20260917; it does not accept arbitrary model prefixes. The
adapter validates the selected operation and requires its target answer; the
existing selection path then validates that target against the candidates for
that operation. There is no Jev provider fallback. The credential is passed
only in the Authorization header; transport errors are generic, and provider
errors log neither response bodies nor credential values. The separate,
pre-existing TYPE_TEXT helper has its own configured text-model request and is
not a Jev Decisions fallback.

The supplied sanitized evidence record reports 15 offline adapter checks
passing and a successful bounded provider-contract probe with zero browser
mutations. The separate provider probe was not rerun. The record is at
`/private/tmp/exo-jev-qa-20261003/sanitized-evidence.json`. The bounded browser
runner allowed 10 steps, a 20-second Dashboard bootstrap wait, at most 15 HTTP
requests including retries and text-helper requests, and 150 seconds total. It
drops unrelated inherited environment values and removes
`OPENROUTER_API_KEY`/`TEXT_MODEL_API_KEY` around Browser/Agent construction so a
persistent harness daemon cannot inherit Phase credentials. Its report contains
no credential values.

The initial browser-harness doctor command was:

```sh
BH_HOME=/private/tmp/exo-jev-browser-harness .venv/bin/browser-harness --doctor
```

At that earlier check it reported browser-harness 0.1.13, no running local
connection, and optional Cloud auth unavailable but not required. The separate
no-model guard command was:

```sh
BH_HOME=/private/tmp/exo-jev-browser-harness BU_CDP_URL=http://127.0.0.1:47832 .venv/bin/python scripts/check_guards.py
```

It failed before Browser creation because CDP was refused: 0 checks passed and 0
model calls occurred. This model-guard command did not pass; the result is
recorded in the sanitized evidence file.

Browser blockers were subsequently lifted with operator Chromium PID 67488 on
CDP 47832 and the local floor restored on port 47830. The earlier direct
Dashboard browser record reported 21 DOM/CDP checks passed with 0 model calls.
It covered controls and state for Floor, Board, Decisions, Outputs, Definition,
and Agents; Demo source, three recorded routes and terminal outcomes; artifact
digest display; Demo/Recorded isolation; hidden Live replay controls; and
shortcuts that do not replay. The UI displayed SHA-256
`6640e407b95d3487f4def51b2ee29c63ecb28b861a00de186deb4153be7666a3` for 1536
bytes; there were no console or resource failures. Evidence:
`/private/tmp/exo-jev-qa-20261003/dashboard-browser-evidence.json`.

The bounded native Jev browser scenario also passed. It made 3 native Decisions
calls (`SELECT`, `CLICK`, `DONE`), 0 text calls, returned the approved pinned
model `typesafe/jev-1.13-20260917`, and ended with independently asserted
Recorded source, Outputs view, and 3 historical runs. This is test-driver
browser QA, not product factory inference. Evidence:
`/private/tmp/exo-jev-qa-20261003/jev-browser-evidence.json`.

```sh
BH_HOME=/private/tmp/exo-jev-browser-harness .venv/bin/browser-harness --doctor
```

It reported browser-harness 0.1.13, no running local connection, and optional
Cloud auth unavailable but not required. The no-model guard command was:

```sh
BH_HOME=/private/tmp/exo-jev-browser-harness BU_CDP_URL=http://127.0.0.1:47832 .venv/bin/python scripts/check_guards.py
```

It failed before browser creation because the local CDP connection was refused:
0 guard checks passed and 0 model calls occurred. This is not a passed model
guard result. Evidence details are in
`/private/tmp/exo-jev-qa-20261003/sanitized-evidence.json`.

### Latest no-model Dashboard browser revalidation — 2026-10-03

After the latest run-picker/role/Demo source changes, the browser check passed
all six views, three Recorded outcomes, both artifact digest controls, Demo
isolation, and absence of Live playback buttons/shortcuts. It reported no
console/resource failures and zero model calls. It did not exercise actual-run
export or the pinned-run picker. Evidence:
`/private/tmp/exo-jev-qa-20261003/dashboard-browser-evidence.json`.

Exact command, run from `/private/tmp/exo-jev-qa-20261003`:

```sh
/usr/bin/lockf -k -t 1 /tmp/exo-qual-suite.lock .venv/bin/python check_dashboard.py
```

## Observed-real factory route — 2026-10-03

The Runtime-owned fresh-home route 1 completed using the normal
`codex-subscription` provider with `gpt-6-luna` at `xhigh`. The collector labels
the record `observed-real`; its overall status is `fail` because required
collector routes and checks remain incomplete. Route 1 itself was accepted and
checks R1-a through R1-e pass. The record contains 13 actual model calls:
authoring 4, Director 4, findings 2 (one validation retry), risks 1,
synthesis 1, and Quality 1. The retry was a findings validation retry, not an
induced defect. No raw request or response excerpts are reproduced here.

The caller Task and context are recorded and bound to one run with the pinned
factory identity and definition metadata. The findings and risks branches have
separate live Task/context identities on the same child run; their execution
intervals overlap, both complete before synthesis starts, and the synthesis
input records both branch artifact digests. This supports S01 and S03 from this
observed-real route. Evidence:
`/private/tmp/exo-sf-evidence-4d63084518b4401ca6148cfcf2f9a262/codex-subscription-4d63084518b4401ca6148cfcf2f9a262.json`.

Independent live Quality accepted first revision `r1` against the exact
synthesized artifact, SHA-256
`7bae3754857b4d939cacc7bff0dfcee21ada84fc68e93f1c899fc9f01abf859b`.
At that route checkpoint the configured release was `http-release (fixture)`.
The later actual local POST/GET receipt and browser-verified download qualify
S04's accessible local-delivery requirement; the earlier fixture release label
remains historical. The saved report is
`/private/tmp/exo-sf-evidence-4d63084518b4401ca6148cfcf2f9a262/codex-subscription-4d63084518b4401ca6148cfcf2f9a262-route1.md`, SHA-256
`128f23ac691fd0299b3efb999363c29858713c4ec3a6b011ef45b926ccd38b36`.

The full collector remains failed: only route 1 of its declared three routes is
present, and SF-2 import-boundary check regressed. Its original G4 file-count
expectation and failed result are preserved as historical collector output.
The qualification runner now has a separate inventory-scoped assessment:
required routes come from `scenario_inventory.route_ids`, and the G4 export
count comes from `scenario_inventory.declared_exports`, not a global minimum.
A one-route record cannot pass a three-route inventory. G4 also requires the
declared exports to be present and scanned, zero findings, redaction success,
and a detected positive control. Missing exports make leak status
inconclusive; they are never treated as evidence of no leak. The prior scan
reported zero hits, redaction pass, positive control detected, and four found
files, but it does not override the collector's overall fail or qualify other
routes. No inference rerun was performed.

## Operational smoke matrix

| ID | Capability | Status | Existing evidence / remaining proof |
| --- | --- | --- | --- |
| S01 | Brief submission and identity | Qualified — observed-real | Route 1 recorded the original caller Task and context bound to one run, the normal `codex-subscription` factory identity, and actual `gpt-6-luna` `xhigh` model/effort. R1 checks pass. Evidence: `/private/tmp/exo-sf-evidence-4d63084518b4401ca6148cfcf2f9a262/codex-subscription-4d63084518b4401ca6148cfcf2f9a262.json`. |
| S02 | Pinned graph and readouts | Qualified — observed-real browser and public export | The latest actual-factory browser evidence passes original Task binding, exact three assignment-attempt readouts, five actual agent bindings, displayed child definition/manifest/package/build pins, pinned-service contract, and manifest version. The v3 child attestor independently verifies the fresh schema-v1 snapshot/bundle, complete graph pins, and same-original-Task/context parent/child binding. The earlier parent-only browser record still has a failed digest check against its serving source at `/private/tmp/exo-jev-qa-20261003/actual-parent-browser-evidence.json`; it remains historical and is not rewritten. Evidence: `/private/tmp/exo-jev-qa-20261003/actual-delivery-usage-browser-evidence.json` and `/private/tmp/exo-verify-v3-child-attestation.json`. |
| S03 | Parallel work and join | Qualified — observed-real | Route 1 recorded separate live findings and risks Tasks with distinct Task/context identities on the same child run. Their execution intervals overlap; both complete before synthesis starts, and synthesis input references both exact branch artifact digests. R1 checks pass. Evidence: `/private/tmp/exo-sf-evidence-4d63084518b4401ca6148cfcf2f9a262/codex-subscription-4d63084518b4401ca6148cfcf2f9a262.json`. |
| S04 | First-pass acceptance | Qualified — observed-real with local-only delivery | Live Quality accepted the exact synthesized artifact at first revision `r1`. The actual local receipt passed POST/GET; the latest eight-assertion browser proof confirms it projects through the shared public snapshot/reducer with exact Task/context, hashes, and writer timestamp; the 1600px HUD says “Saved locally,” old receipts are not counted as recent, and the child has no copied receipt. Earlier harness attempts 1–6 remain preserved as harness-assertion failures on stale-page, responsive breakpoint/case, or missing snapshot events; the final public proof passes with no console/resource failures and zero model/payment calls. Delivery is local-only, not a remote customer destination. Evidence: `/private/tmp/exo-sf-4d63084518b4401ca6148cfcf2f9a262-observation-factorywide-v2/delivery-proof.json`, `/private/tmp/exo-runtime-delivery-observation.json`, `/private/tmp/exo-jev-qa-20261003/actual-receipt-projection-browser-evidence.json`, and `/private/tmp/exo-jev-qa-20261003/actual-delivery-usage-browser-evidence.json`. |
| S05 | Rejection and repair | Unqualified | The new Luna xhigh route had one findings validation retry and no induced defect; Quality accepted first revision. It does not prove rejection and repair. Historical induced-defect evidence used `gpt-6-sol`; requalify the required route through the Observation Interface. |
| S06 | Exhaustion and abort | Unqualified | The new Luna xhigh route did not reach exhaustion or test a dashboard command lifecycle; historical caller-prompted abort evidence remains insufficient. |
| S07 | Extra repair | Unqualified | The Director wait path supports abort after exhaustion only; no finite extra repair budget or separate spend authority is implemented. |
| S08 | Human escalation | Unqualified | The human public mount is pending; no public human resolution path is bound to the original durable Task, and the current Director decision tool supports abort only. No S08 claim is made. |
| S09 | Wait expiry and stale response | Qualified — observed-synthetic runtime behavior | The fresh-home retry observed the configured Temporal timer expire, restart recovery on the original binding, and a late Director response received then rejected through the A2A/Observation path, with no release or delivery. Agent work was deterministic scripted work; no inference ran. This satisfies S09's deadline/stale-response runtime requirement and makes no model-inference claim. Evidence: `/private/tmp/exo-operational-smoke-result-retry-a6dcc8006701.json`. |
| S10 | Decision lifecycle and concurrency | Unqualified | No public-interface race evidence shows conflicting commands resolve once with an authoritative outcome. |
| S11 | Inference attribution | Unqualified | The public aggregate and actual per-agent browser proof confirm eight complete provider-reported token rows across two scoped runs: research_findings 2, research_risks 1, synthesizer 1, quality 1, Director 3; all use `gpt-6-luna` `xhigh`. The browser renders all eight exact rows and per-call labels, five service identities, Task/run attribution, and the legacy nulls. The five assignment rows have null `assignment_id`/`attempt_id`; all three Director rows have `message_id`, while `assignment_id`/`attempt_id` are null. No `authoring_overhead` row is present. Bound authoring and Director coverage report available with no failed, rejected, or conflicting queries; unbound authoring is unavailable because shared model-home factory exclusivity is unproven, and unbound Director is unavailable without an authoritative factory run. Eight of eight pinned-owner queries responded, but use `fixture_bearer_only` and remain `fixture_only`, not live owner proof. `commercial_costs` is `not_included`; cost is unknown, not zero. This remains 8 visible rows, not 13/13 actual-call visibility; browser proof does not resolve the null/unbound/legacy gaps, so S11 stays unqualified. Adapter review found no drop: `_assignment_usage_bindings` forwards only explicit nonempty IDs and ModelAgent retains them. Future genuine runs must carry workflow patch `exo-explicit-assignment-bindings-v1` to mint those IDs; preserve existing null rows and do not retrofill. Offline workflow replay adds no execution or attribution. Evidence: `/private/tmp/exo-jev-qa-20261003/runtime-aggregate-usage.json`, `/private/tmp/exo-jev-qa-20261003/actual-per-agent-usage-browser-evidence.json`, `/private/tmp/exo-verify-v3-child-attestation.json`, and `/private/tmp/exo-jev-qa-20261003/actual-delivery-usage-browser-evidence.json`. |
| S12 | Hosting and markup | Unqualified | Mock prices only; hosting basis, explicit markup, and reproducible charge remain unqualified. |
| S13 | Reservations and budget limits | Unqualified | Commerce's synthetic local two-process acceptance test reports one reservation winner, one rejection, and restart reconciliation; no Runtime admission writer or real terms are wired. |
| S14 | Usage charging and credits | Unqualified | The synthetic local ledger test reconciles late usage and a credit after restart, but no measured provider usage or configured commercial terms feed the obligation. |
| S15 | Capacity and admission queues | Unqualified | Commerce reports three synthetic module tests, including 10-process enqueue at capacity 3 (3 admitted, 7 queued) and a six-process release/enqueue race. Source and service-capacity implementation are assigned to independent lanes, but the module is not mounted in Runtime; no authoritative capacity source, over-capacity queue, or slot-release Interface evidence exists. S15 remains unqualified. |
| S16 | Shared-agent contention | Partial — mounted Service Interface component with fixture authentication | The normal owned `services/model_agent.py` CLI/public HTTP check passed five assertions: capacity-2 QA service declares `factory_id` in its public contract, unknown-factory GET returns 404, unauthenticated capacity read returns 401, and the unknown-factory GET leaves queue-file bytes and mtime unchanged. It used an isolated `/private/tmp` service/admission state and scripted provider, with zero Task submissions, model calls, or payments; the subprocess was stopped after the check. The contract declaration is one check, not qualification by schema. No two physical factory workflows shared the service, so S16 remains partial pending real shared-contention evidence. Evidence: `/private/tmp/exo-jev-qa-20261003/service-capacity-cli-evidence.json`. Exact check command: `pinnedTemporalPython -B /private/tmp/exo-jev-qa-20261003/check_service_capacity_cli.py`. |
| S17 | Versions in flight | Unqualified | Earlier runtime evidence partially covers v1/v2 pinning; route 1 does not verify dashboard version readouts, contract rates, or concurrent old/new publication behavior. |
| S18 | Nested factory and fan-out | Unqualified | Current nesting is a Temporal child workflow, not supplier-style nested A2A fan-out; no nested supplier Task smoke exists. |
| S19 | Nested outcome unknown | Unqualified | No nested A2A Task response-loss path preserves an unknown outcome and reconciles the original Task without resubmission. |
| S20 | Approval and send-back | Unqualified | The Director route is abort-only; no policy/human send-back path creates a new revision and re-review before an authorized decision. |
| S21 | Small graph without a gate | Unqualified | Translation is a mock; no real no-gate contract and `not applicable` acceptance result. |
| S22 | Artifact lineage and delivery | Partial — observed-real artifact and local receipt | The v3 attestor verifies the exact child `r1` Quality JSON report envelope (3502 bytes, SHA-256 `7bae3754857b4d939cacc7bff0dfcee21ada84fc68e93f1c899fc9f01abf859b`) against the public artifact, manifest, and accepted Quality row. The browser-verified 2029-byte Markdown download (SHA-256 `128f23ac691fd0299b3efb999363c29858713c4ec3a6b011ef45b926ccd38b36`) is the exact Markdown field inside that validated envelope; the separate hashes and byte lengths are expected. The local receipt is proven, but the fresh run contains only accepted `r1`: same-Task prior/current revision lineage and public access to the prior artifact are not evidenced. S22 remains partial. Evidence: `/private/tmp/exo-verify-v3-child-attestation.json`, `/private/tmp/exo-sf-4d63084518b4401ca6148cfcf2f9a262-observation-factorywide-v2/delivery-proof.json`, and `/private/tmp/exo-jev-qa-20261003/actual-delivery-usage-browser-evidence.json`. |
| S23 | Worker/harness restart | Partial — observed-synthetic plus observed-real local-delivery retry and offline actual-history replay | The fresh-home synthetic retry passed engine/harness restart, incarnation advance, recovery of the original wait, parent/child Task bindings, and WebSocket snapshot/resume; it did not assert replay without additional usage/charge. A separate actual local-delivery retry PASS shows authenticated POST of the same accepted run/revision/hash returned `duplicate:true`, GET returned the same single original-writer receipt, and destination bytes/mtime were unchanged with the 2029-byte Markdown verified. No model or payment ran; unknown cost is not evidence of zero cost. Separately, the SDK Replayer fetched read-only actual Temporal root/child histories (14/41 events); changed `FactoryRun` replay passed under original build `b-2279e954f1a9`. The retained proof was refreshed after the latest guards and records zero workflow mutations; this remains offline compatibility evidence, not a new workflow execution or usage attribution. Four synthetic UUID/node-propagation envelope tests also passed. Neither result establishes in-flight delivery replay across restart or no-extra-usage/charge replay. S23 remains partial. Evidence: `/private/tmp/exo-operational-smoke-result-retry-a6dcc8006701.json`, `/private/tmp/exo-jev-qa-20261003/actual-local-delivery-replay-proof.json`, and `/private/tmp/exo-jev-qa-20261003/retained-workflow-replay-proof.json`. |
| S24 | Incident, alarm, and acknowledgement | Qualified — observed-real captured observation fault | The actual incarnation-14 Observation failure (`observation_unavailable` plus authenticated delivery-list 500) was reported through the public Operations interface. The initial browser proof showed the exact incident/evidence, claimed loopback owner `fixture-operator`, owner/kind/state, and no false resolution label; acknowledgement/claim did not resolve it. After escalation, the final seven-check browser proof showed the exact incident inspector, `Owner unreported` with no stale claimed owner, no false closure, and no console/resource failures. The initial immediate-snapshot QA attempt remains preserved; later snapshot convergence passed. This was an actual Observation/Runtime fault, not a synthetic agent failure. No model, payment, or execution command. Evidence: `/private/tmp/exo-jev-qa-20261003/runtime-observation-failure-incarnation14.json`, `/private/tmp/exo-jev-qa-20261003/actual-incident-browser-evidence.json`, `/private/tmp/exo-jev-qa-20261003/actual-incident-escalation-evidence.json`, `/private/tmp/exo-jev-qa-20261003/actual-incident-escalated-browser-evidence.json`, and preserved attempt `/private/tmp/exo-jev-qa-20261003/actual-incident-public-attempt1.json`. |
| S25 | Maintenance | Qualified — observed-real permitted escalation path | The same claimed incident followed the permitted escalation path: normal escalation HTTP 200 and final state `escalated`, not closed. A first QA request using free-text reason was rejected by the reason enum; corrected `runtime_verification_required` passed. Final evidence retained the fault-evidence digest and independently asserted that the actual receipt, usage, Task/Quality state, and workflow were unchanged; no workflow restart, model, or payment occurred. The final seven-check browser proof shows the escalated incident with `Owner unreported` and no stale claim. This qualifies the spec's escalation alternative only; recovery was unavailable and is not claimed. The operator was the loopback fixture identity. Evidence: `/private/tmp/exo-jev-qa-20261003/actual-incident-escalation-evidence.json`, `/private/tmp/exo-jev-qa-20261003/actual-incident-browser-evidence.json`, and `/private/tmp/exo-jev-qa-20261003/actual-incident-escalated-browser-evidence.json`. |
| S26 | Improvement | Unqualified | `FactoryOperations` has bounded candidate/evaluation/promotion-request state. Observe implemented the atomic `publication_context(factory_id)` Operations contract, returning only `manifest_digest` and `quality_policy_digest`; 14 focused Operations tests pass. Runtime's real per-factory reader was verified against the active manifest and Quality-policy pins with one active read and zero writes; evidence: `/private/tmp/exo-jev-qa-20261003/runtime-publication-context-read.json`. `record_candidate_validation` records evidence without performing validation, and a promotion request records intent rather than publishing. A bounded validator, independent evaluation, and future-only authorized publication have not been demonstrated, so S26 remains unqualified. Ownership: Observe owns the protocol/consumer; Runtime injects the reader backed by the existing factory's `Director.module.publications.active()` and returns exactly the two approved digests from one active record. Do not resolve through shared `home`, open a database, construct another Director, call Runner, or publish in this read seam. |
| S27 | Optional research program | Unqualified | No bounded campaign runtime or disabled/enabled control path protects Quality and promotion. |
| S28 | Snapshot, reconnect, and gaps | Partial — observed-real public WebSocket | The retained-instance read-only smoke authenticated two observers and confirmed equal snapshot state; exact-cursor resume; replay from an earlier cursor of one append-only Quality CloudEvent with CloudEvent framing; and an unmapped cursor response `resync_required`/`retention_expired` followed by a fresh snapshot. It also checked auth gates, foreign-factory denial, and unchanged terminal execution. It issued zero execution commands, model calls, or payment calls. Actual retention eviction and duplicate-event de-duplication during refresh/disconnect transitions were not induced/asserted, so S28 remains partial. Evidence: `/private/tmp/exo-jev-qa-20261003/actual-ws-evidence.json`; script: `/private/tmp/exo-jev-qa-20261003/check_actual_ws.py`. Exact invocation: `/usr/bin/lockf -k -t1 /tmp/exo-qual-suite.lock env PYTHONPATH=/private/tmp/exo-v2-runtime-qa-deps /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B /private/tmp/exo-jev-qa-20261003/check_actual_ws.py`. |
| S29 | Multiple observers and slow consumer | Partial — observed-real observer isolation | The same retained-instance smoke confirmed two independent observers initially received equal state and that closing the first left the second open; foreign-factory access was denied and terminal execution remained unchanged. It did not induce slow-consumer overflow or verify resynchronization under that condition, so S29 remains partial. Evidence: `/private/tmp/exo-jev-qa-20261003/actual-ws-evidence.json`. |
| S30 | All dashboard views | Partial — observed-real browser view/record checks | The latest Demo/Recorded browser check passed all six views, three routes, digest UI, Demo isolation, hidden Live playback controls, and no console/resource failures; model calls 0. The actual-factory browser check also selected all six views with the root/child snapshot loaded and verified child Definition graph selection plus child Quality and exact artifact digest in Outputs, in Live and Recorded modes; model calls 0. The normal Outputs Save UI browser check reran after guard changes and again passed ten assertions using the same evidence path, reused the actual receipt via duplicate POST, and reported no resource errors, model calls, or payments; this covers Outputs only. The actual-factory script still asserts view selection for Floor, Board, Decisions, and Agents without verifying every required actual record/control in each, so full S30 proof remains incomplete. Evidence: `/private/tmp/exo-jev-qa-20261003/dashboard-browser-evidence.json`, `/private/tmp/exo-jev-qa-20261003/actual-factory-browser-evidence.json`, and `/private/tmp/exo-jev-qa-20261003/actual-outputs-save-browser-evidence.json`. |
| S31 | Demo isolation and parity | Unqualified | Latest no-model browser evidence rechecked Demo isolation and three Recorded outcomes after run-picker/role/Demo source changes; no model calls or console/resource failures. Fixture-backed Live checks displayed rejection truthfully and created no run. Historical locked Dashboard Node suite passed 33/33 after reducer aggregation, pin guard, and outcome regression changes. The final combined 65/65 suite includes wait/snapshot parity, coverage counters, wait-command candidate-binding, and incident reducer behavior. The known Demo multi-job parity gap remains pending additional evidence. Evidence is local Demo/Recorded/fixture behavior, not full factory execution. |
| S32 | Live presentation | Unqualified | The actual-factory browser check confirms Live playback buttons and shortcuts are inactive while showing the retained root/child run; it made zero model calls. This does not cover actual ongoing-state freshness, camera follow/pan/zoom, Calm mode, and accessible-panel behavior together on a live factory run. Earlier camera and authenticated Live interaction evidence is fixture-backed. Remote deployment auth remains unselected. |
| S33 | Observation secrecy | Qualified — current authenticated public payloads and copied leak controls | The current authenticated WS snapshot is tied to Runtime PID 93364/incarnation 16 (two runs; SHA-256 `9f83d3bf5db76014c01acb6137ba095840f6ab4fb545d76cece2a11aa714c28c`) and passed `dashboard/contract.mjs validateSnapshot`; a copied in-memory prompt canary was rejected. Observe made one authenticated read-only `GET /usage/measurements`; the exact 9,142-byte wire body (SHA-256 `2bab59bbad9748ea11742c113789fea90531b52098e4d17df73e4ea5423d7653`) passed `dashboard/usage.mjs validateMeasurementsResponse` with eight rows, and a copied in-memory credential canary was rejected. Its eight rows equal the retained wrapper rows, with matching canonical row digest `a2eb963018f649a29c8062d2e86303b343bc1594f5ff86a809c0c6511ca98893`; the wrapper's metadata mismatch remains preserved as an input-capture error, not a product defect. No raw histories were requested or sent as browser payloads; no workflow/model/payment calls occurred. The optional separate JSON Schema validation was not run; this qualification is for the existing strict public-interface validators and their copied-payload positive controls. Evidence: `/private/tmp/exo-jev-qa-20261003/runtime-operations-ws-snapshot.json`, `/private/tmp/exo-jev-qa-20261003/runtime-usage-wire-body.json`, and historical wrapper rejection `/private/tmp/exo-jev-qa-20261003/actual-public-payload-allowlist-evidence.json`. The one-shot capture was run under the shared lock and its temporary runner removed afterwards. |

## Payment and outcome matrix

| ID | Capability | Status | Existing evidence / remaining proof |
| --- | --- | --- | --- |
| P01 | MPP metered session | Unqualified | No selected method/network or metered-session evidence. |
| P02 | x402 usage scheme | Unqualified | No selected scheme/facilitator/network or bounded usage-settlement evidence. |
| P03 | Protocol-specific fixed charge | Unqualified | No qualified fixed-charge profile. |
| P04 | Purchase binding | Unqualified | No offer/payee/assignment/amount/payment-reference binding evidence. |
| P05 | AP2 authorization mapping | Unqualified | No pinned trust/schema, signed mandate verification, or negative-case evidence. |
| P06 | Nested authority | Unqualified | No local child-spend authority or nested budget smoke. |
| P07 | Payment concurrency/retry | Unqualified | No payment idempotency/restart race evidence. |
| P08 | Settlement uncertainty | Unqualified | No lost-response, held-reservation, and reconciliation evidence. |
| P09 | Failure and refund/credit | Unqualified | No authorization decline, interruption, cancellation, refund, or credit evidence. |
| P10 | Outcome charging | Unqualified | Later profile; no acceptance/delivery-bound payment evidence. |

No payment network, asset, facilitator/relay, wallet custody, trust setup,
funding ceiling, or settlement policy is selected here. No rate, markup, hosting
allocation, or fee treatment is invented. Profile-specific payment qualification remains blocked until
those choices and the Commerce-owned Adapter contract are configured and
authorized. The public seam and declarations now exist, but source declarations
alone do not configure a profile or assert network readiness.

## Independent gap review

Several narrow checks do not require TypeSafe credentials, product factory
inference, or payment terms. Source and offline contract checks can verify
public A2A/Observation declarations, adapter wiring, schema/allowlist behavior,
and no-network preflight. Synthetic local Commercial tests can exercise
reservation races, restart, late usage, and credit accounting under explicitly
synthetic terms. A safe-export positive control can use synthetic canary data.
The lead-owned no-model browser pass has verified local Demo/Recorded controls
and six views without invoking a factory model. The earlier 21-check record
and fresh post-change revalidation are linked in the browser handoff above.

These checks still do not qualify operational rows without their required
public-Interface evidence. S09's deterministic configured-deadline and
stale-response runtime behavior is qualified by the observed-synthetic retry;
this is not model proof. Concrete gaps that remain independent of secrets and
commercial/network choices are: the Director wait is abort-only (S07/S08/S20);
no runtime capacity/admission source (S15), bounded improvement/publication
path (S26), or campaign runtime (S27) is present; S25 is qualified through its
permitted escalation branch only, with no recovery claim. Nested work
uses a Temporal child workflow rather than supplier A2A, with no original-Task
reconciliation for a lost nested response (S18/S19); the actual local-only
receipt is proven, but S22 lacks same-Task prior/current revision lineage and
public access proof for the prior artifact; authenticated public WS observer isolation now has
partial smoke evidence, while actual retention eviction, slow-consumer
overflow, and combined live presentation proof remain pending
(S28/S29/S32); remote deployment auth is unselected; S31 still has the known Demo multi-job parity
gap despite latest browser revalidation; S33's current public snapshot and
usage allowlists and copied positive controls now pass. Commercial rate,
hosting, markup, and payment-profile choices remain unresolved and are not
filled in by this review.

## Preflight and environment

`scenarios/dashboard_qualification.py` is a no-network preflight/evidence
validator. It fails closed when public interface artifacts, references to all
three dashboard adapters in the floor page source, the declared dashboard test
file, or pinned runtime paths are missing.
It does not start processes, dispatch live inference, invoke payment Adapters,
read secrets, or inspect Temporal histories. It requires the
explicitly configured `EXO_TEMPORAL_RUNTIME` and `EXO_TEMPORAL_CLI`; it does not
fall back to old documented paths or search/install replacements. For the
Commerce-owned payment seam, it checks for a public Adapter declaration and
literal MPP, x402, and AP2 v0.2 profile declarations with profile/version,
operation, environment-prerequisite, and status metadata. A passing source
check still reports `network_ready: false`, no configured profiles, and P01–P10
unqualified.

Run the preflight with the pinned Python and explicit operator-provisioned
Temporal paths. It verifies the installed executable binaries by exact path
and executable SHA-256 and reports the release-archive digests separately; it
does not run their version commands or start them. It still prints a blocked
report and exits 2 while a runtime prerequisite is missing or live-model
readiness is unresolved. A present Node test file is reported as available;
the preflight checks its presence but does not execute it. An evidence
candidate is validated offline with
`--validate-evidence <safe-record.json>`; a valid record can be proposed for
review, but this command never changes a matrix verdict.

### Bounded public-export attestation

The runner can also attest a fixed, bounded set of files exported from public
Interfaces. It never opens a runtime database, Temporal history, provider
payload, browser profile, or payment endpoint. The input directory contains
`snapshot.json` (Observation v1), `bundle.json` (Dashboard recorded/demo
envelope with that exact snapshot), `usage.json` (the public
`/usage/measurements` response), `task-bindings.json` (safe parent/child refs
from public A2A Task responses), and `artifacts.json` (a manifest of files
inside `artifacts/`). JSON inputs are limited to 8 MiB; artifact files are
limited to 16 MiB each. An optional `collector-summary.json` must already be a
sanitized inventory summary. The runner hashes but never renders artifact
bytes.

The attestation checks pinned graph digests and graph structure, and records
Task/run relationships without assuming every Temporal child run has a new
A2A Task/context. A delegated A2A child Task must have distinct identities;
continuation of the original Task/context across a distinct Temporal child run
must explicitly identify that relationship and match the exported snapshot.
It correlates assignment attempts and accepted Quality to the exact
run/revision/SHA-256, and verifies artifact bytes against both the public
artifact record and manifest. Usage records are allowlisted to non-financial attribution and token
categories. A reported quantity of `0` stays `reported: 0`; unavailable
quantities stay `unavailable: null`; null run/definition bindings stay null.
The output includes separate source-envelope digests, artifact-byte digests,
and generated attestation JSON-envelope and Markdown-report digests. It writes
only the safe projection, no source path or artifact content, and always sets
`qualification_status: unqualified`. Demo/fixture sources stay synthetic;
historical `gpt-6-sol` stays partial. Passing this offline check is review
material only, not a new smoke.

Factorywide public-export checkpoint (2026-10-03): the supplied directory
contains `snapshot.json`, `bundle.json`, `result.json`, `artifact-refs.json`,
and `usage-summary.json`. Snapshot/bundle hashes are `a16b479b47b9d2127d1457865a29b1cb7609e66a193f5b4da2d469aefeccdca7` and
`71980811c6d1fcf48b5b706636eb2b93f99e95d26ec7d50943c490740760fbb4`;
`result.json` is `f8a9cad816d8a5777595d52f0e3c7aa2627c5499c81017eed3603d276fdca1e6`,
`artifact-refs.json` is `94bc20d02657b7171071d20e13f0aa9ab4315978f208653aa537005e1b3fe094`,
and `usage-summary.json` is `e9aa3953bf4e56221bdc2dc33430274e35eaaadb2075cd4fcee95c68c0c3b408`.
Snapshot carries two runs sharing the original Task/context; the child has a
Quality row. The runtime `result.json` reports `status: pass`, fresh/schema
validation, one route only, two factory graph nodes, one Quality record, two
artifact refs, and verified reference digests. Both `r1` refs identify SHA-256
`7bae3754857b4d939cacc7bff0dfcee21ada84fc68e93f1c899fc9f01abf859b`. It is a
summary report, not independent byte or usage attestation. It does not
replace the original three-route collector failure; that failure remains
preserved above.

The exact attestor invocation against that directory exited 2 and rejected
the input because these canonical safe files are absent: `usage.json`,
`task-bindings.json`, and `artifacts.json`. `usage-summary.json` contains an
aggregate `measurement_count: 0` and no per-call records, so it does not prove
zero use or cost. `artifact-refs.json` carries URLs and digests but no local
artifact bytes. Runtime should export: (1) `usage.json` with the sanitized
`/usage/measurements` per-call rows and coverage, preserving unknown versus
reported-zero categories; (2) `task-bindings.json` mapping the root and child
run IDs while explicitly recording that both use the same original
Task/context (do not fabricate a distinct child A2A Task), with the child row's
`parent_run_id`, `run_id`, and `relationship: "same_original_task_run"`; and (3)
`artifacts.json` rows `{run_id, revision, sha256, file}` plus the referenced
public bytes under `artifacts/`. The prior parent-only browser digest failure
is retained; the later actual-factory browser pass is separately recorded in
S02/S30. Neither browser report nor `result.json` is substituted for this
offline attestation. Command used (root run selected):

```sh
env PYTHONDONTWRITEBYTECODE=1 \
  /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python \
  -B scenarios/dashboard_qualification.py \
  --attest-public-exports /private/tmp/exo-sf-4d63084518b4401ca6148cfcf2f9a262-observation-factorywide-v2 \
  --run-id a40ec20b-2787-438a-8d11-11dd9ccecc16.d18e065039051331a98d
```

The v2 missing-input rejection above is historical. The v3 canonical export
contains all five required files and exact child artifact bytes. The existing
offline attestor was run under `/tmp/exo-qual-suite.lock` for child run
`a40ec20b-2787-438a-8d11-11dd9ccecc16.d18e065039051331a98d:child:82c5bfe305ea`;
it exited 0 with `candidate-for-review` and `qualification_status: unqualified`.
All attestor checks passed, including same-original-Task/context parent-child
binding, complete graph pins, current Luna xhigh usage attribution, and accepted
Quality/artifact/manifest/byte correlation. The child `r1` artifact is 3502
bytes with SHA-256 `7bae3754857b4d939cacc7bff0dfcee21ada84fc68e93f1c899fc9f01abf859b`.
The export contains three complete provider-reported Director rows, but
coverage is partial and cost is not included; the attestor never changes a row
verdict. The bundle retains its historical fixture fact separately; a fresh
public WS snapshot and the eight-assertion browser proof show the actual
local-only receipt projected through the shared snapshot/reducer. The final
browser proof passes; earlier harness assertion failures remain preserved.
Sanitized outputs:
`/private/tmp/exo-verify-v3-child-attestation.json` and
`/private/tmp/exo-verify-v3-child-attestation.md`; output digests are
`9038258a47740d9203aa3065709d756c8e33bc27ae08eabfa33172a92e874ed4` and
`a61b5b1e1d39fafd8dea2b266219d475a3cabca7a075bc7bfd656baa783c7763`.

```sh
PINNED_PYTHON -B scenarios/dashboard_qualification.py \
  --attest-public-exports /path/to/public-export-dir --run-id RUN_ID \
  --attestation-output /private/tmp/run-attestation.json \
  --markdown-output /private/tmp/run-attestation.md
```

The safe collector projection may be assessed independently with
`--assess-collector-summary <collector-summary.json>`. It must enumerate
`scenario_inventory.route_ids` and `scenario_inventory.declared_exports`,
then give `observed_route_ids`, `present_exports`, `scan_paths`, `leak_hits`,
`redaction_pass`, and `positive_control_detected`. The computed G4 minimum is
the number of declared exports. A complete route set and a complete scan of
that set are both required; the assessor reports the collector's original
status unchanged beside its derived scope assessment.

```sh
EXO_TEMPORAL_RUNTIME=/private/tmp/exo-v2-temporal-9u041h3e/server \
EXO_TEMPORAL_CLI=/private/tmp/exo-v2-temporal-9u041h3e/cli/temporal \
/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python \
  -B scenarios/dashboard_qualification.py
```

The shared Dashboard contract lists this Node adapter test command. The file is
present. Its earlier pre-fix run passed 10 tests and is historical. A locked
Dashboard-only run later passed 13 tests; a prior locked Dashboard Node suite
passed 33/33 after reducer aggregation, pin guard, and outcome regression
changes. The current 57-test combined suite is recorded above.
The latest no-model browser revalidation also passed after the run-picker and
Demo source changes. The known Demo multi-job parity gap remains unqualified:

```sh
/usr/bin/lockf -k /tmp/exo-qual-suite.lock node --test dashboard/test/dashboard.test.mjs
```

Run it from `prototype/temporal-factory`:

Environment state recorded for this lane:

| Prerequisite | Recorded state |
| --- | --- |
| Python | Pinned Python 3.12 environment is present. |
| Node/npm | Present on PATH. |
| PostgreSQL | PostgreSQL 16 `initdb`, `pg_ctl`, and `postgres` are present. Runtime reports the earlier shared-memory permission blocker resolved and the official Temporal v1.32.0 SQL schemas installed. The subsequent synthetic S09/S23 retry passed; Runtime executed the sole authorized fresh real route, with no rerun. |
| Temporal Server | Operator-verified `server.tar.gz` release-archive SHA-256: `f95748376241f5941327fa4c4e8e76641e8c4a9acabf77de9c86eb3d8238f4d7`. Installed executable: `/private/tmp/exo-v2-temporal-9u041h3e/server/temporal-server`, SHA-256 `f1663788fd4d8d702576b659db212b4d45a8dbfc0909eb7f7a86d0d442de6a60`, version v1.32.0. The runner pins and verifies the executable digest separately from the archive digest. |
| Temporal CLI | Operator-verified `cli.tar.gz` release-archive SHA-256: `41e0425378fcb4fb5766340b97435e20fe47bbff2d7bf644ec2d51f7662b7c56`. Installed executable: `/private/tmp/exo-v2-temporal-9u041h3e/cli/temporal`, SHA-256 `33c298d41f6eefebf01a28a3511f444480e939e21d3518cb0c12a4abad467c4b`, version v1.9.1, reports Server v1.32.0. The runner pins and verifies the executable digest separately from the archive digest. |
| Runtime test configuration | For non-live runtime tests only, explicitly set `EXO_TEMPORAL_RUNTIME=/private/tmp/exo-v2-temporal-9u041h3e/server` and `EXO_TEMPORAL_CLI=/private/tmp/exo-v2-temporal-9u041h3e/cli/temporal`. These variables are otherwise unset. |
| Former documented paths | `/tmp/exomachina-countertrials/temporal/runtime` is a directory containing `config` and `pg`, not a runtime executable. No Temporal binary was found there or in the checked Homebrew/PATH locations. The documented CLI path `/tmp/exomachina-temporal-evaluation/temporal` is absent. No replacement is selected implicitly. |
| Model configuration | EXO_AUTHOR_PROVIDER and EXO_AUTHOR_MODEL remain unset in the ambient shell; CLI `--provider codex-subscription` selects the existing path. The single operator-authorized `gpt-6-luna` `xhigh` route 1 completed with the existing collector; sanitized evidence and its incomplete overall collector result are recorded above. No inference rerun was performed. An earlier broker status attempt failed with `fchmodSync EPERM` and a masked config error; the latest broker report is `signed_in=true`, `expired=false`. |
| Dashboard authorization | Loopback principal adapter selected for local QA; Runtime/Observation public auth checks passed. The actual-factory browser check used retained authorized live factory records and made zero model calls; its row-specific coverage is recorded in S02/S30/S32. Remote deployment auth remains unselected. |
| Payment configuration | Network, asset, facilitator, custody, trust, limits, and settlement choices are unresolved. |
| Ports | The earlier preflight found shared ranges clear. Current browser QA uses the restored static floor on 47830 and operator Chromium PID 67488 on CDP 47832; recheck before any new run. |

The last recorded no-network preflight ran at 2026-10-03T14:21:49.238528Z,
before route 1, and recorded that default custom broker status had not
established readiness or that Luna xhigh access/support had not been verified
by a fresh live smoke. It does not treat an unset
EXO_AUTHOR_PROVIDER as an operator policy blocker: the normal factory CLI
accepts --provider codex-subscription, and the operator has authorized a fresh
Luna xhigh workflow once broker readiness is established. The preflight
records the pending status without querying the broker, inspecting credentials,
or treating Codex CLI login as evidence. Ambient EXO_AUTHOR_PROVIDER and
EXO_AUTHOR_MODEL are unset. It verified both installed Temporal executable
digests; A2A, Observation, dashboard page/adapters/test, and Commerce
declarations are available. The Observation transport is mounted, all three
dashboard adapters are referenced by the floor page source, and the Node test
file is present. It finds the Commerce-owned Payment Adapter seam and MPP,
x402, and AP2 v0.2 declarations. All declared profiles remain unconfigured;
network_ready is false. Shared port ranges are clear. Ambient
EXO_TEMPORAL_RUNTIME and EXO_TEMPORAL_CLI are unset; the Temporal values are
supplied explicitly in the commands above. The preflight itself does not confer
row qualification; the current matrix reflects later evidence: S01–S33 stand
at 8/33 qualified (S01–S04, S09, S24–S25, S33), with S16, S22, S23, S28, S29, and S30 partial, and P01–P10
remain 0/10. The preflight reads source files and hashes the explicitly
configured runtime binaries; it does not read runtime databases or start
services.

After that preflight, an earlier normal default-broker status attempt failed
before readiness: `fchmodSync` returned `EPERM` on the model-broker path, with
the failure masked by a broker configuration error. The latest reported broker
status is `signed_in=true`, `expired=false`. Runtime reports the PostgreSQL
shared-memory permission blocker resolved and the exact v1.32.0 SQL schemas
installed from the official pinned source. The observed-synthetic retry
qualifies S09 runtime behavior and partially supports S23. Runtime/Observation
restarted with operator-authorized Luna xhigh sessions, and the single route 1
run has completed; see the observed-real evidence section above. Its collector
still fails on missing routes, G4 file-count coverage, and the SF-2 import
boundary. No inference rerun is needed for this assessment.

Historical port blocks overlap across runner, testbed, broker mocks, and unit
tests. Suite runs must serialize with the documented lock:

```sh
/usr/bin/lockf -k /tmp/exo-qual-suite.lock env PYTHONDONTWRITEBYTECODE=1 \
  /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python \
  -B -m unittest tests.test_dashboard_qualification -v
```

Latest result: 19 tests passed in 0.283s. The suite checks the archive/executable digest split,
evidence-label honesty, public-export snapshot/bundle/Task/run/usage/artifact
correlation, output hash separation, collector inventory scope, and fail-closed
preflight/declaration behavior. It creates fixtures only under `/tmp`, makes
no network calls, starts no runtime, and uses no service ports.

The qualification suite uses no shared service ports. A later runtime smoke
requires a fresh port allocation from the lead and a rechecked listener report.

## Direct test approach and remaining work

The owned tests cover matrix completeness, fail-closed prerequisite handling,
Commerce payment-declaration shape without network-readiness inference,
evidence-label honesty (including historical `gpt-6-sol`), public-export
correlation and privacy limits, separately hashed artifact/envelope/Markdown
outputs, and route-scoped collector completeness. They use no network or
provider/payment credentials.

The floor page source references all three adapters and the declared Node
adapter test file is present. The combined Dashboard/broker regression passed
31 tests before the final page fixes; the prior locked Dashboard Node suite
passed 33/33. The fresh no-model browser revalidation passed six views and
three Recorded outcomes. The actual-factory browser check additionally loaded
root and child runs, verified distinct pinned graphs, child Quality, and the
exact artifact digest in Live and Recorded, and passed with zero model calls.
Its assertions still leave the all-view record checks and exact
pin/version/contract/readout requirements incomplete. The factorywide bundle
has two runs and one child Quality record, but independent usage and
artifact-byte attestation inputs are absent.
Qualification can
exercise only the normal A2A identity, dashboard
`discover`/`snapshot`/`observe`/`inspect_artifact`/`command` operations,
Commercial Interface operations, and an explicitly configured and authorized
payment profile. Every live row needs actual smoke evidence with model/effort
where applicable, graph and binding digests, contract versions, Task/attempt
identities, observed usage status (unknown stays unknown), artifact digests, and
safe evidence references. Concurrency claims require overlapping requests.
Fixture/demo checks retain their synthetic labels and cannot qualify live rows.
One fresh gpt-6-luna xhigh workflow was operator-authorized through the existing
CLI --provider codex-subscription path and has completed; its evidence is
recorded above. No duplicate inference run was made. Payment runs remain
separately unresolved and outside this lane.

## Blockers

- Direct Dashboard browser evidence passes six views, three historical
  Recorded routes, camera zoom/drag, ticket follow, Calm mode, Demo/Recorded
  isolation, artifact digest UI, and Live playback/shortcut checks. Camera and
  fixture-backed Live checks had zero model calls and no exceptions; the Live
  fixture rejection displayed truthfully and created no run. The native Jev
  browser scenario passed with three Decisions calls and no text calls. The
  later actual-factory browser pass used retained authorized root/child records,
  verified selected graph/Quality/digest behavior, and made zero model calls;
  its incomplete canonical row coverage is recorded in S02/S30/S32. Remote
  deployment auth remains unselected.
- The installed Temporal executable digests now verify separately from the
  operator-verified release-archive digests. `EXO_TEMPORAL_*` remain unset in
  the shell; the preflight and unit commands above supply them explicitly.
  Fresh-home S09/S23 starts previously stopped before Temporal workflow
  creation because sandbox PostgreSQL denied shared-memory creation; Runtime
  now reports that permission blocker resolved and the official Temporal
  v1.32.0 SQL schemas installed. The later observed-synthetic retry qualifies
  S09 runtime behavior and partially supports S23. The separate observed-real
  route is documented above and is not required for S09's deterministic behavior.
- The Commerce Adapter seam and MPP/x402/AP2 v0.2 declarations exist, but all
  profiles remain unconfigured and `network_ready` is false. Network, asset,
  facilitator, trust, wallet custody, and settlement choices remain unresolved;
  P01–P10 stay unqualified.
- The sole authorized fresh `gpt-6-luna` `xhigh` route 1 has completed; its
  `observed-real` evidence and collector failures are recorded above. No rerun
  was made. The latest broker report is `signed_in=true`, `expired=false`;
  Runtime/Observation are restarted with Luna xhigh sessions. An earlier
  `fchmodSync EPERM`/masked broker-config status failure is retained as
  historical environment evidence, not the current broker status.
- Commercial terms and payment environment choices remain unresolved; P01–P10
  cannot run.


### Final resumed evidence handoff (3 Oct 2026)

The real loopback Runtime check passed session issuance/discovery (HTTP 200),
an authenticated schema-v1 WebSocket snapshot with zero runs, null publication,
and empty graph, unauthenticated discovery rejection (401), and forwarded
session rejection (403). Exact bounded client and sanitized evidence:
`/private/tmp/exo-runtime-public-smoke.py` and
`/private/tmp/exo-runtime-public-smoke-evidence.json`. The successful probe was
not rerun solely to create these pointers. This is actual local public-Interface
evidence, with no model calls, not browser or factory-workflow qualification.
The post-fix Observation/Runtime suite passed 25 tests; the earlier combined
Python regression passed 248 tests.

The final page integration fixes preserve a brief draft and cursor across
Observation rerenders, permit normal text-editing keys while blocking Live
playback shortcuts, separate Demo local decision gates from Live gates, pass
Demo text to its local API, prevent repeated Demo work from collapsing identical
assignment IDs, and separate WebSocket decision readiness from A2A submission
endpoint readiness. Both inline scripts pass syntax checks. Earlier Dashboard
Node runs passed 13 locked tests; the prior locked Dashboard Node suite passed
33/33. The
fresh no-model browser checks pass in the recorded/demo/fixture scope described
above. The browser evidence is at
`/private/tmp/exo-jev-qa-20261003/dashboard-browser-evidence.json` and
`/private/tmp/exo-jev-qa-20261003/jev-browser-evidence.json`. The Dashboard
browser record independently verifies both artifact buttons/digests: SHA-256
`6640e407b95d3487f4def51b2ee29c63ecb28b861a00de186deb4153be7666a3` for 1536
bytes and `2ff2d9e050d378f0a85473db2128551b8a42135f792f579d147cb8f98653f670`
for 1639 bytes. The updated camera record asserts `follow_moves_camera: true`.

Additional camera/follow evidence at
`/private/tmp/exo-jev-qa-20261003/camera-browser-evidence.json` passed zoom,
real pointer-drag pan, ticket-follow state, `follow_moves_camera: true`, Calm
mode, and no-exception checks with zero model calls. The fixture-backed Live browser record at
`/private/tmp/exo-jev-qa-20261003/live-browser-evidence.json` passed authenticated
session login, empty WebSocket snapshot, hidden playback controls, enabled
initial brief without a run, preserved spaces/draft, truthful fixture rejection,
no run creation, and no-exception checks with zero model calls. Fixes also
include single-run ticket follow and truthful A2A Message error display. This is
fixture-backed QA, not Live factory execution.

Pinned tooling and exact commands are in
`/private/tmp/exo-jev-qa-20261003/RESUME.md` and
`/private/tmp/exo-jev-qa-20261003/sanitized-evidence.json`.
The initial doctor check, before operator Chromium launch, ran
`BH_HOME=/private/tmp/exo-jev-browser-harness .venv/bin/browser-harness --doctor`
and reported browser-harness 0.1.13 with no running local connection; optional
Cloud auth was unavailable but not required. The separate no-model guard command
`BH_HOME=/private/tmp/exo-jev-browser-harness BU_CDP_URL=http://127.0.0.1:47832 .venv/bin/python scripts/check_guards.py`
failed before Browser creation because CDP was unavailable: zero guard checks
passed and zero model calls occurred. That model-guard command never passed;
the earlier direct Dashboard check was a 21-check DOM/CDP run and passed with
zero model calls. A fresh post-source-change revalidation is recorded above.
The later native Jev browser run made three Decisions
calls, zero text calls, and was not a factory workflow. Factory broker now
reports `signed_in=true`, `expired=false`. Runtime/Observation have been
restarted with the configured Luna xhigh sessions; the PostgreSQL permission
blocker is resolved and the official Temporal v1.32.0 SQL schemas are
installed. The observed-synthetic retry qualifies S09 runtime behavior and
partially supports S23. The observed-real route supports S01 and S03; the
latest 17-check actual-factory browser evidence plus v3 export attestation
supports S02; the actual-receipt projection browser proof further confirms S04.
The full
collector failed as detailed above; no inference rerun was performed. S22
remains partial because only accepted `r1` is present, so same-Task
prior/current revision lineage and prior-artifact public access are unproven;
the accepted JSON envelope and its Markdown field correctly have distinct
hashes and byte lengths. Runtime reports the local-delivery Observation
projection source function landed, with focused tests and restart pending. The retained-instance
WS smoke adds partial S28/S29 coverage; S30 remains partial because its full
all-view record/control requirements are not asserted. Current row counts are
S01–S33: 8/33 qualified, with S16, S22, S23, S28, S29, and S30 partial; P01–P10:
0/10 qualified.

## 4 October 2026 — bounded Floor communication and draft restoration

This follow-up preserves the completed Basic evidence and all earlier failures. It
adds no workflow, inference, paid Jev call, operator command, or payment. The S/P
roadmap counts above are unchanged.

Implemented in the shared renderer/model:

- Live and Demo drafts remain editable while Submit is unavailable. Drafts are
  held in memory by source/factory and survive view changes, composer reopening,
  source round trips, and observation reconnect. Recorded remains focusable and
  read-only. The existing authorized submission path and gates are unchanged.
- Director references, declared human routes, and presentation-only navigation
  have distinct rendering. Navigation says “View link · no message”; it does not
  establish authorization, escalation, activity, or incident ownership. The Human
  inbox and independent Incident view open the existing Decisions projection.
  Reported incident bindings/owner are retained; absent origin remains unreported.
- Inbox state uses the shared freshness-aware model. A fresh selected run with
  incomplete factory history does not establish an empty/current factory inbox.
  The Floor retains usable unavailable-state navigation when no graph is present;
  explicit grid rows prevent a hidden job rail collapsing the Floor. Replay
  updates preserve inbox/incident heading focus.
- Renderer kinds now preserve nested-factory appearance and render assignment
  nodes without undefined kind labels. Demo work timestamps retain the scenario
  time base, and its explicit human-wait fixture is labeled synthetic. These are
  bounded fixes, not full historical Demo parity.

### Source freshness and remaining backend limitation

Retained harness PID 88778/incarnation 22 is at `http://127.0.0.1:47053/floor`.
Authenticated factory/root/child snapshots use the public Observation Interface.
Only exact Temporal NOT_FOUND histories are excluded; other RPC failures fail
closed. Selected-run coverage comes from actual child-start linkage and successful
reads, captured with source records and committed atomically with projection state
and cursor. A cursor checkpoint never upgrades source freshness. Scoped metadata
is validated by Python, strict JSON Schema, and JS; lists are unique and bounded.

The second Basic root and child are fresh and retain their original Task/context
and definition/package/manifest/build pins. Factory-wide status remains
`disconnected`: the older first-Basic root
`a40ec20b-2787-438a-8d11-11dd9ccecc16.d18e065039051331a98d` and child
`a40ec20b-2787-438a-8d11-11dd9ccecc16.d18e065039051331a98d:child:82c5bfe305ea`
return WorkflowNotFound. Their immutable evidence was not deleted or rewritten.
New submission remains disabled while factory coverage is incomplete. Health's
`submission_ready=true` is structural readiness; `verification_status=not_checked`
and `live_inference_ready=false` do not prove provider readiness.

Runtime evidence: `/private/tmp/exo-jev-qa-20261003/runtime-scoped-freshness-public-proof.json`,
SHA-256 `3536fb651d0b77205e6ec31bc675cbd5beadc29d93d0a445cfc876221494937f`.
Load evidence is `runtime-scoped-freshness-load.json` in the same directory.
Only the owned harness was restarted; runner/services were retained.

### Exact verification and evidence

From the repository root:

```sh
/usr/bin/lockf -k -t 1 /tmp/exo-qual-suite.lock node --test prototype/temporal-factory/dashboard/test/*.test.mjs
/usr/bin/lockf -k -t 1 /tmp/exo-qual-suite.lock /private/tmp/exo-jev-qa-20261003/.venv/bin/python /private/tmp/exo-jev-qa-20261003/check_brief_draft_interactions.py
/usr/bin/lockf -k -t 1 /tmp/exo-qual-suite.lock /private/tmp/exo-jev-qa-20261003/.venv/bin/python /private/tmp/exo-jev-qa-20261003/check_default_recorded_inbox.py
```

Dashboard: **102/102 pass**, log `/private/tmp/exo-floor-communications-node-final-102.log`.
Python locked suites: Observation **33/33**, Runtime Observation **24/24**, scoped
freshness **8/8**, plus the focused schema regression **1/1**. These tests are
synthetic source/contract regressions, not new real factory execution.

No-model CDP browser: **73/73 assertions pass** against retained real parent/child,
Recorded with an explicitly injected retained snapshot, and isolated Supplier Demo.
Physical pointer focus and real keyboard events receive text; browser-native
`Input.insertText` receives the paste-style marker (no OS clipboard/Ctrl-V claim).
Draft retention/isolation, scoped fresh reconnect, graph/pin stability,
Director route-model/DOM parity, inbox/incident navigation, Recorded read-only input,
and distinct communication/navigation legends pass. Live Submit is disabled.
Evidence `/private/tmp/exo-jev-qa-20261003/brief-draft-interaction-evidence-20261004-attempt7.json`,
SHA-256 `f311ef17806743ea692e7d5fe0284f9c59e77ec065ba7467ea9e2bf891989c5c`.
Screenshots in that record were independently inspected. No rows were observed for
current selected waits/incidents; unavailable factory inbox coverage is not proof
that none exist.

Normal default Recorded (no injected replacement): **5/5 assertions pass** for
truthful graph-unavailable labeling and actual pointer navigation/focus through
Human inbox and Incident view. Evidence
`/private/tmp/exo-jev-qa-20261003/default-recorded-inbox-navigation-20261004-attempt4.json`,
SHA-256 `a31ed463372875244dff0c751412afd9d5447a4d7dc273c34de678f0300bbae4`.
The default bundle still lacks a renderable graph; this check does not qualify its
Floor replay. Broader Demo movement, verdict/release, Director activity, alarm,
binding/capacity/budget/program/artifact conversion gaps remain in
`/tmp/exo-additional-regression-inventory.md`. This follow-up does not implement
those deferred capabilities or claim complete scenario parity.

Both browser records report zero workflow-capable requests, A2A sends, operator
commands, provider/payment calls, console exceptions, and resource failures. Local
loopback QA session login is the sole POST. Raw profiles/screenshots/tooling remain
outside product source. Tool checkout is pinned at
`1231850a0bf1a0c0341fe408ef1668dbbfdfac46` (no model policy used here).
Earlier interaction attempts 1–4, route-parity failure attempt 6, and default
Recorded failures 1–3 remain preserved. They include harness DOM-timing/quoting
errors and the corrected grid/focus defects; they are not rewritten as passes.

Runnable review: open the retained URL, select Live and the second Basic root or
child, click Floor “Send work”, type without Submit, open Human inbox/Incident view,
and use Reconnect. Scoped freshness and factory incompleteness remain visible.
Switch to Demo to check the declared synthetic human wait and independent draft;
Recorded is read-only. Restoring factory-wide readiness requires the missing
historical Temporal histories or a separately approved history-retention policy;
no freshness bypass or workflow rerun was used.

### Ordinary browser entry correction (4 October 2026)

Operator reported a blank unauthenticated `/floor`. Prior interaction checks had
explicitly entered `/qa/login`, so they did not cover this initial path. The local
QA adapter stores sessions in memory: a harness reload invalidates prior cookies.
The opt-in loopback `/floor` now redirects missing/invalid/expired sessions to the
existing `/qa/login` button instead of serving an unusable Live bootstrap. Explicit
session creation is still required; non-loopback/forwarded requests fail closed.
The disabled-QA route is unchanged. Server bootstrap declares only the local
`/qa/login` recovery link; no automatic authorization or production-auth claim.

Initial Live selection prefers a recent readable detailed graph, excludes the
explicit unavailable-history list, and requests the selected run through the
existing authenticated scoped snapshot Interface. It does not change run state,
pins, source facts, or factory freshness. A previously saved valid run choice is
preserved. Factory-wide missing-history warnings still disable new submission.

Loaded on retained incarnation 23. Focused auth-route tests **2/2** and shared
presentation/UI tests **10/10** pass under the suite lock. The first two Python
errors were a wrong test-class command and are preserved; the corrected class is
`test_runtime_observation.RuntimeRoutesTests`. The initial shell launch failure and
an auto-following redirect probe are also preserved; a no-follow read confirmed
HTTP 303 to `/qa/login` after the working launch.

Clean isolated CDP browser context, no existing session or injected bootstrap/data:
**9/9 assertions pass**. Physical pointer activation of the session button loads
one factory and its 10-node retained graph, selected-run freshness is fresh, factory
incompleteness remains visible, draft input is editable, and Submit remains blocked.
No workflow/command/provider/payment requests, console errors, or resource failures.
Exact command:

```sh
/usr/bin/lockf -k -t 1 /tmp/exo-qual-suite.lock /private/tmp/exo-jev-qa-20261003/.venv/bin/python /private/tmp/exo-jev-qa-20261003/check_ordinary_floor_entry.py
```

Evidence: `/private/tmp/exo-jev-qa-20261003/ordinary-floor-entry-evidence-20261004-attempt2.json`.
The earlier ordinary-entry pass is preserved separately; it loaded the graph but
kept the initial factory-wide disconnected scope. The final pass adds initial
selected-run readiness without hiding that factory limitation.

Open `http://127.0.0.1:47053/floor`, then click **Start local QA session** if prompted.
The factory loads after that explicit local session step. No paid allowance was
used. Canonical-spec updates for the broader audit inventory remain with the
Console's bounded docs-only ownership interval; this entry correction does not
silently expand implementation to that inventory.

Ordinary-entry final evidence SHA-256:
`6376b37c334aed00aa002d76a46d51c5146abb208544aca0114947421f39fed0`.
The Console's released spec additions (shared provenance/responsibility table,
regression inventory, F01–F10, and stories 31–36) were read after this fix. The
following conservative status aligns earlier evidence with those broader checks;
a passing bounded control check is not full scenario-fidelity qualification.

| Fidelity row | Current bounded evidence and remaining limit |
| --- | --- |
| F01 | Partial: corrected assignment/nested presentation kinds and retained pins/graph browser checks; every profile's instruments/dimensions are not independently asserted |
| F02 | Partial: start basis and work timing regression tests, Translation terminal projection, Supplier human-wait fixture; all original timestamps/end notes and full-profile replay fidelity remain unproved |
| F03 | Unqualified: broader Demo artifact registry, motion, verdict/release, flag/scrap/readout conversion gaps remain |
| F04 | Partial: declared static Director/human wires and bound illustrative wait/inbox are restored; original timed Director activity, alarms, recommendations and notes are not fully restored |
| F05 | Unqualified: Demo binding/capacity/shared occupancy/economics/program/result-kind metadata conversion gaps remain; no Live figures inferred |
| F06 | Partial: actual pointer inbox/incident navigation and empty/unavailable states pass, with source scope and fixture bindings; all stale/pending/origin/action cases are not covered by current browser evidence |
| F07 | Partial: Live/Demo pointer typing and browser-native insert-text, isolated source/view/reopen/reconnect drafts and Recorded read-only pass; full factory-switch matrix and OS clipboard use are not claimed |
| F08 | Partial: shipped default Recorded evidence-only inbox/incident navigation passes with graph-unavailable label; its authentic graph remains absent and all evidence views are not browser-qualified |
| F09 | Partial: retained selected-run reconnect/source isolation pass and duplicate-output regressions exist; full duplicate-event/browser/mode matrix is not claimed |
| F10 | Partial: no-model physical interaction and zero mutation/spend audits pass for the named cases; full original scenario timestamp/profile coverage awaits the remaining fidelity work |

No F-row is promoted from aggregate test counts. No new paid allowance, tracker
publication, third workflow, deferred commerce/operations, or real-money action is
implied by the spec update or this table. Historical Basic/S/P evidence is retained.

### Indefinite Connecting correction (4 October 2026)

Operator reported indefinite centered “Connecting to Observation source…” after
clicking the local-session button at the correct `/floor` URL. The operator's
specific browser failure is not reproduced/diagnosed yet. Source review confirmed
that selectRun swallowed snapshot failures after changing transport status but
left the centered error unchanged. Rendering failures before layout had a similar
stale-message path. Both now replace the centered text with the actual error.

Live discovery fetch/body parsing has a 12-second abort deadline. Reconnect can
retry authorized discovery when no factory was discovered; it remains an explicit
single operator action, with no retry loop. ELK layout has a five-second deadline,
terminates its owned worker, and uses the existing geometry fallback. That fallback
preserves graph facts and does not supply authority/readiness. No harness restart,
workflow, inference, paid Jev call, or payment was needed for these static changes.

Full locked Node suite: **107/107 pass**, log
`/private/tmp/exo-floor-startup-node-final.log`. Includes a stalled discovery request,
centered failure-state regression, and bounded stalled-layout/worker cleanup test.

No-model browser results under the shared lock:

| Case | Result | Evidence file in /private/tmp/exo-jev-qa-20261003 |
| --- | --- | --- |
| Ordinary entry and normal cached reload | 10/10 pass; authentic retained 10-node graph and scoped freshness | floor-startup-normal-final-20261004.json |
| Target-local CDP-held discovery request, then explicit Reconnect | 11/11 pass; actual timeout replaces Connecting and recovery loads the graph | floor-discovery-timeout-recovery-20261004-attempt2.json |
| Controlled never-settling ELK implementation | 10/10 pass; existing fallback geometry renders retained graph on entry/reload | floor-startup-stalled-layout-20261004.json |

SHA-256 respectively:
`1ac94d983188fc629fceb802213516ddfa6b4452968807f6d6963418c065eb0a`,
`961717f4061dbac332e79d356ea211bba68f152e1746cf027b7e63d90708ede6`,
`5d5d36e904d5cf3f0e8ec5acca245756bb21e606e83b7f3b20cde9f7c1a1530a`.
The fault cases are controlled browser transport/layout tests, not synthetic factory
work or provider qualification. The fallback screenshot was independently inspected.
All three records report zero workflow-capable/provider/payment requests and console
errors. Factory-wide history remains incomplete and Submit remains blocked.

Commands: locked QA Python against `check_floor_startup_final.py`,
`check_floor_discovery_timeout.py`, and `check_floor_stalled_layout.py` in the same
temporary tooling directory. The first held-discovery attempt failed on an unguarded
Alpine expression during reload; it is preserved. Two attempted native WebSocket
close fault cases did not establish the intended failed-snapshot condition and
remain failures, not browser proof of that fault. The second diagnostic actually
showed a ready, fresh selected run; it cannot be relabeled as a fault-test pass.

Operator confirmation from the refreshed browser is still pending. Do not claim
this source/QA correction proves the exact operator-side connection failure is
resolved. The updated page should either load or expose the actual error needed
for the next diagnosis; the larger F-row/Demo/default-Recorded limits remain.


### Operator screenshot: stale freshness validator and boot Demo leakage (2026-10-04)

The operator screenshot contains `$snapshot.freshness: invalid freshness` and a
Demo Job 0435 escalation card during failed Live. A faithful older validator that
allows only status/observed_at reproduces that exact error against the real scoped
snapshot. This supports cached-module mismatch as the diagnosis; the operator’s
Safari cache itself was not inspected. The earlier clean Chromium passes did not
cover this condition. All prior failures and limited claims remain preserved.

Runtime now serves Floor/bootstrap with `Cache-Control: no-store` and rewrites
all dashboard asset URLs to a content-digest prefix captured at app startup;
relative imports use the same prefix. Retained harness PID 6539/incarnation 25
serves prefix `/dashboard-assets/442339e22847aa85579f213eb060f50dbe632b28e022e72c14f155817d63ffd7/`.
The digest currently includes dashboard test JavaScript as well as shipped modules;
that can cause extra revision changes but does not weaken validation. No auth or
freshness checks were relaxed.

The shared Floor renderer now gates HUD publication by its presentation source
and clears decision/inspector/toast/old-job/publication state on a source switch.
The initial Demo ticker cannot repopulate failed/loading Live controls. Public
graphs and source facts are unchanged.

Browser command:
`/usr/bin/lockf -k -t 1 /tmp/exo-qual-suite.lock /private/tmp/exo-jev-qa-20261003/.venv/bin/python /private/tmp/exo-jev-qa-20261003/check_floor_asset_version.py`

Final evidence: `/private/tmp/exo-jev-qa-20261003/floor-asset-version-stale-validator-20261004-attempt3.json`: 8/8 assertions pass.
Actual normal login loads the retained ten-node graph; controlled stale-module
response reproduces the exact error without a Demo decision or old-version badge;
removing interception and reloading restores the actual fresh run-scoped graph.
This is a controlled old-validator response, not proof of Safari cache contents
or a claim that the operator has confirmed recovery. Screenshots were inspected.
No workflow, command, model, or payment calls. Attempt 1 failed on a malformed
helper expression and remains preserved; attempt 2 passed before the badge cleanup.

Node command: `/usr/bin/lockf -k -t 1 /tmp/exo-qual-suite.lock node --test prototype/temporal-factory/dashboard/test/*.test.mjs`: 108/108 pass;
log `/private/tmp/exo-floor-cache-node-20261004.log`. Runtime route tests: 2/2
pass for versioned modules and no-store headers. Runtime evidence:
`/private/tmp/exo-jev-qa-20261003/runtime-qa-asset-version-load.json`.
The updated URL was opened through the OS default browser with a benign UI cache
revision query. Harness reload invalidates the memory-only local QA session; if
redirected, the existing explicit Start local QA session button is required.
Factory-wide missing histories and broader F-row/Demo/Recorded limits remain.


### Submit brief feedback and renewed history outage (2026-10-05)

Operator screenshot shows Live, disabled Submit, disconnected freshness and six
unavailable historical histories. No A2A request is expected from that disabled
button; this is not evidence of a successful submission. Runtime health remains
ready structurally, runner running, worker registered, zero unfinished jobs. The
reason for all six missing histories is being diagnosed separately; no freshness
check has been removed.

Submission progress/errors/results now appear inside the composer’s status region
as well as the top bar. An in-flight form prevents duplicate clicks and retains
the draft. Demo feedback explicitly says no real work started. Disabled reasons
have a visible status box and are linked to Submit for accessibility.

`/usr/bin/lockf -k -t 1 /tmp/exo-qual-suite.lock /private/tmp/exo-jev-qa-20261003/.venv/bin/python /private/tmp/exo-jev-qa-20261003/check_submit_feedback.py`: PASS9, evidence
`/private/tmp/exo-jev-qa-20261003/floor-submit-feedback-20261005-attempt1.json`.
Actual pointer typing/clicks verified blocked Live (no POST), enabled local Demo
with inline result, retained drafts and isolation on source switches. Audit:
zero A2A sends, commands, provider/payment requests, console/resource failures;
one explicit local session POST. Screenshot of the blocked Live composer reviewed.
No new factory workflow or inference. This does not qualify working real submission
while the Observation prerequisite remains disconnected.

Node suite: 109/109 pass; log `/private/tmp/exo-submit-node-20261005.log`.
Focused composer suite: 4/4, including inline local outcome and blocked result.


Read-only blocker diagnosis: `/private/tmp/exo-jev-qa-20261003/runtime-submit-blocker-20261005.json`.
Exact public command: `/private/tmp/exo-v2-temporal-9u041h3e/cli/temporal operator namespace describe --namespace exomachina --address 127.0.0.1:47042 --output json`: exit 0, retention 86400 seconds, history/visibility archival disabled.
A bounded `workflow describe` for the retained second root returned exit 1 and
NotFound; raw output was not logged. Runtime independently read a prior root
history and observed RPC NOT_FOUND. Retention expiry is consistent with these
facts; the precise expiry/deletion cause for all six was not individually proved.
The existing engine is healthy and was not restarted. Restoring expired history
requires a pre-expiry backup. A new execution alone does not restore missing
histories or make the existing factory-wide snapshot fresh. No recording is
promoted to live evidence. No replacement workflow, namespace change or spending
was attempted. This is a real remaining Live submission blocker, not a UI pass.

Final composer copy also identifies the selected source explicitly and labels the
disabled reason “Submission blocked”. Follow-up browser evidence
`/private/tmp/exo-jev-qa-20261003/floor-submit-feedback-20261005-attempt2.json`: 11/11
pass, same zero-workflow/provider/payment audit. Final focused composer tests: 4/4
pass. The user’s tab was not reloaded automatically, to preserve their typed draft.


### Current factory interaction restored independently of expired histories (2026-10-05)

Supersedes the previous *current submission remains blocked pending history*
assessment. Operator clarified that a healthy live factory must remain usable;
historical expiry is not current factory unavailability. Historical evidence,
failures and unavailable history warnings remain unchanged.

Implemented authenticated fixed-factory `/submission/readiness` using fresh
existing provider/pinned-owner/worker checks and durable current single-job state.
Busy/uncertain/unfinished/unavailable states block; query selectors and observer
principal are rejected. Strict JS projection validation rejects extra fields,
wrong factories, malformed times/status/reasons; current GET is no-store, bounded
and timestamp checked. New-work POST rechecks this authority and selection/socket
identity; existing A2A preflight and atomic slot remain authoritative. Task-bound
follow-ups/commands retain fresh run/Task/context checks. This does not upgrade
any historical snapshot to fresh or prove a new inference call.

Read-only Dashboard review identified stale selection/reply retargeting and missing
endpoint gating. Source/generation/adapter/render-epoch checks, captured factory,
configured endpoint check and cancellation-before-POST were integrated. An observed
submitted Task is followed by its exact Task/context; a pending marker is factory
bound and does not substitute an older run. A redundant scoped snapshot may still
occur when that Task first appears; this has no workflow/model mutation and has
not been tested with a third real submission. Direct A2A replies now render inline
and persist across readiness refresh, without claiming a new run from plain text.

The no-model browser check found real reconnect renderer defects: graph reuse left
`ready=false` and hidden canvas styles after a selection/snapshot transition.
Valid reused graphs now restore display readiness and surfaces, leaving source
freshness and command gates unchanged. Focused VM regression covers this distinction.

Final retained harness PID 44632/incarnation 27; engine/owners not restarted;
runner running, zero unfinished runs, structural submission_ready true.
Load evidence `/private/tmp/exo-jev-qa-20261003/current-readiness-harness-final-20261005.json`.
The intermediate incarnation 26 load is preserved separately. Model/profile, pins,
source journal, dirty work and prior paid pair preserved; no new workflow/inference.

Browser command: `/usr/bin/lockf -k -t 1 /tmp/exo-qual-suite.lock /private/tmp/exo-jev-qa-20261003/.venv/bin/python /private/tmp/exo-jev-qa-20261003/check_current_factory_readiness.py`.
Final evidence `/private/tmp/exo-jev-qa-20261003/floor-current-readiness-20261005-attempt6.json`:
11/11 pass. Actual authenticated current endpoint returns ready for the exact
factory; Live Submit enabled, real pointer typing, reconnect and draft retention;
expired-run actions disabled; historical freshness still disconnected. Audit:
one explicit session POST, zero A2A sends, commands, provider/payment requests,
console errors or resource failures. Live Submit deliberately not clicked.
Screenshot independently inspected. This proves readiness/UI restoration, not
successful new paid work or end-to-end post-submit following.

Attempts 1–5 remain failures: initial async-return helper mistake; pointer-name
mismatch; then reconnect loaded/visibility defects. Final output does not rewrite
those failures or the earlier collector. Current route test passes (1 focused
method with auth, selectors, ready/provider/busy/uncertain/unfinished/unknown cases)
under the shared lock using pinned Python and established QA deps. First fixture
attempt failed because a partial mocked readiness value contaminated the health
cache; corrected fixture pass preserved by the worker. Pure JS response/cancel/
selection/reuse/reply tests are synthetic, not real smoke.

Final Node suite 116/116: `/usr/bin/lockf -k -t 1 /tmp/exo-qual-suite.lock node --test prototype/temporal-factory/dashboard/test/*.test.mjs`;
log `/private/tmp/exo-current-readiness-node-20261005-complete.log`. No third real
workflow, inference or payment; broad F-row/Demo/default Recorded limits persist.
Production identity/auth, real costs and deferred payment/network work remain
unqualified. Namespace retention was observed, not changed; expired histories
require genuine backup for historical recovery.


### Operator-submitted job visibility diagnosis (2026-10-05)

The operator manually submitted work after the current-readiness fix. The lead did not resubmit or start a replacement workflow. Runtime public reads reported one active Basic job; run-scoped Observation was fresh while factory scope retained six unavailable historical histories. Sanitized readback: `/private/tmp/exo-runtime-current-submission-20261005.json`.

Independent local no-model browser review observed the actual child of Task `8480323f-ed17-4f75-97f7-86cca125901d`, context `deaa5987-9eb0-4359-931f-f424de95c084`, run `a40ec20b-2787-438a-8d11-11dd9ccecc16.e1f707731b6bc18d0e67:child:05c74cddea24`, completed/accepted at `2026-10-05T14:19:12.848Z`. This evidence concerns the existing operator submission, not a new authorized qualification pair or a duplicate smoke. Initial browser capture: `/private/tmp/exo-jev-qa-20261003/existing-user-job-browser-before-20261005-attempt2.json`. Helper attempt 1 failed on transient `StalePage`; preserved. Model-diagnostic capture preserves a screenshot filename collision as a helper failure.

Diagnosed display problems: accepted Task following dropped its explicit run ID and first requested factory scope; assignments lost earlier same-attempt fields in both reducers; Temporal assignment projection omitted explicit activity-input node links; a sparse Floor timeline anchored job age to its first event, making an end-only run look zero-duration. HTML follows the exact accepted run, falls back from unavailable saved runs, hides stale surfaces on snapshot failure, anchors observed duration, and exposes a factual observed-job panel with Board/Outputs links. Quality acceptance does not establish delivery. Node-link/Python reducer loading is tracked separately below.

UI-only browser checkpoint `/private/tmp/exo-jev-qa-20261003/existing-user-job-browser-ui-fix-20261005.json`: 5 assertions pass (visible current job, completed/accepted Quality, nonzero elapsed 254.421 seconds, same Task in Board, Quality in Outputs). One local QA session POST; zero A2A message sends, commands, model/provider/payment calls, or workflow starts; zero recorded console/resource failures. Screenshot independently inspected. This checkpoint predates the pending source correction load and does not prove all work is spatially represented. Focused current-readiness/selection/status tests 8/8; Dashboard checkpoint 118/118 before the additional assignment-merge regression; assignment Dashboard suite 24/24 after that regression. Existing Basic and audit limitations remain; no third paid qualification workflow was run by the lead.


#### Loaded assignment visibility and existing-job browser result

Lead safely reloaded only the owned harness after public `unfinished_runs=0` and Basic available; PID 52149/incarnation 28, runner retained, submission readiness true. Evidence: `/private/tmp/exo-jev-qa-20261003/user-job-projection-load-20261005.json`. Runtime's wrapper attempts failed before stopping any process (wrong urllib method, then wrong interpreter missing websockets); they are not external prerequisites or product failures. Runtime released reload ownership before the lead performed it. Observation suite 34/34 and focused source node-link persistence/restart/schema regression 1/1 passed under the shared lock. Append-only source IDs and facts remain preserved.

Final existing-job child browser evidence: `/private/tmp/exo-jev-qa-20261003/existing-user-job-browser-final-20261005-attempt4.json` — 9/9 assertions pass. Research assignments map to the actual pinned `gather` node, with observed durations 25.470s and 27.853s; the older scheduled attempt remains outcome-unreported. Completed/accepted and 254.421s execution duration are visible. Board preserves original Task/context; Outputs exposes accepted Quality and inspected artifact r2. Physical pointer inspection returned 1,758 bytes with matching SHA-256 `504c6e39182c927511d1ed031756cc1485104f1f5102629ca209e9892c0a77e3`. Zero A2A sends, commands, provider/model/payment calls or workflow starts; zero recorded console/resource failures. This is read-only reuse of the operator's existing job. It does not establish a local delivery receipt; delivery remains unverified.

Failed final attempts 1–3 are preserved: missing SHA from row label in helper selection, then transiently lost/offscreen inspection controls and a verification timeout. Unchanged-checkpoint DOM replacement was fixed with a view revision based on projected facts and authority, not cursor identity. These failures are not rewritten as passes. A physical pointer inspection plus digest assertion passed in attempt 4; stability under every changed-data reconciliation case is not claimed.

Root-only scope evidence `/private/tmp/exo-jev-qa-20261003/existing-user-job-root-scope-20261005-attempt3.json` — 3/3 assertions pass: original Task/context exact, root completed/accepted, parent graph exactly `invoke_child` (nested_factory) → `done` (complete), distinct from child pins/graph. Its snapshot was fresh while stream transport was still connecting at capture; this is not reconnect qualification. Root-only Quality is unreported. Earlier root helper failures incorrectly expected two runs in an exact-run snapshot; preserved as helper assumptions, not a product failure. Selection now reads the accepted root first, then optionally discovers same-Task detailed work from the authorized factory and scopes it independently. Synthetic selection regressions cover failed child fallback and explicit identity matching; no new paid submission was made to repeat this path.

Final focused UI tests cover exact-root-first selection, optional same-Task detail selection/fallback, pending-scene clearing, factual status, unchanged-checkpoint detail preservation, and source authority separation. Full Node final log: `/private/tmp/exo-user-job-dashboard-node-20261005-complete.log`. Existing Basic paid-pair evidence, old collector failures, broader Demo/default Recorded fidelity gaps, partial usage/cost visibility and deferred roadmap/payment statuses remain unchanged.

Native Computer Use connection failed with `Sky Computer Use native pipe startup failed`; the operator's Safari tab is not claimed independently verified. Local isolated Chromium was used for the actual browser assertions. `/qa/login` was opened for the operator after the scoped app reload; its explicit Start local QA session restores the in-memory session. No cookies or credentials were exported. No third workflow or additional inference was started by the implementation team.

### Selected-job summary/composer overlap correction (2026-10-05)

The persistent observed-job summary was positioned on the Floor at z-index 80 and covered Send work. It now renders only in the existing right-hand job inspector; hiding that inspector hides the summary. No runtime facts or submission gates changed. Canonical presentation requirements updated.

Focused regression: `node --test prototype/temporal-factory/dashboard/test/current-submission-readiness.test.mjs` under `/tmp/exo-qual-suite.lock`: 13/13 pass. One initial test syntax error was corrected before passing. Existing local no-model browser controls against retained 47053 verified pointer focus, actual keyboard typing and paste, and Submit brief hit testing at 1600px and 1220px; summary is inside `.insp`, with no floating card. Evidence: `/private/tmp/exo-jev-qa-20261003/job-summary-inspector-20261005-attempt2.json`; screenshot: `/private/tmp/exo-jev-qa-20261003/brief-draft-job-summary-inspector-20261005-20261004-attempt7.png`. All 7 assertions pass; zero A2A sends, operator commands, provider/payment calls, console errors, or resource failures. The initial helper failed its narrower-width text assertion after an ineffective select-all cleanup; evidence is preserved in `job-summary-inspector-20261005.json`. Corrected helper checks retained draft plus newly typed text without that cleanup. No workflow was submitted. Native browser plugin setup failed on missing browser-service module; approved existing isolated CDP tooling was used. App serves HTML per request: no server restart needed; operator page requires reload.

### Live activity/job-card corrections and QA isolation incident (2026-10-05)

The original renderer contains movement/belt animation, but Live projection supplied station work without belt transitions. A separate source bug used opaque Temporal activity IDs as graph nodes for synthesis/Quality. Inline presentation now animates declared inbound belt chevrons for current observed pinned-node activity (fresh, connected Live only) and for active replay work; this is a route cue, never artifact-transfer evidence. No artifact move facts are invented. Same-graph updates now prefer `dashboardRunId` instead of the old renderer run/aggregate and reset old inspector selection when the run changes. Accepted work reveals the existing job panel; child discovery is bounded to 3 reads/1-second spacing, cancellable by user selection.

Lead Node checkpoint: 129/129 pass under suite lock; `/private/tmp/exo-jev-qa-20261003/live-activity-node-tests-20261005.log`. Dashboard behavioral renderer regression: 3/3, `dashboard/test/selected-job-renderer.test.mjs`. Fresh real user-job browser evidence: `/private/tmp/exo-jev-qa-20261003/latest-job-card-20261005.json` (Task `33d7c022-ab8f-4301-bb7f-bff149736190`, root suffix `f9acddcc5eeea1400f87`, child completed/accepted at 15:18:15.657Z). It proves the actual separate job card and terminal state; no active Live animation was witnessed after this job completed. Demo animation evidence: `/private/tmp/exo-jev-qa-20261003/activity-demo-browser-20261005.json`, nodes rf/rr, belts e0/e1; two screenshots 0.6s apart show 59 changed pixels in a belt-only crop [270,575,300,585]. This is illustrative Demo visual proof, not actual factory transit or full Demo fidelity. No workflow/provider/payment calls in those named checks.

**QA isolation failure:** a later browser interaction attempt intended to intercept message/send reused an existing Task response but interception failed. `/private/tmp/exo-jev-qa-20261003/accepted-response-card-browser-20261005-attempt3.json` confirms one real A2A POST and a new run suffix `e97fe2c6b02c17647762`. This was outside the no-new-workflow QA constraint and was disclosed immediately to the operator. Attempt1 has no request audit and may also have reached the factory; owner attribution is pending. Attempt2 was blocked before clicking by a real readiness request timeout. Old helper `model_calls:0` counters describe browser provider requests only and do **not** prove zero server inference; actual server model calls and cost remain unknown pending public usage evidence. These failed attempts are not passing synthetic QA evidence and are preserved. Summary: `/private/tmp/exo-jev-qa-20261003/qa-submission-isolation-incident-20261005.json`. The runner is disabled, with its prior bytes preserved outside product source. No further submit-button/browser-inference attempts are authorized or being made. Runtime is performing bounded read-only attribution.

Follow-up source review: `src/observation_source.py` emits deterministic scheduled/started run-node corrections only for explicit activity-input nodes in the pinned graph. Same-watermark backfill skips terminal histories and corrections older than a later durable run-state/wait fact. Four focused synthetic tests passed under the shared suite lock: three run-node tests (link/restart, terminal preservation, later wait preservation) and existing assignment-node replay. These are not real active-run browser proof. Public attribution `/private/tmp/exo-jev-qa-20261003/runtime-unintended-submit-attribution-20261005.json` identifies exactly one additional accepted Task (`1fda207d-2c79-4dbb-984a-b27a566778c7`) after the user's f9ac… Task; its root and child are completed/accepted. A wrapper reporting-key typo happened after the sanitized file was saved; only the saved local file was reread, not the endpoint. The previous real readiness GET took 0.446s and returned factory_busy while that additional run was underway; the earlier browser abort remains preserved, not evidence of a current readiness defect. Runtime confirms a mounted periodic Basic slot reconciler; no new readiness-route patch is being added from speculation. Actual provider-call coverage/cost still pending/unknown.

Final load/checkpoint for the activity fix: reducer now builds Floor work only from canonical merged attempt rows. Replaying raw running frames cannot overwrite a completed duration; phase-node changes do not invent assignment markers; an unended unknown historical attempt is kept in Board rather than animated as endless work. Added focused behavioral regression. First new fixture omitted mandatory task_id and failed validation (`live-activity-node-tests-20261005-final.log`); corrected fixture and final full suite: **130/130 pass**, `/private/tmp/exo-jev-qa-20261003/live-activity-node-tests-20261005-final-attempt2.log`. Previous 129-pass checkpoints preserved.

Owned app-only reloads preserved the Temporal runner/model agents and were gated by zero unfinished runs and an available Basic slot. First load inc29 preceded the final reducer edit; one explicit follow-up loaded inc30, PID67515. `/private/tmp/exo-jev-qa-20261003/runtime-final-hash-reload-20261005.json` confirms computed/mounted module hash `4a342af3be4045a44aea63761f5ed2b95330a40aa14ff61257ae7934ad44378e`, all four owner pins ready, submission_ready true, zero unfinished runs. No extra reload is required for the test-fixture-only correction.

Final **read-only** browser evidence: `/private/tmp/exo-jev-qa-20261003/latest-user-job-final-browser-20261005.json`; 4/4 assertions passed against the user's actual original Task33d7c022 and child f9ac…, separate job card, completed/accepted Quality, no terminal activity cues. Request audit: one explicit local QA session, zero A2A sends, commands, provider/payment requests, console errors or resource failures. Screenshot: `/private/tmp/exo-jev-qa-20261003/brief-draft-latest-user-job-final-20261005-20261004-attempt7.png`. This check explicitly selected the user's original job, preserving the unrelated unintended QA run in history. Active Live animation after the source patch remains unobserved: all retained jobs are terminal and no further agent-initiated workflow is authorized. Source mappings and current-activity logic are fixture-qualified, Demo motion is visually checked, and the actual terminal user job is browser-checked; these are distinct evidence scopes. Broader F-row Demo/default Recorded gaps remain deferred and documented.

Unintended QA Task usage read (single GET, not repeated): `/private/tmp/exo-jev-qa-20261003/runtime-unintended-submit-usage-read-20261005.json`, SHA-256 `3c5499b45a553ccd2db0bb4ba583b07bec592ad7a9e1d976e117e1f0b24f9f66`. Four complete provider-reported **Director-call** measurement rows were returned. This does not cover assignment/other model calls or establish total inference calls/cost. Cost remains unknown. Attribution and the failed-interception incident remain outside approved workflow qualification; no passing QA claim is made from that unintended execution.

### Demo data-path fidelity restoration, F02–F05 (2026-10-07)

Earlier F02–F05 observations above (Partial/Unqualified) are preserved; this entry adds evidence, it does not promote any row to Qualified.
Cause: the shared Demo Adapter converted only the publicly representable subset of each fixture (job/work/wait/admit/end/decide/sha-bearing spawn) onto a fixed 2026-01-01T00:00Z clock and discarded all operating metadata, so the Floor lost the scenario clock, movement/artifact lifecycle, Director turns, alarms, recommendations, notes, budgets, prices, capacity, versions and digest.
Change (no renderer read of fixtures as truth): `dashboard/contract.mjs` adds one explicitly labelled illustrative layer (`state.demo`, label `Illustrative Demo fixture`, and `com.exomachina.demo.illustration.v1` events, each strictly allowlisted and bounded) that the validators accept only when the caller declares source `demo`; default/Live/Recorded validation rejects it. `adapters/demo.mjs` emits every original timeline entry through that layer on the fixture's own `run.start` clock (per-job start from its job entry; scripted job ids such as `0435` kept as run ids; job-less publication is factory-wide, not a phantom run). `reducer.mjs` keeps the layer only for Demo state and `toFloorModel` projects it (illustrative flags/labels on model and agents; a Demo command that ends a run closes open waits/alarms at the observed end). The Floor resets the previous source's clock/live edge/ticks on source switch.
Tests: new `dashboard/test/demo-fidelity.test.mjs` (7 cases, real shipped fixtures through Adapter → reducer → toFloorModel, plus Live/Recorded rejection of copied illustrative snapshot fields, events and smuggled event fields). Existing demo tests now pass `{source:"demo"}` to validators, and two run-id assertions changed from prefixed (`batch-job-a`, `floor-research-v1`) to the scripted job identity; both encoded the identity/strict-only regression. Gates under the suite lock: dashboard Node 142/142, broker 20/20, Python 451 OK.
Browser (Edge headless, local harness, no product/provider/payment requests, no console errors except a harness favicon 404): before — Verified Research Demo showed `16:55:49 · 31 Dec`, REPLAY, `v2 · Unknown`, 0 alarms, `No budget set`, old-version tooltip `v1 · . `. After, matching the HEAD mockup at the same scenario time: `14:21:26 · 25 Sept` LIVE at T+02:56, `v2 · d41f7a03`, the same 3 active alarm texts, `$7.8k left of $10k`, 20/20 jobs, decisions you 2 / Director 1, `v1 · 6cbb2bd8. v2 moved Quality review…`, agent capacity/illustrative prices/shared occupancy, identical Director console lines; Supplier (`09:43:17 · 24 Sept`, approval wait with Director recommendation, `$1.2k left of $1.5k`) and Translation (`11:17:49 · 24 Sept`, ends released at 32 s, `$342 left of $400`) match HEAD. Evidence `/tmp/demo-fid/demo-fidelity-browser-20261007.json`, SHA-256 `c6faa4c3d0c21c73db7df61ef40686b9e99472ac1282b252ef223d22e65d4246`.
Remaining limits: departments still render as presentation groups (`· graph group`/`· visual group`) instead of the fixture's glyph names; renderer wording differs deliberately (`Illustrative price`, `awaiting observed admission`); Demo decisions on Approve/Send back still have no bounded local transition; the run picker cannot return from a selected job to All jobs (renderer selection, pre-existing); Verified Research Demo load is ~1.6 s versus ~0.9 s for the mockup. F01 instruments now receive declared step cues (loop, responder, escalation) but per-profile instrument dimensions are not independently browser-asserted.

### Live A2A floor flow, branch agent pods, and all-jobs scope (2026-10-07)

Earlier entries and status rows are preserved; this entry adds evidence and does not promote any row.
Operator report: a real Live job lit a station and moved belt chevrons, but no work item travelled; only two agents looked active; a new submission replaced the previous job. Causes confirmed: (1) the non-Demo projection emitted only spawn/work/consume at each assignment's node plus wait cues, and the renderer's Live belt animation was a presentation cue with no items, so no `move` ever existed; (2) the research agents are assignments at the `gather` fan-out (`packet_findings@1`, `packet_risks@1`), not graph nodes; (3) after a submission the floor was run-scoped (one job in the rail; `toFloorModel(...runs:new Map())` while pending), and the real factory's mixed pins disabled "All jobs".
Change (operator-approved A2A model; derived only from Observation facts, never A2A wire shapes): `dashboard/reducer.mjs` `liveFlow`/`branchPods` (non-Demo only; Demo keeps `demoFloorEvents` unchanged) — Tasks occupy stations from observed start to end (assignment attempts; observed run-node transitions at nodes without a declared capability, closed by the node's artifact/verdict/receipt fact); artifacts ride belts from the producing completion to the next observed start (minimum visible hop 0.8 s); report items R1/R2 evidenced by sha256 through verdict and receipt, rejected revisions follow the repair route; Task outputs labelled "Task output · artifact not recorded" with hand-off inferred from pinned graph order; route-case hops labelled undeclared. `toFloorModel(state,{graphOf})` scopes all jobs sharing the anchor run's manifest/definition pins. Renderer (`docs/design/exomachina-floor.html`): `withBranchPods` presentation pods (pinned steps/edges kept as `pinnedSteps`/`pinnedEdges`), pod activity lighting, item Evidence/Observed inspector rows, in-progress-work queue fix, Live/Recorded rail keeps finished jobs, finished Live jobs replayable, ended jobs excluded from "waiting to start", Live default "All jobs" scope with a display-only retained factory snapshot and one-time follow of the anchor/new job. Spec: `docs/specs/factory-dashboard-integration.md` (Live playback rule, motion row, reducer inference rule, B08/S32 wording, new "Live A2A floor flow" section).
Tests: new `dashboard/test/live-flow.test.mjs` (6 cases: streamed Live path through pods/belts/R1 reject loop/R2 release; live-edge update; retained snapshot stays unlocated/untimed; graph-scoped all jobs; renderer pods via node:vm; scope/follow wiring). Updated by rule change: `dashboard.test.mjs` (Live playback regexes), `current-submission-readiness.test.mjs` (activity sentence, pod lighting). Gates under `/tmp/exo-qual-suite.lock`: Node 155/155 (`/tmp/live-flow/node-gate.log`); Python 451 OK (`/tmp/live-flow/py-gate.log`).
Browser (Edge headless against the operator's local factory, current on-disk HTML; no brief submitted; the only non-GET was the existing `/qa/session` login; POST `/` intercepted): Live opens on "All jobs · shared graph" with six retained child jobs in the rail and the newest followed (`/tmp/live-flow/all-jobs.png`). Replay of retained run `…850039f2f2516f5287a6:child:05c74cddea24` (`/tmp/live-flow/replay-*.png`): Task enters, Tasks sit in the `research_findings`/`research_risks` pods for 15.6 s/60.5 s, outputs ride to `compose_report` and wait; R1 appears at `publish` at its receipt time with "delivery unverified (conflicting receipts)". That snapshot has no run-node history and no artifact/verdict times, so compose/Quality occupancy is honestly not shown. Full path entry → pods → join → compose → Quality (R1 rejected) → repair → compose → Quality (R2) → publish → done was verified only with a **synthetic stream-shaped fixture** injected via `page.routeWebSocket` for one fixture run (`/tmp/live-flow/stream.mjs`, `/tmp/live-flow/stream-*.png`); it is not observed runtime evidence. Demo unchanged (`/tmp/live-flow/demo-after.png`; console errors only the pre-existing favicon 401/404).
Remaining limits: retained Live replays cannot show synthesis/Quality/release occupancy until Observation records run-node history or content-free artifact/consumed-input facts (operator decision pending); a real Live job observed through the stream should show the full path, but no new real job was run here.

### A2A v1.0-only wire migration; Live-submission rows need requalification (2026-10-07)

Earlier entries and status rows are preserved; this entry adds evidence and does not change, promote, or demote any row.
Change (operator decision 1, `docs/a2a-v1-mediation-decision-2026-10-07.md`): every Exomachina A2A server and client, including the Dashboard Live Adapter (`dashboard/adapters/live.mjs`) and the floor composer (`docs/design/exomachina-floor.html` `submitBrief`/`briefReplyError`), now speaks A2A v1.0 only on a2a-sdk 1.2.2. Requests use `SendMessage` with `A2A-Version: 1.0`; the composer sends `role: "ROLE_USER"` with a `{text, mediaType}` Part and reads `result.task` / `result.message`. 0.3 `message/send` is refused with `MethodNotFound`, and there is no fallback. Observation, `dashboard/decision.mjs`, `dashboard/contract.mjs`, and `dashboard/recordings/` are unchanged; the A2A Adapter maps `TASK_STATE_*` to the existing Observation states.
Consequence: all evidence for B01, B03, and B09 (and the parts of B02, B04–B07, and B10 observed through those Live submissions) was produced on the a2a-sdk 0.3.26 wire. Those B-rows need requalification on v1 through a real Live submission after the operator restarts the stack onto the new environment (`tools/spikes/2026-10-07/a2a-v1/.venv`). Until then their recorded status describes the 0.3 wire only.
Verification on branch `feat/a2a-v1`: dashboard node tests, broker node tests, and the Python unittest suite on the new environment (counts in the branch handoff); `tests/test_a2a_v1_only.py` proves 0.3 rejection, v1-only Agent Cards, `kind` rejection, and `ExtensionSupportRequiredError`. One fixture-only `scenarios/single_factory.py --provider scripted` run (isolated `/tmp/exo-sf-*` home, no model inference, no submission to the operator's harness) passed every A2A-dependent check; SF-2/G-7 failed only on the pre-existing `services/model_agent.py` import of `src/admission.py`, unchanged from 0.3. No browser Live submission was made.

### Hand-off carriers, gems, seals, and material and control edges (2026-10-07)

Presentation for the [A2A v1 mediation decision](../../docs/a2a-v1-mediation-decision-2026-10-07.md) decisions 3–6, built and proved **only over synthetic, test-only hand-off streams and Demo data**; runtime emission of the facts is a later phase, so no row status changes and S33 requalification is still owed when emission lands. Branch `feat/floor-carriers`.
- Contract: strict exact-field validators for `handoff.produced/consumed/item_ready.v1`, kept apart from the generic allowlist (no existing validator widened); negative tests reject `text`, `data`, `name`, `description`, `artifactId`, `filename`, `url`, `metadata`, `bytes`, `raw`, `parts` at top level, item level and input level (`dashboard/test/handoff-contract.test.mjs`). Pinned graphs accept edge `kind` and node `output`.
- Reducer: carriers from recorded hand-offs (streaming fill, departure at production, material-belt travel with observed dwell and the 0.8 s minimum hop, fan-in merge at the join, gate seal on the same carrier for R1 rejected/R2 accepted, release at the side-effect node on a verified receipt, material bypass belt, +N, mixed recorded/inferred run, digest mismatch, no-content projection) in `dashboard/test/floor-carriers.test.mjs`; gem/badge/inspector helpers and Demo gems in `dashboard/test/floor-gems.test.mjs`.
- Gates: `node --test dashboard/test/*.test.mjs` 177/177; Python `unittest discover -s tests` 451 OK (after a gitignored `npm ci` in `broker/` for this fresh worktree).
- Visual check (headless Edge, worktree HTML served on a private port with a synthetic recorded bundle; screenshots under `/tmp/floor-carriers/`, not committed): filling station, three gems mid-belt after the merge, +N badge with inspector, accepted and rejected seals, control edges, inferred carrier with inspector, low-zoom badges, Demo. Console errors: only the known favicon 404.
- Spec details settled during implementation are recorded under "Implementation notes" in `docs/specs/factory-dashboard-integration.md` and in INTERFACES.md.

### Runtime hand-off records: emission, snapshots, S33 requalification check, and floor proof (2026-10-07)

Earlier entries and status rows are preserved. This entry adds evidence and does not promote or demote any row. The S33 row stays as recorded: everything below is scripted-provider or fixture evidence on private ports, never operator-stack evidence. Branch `feat/a2a-v1`.
- **Runtime.** Activities record content-free `handoff.produced.v1` and `handoff.consumed.v1` facts at the factory's own A2A boundary:
  - output contracts apply, with `output.missing` as a Task incident;
  - digests are keyed HMAC-SHA256 under a per-instance key at `<home>/handoff-digest.key` (mode 0600), which never enters Workflow history;
  - the Workflow patch is `exo-handoff-records-v1`.
- **Observation and snapshots.** Observation projects the facts through exact allowlists. Snapshot run rows retain `handoffs` (deduplicated, at most 256 per list).
- **Streaming.** `item_ready.v1` is accepted end to end. Live `SendStreamingMessage` dispatch is not wired yet, so no ready rows are emitted today.
- **S33 check extended.** `scenarios/dashboard_qualification.py` validates hand-off envelopes and snapshot `handoffs` with exact field sets. It also now accepts the schema-declared freshness scope fields (`scope`, `run_id`, `included_run_ids`, `factory_status`, `unavailable_run_ids`). Without that, every current runtime snapshot was rejected with "snapshot freshness fields are unsupported", a validator lag unrelated to hand-offs.
- **S33 rerun on a real public snapshot.** The snapshot came from a private harness's authenticated WebSocket: two child runs, each with 3 produced and 3 consumed rows. Both the Python check and the Dashboard `validateSnapshot` accepted it. Copied canaries injected into `handoffs` were rejected by both: item `text`, `name` and `url`; row `task_id`; input `metadata`; a foreign `run_id`; an extra list.
- **`single_factory.py --provider scripted`** (fresh attempt id, isolated `/tmp/exo-sf-handoff-3` home, private ports, no model inference): all 25 checks passed.
  - Checks: SF-0..3, R1-a..e, R2-a..d, R3-a..e, G-1..5, G-7, and the new G-8 hand-off chain check.
  - G-8 per route (produced/consumed): route 1 3/3; route 2 4/5, where the repair draft consumes the research and the R1 draft; route 3 5/6.
  - On every route the chain recomputes, the key is absent from history and events, contract validation passes, and the events are content-free.
- **Floor proof** (headless Edge against a private scripted harness; two jobs driven through the loopback QA session; screenshots in `/tmp/handoff-runtime/job2-*.png`, not committed):
  - recorded R1 carriers with green gems leave the `research_risks` and `research_findings` pods;
  - the draft carrier is sealed "ACCEPTED · r1" at `independent_quality`;
  - it rides the material bypass belt to `publish`;
  - `publish` (`output: none`) has only a thin control line to `done`, with no outgoing belt.
- **Floor fixes found by the proof** (both tested in `dashboard/test/floor-carriers.test.mjs`):
  - Replay length used the observed end, which cut off carriers on sub-second scripted runs (hops are drawn at 0.8 s or more). A finished run now replays until its last hop settles.
  - Retained snapshots had verdict rows but no verdict time, so they lost their seals. A retained carrier is now sealed on arrival, marked "verdict time not recorded".
- **Gates** under `/tmp/exo-qual-suite.lock`: dashboard 182/182, broker 20/20, Python 483 OK.
- **Remaining limits.** The release station shows "Delivery unverified" because Observation emits two receipts with the same `receipt_id` but different `destination_id` and `delivered_at` values: the release Activity's receipt and the workflow's final-delivery receipt. This predates hand-off records, and the carrier is consumed at `publish`, not released. B-row and live S33 requalification still need the operator's stack on the new build.
