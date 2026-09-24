# Single factory, real agent work, three core routes

**Spike result, 24 September 2026.** Branch `spike/single-factory-core-routes`, local only (not pushed). It started from `origin/main` `2d609e3`. The pre-registered brief is `briefs/single-factory.md` (`bbc8c03`), with Amendment A1 (`8820d0f`).

## Verdict

**The live claim qualifies on attempt 3, the last of the 3 pre-registered live attempts.**
- It ran in one fresh home (`/tmp/exo-sf-live-3`) and one scenario run, with `gpt-6-sol` through the Codex-subscription broker.
- All 24 automated checks pass, and the post-write attestation of the final evidence bytes passes (`g4_final.pass: true`).
- The pre-registered final verdict rule is structural pass **and** attestation pass.
- The R1-d semantic reading (orchestrator reading, AI judgment, per A1) is **answered and supported** for every required part, with no unsupported claim.
- G-6 suites are green at the code used for the run.
- Independent review #4 (`handoff/sf-review-4.md`) re-derived every check from the raw stores, agrees with all 24 checks and would flip none. It found 0 blockers, 0 major, 1 minor and 5 notes.
  - The minor finding (N1): `scripted-12-console.txt` was copied into evidence after the scripted attestation, so that attestation does not cover it. It is covered by live-3's attestation and does not affect G-7.
  - Review #4 did not flag the route-1 label defect described below.

Earlier live attempts:
- Attempt 1 is a recorded **fail**.
- Attempt 2 passed its 24 checks as recorded, but it **does not qualify**. Review #3 showed two problems:
  - its G-4 scan did not cover its own final evidence bytes;
  - the scenario read the Director identity token to run a value comparison, which contradicts G-4's literal clause "no token was read or printed by scenario or worker code".
- Attempt 2's post-hoc attestation fails (`redaction:false`, `checker_bound:false`). Its evidence is kept unchanged.

What attempt 3 proves, and what it does not:
- **Proves (observed-real, live `gpt-6-sol`).**
  - One factory identity: a single Strands/A2A harness in factory mode.
  - A live-authored and published graph (1 round, valid first draft).
  - Four black-box model agents behind async A2A. Each agent has its own identity, SQLite Task store and model session per Task.
  - The run: two research Tasks, then synthesis, then an independent Quality review, then routing.
  - The three routes:
    - first-pass acceptance with one release;
    - rejection, then agent repair, then acceptance of the exact repaired revision (k = 2), with one release;
    - bounded exhaustion (3 rejected revisions, `repair:exhausted`), then a Director wait on the original Task, then an abort, with zero releases.
  - Every Quality verdict is a **live model verdict** (`decided_by:"model"`), bound to the exact candidate sha256.
- **Does not prove.**
  - Real release: the release receiver is still an **HTTP fixture**.
  - Unprompted defect discovery: the route 2 and route 3 defects were **induced** by a test-only stimulus.
  - An autonomous Director choice: the abort is **caller-prompted, model-decided**, and abort is the only action the graph permits at `repair_exhausted`.
  - Reliability across accounts, models, questions or repeated runs: two live runs (2 and 3) showed the same behaviour, but that is too few to measure reliability.

## Attempts (all preserved)

Homes stay under `/tmp`. Evidence is in `evidence/single-factory/`. "Code" is the checkout the run executed (`git_commit` in the evidence). "Evidence commit" is where the evidence was committed.

| Attempt | Code | Evidence commit | Result | Notes |
| --- | --- | --- | --- | --- |
| agent probe scripted-1 / live-1 | lane A | `2a2d0a9` | pass, **not counted** | Pre-integration probe of one async model agent (`agent-probe-*`) |
| scripted-1..4 (`/tmp/exo-sf-syn-1..4`) | lane D preflight | `e9a8f3a` | fail (scripted-4: G-2, G-7, R1-b, SF-1, SF-2) | Collector and product defects found. Raw route histories from scripted-2..4 carried the Director token in base64 payloads; they were moved out of the repo to `/tmp/exo-sf-syn-<n>/evidence-raw/` |
| scripted-5..7 | `32834c2` + lane D fixes | `e578f14` | structural pass | During the review #2 checker fixes (F1–F11) |
| scripted-8, 8-final | `83cbd3d` | `7c0d2f2` | structural pass | After the pure-replay fix |
| scripted-9 | `fed341d` | `2dc6ec8` | structural pass | After the G-7 binding fix |
| scripted-10 | `8865520` | `6fa9537` | structural pass | Prerequisite for live-2 |
| **live-1** (`/tmp/exo-sf-live-1`) | `2dc6ec8` | `a27231d` | **fail**: R2-b, R3-b, R3-d, G-2, G-5 | Diagnosed below |
| **live-2** (`/tmp/exo-sf-live-2`) | `6fa9537` | `92628b7`; attestation `cd13b91` | recorded 24/24; **does not qualify** | Review #3 F1/F2; post-hoc attestation `g4_final.pass:false`; replay under the review-3 checker gives 21/24 (SF-0, G-4, G-7) |
| scripted-11 (`/tmp/exo-sf-syn-11`) | review-3 fixes (draft) | `f4c376e` | does not qualify | Checker changed after it ran; its attestation is `checker_bound:false` |
| **scripted-12** (`/tmp/exo-sf-syn-12`) | review-3 fixes | `f4c376e` | **24/24 + attestation pass** | G-7 prerequisite for live-3 (`observed-synthetic`); its checker sha256 equals live-3's. Its console file was copied in after its attestation and is covered by live-3's attestation instead (review #4 N1) |
| **live-3** (`/tmp/exo-sf-live-3`) | `3d2ed62` | `cd13b91` | **24/24 + attestation pass; qualifies** | `codex-subscription-3*.json/md`, console, attestation |

### Live attempt 1 diagnosis

Lane D ran a term-by-term, read-only diagnosis (`handoff/sf-director.md`, "Live attempt 1 diagnosis").
- **R2-b, R3-b: checker defect.** The model's blocking findings named the planted claim (`C6`, `C7`), but the checker read a null `claim_id` from the stimulus log.
- **R3-d: checker defect.** The checker counted the Director's extra `inspect_run` on the start turn.
- **G-5: checker defect.** The checker required 40 ports; the live block has 39.
- **G-2: checker defect plus environment.**
  - The checker required an in-window broker start.
  - Separately, the running broker (PID 59833, started 23 September from the old `/Exomachina` checkout) predated the `session` field, so every stream event had `session:null`.

Fixes are in `88136a9` (merged at `8865520`), each quoting the pre-registered sentence it implements and each with a regression test. Replay under the corrected checker (not a new run): 23/24; G-2 still fails.

The orchestrator restarted the broker before live-2 with the documented `ModelBroker.stop()` / `ensure_started` from integration code. The new PID is 28404, and it was left running.

### Live attempt 2 and review #3

The review-3 findings (`handoff/sf-review-3.md`):
- **F1:** the in-run scan hashed an intermediate evidence JSON, not the final bytes.
- **F2:** the scenario selected the Director identity token for an exact-value absence search.

The orchestrator had asked for that comparison in the review-2 fix job. It also ran its own in-process value scan on live-2. The token was never printed, but that scan also read it.

Lane D's fixes (`f4c376e`):
- The token read is gone. G-4 now verifies structurally that every sensitive key and Temporal `input` in every exported file, including recursively decoded base64 payloads, is a redaction object.
- New `scenarios/sf_attest.py`, run after the scenario exits, adds:
  - a fresh positive control;
  - a scan of the home, evidence, model-home logs and the commit-candidate set, including the final evidence JSON and console;
  - a sha-bound manifest;
  - structural redaction verification;
  - a packet-provenance cross-check.
- G-7 requires the synthetic prerequisite's passing attestation.
- SF-0 now reads preserved preflight artifacts: the pre-launch `lstat`, the served card body and its sha256, and the used broker status fields.
- Behaviour labels now name induced defects.
- Regression tests cover each fix.

Nothing in the scenario or attester, and nothing the orchestrator ran for live-3, selected the Director token or opened a credential file.

## Live attempt 3: per-check verdicts

All checks are `observed-real` except G-7, which is `observed-synthetic` because it binds to scripted-12. The last column gives this run's numbers.

| Check | Verdict | Behaviour label | Key observation |
| --- | --- | --- | --- |
| SF-0 | pass | — | Pre-launch `lstat` gave `FileNotFoundError`. 1 factory. Served card has 1 skill (body and sha256 preserved). Broker `signed_in:true, expired:false`, account `sha256:188b022d6e97`. `EXO_MODEL_HOME`, `EXO_CODEX_BASE_URL`, `OPENAI_API_KEY` and `ANTHROPIC_API_KEY` all unset |
| SF-1 | pass | — | Live authoring: `codex-subscription`/`gpt-6-sol`, 4 model calls (limit 12). **1 round, first_pass_valid = true**. Published, active and the only publication. Bindings digest equals the testbed's. Packet `4973b5f5…` |
| SF-2 | pass | — | 5 distinct identities, ports and state directories; 4 SQLite Task stores plus release. Pin verification before every send and poll. Import audit clean (the A1 launcher exception is reported) |
| SF-3 | pass | — | All 6 workflows pinned to manifest `6cbb2bd8…` on build `b-3e196d3f362c`. Caller messages are text only |
| R1-a | pass | spontaneous | 1 live Director turn: accepted `start_research`, plus an unprompted `inspect_run`. Task `working` → `completed` |
| R1-b | pass | spontaneous | 2 research Tasks, each in its own store, artifact-bound, live, with distinct sessions |
| R1-c | pass | *recorded label is wrong*; see note | Live Quality accepted `r1` (`decided_by:model`, 0 findings). Reviewer ≠ author |
| R1-d (structural) | pass | — | 1 release (`r1` `7df919cd…`). Acceptance = receipt = Task artifact = saved markdown bytes |
| R1-d (semantic) | **answered and supported** | orchestrator reading (AI judgment) | See below |
| R1-e | pass | *recorded label is wrong*; see note | 0 rejections; exactly 1 verdict |
| R2-a | pass | induced | Exactly 1 stimulus row: route 2, `r1`. No control key in caller messages, workflow inputs, briefs or Director arguments |
| R2-b | pass | spontaneous verdict on induced defect | Live Quality rejected `r1` with blocking `C6` (the planted claim): "The packet does not say that a Quality reviewer evaluated the report with a live model" |
| R2-c | pass | live repair content on assigned repair | `r2` repair brief carried the `r1` sha and findings. The new sha has no stimulus, and the planted text is absent from the released markdown |
| R2-d | pass | spontaneous verdict | Live Quality accepted **`r_k` with k = 2** (`74d6d919…`). Acceptance = receipt = Task artifact. 1 release |
| R3-a | pass | induced | Planted claim on `r1`, `r2` and `r3` of route 3 only |
| R3-b | pass | spontaneous verdict on induced defect | Live Quality rejected `r1`, `r2` and `r3`, each with blocking `C5` on the planted claim ("…validated against live production services, but both remain fixtures"). `repair_count = max_repairs = 2`; `repair:exhausted` |
| R3-c | pass | — | Original Task observed `input-required`; phase `awaiting-director` |
| R3-d | pass | caller-prompted, model-decided (abort is the only permitted action) | Exact neutral follow-up on the original Task and context. The live turn called `inspect_run`, then `decide_wait(abort, r3, current sha)`. Its rationale named the fixture/live misstatement. Task `completed`/`aborted`; 0 releases; no artifact |
| R3-e | pass | same | Turns 1/1/2, each 3 model calls and 2 tool calls; slowest 24.6 s (limits 4 / 4 / 90 s) |
| G-1 | pass | — | Every journal row for the 3 runs `confirmed`; 0 incidents |
| G-2 | pass | — | Broker PID 28404 at start, at all 7 samples and at the end, with no start event. 18 agent Tasks, each 1 live call, with a unique session present in broker streams. Authoring 4 and Director 12 streams, counted separately |
| G-3 | pass | — | 18 Tasks, maximum 1 model call, maximum 17.0 s (limits 3 / 240 s) |
| G-4 | pass (+ attestation) | — | In-run scan: 2,684 files, 326 candidates, 0 hits, positive control detected. Structural redaction over 9 exported files, 32 decoded payloads and 71 redaction objects: 0 violations. **Final-byte attestation** (`codex-subscription-3-attestation.json`): fresh positive control detected, 0 hits, manifest bound to the final evidence and console bytes, redaction and checker bound |
| G-5 | pass | — | 10 started process groups exited; 0 listeners in the 39-port block; broker PID unchanged |
| G-6 | pass | — | See Suites |
| G-7 | pass | observed-synthetic | Bound to `scripted-12.json` and its passing attestation by path and sha256. Checker sha256 is equal (`34737983…`); same interpreter build |

Release count by route: **1 / 1 / 0**.

Agent Tasks: research_findings 3, research_risks 3, synthesizer 6, quality 6.

Model calls: 18 agent + 12 Director + 4 authoring = 34 live `gpt-6-sol` calls, through one broker process and one account.

**Label defect (found by the orchestrator after the run).**
- Live-3 records R1-c and R1-e as "spontaneous verdict on induced defect", but route 1 has no stimulus.
- Cause: the review-3 label change reused a shared variable. Pass/fail is unaffected.
- The correct label is "spontaneous verdict (no stimulus)". The committed evidence is left unchanged.
- Lane D fixed the label after the run (`02b8262`, merged at `4999579`).
- An in-memory replay of live-3 under the fixed checker gives identical `pass` values for all 24 checks; only the R1-c and R1-e labels change.
- Live-3's recorded checker sha256 (`34737983…`) is therefore the pre-fix checker and differs from HEAD (`c09db693…`) by this label-only change.

**Behaviour compared with live-2.** Live-3 behaved the same way:
- live Quality detected every planted claim;
- route 2 was accepted at k = 2;
- every Director start turn also called `inspect_run` unprompted;
- route 3 inspected, then aborted.

The planted claim id differed between runs (route 3: `C5` in live-3, `C6` in live-2). The binding is by exact claim text, so this does not affect the checks.

### R1-d semantic reading (orchestrator reading, AI judgment, A1)

**Inputs.**
- Report: `evidence/single-factory/codex-subscription-3-route1.md` (`r1`, `7df919cd…`, live Quality accepted).
- Structured claims C1–C5.
- Packet: `packets/exo-qualification-2026-09-23/packet.json` (E1–E10). Packet provenance matches 10/10 in the attestation.

**Live vs fixture status: answered, supported.**
- All three spikes passed on the merged tree: E10.
- A and B ran real processes with a fixture Director: E3, E8, E1.
- C had a live broker-backed `gpt-6-sol` Director: E3, E8.
- Authorized start, `working` → `input-required`, code rejection of a forbidden input, and one caller-directed abort with zero releases: E5 (C-1..C-3), E6, E9.
- Stale and unauthorized probes were fixture-injected: E5 (C-4).
- Quality, capability content and the HTTP release receiver are fixtures; the external agent is a local fixture; passing does not establish a live end-to-end release: E4, E6, E7, E2.

**Remaining gaps: answered, supported.**
- Director limited to one account, one model and abort; autonomous decisions and reliability unmeasured: E6, E7.
- Signing/attestation, remote authentication, directory and workflow-level polling: E7.
- Two instances, no `create_app()` lock, no stress testing: E7.
- Old-build retirement, hardening and memory ceiling: E7.

Omissions, which are not errors:
- contract attestation (E7);
- the Director token in history (E7);
- `message/send` blocking for the whole Director turn (E6).

**Next priority: answered, supported as inference.** The report recommends end-to-end qualification with real Quality, capability content and release, and live Director coverage beyond caller-directed abort. The packet has no explicit priority statement. The recommendation follows from the gaps in E6 and E7, and the report frames it as guidance ("Until then, do not treat passing fixture checks as production release proof", E2).

**Unsupported claims: none found.**

Citations in the structured claims: C1: E3, E8, E10; C2: E5, E6, E9; C3: E2, E4, E6, E7; C4: E6, E7; C5: E2, E6, E7. All exist, and each supports its claim. The markdown itself has no inline `[E…]` citations.

For context, the packet predates this spike, so "Quality remains a fixture" is correct for the packet. This spike's own live Quality is the new result, not something the report should know.

## Suites (G-6)

- **Baseline** at `2d609e3`: Python 112 OK; Node 18.
- **At the live-3 code** (`54c07a1`, the same source tree as the run at `3d2ed62`) under `/usr/bin/lockf -k /tmp/exo-qual-suite.lock`: **Python 174 OK**, **Node 19/19** (`evidence/single-factory/g6-{python,node}-suite-54c07a1.txt`).
- Earlier logs are at `8865520` (167 / 19).
- **Final branch head** (`4999579`, after the label fix): **Python 175 OK**, **Node 19/19** (`g6-{python,node}-suite-4999579.txt`).
- The broker PID was 28404 before and after every suite run.

Tests changed because they encoded the removed caller-steered `outcome_mode`, old branch types or in-factory synthesis:
- `test_authoring.py`
- `test_authoring_budget.py`
- `test_binding.py`
- `test_contract.py`
- `test_interpreter.py`
- `test_long_client_async.py`
- `test_testbed.py` (report profile)
- `test_director_agent.py` (`start_research(question)` only)
- `test_harness_modes.py` (legacy structured commands now require explicit opt-in)

New test files:
- `test_agent_roles.py`
- `test_model_agent.py`
- `test_report_async.py`
- `test_report_contract.py`
- `test_factory_boundary.py`
- `test_sf_checks.py`
- `broker/test/director-mock.test.mjs`

## Process

- The brief was pre-registered before any code (`bbc8c03`).
- A1 was recorded before any scenario run.
- The orchestrator's own work: brief, interface arbitration, reviews, integration and evidence.
- Four lanes (I interpreter, A agents, R roles and packet, D Director and scenario) were coded by Codex workers in separate worktrees.
- Four independent reviews, each preserved in `handoff/sf-review-{1..4}.md`:
  1. usefulness passed "TBD" sections;
  2. 11 checker false passes, a DataPart bypass, and the Director token in exported history;
  3. live-2 final-byte coverage and the token read;
  4. live-3.
- Every finding was fixed by its owning lane, with tests, before the runs it affects. The exception is the live-3 route-1 label defect: it was found and fixed after the final attempt, and it is label-only.

## Limitations and follow-ups

1. **The release receiver is an HTTP fixture.** "One release" is a fixture receipt.
2. **The defects in routes 2 and 3 were induced.**
   - A test-only synthesizer stimulus (`--test-controls`) appended a false claim after the model wrote each armed revision.
   - Detection by live Quality is spontaneous. The defects are not.
   - Route 3 re-applies the stimulus to every revision, so exhaustion is forced by construction.
3. **The abort is caller-prompted and model-decided.** The follow-up is neutral, but the Director prompt and the graph permit only `abort` at `repair_exhausted`.
4. **Every Director start turn also called `inspect_run` unprompted.** This is within budget.
5. **Small sample.** Two live structural passes (live-2 and live-3), one qualifying, on one account, one model and one question. Reliability is unmeasured.
6. **Broker environment.**
   - Live-1 ran against a stale broker from the old checkout.
   - The orchestrator restarted the broker before live-2.
   - PID 28404 executes from this integration worktree (`Exomachina-single-factory/.../broker/exo-model.mjs`). Restart it from the main checkout before removing the worktree.
7. **Token handling.**
   - In live-2, the scenario, and separately the orchestrator's own audit, read the fixture Director token in-process (never printed) for value comparisons. Live-3 did not.
   - The Director token still travels in internal Temporal history (E7). Exported evidence is redacted.
   - **Follow-up:** 10 files already on `main` contain the fixture Director token inside base64 history payloads:
     - `evidence/histories-r2/*`;
     - `evidence/live-authoring-codex-subscription/{parent,child}.json`;
     - `evidence/live-authoring-synthetic-loopback/{parent,child}.json`.
   - They need redaction; the token belongs to a fixture home.
8. **Deliberately out of scope:** directory, second factory, sandboxing, multi-tenancy, installer, load testing and hardening.
9. **Not covered end to end:** A2A idempotent replay and conflicting-payload rejection, and the failure-to-incident route in this report graph. They are unit-tested only (review #3 F5).
