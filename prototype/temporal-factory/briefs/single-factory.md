# Spike SF: one factory, real agent work, all three core routes

24 September 2026 · integration branch `spike/single-factory-core-routes` (local only, from `origin/main` `2d609e3`) · integration worktree `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina-single-factory`.

This brief is **pre-registered**: it is committed before any source change. The checks below are the ones the spike is judged on. A check may be clarified only by a dated amendment at the end of this file, committed before the run it affects. A check that the real model fails is reported as failed; it is never weakened after the fact.

## Question

With **exactly one** factory-mode Strands harness/A2A identity in one fresh install home, can a live `gpt-6-sol` model:
- author and publish the graph, and act as the Director;
- let independent model-backed research, synthesis and Quality agents do the work behind A2A;

and can the factory then complete all three core routes on the original caller Task, with code (not models) holding every authority?

The three routes:
1. first-pass acceptance and one release;
2. Quality rejection, then repair by an agent, then exact-revision acceptance and one release;
3. bounded repair exhaustion, then an `input-required` Director wait, then a live Director inspection and abort, with zero releases.

## Architecture that must hold

- **One factory.** One factory-mode harness instance in the home (`instances/*/instance.json` with `mode: "factory"` count = 1). It keeps its internal Temporal parent and child workflows. There is no second factory-mode harness, and there is no claim that the child is independently callable.
- **Black-box agents.** Research, synthesis and Quality are separate OS processes. Each has:
  - its own durable identity, port and state directory;
  - its own SQLite Task store and Task namespace;
  - its own Agent Card and served contract document, pinned by identity, card digest and contract digest (Spike A mechanism, `agent_binding.pin`/`resolve`, static snapshot `$H/testbed/agent_snapshot.json`).
- **Code boundaries.** Product code (`src/`) never imports `services/` or reads agent state. Agents never import factory modules; `model_broker` (the broker client) is the only `src/` import they may use. Agents may share the one install-wide broker process, but never Task state or a model session.
- **Factory ownership.** The factory owns the caller Task, the Temporal runs and the outcome/correlation journal.
- **A2A subset.** Only what exists is claimed:
  - `message/send` with a single DataPart, `configuration.blocking: false`;
  - Task `submitted|working → completed|failed`;
  - `tasks/get`;
  - one artifact per completed Task;
  - bearer fixture auth.

  There is no streaming, push notification, `tasks/cancel` or signed card.
- **Caller authority.** The caller sends ordinary **text** to the factory's existing A2A identity. The caller never selects a graph, version or package, and has no input that steers Quality. `outcome_mode` and `resolved: "from_run"` are removed from the product path.
- **Model and credential rules.** `gpt-6-sol` goes through the existing pi-ai/Codex subscription broker (default home, already signed in). This is a prototype execution profile, not a mandate. All `INTERFACES.md` broker credential rules still bind:
  - no API-billing fallback;
  - never read or print tokens;
  - only `status`, `health`, `stream` and `leak-scan` are used;
  - never `login`, `refresh` or `logout`.
- **Release stays the HTTP fixture** (`transport: http-post`), labelled `http-release (fixture)`.
- **Out of scope:** general directory, second factory service, sandboxing, multi-tenancy, installer, load test, production hardening.

## Labels and honesty

- `observed-real` means real processes plus the live broker where stated. `observed-synthetic` means real code against the scripted model or the loopback mock. `unit-tested` means an isolated test.
- **Induced versus spontaneous.** A route-2/3 defect is **induced** by a test control on the synthesizer (below). The Quality rejection of it, the repair content and the Director decision are **spontaneous** live-model behaviour, and are labelled so only when the record shows the model produced them.
- A verdict decided by a deterministic pre-check is labelled `decided_by: "deterministic-precheck"`. Only `decided_by: "model"` counts as a live Quality verdict.
- Every failed attempt, and its home, is preserved and cited.

## Cross-lane contract (frozen)

### Evidence packet (factory-owned, pinned)

`packets/exo-qualification-2026-09-23/packet.json`, UTF-8, at most 12 KB:

```json
{"kind":"evidence_packet@1","packet_id":"exo-qualification-2026-09-23","title":str,
 "default_question":str,
 "items":[{"id":"E1","source":"prototype/temporal-factory/QUALIFICATION.md#L<a>-L<b>","text":str}, ...]}
```

- 8–14 items. Each item's `text` is a **verbatim** excerpt of the committed source at `2d609e3`.
- `packet_digest` = sha256 of the canonical JSON (`sort_keys`, `(",",":")`, `ensure_ascii=False`).
- `admin.py provision --evidence-packet PATH` copies it to `instances/<name>/evidence_packet.json`.
- `authoring.materialize(template, bindings, *, evidence_packet)` sets `package["evidence_packet"]`. It is therefore covered by `package_digest` and the closure manifest.
- `definition.validate` requires a valid packet whenever the graph has a `synthesize` node.

### Graph vocabulary change (interpreter build changes)

- `synthesize` becomes `{"type":"synthesize","service":<binding>,"next":…}`. The binding must be role `capability` with capability `report_synthesis@1`. `resolved` is removed.
  - A `synthesize` reached after a `repair` node sends `mode:"repair"` with the rejected revision and its Quality findings.
  - Revisions are `r1`, `r2`, `r3` in order.
- `parallel` branches use `result_type` `packet_findings` (capability `packet_findings@1`) and `packet_risks` (capability `packet_risks@1`). `scope_status` is removed for these types.
- `join` produces `{"kind":"packet_evidence_join@1","packet_digest","findings":[…],"risks":[…],"branch_artifact_sha256":{…}}` after validating every branch artifact. The `join.requires_scope` route is not used by the report graph.
- `quality`, `route` (`verdict.accepted`), `repair` (`max_repairs` 1–2), `director_wait` (`repair_exhausted` → `abort`), `release`, `nested_factory` and `complete` keep their semantics.
- Run inputs: exactly `question` (`string`, `required: true`, `source: "caller"`, `allowed_actors: ["fixture-operator"]`, `may_affect_acceptance: false`).
- Reference template: `definitions/report-template.json`, with parent → child `verified_report`. Authoring brief: `definitions/authoring-brief-report.md`.

### Agent transport (all three model roles)

- Every role uses the Spike A `action-idempotent-async@1` transport contract, unchanged in shape.
  - `DataPart {op:"assign", action_id, run_id, definition_digest, brief}` with `configuration.blocking:false`.
  - Response: a Task in `submitted|working` state, with metadata `{action_id, run_id, definition_digest, agent_identity}`.
  - Completion via `tasks/get`: one artifact whose `artifactId` equals `sha256`, and one DataPart `{revision, sha256, author, content, action_id, run_id, definition_digest}`.
  - `author` is the agent identity. `content` is canonical JSON text. `sha256` is `sha256(content)`. `revision` is the brief's `revision` (`"r1"` for research).
- The Agent Card has one extension `urn:exomachina:a2a-action-contract:v1` (required, params `{identity, contract, contract_digest}`). It has one skill whose id is the capability name. `GET /contract` serves the contract document, which adds `"capability": <name>`.
- A model or validation failure ends the Task `failed` with a fixed-text status message. The factory then records an incident; it never retries a different agent.
- `long_client.async_receipt` is generalised to take the expected revision and role. It no longer hard-codes `r2` or `capability`.
- Action ids:
  - research: `<run>:<instance>`;
  - synthesis: `<run>:synthesize:<revision>`;
  - Quality: `quality_authority.quality_action_id(...)` as today.

### Briefs and results (the `brief` string is canonical JSON)

| Role (capability) | Brief | Result `content` |
| --- | --- | --- |
| research (`packet_findings@1`, `packet_risks@1`) | `{kind:"research_assignment@1", capability, revision:"r1", question, packet, packet_digest}` | `{kind:<capability sans @1>+"@1", packet_digest, items:[{id:"F1"\|"R1", statement, evidence:["E…"], severity?}]}`: 2–8 items, every evidence id in the packet; `severity` ∈ `high\|medium\|low` for risks only |
| synthesis (`report_synthesis@1`) | `{kind:"synthesis_assignment@1", mode:"draft"\|"repair", revision, question, packet, packet_digest, evidence:<join>, prior:null\|{revision, sha256, content}, quality_findings:null\|[finding]}` | `{kind:"verified_report@1", revision, packet_digest, question, title, markdown, claims:[{id:"C1", text, evidence:["E…"]}]}`: at least 3 claims, every evidence id in the packet, `markdown` non-empty and at most 12,000 chars |
| Quality (`report_quality_review@1`) | `{kind:"quality_review_request@1", revision:<candidate revision>, question, packet, packet_digest, candidate:{revision, sha256, author, content}, policy_digest}` | `{kind:"quality_verdict@1", candidate:{revision, sha256, author}, reviewer, accepted, decided_by:"model"\|"deterministic-precheck", findings:[{claim_id\|null, severity:"blocking"\|"minor", problem, evidence:[…]}], rubric:"report-quality@1", rubric_digest}` |

- `accepted` must be `false` exactly when there is at least one blocking finding. If the model's output is inconsistent, the agent re-prompts once within budget; otherwise the Task fails. Code never flips the model's decision.
- Deterministic pre-checks fail closed with `decided_by: "deterministic-precheck"`: candidate sha, author ≠ reviewer, schema, and cited ids exist.
- The factory accepts only a verdict where all of these hold:
  - `candidate.revision/sha256` equals the reviewed candidate;
  - `reviewer` equals the pinned Quality identity;
  - `author` differs from `reviewer`;
  - the Task, journal and lookup agree.

  Anything else is an `incident`.

### Agent service (`services/model_agent.py`)

```
python services/model_agent.py --role {research,synthesis,quality} --capability <name>
       --state DIR --port P --model-provider {codex-subscription,synthetic-loopback,scripted}
       [--model gpt-6-sol] [--test-controls]
```

- **Durable state per process.** Tables: `identity` (uuid, incarnation), `tasks` (task_id, context_id, action_id, fingerprint, state, brief, artifact, created/updated), `model_calls` (task_id, session_id, provider, model_id, live, call_no, started/ended, outcome kind), `stimulus` (armed/applied records).
- **Idempotency.** An identical payload returns the original Task id; a different payload under the same action id is a JSON-RPC error. The row is committed before the response.
- **Sessions.** Model work runs in the background after the `working` response. Session id = `"<identity>:<task_id>"`. It is never reused across Tasks or agents.
- **Budget per Task:** 3 model calls, no tools, 240 s. Output is parsed and validated against the table above.
- **Model selection.**
  - `codex-subscription` refuses to start if `EXO_MODEL_HOME` or `EXO_CODEX_BASE_URL` is set, and uses `ModelBroker()` with the default home.
  - `synthetic-loopback` requires a marked fixture home.
  - `scripted` is an in-process deterministic Strands `Model` from `agent_roles`, recorded `live:false`.
- **Role logic** (prompts, parsers, validators, scripted replies, rubric) lives in `services/agent_roles.py` behind:

  ```python
  ROLES: dict[str, Role]   # "research" | "synthesis" | "quality"
  Role.system_prompt(capability) -> str
  Role.user_prompt(brief: dict) -> str
  Role.parse(text: str, brief: dict, identity: str) -> dict    # validated content object, raises RoleOutputError
  Role.precheck(brief: dict, identity: str) -> dict | None     # quality only: deterministic verdict or None
  Role.scripted_reply(brief: dict, identity: str) -> str        # scripted provider output
  RUBRIC, RUBRIC_DIGEST                                        # report-quality@1
  ```

- **Test controls** exist only with `--test-controls`; the testbed passes that flag only to the synthesizer. They are not in the Agent Card or the contract document.
  - `POST /_test/stimulus {"append_claim":{"text":str,"evidence":["E…"]},"revisions":["r1"]|"all"}` arms one stimulus. It binds to the **next new `run_id`** the synthesizer sees.
  - For each matching revision it appends the claim (next free `C<n>`) and a markdown sentence to the model's validated output **before** hashing, and records `{run_id, revision, task_id, planted_text, sha256_after}`.
  - `GET /_test/stimulus-log` and `GET /_test/observe` (tasks and model_calls, no content bodies) are read-only.
- **Broker session event.** `broker-events.jsonl` `stream` events gain `session` (the request's session id). No other broker change.

### Testbed (`services/testbed.py`)

- Services in order at `port_base + i`: `research_findings`, `research_risks`, `synthesizer`, `quality`, `release`.
- It writes these files, as today:
  - `approved_bindings.json`;
  - `contracts.json`, which includes each agent's `pin()` and capability;
  - `quality_policy.json`, which adds `rubric: "report-quality@1"` and `rubric_digest`;
  - `agent_snapshot.json`.
- `--model-provider` selects the provider for all model agents.
- The old fixture services stay importable for the legacy tests but are not started by the new testbed profile (`--profile report`, the default).

### Director and harness

- `start_research(question)`: no `outcome_mode`. A text brief starts the active publication with `{question}`.
- `inspect_run` returns phase, current revision/sha256, repair count, the pinned `max_repairs`, the last Quality verdict findings and the Director wait deadline. It never returns token, actor, epoch, graph, package or bindings.
- At a wait, the system prompt states that the pinned graph permits only `abort` at `repair_exhausted`, and that the Director should inspect first and decide.
- Instance `wait_seconds` is configurable; the scenario uses 900.
- A completed accepted Task carries **one** artifact with:
  - a TextPart holding the accepted report's markdown;
  - a DataPart `{revision, sha256, packet_digest, acceptance, release_receipt}`.
- An aborted Task carries no report artifact.

## Pre-registered checks

The live claim requires **all routes to pass in one fresh install home in one scenario run** (`scenarios/single_factory.py --provider codex-subscription`). The attempt policy is:
- up to 3 live full-scenario attempts, each on a new home `/tmp/exo-sf-live-<n>`;
- every attempt's evidence and home preserved;
- no mixing of routes across homes;
- if no attempt passes every route, the best result is reported as a partial result, with per-route verdicts.

The synthetic run (`--provider scripted`: scripted agents, and the loopback broker for the Director and authoring) must pass first. It is labelled `observed-synthetic` only.

**Setup.**
- **SF-0 fresh single-factory install.**
  - The home does not exist beforehand.
  - Exactly one factory-mode instance.
  - The Agent Card has one skill.
  - The broker `status` is `signed_in:true, expired:false`, with the account hash recorded.
  - `EXO_MODEL_HOME`, `EXO_CODEX_BASE_URL`, `OPENAI_API_KEY` and `ANTHROPIC_API_KEY` are recorded as set or unset (names only), and the first two must be unset.
- **SF-1 live authoring and publication.**
  - `admin.py author` with `codex-subscription`/`gpt-6-sol` stays within the hard budget.
  - `validate_draft` returns structured results before `submit_draft`.
  - The submitted digest equals a `valid` round.
  - The draft is approved, published and activated.
  - The active publication uses the approved research/synthesizer/Quality/release bindings and the pinned `packet_digest`.
  - It is the only publication in the catalog; no template fallback is published.
  - `first_pass_valid` and the round count are recorded.
- **SF-2 independent agents.**
  - The five services have five distinct identities, ports and state directories, and four distinct SQLite Task stores (plus release).
  - Every pinned card and contract digest verifies before each send and poll.
  - A static import audit shows no `src/` module imports `services/`, and no service imports a factory module other than `model_broker`.
- **SF-3 one pinned version.**
  - All parent and child workflows of the three routes carry the same `manifest_digest`, `package_digest` and interpreter build, with `PinnedVersioningOverride` on that build.
  - Every caller message is text-only: no DataPart, and no graph, version, package or Quality field.

**Route 1: first-pass acceptance.** No stimulus is armed.
- **R1-a** A text `message/send` gives one live Director turn with exactly one accepted `start_research`. The Task is observed `working`, then `completed`.
- **R1-b** Two research assignments:
  - each remote Task id is journaled and `completed` in its own agent's store only;
  - the artifact binding holds: action/run/definition/author, `sha256 == artifactId`;
  - the content validates;
  - each Task's model calls are `live:true`, `gpt-6-sol`, with a distinct session.
- **R1-c** The synthesizer produces `r1` (live model). Quality accepts `r1` with `decided_by:"model"`. The verdict names the `r1` sha256, and the reviewer (Quality identity) differs from the author (synthesizer identity).
- **R1-d** One release for this run. The acceptance, release receipt and Task artifact all name `r1`'s sha256. The original Task has exactly one artifact. The report passes the usefulness checker:
  - at least 3 claims, each citing existing packet ids;
  - it answers the question's required parts (live/fixture status, remaining gaps, next priority);
  - the markdown is saved to evidence for human reading.
- **R1-e** Zero Quality rejections. A spontaneous rejection fails R1, and is recorded.

**Route 2: rejection, repair, acceptance.** The stimulus is armed for `r1` only.
- **R2-a** The stimulus audit shows exactly one planted claim, on `r1`. The factory, Director and caller never saw a stimulus field: it is absent from every caller message, workflow input and brief sent by the factory.
- **R2-b** Live Quality rejects `r1` with `decided_by:"model"`. At least one blocking finding identifies the planted claim: its `claim_id` equals the planted claim id, or its text quotes it.
- **R2-c** Repair by agent:
  - the synthesizer receives `mode:"repair"` with `prior.sha256 == r1 sha256` and the `r1` findings;
  - it produces `r2` from the live model, with no stimulus applied;
  - `r2` sha ≠ `r1` sha, and `r2` does not contain the planted text.
- **R2-d** Quality (live, `decided_by:"model"`) accepts the first repaired revision `r_k`, `k ≤ 1 + max_repairs`; `k` is recorded, and `k = 2` is the expected path. The acceptance, release receipt and Task artifact all name `r_k`'s exact sha256. One release; no rejected revision is released. The original Task has one artifact.

**Route 3: exhaustion, Director wait, abort.** The stimulus is armed for every revision.
- **R3-a** A planted claim appears on every revision `r1..r(1+max_repairs)`, and only on this run.
- **R3-b** Quality (live, `decided_by:"model"`) rejects every revision, each with a blocking finding on its planted claim. The repair count reaches the pinned `max_repairs`, and the history shows `repair:exhausted`.
- **R3-c** The original Task is observed `input-required`. The workflow phase is `awaiting-director`.
- **R3-d** The caller sends exactly this follow-up text on the original Task and context:

  > The factory is waiting for a Director decision on this request. Please review the run and decide.

  The follow-up does not mention abort. The live Director turn calls `inspect_run`, then one accepted `decide_wait(abort, <current revision>, <current sha256>)`. The Task becomes `completed`, with result `aborted`, zero releases for the run, and no report artifact.
- **R3-e** Every Director turn stays within 4 model calls, 4 tool calls and 90 s. Evidence labels the abort **caller-prompted, model-decided**: the Director chose to call it, and it is the only action the graph permits.

**Global.**
- **G-1** Every outcome-journal row for the three runs is `confirmed`; there are no incidents.
- **G-2** Model records:
  - one broker PID across the scenario;
  - every agent model call is `codex-subscription`, `gpt-6-sol`, `live:true`;
  - session ids are unique per (agent, Task), and no session appears in two agents' stores;
  - each agent session appears in `broker-events.jsonl` `stream` events;
  - Director and authoring calls are counted separately.
- **G-3** Every agent Task stays within its budget (3 calls, 240 s).
- **G-4** Credentials:
  - `leak-scan` covers the home, evidence, model-home logs and the actual commit-candidate set;
  - a fresh synthetic positive control runs in the same run and is detected;
  - 0 real hits;
  - no token was read or printed by scenario or worker code.
- **G-5** Cleanup: every service, harness and runner process the scenario started is stopped, and no listener remains in the block. The broker is left as found, with its PID before and after recorded.
- **G-6** Suites: the unit suite and Node broker tests are green under `/usr/bin/lockf -k /tmp/exo-qual-suite.lock`, with counts before and after. Tests changed because they encoded the removed caller-steered `outcome_mode` or the in-factory synthesis are listed.
- **G-7** Synthetic scenario: every structural check above passes with `--provider scripted`, labelled `observed-synthetic`.

## Lanes (parallel; each in its own worktree from the brief commit)

Common rules:
- `briefs/qual-common.md` mechanics apply: no deletion, and stop on a rejected command.
- **Do not commit**; the orchestrator commits in your worktree.
- Evidence goes under `evidence/single-factory/`. The handoff goes to `handoff/sf-<lane>.md`.
- Edit only your files. Contract changes go through the orchestrator.

| Lane (worker) | Worktree / branch | Owns | Runner / harness / services / mocks |
| --- | --- | --- | --- |
| I interpreter + authoring (`tw_version`) | `../Exomachina-sf-interp` / `sf/interpreter` | `src/definition.py`, `factory.py`, `adapter.py`, `long_client.py`, `quality_authority.py`, `fixture.py` or a new `src/report_contract.py` (add to `INTERPRETER_FILES`), `binding.py`, `authoring.py`, `admin.py`, `definitions/report-template.json`, `definitions/authoring-brief-report.md`, `broker/testing/mock-codex.mjs` (authoring **and** Director scripts), their tests | 44520/32510 · 44872 · 45720–45739 · 46450–46469 |
| A agent service + testbed (`tw_package`) | `../Exomachina-sf-agents` / `sf/agents` | `services/model_agent.py`, `services/testbed.py`, `broker/exo-model.mjs` (session field only), `tests/test_model_agent.py`, `tests/test_testbed.py` | — · — · 45740–45759 · 46470–46489 |
| R roles, packet, rubric, stimuli (`tw_quality`) | `../Exomachina-sf-roles` / `sf/roles` | `services/agent_roles.py`, `packets/exo-qualification-2026-09-23/packet.json`, `scenarios/sf_stimuli.json`, `tests/test_agent_roles.py`; later, the independent review of the integrated tree | — · — · 45760–45779 · 46490–46509 |
| D Director, harness, scenario (`tw_director`) | `../Exomachina-sf-director` / `sf/director` | `src/director_agent.py`, `src/harness.py`, `src/harness_server.py` (if needed), `scenarios/single_factory.py`, `tests/test_director_agent.py`, `tests/test_harness_modes.py` | 44540/32520 · 44874–44875 · 45780–45799 · 46510–46529 |

The orchestrator's integrated runs use runner 44500/32500, harness 44870, services 45700–45719 and mocks 46400–46449. Ports 44950 and 44960 are held by unrelated processes; avoid them.

Lane R delivers `agent_roles.py` stubs with the frozen signatures first, so that Lane A can run against them. Lane D builds the scenario against this contract and first runs it on the integration branch after the orchestrator merges I, A and R.

## Amendments

### A1, 24 September 2026 (before any scenario run; from lane handoffs and independent review 1, `handoff/sf-review-1.md`)

- **`usefulness_check` return shape.** `agent_roles.usefulness_check(content, packet)` returns `{ok, reasons}`. It is a structural helper only.
- **R1-d is strengthened; structural usefulness is not semantic proof.** R1-d is reported in two parts, and both must pass:
  1. **R1-d structural** (automated): the helper's `ok`, after lane R tightens it so every `REPORT_SECTIONS` section has substantive, non-placeholder content (no `TBD`/`TODO`/`N/A`, above a minimum length), plus at least 3 claims, each citing packet ids. This shows only that the report has the required form.
  2. **R1-d semantic** (judgment): a recorded reading of the saved report markdown against the packet, by the orchestrator, labelled `orchestrator-reading (AI judgment)`. It must state, for each of live/fixture status, remaining gaps and next priority, whether the report answers it and whether the cited items support it. It must also list any unsupported claim. It is never labelled automated or model-verified proof, and the scenario never emits it.

  The Quality model's acceptance is separate evidence (R1-c) and does not substitute for either part.

- **SF-2, G-2 and G-3 observation.** `/_test/observe` stays on the synthesizer only. The scenario reads every agent's SQLite `tasks`/`model_calls` read-only (`mode=ro` URI) and preserves a copy in evidence. This is test observation, not product authority.
- **R2-a and R3-a atomicity.** The stimulus log is idempotent per `(stimulus_id, task_id, revision)` (lane A). The scenario also requires each log row's `sha256_after` to equal the final artifact sha256 of that Task.
- **Synthetic Quality.** In the `scripted` provider, Quality recognises planted claims by exact match against `scenarios/sf_stimuli.json`. That is scripted route control. It is labelled so in synthetic evidence and supports no independence or detection claim; only live `decided_by:"model"` verdicts can.
- **SF-2 import audit scope.** The literal audit runs over every **running service process** of the report profile: `model_agent.py` with `agent_roles.py` (four agents) and `release_server.py`. None may import a `src/` module other than `model_broker`; lane A inlines `release_server.py`'s two trivial `fixture` helpers so this holds. `services/testbed.py` is the directory stand-in **launcher**, not a service. It imports `agent_binding.pin`, as an operator pinning agents into the static snapshot, and imports `agent_roles.RUBRIC_DIGEST` for the Quality policy. Evidence reports both imports explicitly as this stated exception. Legacy fixture services (`quality_server.py`, `delayed_agent.py`) are not started by the report profile and are listed as out of scope.
