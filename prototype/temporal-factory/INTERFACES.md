# Integrated Temporal factory prototype: shared interfaces

Branch `feat/temporal-factory-prototype`. Root: `prototype/temporal-factory/`. This is the contract between the parallel lanes. Change it only through the orchestrator.

## Architecture being built

- One **customized Strands harness instance** (`src/harness.py`, orchestrator-owned) is configured as `mode: agent` or `mode: factory`. In factory mode, its Director agent and Factory Module run **inside** the instance and expose only that instance's normal A2A identity, Agent Card, and capability contract (`verified-research@1`). There is no separate factory endpoint. Callers never supply a package digest, graph, or version.
- A new graph definition/version or run **does not** spawn a harness service. New runs use the instance's active publication. Waiting runs keep their pinned closure.
- The bundled **local runner** (PostgreSQL + Temporal + interpreter workers, `src/runner.py`) starts **lazily**: on the first factory work, or at harness startup only when the instance has unfinished runs to recover. Routine harness startup must not start it. It is a detached process shared by harness instances on the install. It is not owned by one harness process.
- External agent nodes are **independent black-box services**. Capabilities and Quality are called over A2A. **Exception:** the release receiver is a plain HTTP fixture, not A2A. Its binding advertises `transport: "http-post"`, and `src/receiver_client.py` calls `POST /release` and `GET /receipts/<id>`. Release evidence therefore proves an idempotent HTTP side effect with a receipt, not A2A release delivery. Pinned test services (`services/testbed.py`) stand in for the future directory. No directory discovery.
- The bounded declarative graph interpreter (`src/factory.py` + `src/definition.py`) stays. Every run pins its definition, service bindings and contracts, Quality policy, and interpreter/worker build (`src/binding.py` closure).

## Conventions (all lanes)

- Python: `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python` (3.12.9, temporalio 1.33.0, strands-agents 1.57.0, a2a-sdk 0.3.26). Run tests with `-B`.
- All runtime modules are flat in `src/`. Import them by module name, with `src/` on `sys.path`. `services/` modules add `../src` to `sys.path`. Nothing imports from `tools/spikes/`.
- **No deletion.** Do not run `rm`, `shutil.rmtree`, `Path.unlink`, `os.remove`, or equivalent on anything. Leave trial state behind and report its path. If any command or automatic approval is rejected, stop that action and report it. Do not reword it, retry it another way, or route around it.
- Trial state lives only under `/tmp/exo-proto-<lane>-<suffix>/`. Stay inside your assigned port block.
- Edit only the files your brief assigns. Do not commit; the orchestrator commits. At the end, write `handoff/<lane>.md`: files changed, exact commands run with pass/fail output, evidence paths, known gaps, and anything you could not do.
- Test fault profiles must not be reachable from any product path.

## Install layout (`EXO_HOME`)

```
$EXO_HOME/
  runner/                      # shared lazy runner (runner lane)
    secrets/{postgres-password,temporal-password}   0600
    pgdata/  pgsocket/  server.yaml  temporal.log  postgres.log
    builds/<build_id>/          # immutable interpreter snapshot: INTERPRETER_FILES + build.json
    outcomes.sqlite3            # A2A outcome journal (EXO_OUTCOME_DB)
    activities.jsonl            # EXO_ACTIVITY_LOG
    runner-events.jsonl  runner.lock  runner-ready.json
  instances/<name>/            # one harness instance (orchestrator)
    instance.json  director.sqlite3  catalog/
  services/<name>/              # pinned test A2A services (testbed lane)
```

## Interpreter lane (`src/`: factory, adapter, binding, buildinfo, worker, definition, fixture, projections)

- `binding.py`: `DEPLOYMENT = "exo-factory"`, `QUEUE = "exo-factory"`, `NAMESPACE = "exomachina"`. `INTERPRETER_FILES`: tuple of every flat `src/` file a worker build needs. `source_digest(directory)` hashes exactly those files. `build_id_for(source_digest) -> "b-" + digest[:12]`. `make_manifest`, `verify_closure`, `may_retire` as in the version lane. `PublicationStore(catalog)` has `publish(package, closure, *, label) -> manifest_digest`, `activate(manifest_digest, *, registered_version, registered_source_digest)`, `active() -> record`, `get(manifest_digest) -> record`, and `list() -> [record]`. A record holds `{manifest_digest, package_digest, build_id, label, closure}`.
- `buildinfo.py`: `BUILD_ID`, `SOURCE_DIGEST` read once from env `EXO_WORKER_BUILD_ID` and `EXO_WORKER_SOURCE_DIGEST`. It is imported by `factory.py` as a passed-through import.
- `worker.py`: verifies that `source_digest(own dir) == EXO_WORKER_SOURCE_DIGEST`. It connects to `EXO_TEMPORAL_ADDRESS` in namespace `exomachina` and polls `QUEUE` as `WorkerDeploymentVersion(DEPLOYMENT, BUILD_ID)` with `PINNED` default and `workflow_failure_exception_types=[ValueError]`. It uses env `EXO_OUTCOME_DB` and `EXO_ACTIVITY_LOG`.
- **Workflow start input** (Director → `FactoryRun.run`), exact keys:
  `{run, definition_digest, package_digest, document, package, closure, director:{identity, token, epoch}, run_inputs, run_inputs_digest, authorized_actor, input_authority, wait_seconds}`. There is no `faults` key.
- **Status query** `FactoryRun.status`: lane-1 fields plus `manifest_digest` and `interpreter_build`.
- **Updates**: `claim_owner({actor, token, epoch})` and `director_command({command_id, action:"abort", actor, token, run, definition_digest, revision, sha256, epoch})`, unchanged from lane 1.
- **Results**: `accepted` (with `acceptance`, `receipt`, `released: true`), `aborted`, `expired`, `failed` (child failure incident), and `incident` (unresolved assignment, release, or Quality-inconsistent evidence, from `incident_result`). The parent propagates a child's non-accepted status.
- The run input `question` (optional caller string) flows into each assignment brief: `fixture.branch_brief(..., question=...)`. A `None` question keeps the fixture default.
- `failure_projection.project(status, execution, result)` maps a result `incident` and a closed failed execution to A2A `failed`, `accepted/aborted/expired` to `completed`, and Director/child waits to `input-required`.

## Runner lane (`src/runner.py`; folds in `runtime.py`/`supervisor.py`)

```python
class Runner:
    def __init__(self, home: Path, *, port_base: int | None = None, member_base: int | None = None): ...
    address: str            # "127.0.0.1:<frontend>"
    namespace: str          # "exomachina"
    def is_running(self) -> bool
    def ensure_started(self, *, reason: str, timeout: float = 180) -> dict   # idempotent attach-or-start
    def ensure_build(self, source_dir: Path) -> dict   # {build_id, source_digest, path}; immutable snapshot
    def wait_worker(self, build_id: str, timeout: float = 60) -> dict   # Temporal reports that version polling
    def stop(self, timeout: float = 60) -> dict
    def status(self) -> dict
```
CLI: `python src/runner.py {serve,start,stop,status} --home H`. `ensure_started` spawns a detached `serve` process when needed and waits for `runner-ready.json`. The `serve` supervisor creates secrets, initdb with SCRAM, the Temporal schema, and namespace `exomachina` on first start. It restarts crashed children. It runs one worker per non-retired build under `builds/`, and picks up new builds without a restart. Every start and attach is recorded with its `reason` in `runner-events.jsonl`. Binaries default to `/tmp/exomachina-countertrials/temporal/runtime`, `/opt/homebrew/opt/postgresql@16/bin`, and `/tmp/exomachina-temporal-evaluation/temporal`, with env overrides `EXO_TEMPORAL_RUNTIME`, `EXO_PG_BIN`, and `EXO_TEMPORAL_CLI`. Ports: frontend = `port_base+2`, http `+3`, matching `+4`, history `+5`, worker `+6`, pprof `+11`, metrics `+12`; postgres = `member_base`, membership `member_base+1..4`. Defaults are `port_base=44000` and `member_base=32400`, with env overrides `EXO_RUNNER_PORT_BASE` and `EXO_RUNNER_MEMBER_BASE`.

## Authoring lane (`src/authoring.py`)

- `authoring_vocabulary(approved_bindings) -> dict`: the node types, fields, typed route values, result types, bounds and rules an author may use, generated from `definition.py` constants.
- `materialize(template, bindings) -> package`: `template = {schema, root, child, run_inputs}`, with `"@child"` digest substitution as in lane-1 `author.py`.
- `class GraphAuthor(Protocol)`: the authoring Interface. `StrandsGraphAuthor(model)` is a real Strands `Agent` with tools `describe_vocabulary`, `validate_draft(template_json)`, and `submit_draft(template_json)`. Validation errors go back to the agent, which revises. The session caps rounds. Submission succeeds only if `definition.validate(package, approved_bindings)` passes.
- `AuthoringSession(author, *, approved_bindings, max_rounds=4).run(brief, base_template=None) -> AuthoringOutcome{status, template, package, package_digest, rounds:[{round, draft_digest, valid, errors}], model:{kind, id, live: bool}}`.
- **Hard authoring budget (final-phase amendment).** A round cap counts distinct evaluated drafts only and does not bound model calls. `AuthoringSession(author, *, approved_bindings, max_rounds=4, max_model_calls=12, max_tool_calls=24, deadline_seconds=600)` enforces all four limits. The model is wrapped so the call that would exceed `max_model_calls`, or any call after the deadline, raises `AuthoringBudgetExhausted` **before** reaching the inner model. No provider request is sent after exhaustion. The wall-clock deadline also cancels an in-flight stream, which sends the broker `cancel`. Tool calls past `max_tool_calls`, and every tool call after `round_limit` is reached, terminate the session instead of returning another result for the agent to loop on. The outcome is `status: "aborted"` with `abort: {reason: "round_limit"|"model_call_limit"|"tool_call_limit"|"deadline", model_calls, tool_calls, elapsed_seconds}`, plus the rounds and calls so far. Nothing is approved or published. `admin.py author` returns that outcome and exits non-zero. All four limits are reported in the outcome and in evidence.
- `approve(package, *, approver, policy) -> approval record` (auto-approval within the bounded profile; human approval is a policy choice).
- `model_from_environment() -> (model | None, reason)`. It reports why no live provider is available and never prompts for credentials.
- `ScriptedAuthoringModel`: a deterministic Strands `Model` that drives the same tool loop. Its first draft has a defect, and it revises from the returned error. It is always labelled `live: false`.

## Model broker lane (`broker/`, `src/model_broker.py`) — final phase

Decision (debate consensus, 23 Sep 2026): the harness stays **Strands Python**; live model calls go through one **install-wide, persistent Node process running the published `@earendil-works/pi-ai` 0.87.1** behind a custom Strands `Model`. The embedded Pi SDK is the runner-up and is not built here. Proven offline basis: `/tmp/exomachina-pi-strands-debate/matched/strands-broker-interleaved/` (`broker.mjs`, `pi_broker_model.py` with the `contentIndex` ordered-fragment fix and index-restoring reasoning replay, strict mock). Reuse it; do not fork pi-ai or patch `node_modules`.

**Credential policy (hard rules).**
- Only the ChatGPT/Codex **subscription** OAuth credential (pi-ai provider `openai-codex`, credential `type: "oauth"`). It is obtained by Exomachina's **own** sign-in (`exo-model login`). Never read, copy or import `~/.codex/auth.json`, `~/.pi/**`, or any other tool's credential. pi-ai's `authContext` must return nothing for env and files so `OPENAI_API_KEY` etc. are never consulted.
- **No API-billing fallback.** A subscription failure (`reauth_required`, quota, rate limit, entitlement) surfaces as an explicit error. It never falls through to `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, AWS, or any other provider.
- Tokens exist only inside the broker/login Node processes and the credential file. Python, the harness, Temporal workflow inputs/history, A2A messages/artifacts, `evidence/`, logs and git never contain access tokens, refresh tokens, id tokens or authorization codes. Never print a token, even partially. Nobody asks the user for a raw token.
- Owner-only persistence: `$EXO_MODEL_HOME` is `0700`, the credential file `0600`, written atomically (tmp + rename) under a lock. Refresh happens only inside the broker (pi-ai `Models.getAuth`, which refreshes under the store lock) or via the explicit `refresh` op.
- Product runtime may remove only its own runtime artifacts (a proven-stale socket or lock). Workers still follow the no-deletion rule for everything else.

**Install layout.** `EXO_MODEL_HOME` (default `~/.exomachina/model-broker`; tests use `/tmp/exo-proto-<lane>-<suffix>/model`). It is install-wide and per OS user, independent of any `EXO_HOME` trial directory, so one sign-in serves every harness instance.
```
$EXO_MODEL_HOME/                     0700
  secrets/openai-codex.json          0600  pi-ai OAuth credential {type:"oauth", access, refresh, expires, accountId}
  secrets/openai-codex.json.lock/          mkdir lock holding owner pid; stale (dead pid) lock is recovered
  run/broker.sock                    0600  newline-JSON protocol below (sun_path ≤ 104 bytes)
  run/broker-ready.json                    {pid, socket, started_at, pi_ai:"0.87.1", provider:"openai-codex", originator}
  broker-events.jsonl                0600  start/attach/refresh/stream/error events: ids, model, timings, error kinds only
```

**Node package `broker/`** (owner tw_package). `package.json` pins `"@earendil-works/pi-ai": "0.87.1"` exactly, with `package-lock.json`; `broker/node_modules/` is gitignored and installed with `npm ci` (offline cache `/tmp/exomachina-pi-strands-debate/npm-cache` may be used). Entry point `node broker/exo-model.mjs <command>`:
- `serve` — persistent singleton for this `EXO_MODEL_HOME`: if a live broker answers `health`, exit 0 reporting attach; replace only a proven-stale socket. Writes `run/broker-ready.json`.
- `login [--device | --browser]` — interactive sign-in through public `Models.login("openai-codex", "oauth", interaction)`. Default `--device` (prints only the verification URL and one-time user code). Persists via the same store. Prints `{signed_in, account: "sha256:<12 hex>", expires_at}` only.
- `status` — `{signed_in, account, expires_at, expired}` with no network call and no token material.
- `refresh` — forces one refresh through the running broker's `refresh` op (starting it if needed); prints `{refreshed, expires_at_before, expires_at_after}`.
- `logout` — pi-ai `Models.logout`.
- `leak-scan <path>...` — reads the credential in-process and scans files/directories (binary-safe, including SQLite and JSONL) for the access token, refresh token, and any `id_token`/JWT fragment of them. It excludes **exactly one** file, the canonical credential file (`realpath($EXO_MODEL_HOME/secrets/openai-codex.json)`), and reports it under `excluded`. Every other file is scanned, including other files under `secrets/` and any copy of the credential. Prints `{files_scanned, bytes_scanned, excluded:[path], hits:[{path, kind}]}`, never the matched text. Every scenario run that reports zero hits must also run a **positive control** in the same run, and the control **never reads or copies the real store**:
1. Create a fresh `FIXTURE_STORE`-marked store with a newly generated synthetic credential.
2. Plant a copy of that synthetic credential file, plus its bare synthetic access token, outside that store's canonical path.
3. Run `leak-scan` with `EXO_MODEL_HOME` pointing at the fixture store, and show it reports both.
4. Record the result in evidence.

The real-store scan is separate. It runs against the default home, and its scanned paths include the control directory.
- **OAuth identity (from `handoff/pi-ai-credential-audit.md`).** Every OAuth call uses pi-ai's hard-coded Codex CLI `client_id` (`app_EMoamEEZ73f0CkXaXp7hrann`). pi-ai has no public override, and it is recorded as a compatibility and commercial uncertainty. The broker adds `originator: exomachina` and its User-Agent to device start, poll, code exchange and refresh calls. pi-ai itself sends no originator on the device flow. `--browser` rewrites only the authorize URL's `originator=pi` query value to `exomachina`. Whether ChatGPT accepts any of this for a Pro subscription, and whether that use is entitled under the applicable terms, is not verified by Exomachina and is recorded, not assumed.
- Codex SSE requests carry `originator: exomachina` and `User-Agent: exomachina-model-broker/<version> (pi-ai/0.87.1)`, applied through pi-ai's public `fetch` stream option. If the live backend rejects that originator, report it; do not silently switch to another client's originator.
- Egress: only `chatgpt.com`, `auth.openai.com`, and loopback.
- **Base-URL override is fixture-only (must-fix for live auth qualification).** A loopback `EXO_CODEX_BASE_URL` would otherwise receive the real subscription `Authorization` header. The broker accepts `EXO_CODEX_BASE_URL` only when all of these hold: the URL is loopback; `EXO_MODEL_HOME` is set explicitly and its realpath is **not** the default install-wide home; and the store carries the fixture marker `$EXO_MODEL_HOME/FIXTURE_STORE`, written only by test code. Otherwise `serve` (and `refresh`, `login`, `status`) exits with a `config` error **before any credential read that could reach a request and before any HTTP request**. `login` also refuses to run in a fixture-marked store, so a real credential can never land where an override is allowed. A negative test must prove refusal with a recording loopback server that observes **zero** requests.
- **Error boundary.** pi-ai error text can embed raw provider response bodies, and token/JWT redaction is not a general sanitizer. So no provider free text leaves the broker process. A socket error is `{kind, message, status?, code?}`, where `message` is fixed public text per `kind`, `status` is the HTTP status integer, and `code` is a structured provider code that matches `^[a-z0-9_.-]{1,64}$` and a broker allowlist, or `other`. `broker-events.jsonl`, stdout/stderr, Python exceptions and evidence carry only kind/status/code. No raw diagnostic is retained in any file. Exact-token redaction stays on every line as defence in depth. It is the one permitted alteration of stream content: if model output contains a live credential value byte for byte, that value becomes `[redacted]` in `ev` and `done`. JWT-shaped text that is not the credential is never altered.
- **Threat model.** The broker defends against accidental exposure: misconfiguration, test overrides, logs, artifacts and git. It also defends against other local users (owner-only modes, symlink/ownership refusal, single-link credential, and an override store whose credential inode must differ from the default store's). It does **not** defend against a concurrent attacker running as the same OS user. Such an attacker can read the store directly, so check-then-use windows open only to that user are accepted and documented: ancestor-directory swaps, and replacing a stale socket between the refusal check and unlink. Races between Exomachina's own processes are **in scope** and must be fixed, for example two brokers recovering the same dead-PID lock.

**Install-wide sharing proof (acceptance).** Sequential attach alone does not qualify as install-wide sharing. Required:
- **Node (tw_package):** at least two `serve` processes racing on one fixture `EXO_MODEL_HOME` yield exactly one live broker PID. Each loser attaches or exits 0 and never unlinks or replaces the live socket, shown by the socket inode and the ready-file PID staying unchanged.
- **Python (tw_version):** at least two independent OS processes race `ModelBroker.ensure_started` on the same fixture home, observing one PID. Each then streams concurrently with a different `session_id`. The mock records distinct `session-id` headers and a separate replay history per session, with no cross-session events.

**Model scope in this prototype.** The broker powers **graph authoring only** (`admin.py author` → `StrandsGraphAuthor`). The factory Director agent inside the harness (`src/harness.py`, `ToolCallingModelFixture`) that handles A2A work remains a **scripted fixture model**. Director decisions in any run are synthetic, even when authoring is live, and evidence must label them `director_model: fixture`. A broker-backed Director is not built here.

**Socket protocol** (one JSON object per line; `id` echoes the request):
- `{id, op:"health"}` → `{id, health:{pid, pi_ai, provider:"openai-codex", originator, signed_in, expires_at, account}}`
- `{id, op:"stream", model, session, context:{systemPrompt, messages, tools}, options:{reasoningEffort?}}` → `{id, ev:{type, contentIndex, delta?, toolCall?:{id,name}}}`* then exactly one terminal `{id, done:<pi AssistantMessage>}` or `{id, error:{kind, message}}`. Error kinds: `reauth_required`, `rate_limit`, `quota`, `config`, `provider`, `aborted`, `broker`.
- `{id, op:"cancel"}` aborts that stream; a closed client connection aborts its streams.
- `{id, op:"refresh"}` → `{id, refresh:{refreshed:true, expires_at_before, expires_at_after}}` or `{id, error}`.

**Python `src/model_broker.py`** (owner tw_version). Imports nothing from `tools/spikes/` or `/tmp`.
- `ModelBroker(home: Path | None = None)`: `.socket`; `health() -> dict`; `is_running() -> bool`; `ensure_started(*, reason: str, timeout: float = 30) -> dict` (lazy, idempotent attach-or-spawn of a detached `node broker/exo-model.mjs serve`, recording `reason` in `broker-events.jsonl`); `stop()`. Node binary from `EXO_NODE` or `PATH`.
- `PiBrokerModel(strands.models.Model)`: `PiBrokerModel(broker, *, model_id, session_id, reasoning_effort="low")`, the interleaved spike adapter (per-`contentIndex` routing, reasoning emitted at `done` with its original index, lossless replay, `BrokerLost` on a connection lost before a terminal event, `ModelThrottledException` for `rate_limit`/`quota`, `SubscriptionAuthRequired` for `reauth_required`). It lazily calls `broker.ensure_started(reason="model-call")`.
- `authoring.model_from_environment()` selects by `EXO_AUTHOR_PROVIDER`: unset or `codex-subscription` → the broker only (default model `EXO_AUTHOR_MODEL`, else `gpt-6-sol`); if not signed in it returns `(None, "codex-subscription: not signed in; run node broker/exo-model.mjs login")` and **does not look at any API key**. `anthropic`, `bedrock`, `openai-api` are used only when named explicitly. `synthetic-loopback` selects the broker against a loopback `EXO_CODEX_BASE_URL` for tests, and requires an explicit, non-default, fixture-marked `EXO_MODEL_HOME`. `codex-subscription` returns `(None, reason)` without contacting or starting the broker if `EXO_CODEX_BASE_URL` is set at all. The outcome's `model` record is `{kind, id, provider, billing, live}`, where `live: true` only for `codex-subscription` against the real backend.

**Live authoring scenario** (owner tw_director): `scenarios/live_authoring.py --home H --provider {synthetic-loopback,codex-subscription}`. It brings up the testbed, provisions a factory-mode instance, publishes v1, runs `admin.py author` with the selected broker model (no `--allow-scripted`), auto-approves and publishes v2, runs v2 through the harness's normal A2A `message/send` (lazy runner, Temporal, pinned build) to a terminal state, exports the Temporal histories, and runs `leak-scan` over `H`, the evidence files, the model home's logs, and the **actual commit-candidate file set**, with the positive control. The candidate set is every file under `prototype/temporal-factory/` from `git ls-files --cached --others --exclude-standard`, which covers tracked, staged and untracked non-ignored files; the scan records the file count. A `git diff` excerpt alone is not a leak claim. The positive control is defined under `leak-scan`. In `codex-subscription` mode, the scenario exits with an error before sign-in checks, broker start or any request if `EXO_CODEX_BASE_URL` is inherited from the environment, and it never sets one. In `synthetic-loopback` mode, the fixture model home is `H/model` with the `FIXTURE_STORE` marker. The release step is the HTTP fixture exception above; evidence labels it `http-release (fixture)`, not A2A. Evidence: `evidence/live-authoring-<provider>.json` with every claim labelled `real` or `synthetic`.

**Live qualification status (23 Sep 2026, one account, `sha256:188b022d6e97`).** Observed against the real ChatGPT/Codex backend:
- device-code sign-in into the default store;
- one forced refresh through the token endpoint, carrying the broker's `originator`/User-Agent (`evidence/live-refresh-codex-subscription.json`);
- SSE requests with `originator: exomachina` accepted;
- one `gpt-6-sol` authoring run through the harness, A2A and pinned Temporal (`evidence/live-authoring-codex-subscription.json`, attempt 2).

Not live-tested:
- the `--browser` authorize-URL rewrite and callback;
- a live repair round;
- rate-limit, quota and `reauth_required` handling;
- entitlement and terms;
- a broker-backed Director.

**Authoring acceptance by provider (revised after live attempt 1).**
- **`synthetic-loopback` must exercise the repair loop.** The mock forces an invalid first draft, so round 1 has structured validation errors, a later round is valid, and the corrected draft is derived from those errors. The scenario still fails if that loop is missing.
- **`codex-subscription` is judged on outcome, not path.** A live model is not required to fail first. It must stay within the hard budget. It must call `validate_draft`, and receive structured results, before `submit_draft`. The submitted digest must equal a round recorded as `valid`, and approval and publication must follow. The evidence records `first_pass_valid: true|false` and the round count. If the live model does revise, the invalid rounds and their errors are recorded too.
- Repair-loop behaviour is proven by the synthetic run and the unit tests. It is not claimed from a live run unless that run actually revised. Ports: runner `EXO_RUNNER_PORT_BASE=44100`, `EXO_RUNNER_MEMBER_BASE=32420`, harness 44830, testbed 45300–45305. Unit/Node tests and mock backends use 46100–46149.

**Test fixtures** (owner tw_quality): `broker/testing/` holds loopback-only test code: the strict-history mock Codex SSE backend (from the interleaved spike, plus an authoring-script mode that drives `describe_vocabulary` → defective `validate_draft` → corrected draft derived from the returned error → `submit_draft`), a `--script runaway` mode that never submits and keeps calling `describe_vocabulary` and re-validating the same invalid draft after every limit is exhausted, and a `node --import` preload that intercepts the OAuth token endpoint for refresh tests. A budget test runs a real Strands session through `PiBrokerModel` → Node broker → runaway mock. It asserts `status: "aborted"` for each limit reason, and that the mock recorded exactly the permitted number of model requests and none after exhaustion. Product code never imports `broker/testing/`.

## Testbed lane (`services/`, `definitions/`)

- `services/testbed.py {up,down,status} --home H [--port-base 45200]`. It starts independent pinned test services, each with durable identity under `$H/services/<name>` (five A2A; `release` is the HTTP exception): `source_alpha`, `source_beta`, `counter_alpha`, and `counter_beta` (capability, via `src/harness_server.py --role capability`), `quality` (`services/quality_server.py`), and `release` (`services/release_server.py --mode participating`). Ports are `port_base + i` in that order.
- `up` writes `$H/testbed/approved_bindings.json` (`{name: {role, url, identity, approved: true}}`, the exact shape `definition.validate` requires), `contracts.json` (one fixture-authored capability contract per binding name), and `quality_policy.json`. It is idempotent. On restart, identities are unchanged.
- `definitions/v1-template.json`: the lane-1 mixed v4 template, plus a caller input `question` (string, not required, `allowed_actors: ["fixture-operator"]`, `may_affect_acceptance: false`).

## Qualification amendments (23 Sep 2026, `QUALIFICATION.md`)

These supersede the statements above where they conflict.

- **Runner configuration is install-scoped (Spike B).**
  - `Runner(home)` reads and pins `port_base`/`member_base` in `$EXO_HOME/runner-config.json` under a lock. Explicit conflicting ports raise.
  - `init_instance` stores no runner ports. It rejects a harness port already configured for another instance in the home.
  - `harness.py serve` holds `instances/<name>/harness.lock` before the Director claims an incarnation. A second process for the same instance exits with `instance already serving`.
- **Async external agents (Spike A).**
  - A binding may pin, in its closure contract, `{card_sha256 (url-less card), a2a_extension:{uri:"urn:exomachina:a2a-action-contract:v1", contract, contract_digest}, reconcile}`.
  - `reconcile` is `a2a-idempotent-resend` only when the served contract document declares action-id keying, same-payload replay to the original Task, and commit-before-response. Otherwise it is `opaque`. Existing fixtures keep `fixture-lookup`.
  - Before every send, resend and poll, `agent_binding.resolve` maps the pinned identity to a URL through `$EXO_HOME/testbed/agent_snapshot.json` (`{snapshot_version:1, agents:{identity:{url}}}`) and re-verifies the card and contract.
  - The outcome journal adds phase `working`, `task_id`, `payload_sha256`, `pinned_identity` and a compare-and-set `sequence`. Terminal rows are immutable.
  - `assign` heartbeats (timeout 15 s). HTTP calls time out at 10 s.
  - `agent_binding.py` is in `INTERPRETER_FILES`.
- **Broker-backed Director (Spike C).** This replaces "a broker-backed Director is not built here".
  - Instance config `director_model: {provider: fixture|synthetic-loopback|codex-subscription, model?}`; the default is `fixture`.
  - An A2A **text** part goes to `director_agent.DirectorTurn`, whose tools are `start_research(question, outcome_mode)`, `inspect_run()` and `decide_wait(action, revision, sha256, rationale)`. The per-turn budget is 4 model calls, 4 tool calls and 90 s.
  - Code derives `action_id` from sha256 of the Task id, operation and message id. The model never sees the actor, token, epoch, run, graph, package, version or bindings.
  - `inspect` and `abort` require the run's `authorized_actor` and the original `context_id`.
  - A second fixture bearer, `fixture-observer`, authenticates but is not an allowed actor.
  - Tool calls and turns are audited in `director_tool_calls` and `director_turns`.

## Single-factory spike amendments (24 Sep 2026, `briefs/single-factory.md`)

The frozen cross-lane contract for the single-factory core-routes spike is in [`briefs/single-factory.md`](briefs/single-factory.md#cross-lane-contract-frozen). It supersedes the statements above where they conflict. In particular:
- `synthesize` delegates to an external `report_synthesis@1` agent;
- Quality is a model-backed async A2A agent;
- the caller-steered `outcome_mode` is removed;
- the evidence packet is pinned in the package.

## Factory dashboard v2 contract (2 Oct 2026)

This section is the shared implementation contract for the dashboard integration spec. The
specification remains the source of requirements; this file defines the interfaces the lanes build.
The lead owns changes to this section and merges lane changes after review.

### One dashboard-facing Interface

Recorded, isolated demo, and live Adapters implement the same browser contract:

```text
discover() -> accessible factories and active publications
snapshot(factory_id, run_id?) -> {schema_version, cursor, captured_at, freshness, state}
observe(factory_id, after_cursor, run_id?) -> ordered CloudEvents and a continuation cursor
inspect_artifact(run_id, revision, sha256) -> authorized bytes plus digest metadata
command(command_id, task_id, context_id, action, expected_state, expected_revision?)
    -> received, validated/rejected, and applied/failed outcomes
submit(factory_identity, capability, brief, caller_context) -> original A2A Task/run binding
```

The first runnable tracer slice is a run using the ordinary `verified-research@1` A2A identity, its
existing pinned publication, current Temporal execution, and the original Task. Do not add a parallel
scheduler, graph selector, or authority ledger. A recorded bundle and isolated deterministic demo
must validate against the same schema and pass through the same reducer and renderer. Demo commands
never reach runtime credentials, A2A, Temporal, the Commercial Module, or payment Adapters.

Demo scenarios additionally carry one explicitly labelled illustrative layer through that same Seam:
`state.demo` on the snapshot (label `Illustrative Demo fixture`; scenario clock/start/now, simulated
budget, agent prices/capacity/shared occupancy, programs, result kind, versions, step cues) and
`com.exomachina.demo.illustration.v1` events (one allowlisted original timeline entry each: movement,
artifacts, verdicts, releases, readouts, Director turns, alarms, recommendations, notes, holds). The
validators accept this layer only when the caller declares source `demo`; the default, Live, and
Recorded validation rejects it, and `toFloorModel` reads it only for Demo state. It is never a Live fact
and is not part of the public JSON schemas.

The live transport is WebSocket. The initial transport message binds an authenticated principal,
factory identity, optional run filter, and last applied cursor. The server emits either an atomic
snapshot at cursor C followed by every relevant event after C, or a resumption result for the supplied
cursor. If the cursor is older than retained data, emit `resync_required` and a fresh snapshot; never
skip an authoritative transition. A run-filtered consumer receives a continuation cursor that advances
over unrelated factory events.
Freshness is a source-read fact; a cursor checkpoint does not make disconnected or
stale source data fresh. Optional bounded snapshot freshness fields are `scope`
(`factory` or `run`), `run_id`, `included_run_ids`, `factory_status`, and
`unavailable_run_ids` (unique safe-ID lists, at most 256). A run scope is fresh
only after successful reads of that exact owned run and descendants linked by
actual Temporal child-start events. Missing unrelated histories keep factory-wide
status disconnected and warnings visible. Other RPC failures remain fail closed.
The owned source may expose authenticated
`get_run_freshness(principal, factory_id, run_id)` as a cached, authenticated reader.
Its `SourcePage.run_freshness` optional map is captured under the source lock
with the factory freshness and records; projection validates the bindings and
persists that map atomically with state and cursor. Snapshot construction reads
this committed capture, never a later mutable source cache. Legacy sources retain
factory-wide freshness. Incomplete source paging cannot establish fresh scope. Scoped freshness
never authorizes a new factory submission while factory-wide coverage is incomplete. Buffering is bounded; overflow explicitly requests resynchronization or
closes the slow connection. Duplicate delivery is permitted and reduction is idempotent.

Each event is a CloudEvents 1.0.2 structured envelope with `id`, `source`, `type`, `time`, `subject`,
`datacontenttype`, `dataschema`, and versioned Exomachina `data`. The stable event ID is derived from
its durable source record, not the WebSocket delivery. The ordered cursor is transport/projection
metadata and is not inferred from event time. Payloads carry applicable factory, run, assignment,
attempt, original Task, pinned publication/manifest/package/build, artifact revision/digest, decision,
commercial-record, and evidence identities. Only explicitly allowlisted fields are projected. Raw
Temporal history, run inputs, credentials, signing material, and mandate presentations never enter
browser payloads. Artifact bytes use an authorized HTTP path and are digest-checked.

The protocol is documented with AsyncAPI and versioned JSON Schemas. WebSocket commands include a
stable command ID and expected state/revision/digest. Their transport acknowledgement means only
`received`; validation/rejection and applied/failure are separate authoritative outcomes. Repeating an
identical command returns its original outcome. Reusing its ID with different intent is rejected.
Server-side authority binds the principal to the original Task/context and current run state. The
browser never supplies an actor identity as authority and never paints a command as applied before the
authoritative result arrives. Live mode has no playback controls or playback keyboard shortcuts.

The v1 WebSocket uses JSON messages with `op`; each embedded snapshot and CloudEvent carries its
schema version. A client starts with `{op:"subscribe", factory_id, run_id?, after_cursor?}`. The server
returns `{op:"snapshot", snapshot:{schema_version:1, cursor, captured_at, freshness, state}}` or
`{op:"resumed", after_cursor, continuation_cursor}`. Subsequent messages are
`{op:"event", cursor, event:<CloudEvent>}`, `{op:"checkpoint", cursor}`, or
`{op:"resync_required", reason, minimum_cursor?, latest_cursor?}`. A command is
`{op:"command", factory_id, command_id, task_id, context_id, expected_state,
expected_revision?, expected_sha256?, action}`. Its immediate acknowledgement is
`{op:"command_ack", command_id, lifecycle:"received"}`. The later authoritative outcome arrives as a
CloudEvent and is never implied by the acknowledgement. Cursors are opaque strings. The ordered
projection cursor travels alongside the CloudEvent as WebSocket delivery metadata; the CloudEvent `id`
remains stable across delivery attempts. Authentication comes from the server-side session/principal
resolver; credentials are not carried in a query string, event, command, or other application message.

### Commercial Interface

The Commercial Module owns supplier offer pinning, authorization, reservations, metering attribution,
accrued obligations, credit and settlement reconciliation. It does not own execution transitions,
artifact acceptance, or customer price. A purchase pins the offer/version, supplier and assignment,
currency and precision, billable units, cost/hosting disclosure basis, explicit basis-point markup and
its base, maximum authorized charge, expiry, payment trigger, and failure/repair/cancellation/credit
terms before work is admitted.

Amounts use integer atomic units plus an explicit currency/scale; rates and conversions never use
binary floating point. Usage values include unit, source, completeness, and assignment/attempt
identity. Amount evidence is one of `measured`, `calculated_from_measured_usage`,
`provider_reported`, `estimated`, `unknown`, or `undisclosed`. Missing usage or cost is never zero. Inference cost, hosting cost,
markup, supplier service charge, payment/network fees, owner overhead, and factory customer price are
distinct records. A subscription token count is not an invoice. Hosting without a selected metering and
allocation basis remains unknown; no default rates or markup are supplied here.

Price basis (`usage`, `fixed_assignment`, `fixed_attempt`, `accepted_outcome`) is separate from payment
trigger (`upfront`, `incremental_use`, `attempt_completion`, `acceptance`). Only a profile implemented
and qualified by its Adapter may be advertised as supported. Settlement states distinguish
authorized, reserved, accrued, settlement-pending, settled, credited/refunded, failed, and unresolved.
Reservations are atomic across runs/processes, remain held for unresolved liabilities, and reconcile
late usage and credits without rewriting source usage. MPP and x402 remain separate payment Adapter
profiles. AP2 v0.2 is an authorization Adapter; it grants no implicit nested-factory delegation. This
contract selects no inference access method, rate, markup, hosting allocation, fee treatment, payment
network, asset, facilitator, wallet custody, or trust configuration.

Payment Adapter capability declarations are separate from the Commercial Module's purchase and
usage ledger. The Module exposes an Adapter seam for MPP bounded metered sessions, x402 bounded
authorization/settlement, and AP2 v0.2 purchase-authorization verification. Each declaration pins
protocol/profile and version, operation set, environment prerequisites, and status. `available` may
be advertised only after implementation and profile-specific qualification; `unconfigured`,
`unsupported`, and `unqualified` remain explicit and cannot authorize work. Adapter evidence exposes
safe receipt references and outcomes, never credentials, mandate presentations, signatures, wallet
secrets, or raw payment headers. This prototype supplies only the seam and declarations: no network
Adapter is selected or invoked, and P01-P10 remain unqualified.

Commercial Observation events are sourced only from the Commercial Module's public read Interface.
Usage rows preserve run, assignment, attempt, service identity, and model-call identity when available.
An unreported quantity is omitted and marked `unknown` or `undisclosed`; missing records never imply
zero usage or zero cost. Inference cost, hosting cost, markup, supplier charge, fees, and customer
price are separate cost-component facts. Each money fact carries integer `amount_atoms` or `null`
only when its evidence is `unknown` or `undisclosed`, with explicit currency and `atomic_scale`.
Absent cost evidence is rendered as unreported/unknown rather than a zero balance. The current
prototype does not infer provider usage from token estimates or customer subscription limits.

On the wire, `commercial.usage` includes `usage_id`, `service_identity`, `unit`,
`measurement_source`, `completeness`, and `evidence_status`, plus applicable `model_call_id`,
`model_id`, and `reasoning_effort`. A reported quantity is an exact decimal string with
`completeness: "complete"`; an unreported quantity is omitted with completeness `unknown` or
`undisclosed`. `evidence_status` independently records whether a reported value is measured,
calculated from measured usage, provider reported, or estimated. `commercial.obligation`
uses one row per `component` (`inference_cost`, `hosting_cost`, `markup`, `supplier_charge`,
`payment_fees`, `owner_overhead`, `production_cost`, or `customer_price`) and carries
`obligation_id`, `offer_digest`, `amount_atoms`, `currency`, `atomic_scale`, `evidence_status`,
`price_basis`, and the pinned `payment_trigger`/`markup_bps` when applicable. Unknown or undisclosed
amounts carry `amount_atoms: null`; they are never encoded as numeric zero.

#### Model-usage measurement read Interface

Provider-reported per-call model measurements are a separate, non-financial read Interface. They do
not create a purchase, supplier obligation, customer charge, or cost estimate. The approved public
shape is specified here; this contract does not claim that the aggregate endpoint is mounted or
that every source already populates it:

```text
GET /usage/measurements
    ?run_id=...&task_id=...&assignment_id=...&attempt_id=...&model_call_id=...&call_scope=...
-> {"measurements": [MeasurementView, ...]}

MeasurementReader.list_measurements(*, run_id?, task_id?, assignment_id?, attempt_id?,
                                    model_call_id?, call_scope?) -> list[MeasurementView]
```

Filters are optional and combine as narrowing predicates. Results have deterministic
`recorded_at, model_call_id` order. `GET` is read-only. The Runtime factory app is the aggregate
endpoint authority: it uses the existing authenticated Runtime principal and scopes results to the
factory instance that principal may observe. It reads only each journal owner's public
`list_measurements` Interface (or that owner's authenticated measurement endpoint), using existing
pinned service bindings; it never opens another component's SQLite database or accepts a caller
supplied database path, source URL, service identity, or principal. A source owner assigns
`recorded_at` when it persists the fact; callers cannot set or backdate it.

Each `MeasurementView` contains `measurement_id`, `model_call_id`, `recorded_at`, `call_scope`,
`provider`, `model_id`, `reasoning_effort`, `unit: "tokens"`, `measurement_source`, `completeness`,
`evidence_status`, and the safe per-category usage projection. The supported token categories are
`input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_write_tokens`, and `total_tokens`; each
is `{value: nonnegative integer, status: "reported"}` when the provider reported it (including a
reported zero), or `{value: null, status: "unavailable"}` when it did not. No token estimates are
substituted. The view may also carry `service_identity`, `task_id`, `message_id`, `run_id`,
`definition_digest`, `assignment_id`, and `attempt_id`; each is nullable until an authoritative fact
exists. `attempt_id` follows the existing public ID representation and is populated only from the
actual workflow attempt. An opaque `action_id` is not parsed to manufacture assignment or attempt
bindings.

`call_scope` is `authoring_overhead`, `director_call`, or `assignment_call`. An authoring overhead
measurement has `call_scope: "authoring_overhead"`; `task_id`, `run_id`, `assignment_id`, and
`attempt_id` are null when authoring occurred without those genuine bindings. A Director call keeps
its real Task and message IDs, while run and definition bindings remain null until the Director
resolves the Task to an actual run and pinned definition. Assignment measurements carry only IDs
received from authoritative workflow/A2A facts. Missing bindings remain null; no sentinel IDs are
allowed.

The measurement response contains no prompt, model arguments, raw provider request/response,
credentials, or cost fields. Inference cost, hosting cost, markup, supplier charge, payment fees,
owner overhead, and customer price require separate Commercial evidence. A token count is not a
cost and never implies a zero cost. An unbound authoring overhead view is available through this
read Interface only; the current `commercial.usage` CloudEvent requires run, assignment, and attempt
IDs, so it must not be emitted for such a row without a separately approved event-schema change.

### Shared file ownership for this implementation

Unless a path starts with `../../`, paths below are relative to
`prototype/temporal-factory`.

| Lane | Owned implementation files |
| --- | --- |
| Lead | This contract; cross-lane integration and review |
| Observation | `src/observation.py`, `src/observation_transport.py`, `schemas/dashboard/v1/*.schema.json`, `specs/dashboard-asyncapi.yaml`, `tests/test_observation.py`, `tests/test_observation_transport.py` |
| Runtime | `src/harness.py`, `src/factory.py`, `src/director_agent.py`, `src/receiver_client.py`, new `src/artifact_delivery.py`, new `src/observation_source.py`, and their runtime tests; preserve pre-existing model-migration hunks |
| Dashboard | `../../docs/design/exomachina-floor.html`, `dashboard/contract.mjs`, `dashboard/reducer.mjs`, `dashboard/adapters/{recorded,demo,live}.mjs`, `dashboard/test/dashboard.test.mjs` |
| Commerce | `src/commercial.py`, `tests/test_commercial.py`; Commercial ledger, Payment Adapter seam, and MPP/x402/AP2 capability declarations; no edits to pre-existing model-migration files without a separately agreed handoff |
| Qualification | `DASHBOARD-QUALIFICATION.md`, `scenarios/dashboard_qualification.py`, `tests/test_dashboard_qualification.py` |

`src/harness.py` is runtime-owned: the Observation lane exposes a transport module but does not mount
it there. The Runtime lane calls the Observation transport from the application mount point. The
Dashboard lane imports the browser schema/reducer contract and does not redefine protocol semantics.
Qualification does not edit another lane's tests or implementation. All live claims remain
unqualified until a real smoke record proves them; fixture and deterministic cases keep their labels.

### Resumed qualification allocation (3 Oct 2026)

The lead owns `docs/design/exomachina-floor.html` during browser integration. The Dashboard lane
owns its adapters, reducer, and tests; page changes are handed to the lead for review and integration.
Runtime owns the opt-in loopback QA session adapter and its rejection tests. Observation owns
actual loopback WebSocket smoke checks. Commerce additionally owns `src/model_usage.py`,
its tests, and additive measurement changes in `services/model_agent.py` and `src/model_broker.py`.
Those changes must preserve the pre-existing model migration and distinguish missing usage from
reported zero. Provider usage measurements do not authorize spending or select prices. Commerce
also owns additive usage journaling in `src/director_agent.py` during this pass; missing initial
run bindings must stay unavailable until actual work exists. Harness integration remains
Runtime-owned. The lead owns the narrow fixture-error envelope fix in `src/harness_server.py`
and `tests/test_fixture_failure.py`. All shared-port suites use `/tmp/exo-qual-suite.lock`.

Browser QA tooling lives outside product source in a pinned temporary Jev checkout. The operator
selected native OpenRouter Decisions with `typesafe/jev-1.13` for Jev QA, overriding upstream's
direct TypeSafe default. This does not change the product's `gpt-6-luna`/`xhigh` inference path.
Credential values are injected only into bounded QA children through Phase; evidence records only
safe provider metadata, assertions, source revisions and patch digests.

## Local accepted-report delivery

`local_delivery.LocalDelivery(database, destination, *, factory_id,
destination_identity)` owns the explicitly configured local destination and its
receipt journal. `deliver(*, run_id, task_id, context_id, artifact)` accepts a
verified `DeliveredMarkdown` from the authenticated public Observation artifact
reader and `accepted_markdown`; `list_receipts(*, run_id=None)` returns safe
receipts after verifying the deposited bytes. It exposes no SQL connection,
filesystem path, report text, price, or payment information in its views.

`local_delivery_routes.install_local_delivery_routes` mounts `POST /deliveries`
and `GET /deliveries` on the existing Runtime factory app. Runtime supplies its
principal resolver and factory identity. The optional server configuration is
exactly `local_delivery: {destination: <absolute owned path>, identity: <stable
destination identity>}`; absence reports unavailable. Requests cannot configure
a destination. POST accepts only `{run_id, revision, sha256}`, verifies the final
observed accepted artifact and original Task/context, then deposits the exact
Markdown once. A changed binding or changed deposited file fails closed.

The safe receipt has `receipt_id`, `factory_id`, `run_id`, `task_id`, `context_id`,
`artifact_revision`, `artifact_sha256` (accepted report envelope),
`markdown_sha256` (exact deposited Markdown), `destination_identity`,
`state: delivered`, `delivery_kind: local_file`, `byte_length`, and writer UTC
`recorded_at`; the POST result also reports `duplicate`. This is a distinct local
delivery receipt. It does not establish a remote customer's receipt or payment.
The original workflow's HTTP fixture receipt remains separately identified.
The direct HTTP receipt fields are not silently added to Observation CloudEvents.

### Approved local delivery Observation projection

The lead-approved `delivery.receipt` extension allows `markdown_sha256`,
`destination_identity`, `delivery_kind`, and `byte_length`. When
`delivery_kind: local_file` is present, the original Task/context, run, receipt,
accepted artifact revision/digest, Markdown digest, destination identity,
byte length, and `delivered_at` must all be present; `outcome` must be
`local-file-delivered`. The timestamp comes from the receipt writer's UTC fact.
Paths, report text, cost, and payment fields remain excluded.

Runtime projects only its injected LocalDelivery public `list_receipts` reader,
with an immutable source identity per receipt. Existing fixture receipt facts
remain separate. The shared renderer labels the local outcome “Saved to local
destination”; it does not assert receipt by a remote customer.

The Floor's local receipt counter uses `delivered_at` over a rolling five-minute
window, deduplicated by receipt identity. It does not use workflow completion as
the deposit time. Historical receipts remain inspectable in Outputs even when
outside that window. Demo's declared simulated delivery counter is unchanged.

Outputs offers “Save to local destination” for an exact observed artifact through
this authenticated public POST. The request contains only run/revision/hash;
Runtime verifies final acceptance and chooses its explicitly configured local
destination. A retry reuses the existing receipt. The browser does not choose a
filesystem path or acquire a payment authority.

New Workflow executions behind Temporal patch
`exo-explicit-assignment-bindings-v1` carry engine-issued opaque assignment and
attempt UUIDs separately from model-call IDs and Quality's ordinal `attempt`.
Assignment identity remains stable per graph node/parallel branch across logical
revisions; a logical invocation receives a new attempt identity. Activity retries
reuse the persisted input. Actual `node` is explicit. Legacy histories keep their
original activity envelopes and do not acquire invented identifiers.

### Optional bounded human escalation

A newly published `director_wait` may declare
`human: {actor: <explicit safe identity>, timeout_seconds: <integer 1..3600>}`.
Absence retains the existing abort-only Director policy. The declaration is part
of the pinned definition; it selects no production actor or deadline by default.
Director can escalate an exhausted rejected candidate to that actor on the same
original Task/run/revision/digest. Human can abort only, with a separate declared
deadline and the same owner fence. Stale/wrong-actor/concurrent answers fail.
Self-escalation to the Director identity fails before model work. Applied decision
receipts remain available for resolving a lost reply after escalation.

This workflow slice does not grant extra repair, alter Quality acceptance, or
increase spending authority. Public Runtime/A2A mounting and operational browser
proof remain required; synthetic workflow tests are not S08 qualification.

### Current wait commands from the dashboard

The Live dashboard builds a wait command only from current observed `awaiting-director` or `awaiting-human` facts, the published permitted actions, the original Task/context, and the exact current rejected Quality/artifact revision pair. It fetches and verifies those candidate bytes through the owned artifact Interface before sending `expected_revision` and `expected_sha256`, then rechecks the current observed wait. Missing facts, a changed candidate, accepted Quality, or unverifiable bytes leave commands unavailable. This browser check does not grant authority: Runtime must still validate the authenticated principal, pinned actor, original Task, current phase, deadline, candidate and update fence.

Historical waits without these facts remain unknown. Recorded sources are read-only. The command builder and bounded regressions are implemented; Runtime human/escalation mounting and operational qualification remain separate pending evidence.

### Current bounded integration allocation and shared capacity approval

The lead now owns `src/factory.py`, `src/definition.py`, `dashboard/contract.mjs`, `dashboard/decision.mjs`, and the page outside explicitly allocated Dashboard sections. Runtime retains the harness, runtime source, runtime tests and the human Task projection integration. Observation's temporary Operations source constructor/producer/refresh allocation returns to Runtime after its reviewed handoff; Observation retains its new focused integration test. Dashboard owns usage validation/tests and its current Decisions wait-control section. Commerce owns additive normal model-agent scheduling, adapter factory binding, and their tests. Every allocation preserves earlier migration edits.

S16 approves the additive optional A2A assign `factory_id`, supplied only by actual workflow Director authority under `exo-explicit-factory-binding-v1`. Missing identity remains unknown historically; no run/action parsing or retrofill is permitted. Shared execution capacity requires the explicit pair `--admission-db` and `--execution-capacity`, with no default; zero pauses admission. One queue is owned by the pinned service identity across all caller factories. Accepted Task facts retain explicit caller factory identity, while authenticated occupancy reads report scoped own/other counts without other factories' Task IDs. A GET never constructs/configures a queue; unknown factory identities are unavailable. Capacity-enabled scheduling rejects a missing factory binding before model work. Read-only retained usage owners remain frozen and do not accept capacity configuration. Fixture authentication and synthetic scheduler tests do not qualify production authentication or S16.

`AdmissionQueue.read_snapshot()` returns capacity totals and the existing safe
request projections from one read transaction. Service occupancy groups that
snapshot using its own accepted Task factory facts; external responses expose
counts, not other factories' Task identities. Readers must not combine separate
capacity and request reads when asserting consistent occupancy.

Engineering's approved additive `OperationsAdapter.publication_context(factory_id)`
returns exactly `{manifest_digest, quality_policy_digest}` from one verified
active publication closure. Evaluation and research requests must use that
atomic context and fail closed when unavailable; they cannot fall back to two
independent reads. Manifest-only operations retain their existing reader. This
contract does not enable candidate evaluation, promotion or research execution
by itself.

### Bounded supplier destination allocation (implementation in progress)

Commerce owns `src/supplier_protocol.py` and its focused tests; Runtime owns the
normal factory harness/Director destination integration. An explicitly enabled
`nested_supplier_enabled` profile may accept `nested_factory` with the exact
SupplierFanout parent/child binding tuple and `payload: {inputs: {...}}`.
The requested child definition must match the destination's active publication.
Only this separate operation may accept a caller-issued opaque child run ID as
its actual workflow ID; collisions and a changed accepted tuple fail closed.
Ordinary start identity and publication selection retain their existing rules.

The destination persists the accepted tuple with its original remote A2A Task
and context and executes through normal admission/Temporal paths. An enabled
profile must echo the accepted tuple in Task metadata and its structured
artifact, preserving the exact accepted report bytes. A normal ModelAgent does
not own this operation and must not advertise it. Opaque reconciliation remains
unknown without a durable action lookup; no fixture lookup is advertised by a
generic destination. This allocation does not qualify S18/S19 or authorize new
paid execution, and the retained instance keeps the profile disabled.

### Basic prototype milestone — operator scope change (2026-10-03)

Deferred supplier, extended human/repair, capacity queue/contention, Engineering,
research, payment and full recovery/version qualification work is paused at safe
edit boundaries. Existing changes and evidence remain preserved. The operator
owns the canonical spec and CONTEXT scope update. Current implementation work
prioritizes truthful Board progress, normal dashboard submission/provider
readiness, a single active job with explicit rejection of another, usage with
honest gaps and unknown costs, reconnect and source isolation. Two dashboard
submitted real Luna xhigh cases require resolved execution allowance; retained
recordings do not satisfy that gate. No additional paid execution is authorized
by this coordination checkpoint.

Basic provider readiness consumes additive public model-agent health fields
`provider`, `model_id`, `reasoning_effort` (nullable safe strings),
`inference_enabled` and `read_only_usage` (booleans). They describe configuration
and writable mode, not sign-in or successful inference. Read-only owners expose
null model metadata and inference disabled; scripted work exposes no model/effort
and is not real provider evidence. Runtime must verify identities and contracts
against the pinned publication and separately check redacted broker readiness
before allowing a new live job. No pricing fact follows from token measurements.

The operator subsequently completed the B01–B10 canonical scope update and
explicitly authorized the two necessary real dashboard workflows in B03. That
new instruction resolves the earlier basic-run allowance checkpoint for those
two workflows only. Use existing approved Luna xhigh access and finite existing
Director, model-call and repair limits; do not repeat failed paid workflows or
start a third paid workflow automatically. B06 may use deterministic normal
Runtime command proof. The canonical basic gate governs current qualification;
historical S/P evidence remains preserved as later roadmap evidence.

Basic admission uses the existing normal factory A2A `message/send` path. The
explicit instance option `basic_single_active_job: true` reserves one durable
Director-owned slot before provider preflight or model selection, including the
interval before a run exists. Another original Task is rejected rather than
queued. This option refuses legacy structured commands and external nested
supplier entry points. It does not change local Temporal child semantics.
The Runtime supplies a synchronous `Director.submission_preflight` callback;
missing readiness fails closed before model scheduling. Completed submission
replay is bound to the same message, original Task/context, actor and brief
fingerprint. Pending or uncertain delivery never schedules a duplicate turn.
Terminal release uses the owned FactoryTaskStore reader and the Director's
durable closed outcome, never an inferred timestamp or dashboard count.
Uncertain interrupted model outcomes remain occupied; automatic recovery is
deferred. `basic_job_status()` exposes occupancy only, without Task IDs or caller
identity. Twelve focused synthetic gate tests and eighteen existing harness
regressions pass together. Bound uncertainty is reconciled only through its
durable original Task alias; unbound uncertainty remains fail-closed. Actual
simultaneous-dashboard rejection remains to be demonstrated during the
authorized pair.


## Current submission readiness versus historical observation (2026-10-05)

Authenticated `GET /submission/readiness` is bound to this Runtime factory and
rejects query selectors. Loopback operator session/fixture operator is supported;
observer principal is denied. It uses a fresh existing submission-readiness check
(pinned model owners, approved subscription profile, registered worker/pollers)
and public Director `basic_job_status()` / `unfinished()` readers. It starts no
Task, workflow, model call, reservation or payment. Unknown state fails closed.

Response fields are exactly `schema_version:1`, `factory_id`, writer-assigned UTC
`observed_at`, `status:ready|blocked`, `reason_code:null|enum`; no-store. Blocker
codes: `director_profile_unapproved`, `default_broker_path_overridden`,
`subscription_status_unavailable`, `pinned_writable_model_owners_unavailable`,
`pinned_worker_pollers_unavailable`, `factory_busy`, `factory_uncertain`,
`unfinished_runs`, `current_state_unavailable`. This is a point-in-time view,
not a slot lease, inference proof, cost estimate or production-auth qualification.

Bootstrap supplies `submissionReadinessEndpoint`. The Live Adapter requires an
open current factory socket, checks the authenticated same-origin endpoint anew
before a new-work POST (5s request bound; observed time within 30s), and aborts
before POST if selection/socket changed. The existing A2A preflight and atomic
single-job fence remain authoritative. Responses cannot select a different
factory or arbitrary source URL. Without the configured Interface the old
conservative gate remains in place.

Task-bound follow-ups and operator commands still require fresh run observation
and original Task/context. Terminal histories may be expired/disconnected while
current factory submission is ready; those historical facts are not upgraded.
Direct A2A text replies are displayed as text, bounded, inside the composer;
only an actual returned factory Task with run metadata is followed as workflow
work. Missing observation of that Task does not create a substitute run.

#### Exact submitted Task following and assignment linkage (2026-10-05)

The dashboard follows an accepted A2A Task using the returned explicit `metadata.run_id`, Task ID and context ID. Authenticated scoped Observation must retain that same Task/context before its graph is selected. A factory-wide unavailable history is not a substitute for the returned run. While awaiting a scoped snapshot, old job surfaces and summaries are cleared; failure displays unavailable instead of the previous job. Restoring an unavailable historical selection can prefer an available pinned run, retaining the historical-coverage warning.

Both reducers carry only `started_at`, `provider_identity` and `node` across updates of the same assignment/attempt when omitted by the newer fact. They do not carry queue positions or transfer fields to another attempt. Nonterminal/unknown updates without `ended_at` remove the previously projected end. Raw source facts remain unchanged.

Runtime may append a deterministic `projection-correction:assignment-node-link-v1:` fact using only an explicit activity-input `node` that is present in that workflow's pinned definition. The correction retains the original event time, state, capability, provider and assignment/attempt bindings. An opaque Temporal activity ID is not node evidence. The shared Floor presents its selected observed state and Quality independently from delivery and exposes Board/Outputs without triggering submissions.

## A2A v1 baseline and hand-off records (operator decision, 7 Oct 2026)

Authoritative direction: [A2A v1 baseline, factory mediation, and hand-off records](../../docs/a2a-v1-mediation-decision-2026-10-07.md).
This section supersedes the `a2a-sdk 0.3.26` pin in Conventions once the
migration lands, and supersedes every `message/send`, `kind`-discriminated part,
and lowercase Task-state reference in this file and its lanes.

**Wire protocol.** Every A2A server (harness in factory and agent mode,
capability services, model agents, Quality, supplier fixtures) and every A2A
client (`src/adapter.py`, `src/long_client.py`, supplier client, dashboard Live
Adapter, floor composer) uses A2A v1.0 only.

- Methods are `SendMessage` and `SendStreamingMessage`; responses are
  `{task}` or `{message}`.
- Task states are `TASK_STATE_*`.
- Parts are unified (`text` | `raw` | `url` | `data`, plus `mediaType` and
  `filename`), with no `kind` field.
- Agent Cards list `supportedInterfaces` with protocol version 1.0.
- There is no 0.3 interface or fallback.
- The JSON-RPC binding is kept.
- The factory's extensions are declared on the Agent Card; a missing required
  extension fails with `ExtensionSupportRequiredError`.

The migration runs in a new pinned Python environment with `a2a-sdk` 1.x,
recorded here when created. The shared 2026-09-22 spike environment is not
modified, and the operator's running stack is restarted onto v1 only with
operator approval.

**Observation stays version-neutral.** The A2A Adapter maps `TASK_STATE_*` to the
existing Observation state vocabulary (`working`, `input-required`, …). No
existing Observation event type, field, or recording changes because of the wire
migration.

**Definition additions** (validated at publication):

- `output`, on each node binding: `"artifacts"` (default, strict),
  `"message"` (labelled exception), or `"none"` (side-effect). A completed
  Task with no artifact on an `artifacts` node fails the attempt with
  `output.missing`.
- `kind`, on each edge: `"material"` (default) or `"control"`. A node with
  `output: "none"` may not have an outgoing material edge. Route-node targets
  (repair, Director wait, release, terminal) are declared as control edges.
- The release receiver binding (`transport: "http-post"`) is `output: "none"`.

**New Observation facts** (allowlist additions; S33 requalification required).
All are content-free. They never carry text, data, bytes, artifact names,
descriptions, `artifactId`s, filenames, URLs, or metadata.

- `com.exomachina.handoff.produced.v1`, emitted on complete:
  - `run_id`, `assignment_id`, `attempt_id`, `node`;
  - `handoff_id` and `handoff_revision`;
  - `produced_at`;
  - `items[]`, each with `item_index`, `source` (`artifact` | `message`),
    `part_kinds[]`, `media_type`, `byte_length` (null for `url`),
    `ready_at`, and `digest` (keyed);
  - optional `artifact_revision` and `artifact_sha256`, when the item is the
    report artifact.
- `com.exomachina.handoff.consumed.v1`, emitted before dispatch:
  - `run_id`, `assignment_id`, `attempt_id`, `node`;
  - `consumed_at`;
  - `inputs[]`, each with `handoff_id` and `item_digests[]`.
- `com.exomachina.handoff.item_ready.v1`, emitted only when the agent streams,
  when an artifact's `lastChunk` arrives: `run_id`, `assignment_id`,
  `attempt_id`, `node`, `handoff_id`, `item_index`, `part_kinds[]`,
  `media_type`, and `ready_at`.

Digests are HMAC-SHA256 under a per-factory-instance key stored in the instance
home. The key is never logged, observed, or exported. Report artifacts keep the
existing plain sha256 chain. Run snapshots retain hand-off records with their
times so retained runs replay their full path.

Dashboard validator details (7 Oct 2026, ahead of runtime emission): each
hand-off fact allows exactly `schema_version`, `factory_id` and the fields
above, at every depth; `media_type` may be null (A2A `mediaType` is
optional); `byte_length` may be null only when `part_kinds` includes `url`;
`handoff_revision` is a positive integer; `digest`, `item_digests[]` and
`artifact_sha256` are 64 lowercase hex; a `message` hand-off has exactly one
item; `artifact_revision` and `artifact_sha256` appear together, only on an
`artifact` item. Pinned public graphs accept edge `kind` and node `output`; a
node with `output: "none"` and an outgoing material edge is rejected. The
event-stream `graph_nodes` form accepts `output` but has no way to declare an
edge kind yet.
