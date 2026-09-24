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
