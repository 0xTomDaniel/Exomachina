# Integrated Temporal factory prototype: shared interfaces

Branch `feat/temporal-factory-prototype`. Root: `prototype/temporal-factory/`. This is the contract between the parallel lanes. Change it only through the orchestrator.

## Architecture being built

- One **customized Strands harness instance** (`src/harness.py`, orchestrator-owned) is configured as `mode: agent` or `mode: factory`. In factory mode, its Director agent and Factory Module run **inside** the instance and expose only that instance's normal A2A identity, Agent Card, and capability contract (`verified-research@1`). There is no separate factory endpoint. Callers never supply a package digest, graph, or version.
- A new graph definition/version or run **does not** spawn a harness service. New runs use the instance's active publication. Waiting runs keep their pinned closure.
- The bundled **local runner** (PostgreSQL + Temporal + interpreter workers, `src/runner.py`) starts **lazily**: on the first factory work, or at harness startup only when the instance has unfinished runs to recover. Routine harness startup must not start it. It is a detached process shared by harness instances on the install. It is not owned by one harness process.
- External A2A agent nodes (capabilities, Quality, release receiver) are **independent black-box services**. Pinned test services (`services/testbed.py`) stand in for the future directory. No directory discovery.
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
- `approve(package, *, approver, policy) -> approval record` (auto-approval within the bounded profile; human approval is a policy choice).
- `model_from_environment() -> (model | None, reason)`. It reports why no live provider is available and never prompts for credentials.
- `ScriptedAuthoringModel`: a deterministic Strands `Model` that drives the same tool loop. Its first draft has a defect, and it revises from the returned error. It is always labelled `live: false`.

## Testbed lane (`services/`, `definitions/`)

- `services/testbed.py {up,down,status} --home H [--port-base 45200]`. It starts independent pinned test A2A services, each with durable identity under `$H/services/<name>`: `source_alpha`, `source_beta`, `counter_alpha`, and `counter_beta` (capability, via `src/harness_server.py --role capability`), `quality` (`services/quality_server.py`), and `release` (`services/release_server.py --mode participating`). Ports are `port_base + i` in that order.
- `up` writes `$H/testbed/approved_bindings.json` (`{name: {role, url, identity, approved: true}}`, the exact shape `definition.validate` requires), `contracts.json` (one fixture-authored capability contract per binding name), and `quality_policy.json`. It is idempotent. On restart, identities are unchanged.
- `definitions/v1-template.json`: the lane-1 mixed v4 template, plus a caller input `question` (string, not required, `allowed_actors: ["fixture-operator"]`, `may_affect_acceptance: false`).
