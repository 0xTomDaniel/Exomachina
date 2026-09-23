# Lane: pinned test A2A services (pane wG:p8)

Read `prototype/temporal-factory/INTERFACES.md` first; it is binding. Work only in `prototype/temporal-factory/`.

Goal: independent, pinned black-box A2A services that stand in for the future directory, plus the v1 definition template. Callers pin these services by URL and durable identity. They are not started or managed by the factory.

Files you own: `services/testbed.py` (new), `services/quality_server.py`, `services/release_server.py`, `definitions/v1-template.json`, `tests/test_testbed.py`. Read-only: `src/harness_server.py` (the capability service, `--role capability`), `src/fixture.py` (another worker may add an optional `question` parameter to `branch_brief`; don't edit it), `src/definition.py`.

Required:
1. Fix `services/quality_server.py` and `services/release_server.py` imports so they run from `services/` with `../src` on sys.path. No `tools/spikes` paths. Keep their behavior.
2. `services/testbed.py {up,down,status} --home H [--port-base 45200]` behaves as in INTERFACES.md. Each service is a separate detached process (`start_new_session=True`) with its own durable state under `$H/services/<name>` and a log. `up` is idempotent: it health-checks already-running services and doesn't restart them. It writes `$H/testbed/approved_bindings.json`, `contracts.json`, `quality_policy.json`, and `pids.json`. `down` sends SIGTERM to the recorded pids and waits; it deletes nothing. `status` prints health for each service. Contracts are fixture-authored JSON per binding: `{name, role, capability (e.g. source_evidence@1 / counter_evidence@1 / quality-review@1 / release@1), a2a_protocol: "0.3.0", input, output, operations:{idempotent_action_id: true, lookup: "/fixture/actions/{id}" or "/receipts/{id}"}}`. Add `attested: false` to mark them as not attested. Quality policy is `{policy: "exact-revision-independent-review@1", reviewer_binding: "quality", author_may_not_review: true, repair_bound_max: 2}`.
3. `definitions/v1-template.json` is the existing lane-1 mixed v4 template. Add the optional caller input `question` as specified. Keep `outcome_mode` exactly as is.
4. Live smoke, required: use `H=/tmp/exo-proto-testbed-<suffix>` and port base 45100 only. Run `up`, then `status`, then an A2A `message/send` to one capability service and to quality (use `src/long_client.py`'s `send` shape or plain urllib JSON-RPC with `Authorization: Bearer fixture-token`), then `down`, then `up` again and show identities unchanged, then `down`. Save the results to `evidence/testbed-smoke.json`. Don't delete the state dir.
5. `tests/test_testbed.py` covers pure parts: binding/contract file shapes and that `definition.validate` accepts a package materialized from `v1-template.json` with the generated-shape bindings. Inline the `@child` substitution from `tools/spikes/2026-09-23/temporal-director-contract/author.py`; don't import from tools/.

Python: `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B`.

Handoff: `handoff/testbed.md`.
