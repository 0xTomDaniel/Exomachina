# Lane: broker test fixtures, then security review (tw_quality, pane wG:p8)

Read `prototype/temporal-factory/INTERFACES.md` first, especially "Model broker lane"; it is binding. Then read `/tmp/exomachina-pi-strands-debate/matched/strands-broker-interleaved/{mock_backend.mjs,controls.py,interleaved-result.md,scenario.py}` and `matched/strands-broker/worker-audit.md`. Work only in `prototype/temporal-factory/`.

## Phase 1 (now): fixtures

Files you own: `broker/testing/mock-codex.mjs`, `broker/testing/mock-oauth-fetch.mjs`, `broker/testing/README.md`. tw_package (Node broker) and tw_director (integration scenario) consume them in parallel, so land a first working version quickly and keep the CLI stable.

1. `mock-codex.mjs --port P --record FILE [--script authoring --draft FILE | --script interleaved]`: loopback-only Codex Responses SSE backend ported from the interleaved spike, keeping its strict per-session ordered replay checks (exact reasoning id/ciphertext, function item ids, call ids/names/arguments, outputs) and interleaved argument fragments. It records every request's headers (`originator`, `user-agent`, `chatgpt-account-id`, presence-only for `authorization`) and bodies to `FILE` — record the authorization header as `present`/`absent` only, never its value.
2. Authoring script mode drives a real Strands authoring session: `describe_vocabulary`, then `validate_draft` with the seeded defective draft from `--draft`, then a corrected draft **derived from the validation error text the request carries** (as the spike patched missing route cases), then `submit_draft`, then a final text message. Produce synthetic encrypted reasoning (`ENC_…`) only in `response.completed`, as the spike did.
3. `mock-oauth-fetch.mjs`: a `node --import` preload that intercepts only `https://auth.openai.com/oauth/token` with a refresh_token grant, returns a rotated synthetic JWT-shaped access token carrying `https://api.openai.com/auth.chatgpt_account_id`, a new refresh token and expiry, and records grants to a file; every other non-loopback URL throws. Test-only; product code never imports it.
4. A short self-check (`node broker/testing/…` or a `node --test` file under `broker/testing/`) proving each negative control from the spike still fails with its specific reason.

Ports 46120–46149. No real network. Do not delete anything. Do not commit. Write `handoff/broker-fixtures.md` when phase 1 is done and report the CLI in one paragraph.

## Phase 2 (after the orchestrator says so): independent security and correctness review

You will get a separate prompt. Expect: credential file modes and atomicity, token absence from every sink (Temporal history, A2A artifacts, evidence, logs, git), no API-billing fallback, `~/.codex/auth.json` never read (verify with `fs_usage`/code audit), originator/egress, broker singleton/stale recovery, and doc accuracy. You will also update README/INTERFACES wording and the affected architecture docs.
