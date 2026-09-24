# Broker package handoff

Status: implemented and tested locally. No real sign-in, real token, or remote auth request was used. No commit was made.

## Files

- `.gitignore`: ignores only `prototype/temporal-factory/broker/node_modules/`.
- `broker/package.json`, `broker/package-lock.json`: exact published `@earendil-works/pi-ai` `0.87.1` pin.
- `broker/exo-model.mjs`: `serve`, `login`, `status`, `refresh`, `logout`, `leak-scan`; newline JSON socket protocol; fixture-only loopback override; egress gate; honest request headers; generic redacted errors.
- `broker/lib/store.mjs`: owner-only atomic credential file and dead-PID lock recovery.
- `broker/test/broker.test.mjs`: offline Node tests, all on loopback ports 46100–46106.

## Commands and results

Working directory for npm commands: `prototype/temporal-factory/broker/`.

- `npm install --package-lock-only --offline --cache /tmp/exomachina-pi-strands-debate/npm-cache` — exit 0; up to date, audited 86 packages; 0 vulnerabilities. npm warned that two dependency install scripts were blocked.
- `npm ci --offline --cache /tmp/exomachina-pi-strands-debate/npm-cache` — exit 0; added 85 packages, audited 86; 0 vulnerabilities. Same blocked-script warning.
- `node -e "const l=require('./package-lock.json'); console.log(JSON.stringify({declared:l.packages[''].dependencies['@earendil-works/pi-ai'],resolved:l.packages['node_modules/@earendil-works/pi-ai'].version}))"` — exit 0; `{"declared":"0.87.1","resolved":"0.87.1"}`.
- `node --check broker/exo-model.mjs` from `prototype/temporal-factory/` — exit 0, no output.
- `node --test broker/test/*.test.mjs` from `prototype/temporal-factory/` — exit 0; 7 tests passed, 0 failed. Covers invalid override with zero requests observed, racing `serve` processes with one PID and unchanged socket inode/ready PID, distinct sessions and identity headers, HTTP 401/403/429 kinds, stale socket and dead-PID credential lock, interleaved tool fragments, client-close cancellation, refresh token rotation via preload, positive leak-scan controls, and API-key fallback refusal.
- `node --test test/` from `broker/` — exit 1 because Node 26 treats the directory as a module path; `Cannot find module .../broker/test`. The package `test` script uses the working glob syntax.

Trial state from test runs remains under `/tmp/exo-proto-broker-<pid>-<timestamp>/`; the tests do not remove it. Mock tokens in those directories are synthetic.

## Originator and limits

Inspection of published pi-ai 0.87.1 shows its browser authorize URL defaults to `originator=pi` in `createAuthorizationFlow`; the device-code usercode and polling requests do not send an originator. Broker SSE uses pi-ai's public `fetch` option to set `originator: exomachina` and `User-Agent: exomachina-model-broker/0.1.0 (pi-ai/0.87.1)`. A live provider may reject that identity; no live qualification was attempted.

The fixture override requires an explicit, non-default realpath `EXO_MODEL_HOME`, its `FIXTURE_STORE` marker, and loopback URL. Login refuses fixture stores. The live scenario owner must also reject any inherited `EXO_CODEX_BASE_URL` before sign-in or broker start, as specified in `INTERFACES.md`; that file is outside this lane. Leak-scan excludes only the canonical credential file and reports it in `excluded`, while detecting a planted copy and a bare token in a SQLite-like file. No actual SQLite parser is needed because the scan is binary-safe.

## Final fix batch (23 Sep 2026)

The broker now checks home, `secrets/`, `run/`, credential and credential-lock ownership, type, symlink status and owner-only modes regardless of the backend override. Existing paths are checked with `lstat` before chmod; credential reads use `O_NOFOLLOW` and compare inode and device through the opened descriptor. A bad path returns a `config` error before any credential read. The OAuth callback host is restricted to `127.0.0.1`, `localhost`, or `::1` for login.

The fetch wrapper adds `originator: exomachina` and `User-Agent: exomachina-model-broker/0.1.0 (pi-ai/0.87.1)` to allowed `auth.openai.com` calls, covering device start, poll, code exchange and refresh. Browser login rewrites only the authorize URL's `originator` query value before printing it. Device code remains the default. pi-ai's `app_EMoamEEZ73f0CkXaXp7hrann` is its hard-coded Codex OAuth `client_id` with no public override. That is a compatibility and commercial uncertainty, not a broker fix; live acceptance was not tested.

Socket lines and log lines redact exact credential token values using a retained in-memory set, reloaded when the credential file's inode or mtime changes. The generic JWT-shape redactor applies to error payloads and log lines only; stream deltas and `done` content retain JWT-shaped model text. A mock provider error that echoed the current access token after an inode rotation was withheld from the public reply. The browser authorize URL test exercises the same pure rewrite helper used by login; it does not start pi-ai's fixed-port browser callback listener.

`node --test broker/test/*.test.mjs` from `prototype/temporal-factory/` passed: 11 tests, 0 failures. The tests used synthetic credentials, intercepted OAuth calls, loopback mock ports 46100–46109, and fixture homes under `/tmp/exo-proto-broker-*`; they made no real sign-in or OpenAI request. No files were deleted and no commit was made.

## Orchestrator error-boundary review

Provider error replies now contain only a fixed message for the classified kind, an integer HTTP status when known, and a structured code that passes `^[a-z0-9_.-]{1,64}$` and an explicit allowlist; unknown or invalid codes become `other`. The SSE fetch wrapper reads a clone of a failed JSON response only to extract that code in memory. Error event logs contain only `kind`, `status`, and `code` beyond their event name and timestamp. Raw pi-ai error messages and provider response bodies are never written to replies, logs, stdout or stderr. A synthetic HTTP 400 with an originator diagnostic and a non-JWT secret-looking string yielded only `{kind:"provider", message:"Codex provider request failed", status:400, code:"unsupported_originator"}`; an unknown code yielded `other`. The test checked that the planted string was absent from the reply, event log and broker stderr. This review replaces the earlier raw provider-detail behavior; no real provider request was made.

After this review, `node --test broker/test/*.test.mjs` from `prototype/temporal-factory/` passed: 12 tests, 0 failures. The added mock used loopback port 46110. No commit was made.
