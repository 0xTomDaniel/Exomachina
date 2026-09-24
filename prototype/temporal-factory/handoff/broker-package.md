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
