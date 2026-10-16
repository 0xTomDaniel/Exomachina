# Broker fixtures, Phase 1 handoff

Status: implemented and locally checked. No commit. Files changed: `broker/testing/mock-codex.mjs`, `broker/testing/mock-oauth-fetch.mjs`, `broker/testing/README.md`, this handoff.

## CLI for dependent workers

```sh
node broker/testing/mock-codex.mjs --port 46130 --record /tmp/exo-proto-fixtures-CASE/requests.jsonl --script authoring --draft /tmp/exo-proto-fixtures-CASE/draft.json
node broker/testing/mock-codex.mjs --port 46131 --record /tmp/exo-proto-fixtures-CASE/interleaved.jsonl --script interleaved
node broker/testing/mock-codex.mjs --port 46132 --record /tmp/exo-proto-fixtures-CASE/runaway.jsonl --script runaway --draft /tmp/exo-proto-fixtures-CASE/draft.json --hold-ms 500
node broker/testing/mock-codex.mjs --self-check
```

`runaway` also works without `--draft` and then repeats the same invalid `{"schema":1}`. It never calls `submit_draft`. `--hold-ms` opens SSE headers and delays every runaway response; a `kind:"stream", aborted:true` record means the client closed it. Count `kind:"request"` rows to check model-call limits. Every request record contains its full decoded body and `headers.session-id`; `headers.authorization` is only `present` or `absent`. Replay state uses `session-id` per session, with `prompt_cache_key` as a fallback. A mismatched header and cache key is rejected. The two-session smoke accepted three complete requests per session with no cross-session history and recorded one aborted held stream.

For refresh tests: `EXO_MOCK_OAUTH_RECORD=/tmp/exo-proto-fixtures-CASE/oauth-grants.jsonl node --import ./broker/testing/mock-oauth-fetch.mjs broker/exo-model.mjs refresh`. The preload records only grant metadata, rotates fabricated credentials, and rejects non-loopback fetches other than the exact OAuth refresh endpoint.

Fixture model homes must be explicit non-default `EXO_MODEL_HOME` paths, mode `0700`, with `FIXTURE_STORE` and synthetic credential files mode `0600`; test code must refuse to write into `~/.exomachina/model-broker`. These fixture files do not write a credential store. The mock does not need the marker by itself; the broker requires it when `EXO_CODEX_BASE_URL` is set. `scenarios/live_authoring.py` currently writes the marker with `Path.write_text` and should set its mode to `0600` in its own lane.

## Verification and evidence

Trial state: `/tmp/exo-proto-fixtures-eW86Sy/` (left in place). Commands run from the repository root, all exit 0 unless noted:

```sh
node prototype/temporal-factory/broker/testing/mock-codex.mjs --self-check
# {"passed":true}; six specific replay rejection reasons, including all five spike controls.

node --check prototype/temporal-factory/broker/testing/mock-codex.mjs
node --check prototype/temporal-factory/broker/testing/mock-oauth-fetch.mjs
# Both exit 0 with no output.

node prototype/temporal-factory/broker/testing/mock-codex.mjs --port 46127 --record /tmp/exo-proto-fixtures-eW86Sy/requests.jsonl --script authoring --draft /tmp/exo-proto-fixtures-eW86Sy/draft.json
# Ready: {"mock":46127,"script":"authoring"}; stopped with Ctrl-C after the smoke.

node prototype/temporal-factory/broker/testing/mock-codex.mjs --port 46128 --record /tmp/exo-proto-fixtures-eW86Sy/interleaved.jsonl --script interleaved
# Ready: {"mock":46128,"script":"interleaved"}; stopped with Ctrl-C after the smoke.

node prototype/temporal-factory/broker/testing/mock-codex.mjs --port 46129 --record /tmp/exo-proto-fixtures-eW86Sy/runaway.jsonl --script runaway --draft /tmp/exo-proto-fixtures-eW86Sy/draft.json --hold-ms 200
# Ready: {"mock":46129,"script":"runaway"}; stopped with Ctrl-C after the smoke.
```

Inline Node HTTP clients (run with `node --input-type=module - <<'JS'`) drove authoring through four requests and observed the exact tool sequence `describe_vocabulary, validate_draft, submit_draft`, with `requires_scope` added only from returned validation feedback. An interleaved client observed alternating fragments `fc_call_vocab_1, fc_call_validate_1` repeated three times and ciphertext only at `response.completed`. Two concurrent runaway clients each completed three strict replay requests under distinct `session-id` headers; a fourth held request was cancelled. An inline OAuth client (run with `EXO_MOCK_OAUTH_RECORD=/tmp/exo-proto-fixtures-eW86Sy/oauth-grants.jsonl node --import ./prototype/temporal-factory/broker/testing/mock-oauth-fetch.mjs --input-type=module - <<'JS'`) observed two different access and refresh values, the synthetic account claim, and a blocked external fetch. The clients printed only pass summaries, no credential values.

Log audit: `requests.jsonl` has 4 requests, `interleaved.jsonl` 1, `runaway.jsonl` 7; all have zero rejections. Runaway log has session counts 4 and 3, six completed streams and one `aborted:true`. OAuth log has two grant metadata rows and no credential fields. Request logs contain authorization presence only. Synthetic trial data was not added to the repository.

Known limit: the full real Strands → Node broker → mock scenario and the new budget tests belong to the integration and Python lanes and were not run here. No command or automatic approval was rejected.
