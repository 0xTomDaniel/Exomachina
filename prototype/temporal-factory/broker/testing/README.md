# Broker test fixtures

These files are test-only. The mock Codex endpoint listens on `127.0.0.1` and accepts ports 46120–46149. It writes JSONL request records with the decoded body and `originator`, `user-agent`, `chatgpt-account-id`, and `session-id` headers. `authorization` is recorded only as `present` or `absent`. Strict replay history is keyed by `session-id` (falling back to `prompt_cache_key`) and checks ordered reasoning ID and ciphertext, function item IDs, call IDs, names, arguments, and outputs. Synthetic encrypted reasoning appears only in `response.completed`.

```sh
node broker/testing/mock-codex.mjs --port 46130 --record /tmp/exo-proto-fixtures-CASE/requests.jsonl --script authoring --draft /tmp/exo-proto-fixtures-CASE/draft.json
node broker/testing/mock-codex.mjs --port 46131 --record /tmp/exo-proto-fixtures-CASE/interleaved.jsonl --script interleaved
node broker/testing/mock-codex.mjs --port 46132 --record /tmp/exo-proto-fixtures-CASE/runaway.jsonl --script runaway --draft /tmp/exo-proto-fixtures-CASE/draft.json --hold-ms 500
node broker/testing/mock-codex.mjs --self-check
```

`authoring` requires a seeded defective template JSON. It calls `describe_vocabulary`, validates that draft, adds only route cases named in the returned validation error, submits the corrected draft, then emits final text. `interleaved` starts two function calls together and alternates their argument fragments; `--draft` is optional in this mode. `runaway` never submits: it alternates `describe_vocabulary` and `validate_draft` of the same invalid draft indefinitely. Its `--draft` is optional and defaults to an invalid `{"schema":1}`. `--hold-ms` delays each runaway SSE response after opening the stream; JSONL `kind:"stream"` records `aborted:true` when the client closes before completion. Count `kind:"request"` rows to assert the model request budget.

For refresh tests, preload the OAuth fetch shim and set a record path:

```sh
EXO_MOCK_OAUTH_RECORD=/tmp/exo-proto-fixtures-CASE/oauth-grants.jsonl node --import ./broker/testing/mock-oauth-fetch.mjs broker/exo-model.mjs refresh
```

The shim intercepts only a refresh-token grant to `https://auth.openai.com/oauth/token`, returns rotated synthetic credentials, and logs grant metadata without token values. Every other non-loopback fetch throws. Loopback fetches use Node's original `fetch`.

The backend and preload do not create a credential store. Test code that creates a synthetic store must use an explicit `EXO_MODEL_HOME` under `/tmp/exo-proto-<lane>-<suffix>/`, create `$EXO_MODEL_HOME/FIXTURE_STORE`, use mode `0700` for the home and `0600` for the credential and marker, and refuse to write under the default `~/.exomachina/model-broker`. The broker rejects `EXO_CODEX_BASE_URL` unless that marker exists in a non-default fixture store. Product code must not import these fixtures.
