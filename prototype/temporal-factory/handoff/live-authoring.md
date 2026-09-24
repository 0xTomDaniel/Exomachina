# Live authoring lane handoff

Status: synthetic loopback end to end **PASS**. No commit or push. No live subscription run or external auth request.

## Files

- `scenarios/live_authoring.py` (new): fixture-marked broker authoring, ordinary factory A2A run, Temporal history export, failure evidence, leak-scan negative and planted positive controls, cleanup, and a live-override guard self-test.
- `evidence/live-authoring-synthetic-loopback.json` (new): latest passing run, `/tmp/exo-proto-live-syn-3`.
- `evidence/live-authoring-synthetic-loopback/{parent,child}.json` (new): raw v2 Temporal histories.
- `handoff/live-authoring.md` (this file).

## Commands and results

From `prototype/temporal-factory`, with `PY=/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python`:

```sh
$PY -B -m py_compile scenarios/live_authoring.py
# exit 0
$PY -B scenarios/live_authoring.py --self-test-live-guard
# {"status":"pass","test":"live_override_guard","loopback_requests":0}
$PY -B scenarios/live_authoring.py --home /tmp/exo-proto-live-syn-1 --provider synthetic-loopback
# first invocation failed at the trial-home prefix check because macOS resolves /tmp to /private/tmp; no trial home was created by that invocation
# after prefix fix: {"status":"pass", ...}
$PY -B scenarios/live_authoring.py --home /tmp/exo-proto-live-syn-2 --provider synthetic-loopback
# {"status":"pass", ...}
$PY -B scenarios/live_authoring.py --home /tmp/exo-proto-live-syn-3 --provider synthetic-loopback
# {"status":"pass","evidence":".../evidence/live-authoring-synthetic-loopback.json","run_id":"2041d4e7-4f52-4b60-99e3-a4773af55e9d.574e200fbc7596bda62d","provider":"synthetic-loopback"}
$PY -B - <<'PY'
import json, os, sys
from pathlib import Path
sys.path.insert(0, 'scenarios')
from live_authoring import leak_scan
from common import write_evidence
home = Path('/tmp/exo-proto-live-syn-3').resolve()
os.environ['EXO_MODEL_HOME'] = str(home / 'model')
path = Path('evidence/live-authoring-synthetic-loopback.json').resolve()
histories = Path('evidence/live-authoring-synthetic-loopback').resolve()
record = json.loads(path.read_text())
result = leak_scan(home, home / 'model', path, histories)
record['claims']['leak_scan'] = {'level': 'real', 'value': result}
write_evidence(path.name, record)
checked = leak_scan(home, home / 'model', path, histories)
assert checked['hits'] == [] and checked['candidate_file_count'] == result['candidate_file_count']
print(json.dumps({'candidate_files': checked['candidate_file_count'], 'hits': len(checked['hits'])}))
PY
# {"candidate_files":92,"hits":0}
```

Latest evidence: 2 authoring rounds (invalid then valid), 4 broker model calls within the 12-call limit, 3 tool calls within the 24-call limit, v2 completed via the harness A2A identity on its pinned Temporal build, one `http-release (fixture)` effect, 2 raw workflow histories, 92 commit-candidate source/evidence files included in the scan, 2,002 files scanned overall, zero credential hits. The planted credential copy and bare token under `/tmp/exo-proto-live-syn-3-leak-control` were both detected. The fixture marker and credential are both mode `0600`. Cleanup reported no errors; the synthetic broker is stopped. Earlier trial state remains under `/tmp/exo-proto-live-syn-{1,2}` and their positive-control directories.

The evidence explicitly marks `director_model: fixture`: the broker powers graph authoring only; the A2A Director remains `ToolCallingModelFixture`. The release receiver is labelled as an HTTP fixture. No API-key provider was selected; a synthetic `OPENAI_API_KEY` poison value was present during the broker model calls.

## Gap

`scenarios/live_authoring.py` accepts `--max-rounds N` and forwards nondefault values to `admin.py author`, but the current read-only `src/admin.py` does not expose that option. Default `N=4` passed. The admin owner must add the CLI option to make nondefault runs work. The `codex-subscription` path was not run, per instruction. The local guard self-test proves its refusal function leaves a recording loopback server at zero requests; it does not execute the live provider path.

## Final synthetic run

Committed code SHA: `67cbec08399cff5dcd969077e47ab350cd82f05f`. Trial home: `/tmp/exo-proto-live-syn-final-20260923-67cbec0-7e2a` (resolved by macOS to `/private/tmp/exo-proto-live-syn-final-20260923-67cbec0-7e2a`). The fresh-home check passed before the run. No `codex-subscription` run or OpenAI request was made.

Commands from `prototype/temporal-factory` and their output:

```sh
git status --short -- src scenarios broker tests
# exit 0; no output
git rev-parse HEAD
# 67cbec08399cff5dcd969077e47ab350cd82f05f
test ! -e /tmp/exo-proto-live-syn-final-20260923-67cbec0-7e2a
# exit 0; no output
/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B scenarios/live_authoring.py --self-test-live-guard
# {"status": "pass", "test": "live_override_guard", "loopback_requests": 0}
/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B scenarios/live_authoring.py --home /tmp/exo-proto-live-syn-final-20260923-67cbec0-7e2a --provider synthetic-loopback
# {"status": "pass", "evidence": "/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/prototype/temporal-factory/evidence/live-authoring-synthetic-loopback.json", "scan_evidence": "/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/prototype/temporal-factory/evidence/live-authoring-synthetic-loopback-scan.json", "provider": "synthetic-loopback", "run_id": "52aa74eb-3dc4-4062-8c43-4a490d96e61f.574e200fbc7596bda62d"}
```

The evidence and sidecar were read and assertions over their recorded values passed: `{"status": "pass", "rounds": 2, "model_calls": 4, "tool_calls": 3, "release_effects": 1, "history_events": {"child": 44, "parent": 14}, "files_scanned": 2007, "candidate_files": 99, "hits": 0, "positive_control_hits": 9, "final_scan_hits": 0, "broker": "stopped", "runner_running": false}`. Limits were 4 rounds, 12 model calls, 24 tool calls, and 600 seconds; the two drafts were invalid then valid. The completed v2 run recorded `director_model: fixture` and one `http-release (fixture)` effect with `accepted_effect_count: 1`. The planted bare access token and copied credential were both detected in the positive control. The final sidecar scan had zero hits, and its evidence SHA-256 matched the JSON file. Cleanup recorded no errors, with the broker, runner, and testbed stopped. The run left its trial state and raw parent/child histories in place.

## Live codex-subscription run

**FAIL** at scenario step `broker authoring and v2 publication` (`AssertionError`). The real subscription provider accepted the request with `originator: exomachina`; there was no provider error (`kind/status/code: none`). The author published v2 after one valid draft, so the scenario's requirement for an invalid draft followed by a corrected draft failed. This was not an authoring budget abort, so the one permitted budget retry was not run. The A2A task, release, Temporal histories, and pinning were not reached.

Code SHA: `e2554c6b6a76a67ada48562d977914dbffd6dc63`. Trial home: `/tmp/exo-proto-live-codex-20260923-e2554c6-4d8a1` (resolved to `/private/tmp/exo-proto-live-codex-20260923-e2554c6-4d8a1`). No login, code edit, deletion, or commit was performed.

Commands from `prototype/temporal-factory` (`PY=/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python`):

```sh
git status --short -- src scenarios broker tests
# exit 0; no output
git rev-parse HEAD
# e2554c6b6a76a67ada48562d977914dbffd6dc63
test ! -e /tmp/exo-proto-live-codex-20260923-e2554c6-4d8a1
# exit 0; fresh home
env -u EXO_MODEL_HOME -u EXO_CODEX_BASE_URL -u PI_OAUTH_CALLBACK_HOST $PY -B scenarios/live_authoring.py --home /tmp/exo-proto-live-codex-20260923-e2554c6-4d8a1 --provider codex-subscription
# exit 1; status fail; step broker authoring and v2 publication; error_type AssertionError
env -u EXO_MODEL_HOME -u EXO_CODEX_BASE_URL -u PI_OAUTH_CALLBACK_HOST node broker/exo-model.mjs leak-scan /tmp/exo-proto-live-codex-20260923-e2554c6-4d8a1 evidence/ evidence/live-authoring-codex-subscription-scan.json
# exit 0; files_scanned 75; bytes_scanned 1121566; hits []
```

Evidence: `evidence/live-authoring-codex-subscription.json` and `evidence/live-authoring-codex-subscription-scan.json`. Limits were 4 rounds, 12 model calls, 24 tool calls, and 600 seconds. Authoring recorded 1 round (`valid: true`, `errors: []`), 4 model calls, and 3 tools in order: `describe_vocabulary`, `validate_draft`, `submit_draft`. Model evidence says `live: true`, `provider: codex-subscription`, `billing: subscription`, model `gpt-6-sol`. Auto approval and v2 publication were recorded (`build_id: b-05b5760359e6`, package digest `66934029ca324b117f85617ce97a0fd87f2402fd28b424cbf03a25b1c7f2896e`). Broker health recorded signed-in account `sha256:188b022d6e97`, pi-ai `0.87.1`, and `originator: exomachina`; originator acceptance was `success`. The top-level labels remain `director_model: fixture` and `release: http-release (fixture)`, but no run exercised either component.

The scenario leak scan covered 158 files including 102 commit candidates and found zero real-credential hits; its synthetic positive control detected both planted files (9 hits). The final sidecar scan also covered 158 files with zero hits, and its evidence SHA-256 matched the evidence JSON. Cleanup reported no errors; the trial runner and testbed were stopped, while the pre-existing persistent broker remained available.

## Live codex-subscription run: attempt 2

**PASS** on committed code `49b4c449a1b6f3d64ad876b69fc1eeee1fe20e5b`. The scoped status check (`git status --short -- src scenarios broker tests`) was empty before the runs. No budget retry was needed. No login, code edit, deletion, or commit was performed in this phase.

Commands from `prototype/temporal-factory`, with `PY=/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python`:

```sh
git status --short -- src scenarios broker tests
# empty
git rev-parse HEAD
# 49b4c449a1b6f3d64ad876b69fc1eeee1fe20e5b
test ! -e /tmp/exo-proto-live-syn-final-49b4c44-8174def4
# exit 0; fresh home
$PY -B scenarios/live_authoring.py --home /tmp/exo-proto-live-syn-final-49b4c44-8174def4 --provider synthetic-loopback
# exit 0; status pass
test ! -e /tmp/exo-proto-live-codex-49b4c44-attempt2-83f74a4e
# exit 0; fresh home
env -u EXO_MODEL_HOME -u EXO_CODEX_BASE_URL -u PI_OAUTH_CALLBACK_HOST $PY -B scenarios/live_authoring.py --home /tmp/exo-proto-live-codex-49b4c44-attempt2-83f74a4e --provider codex-subscription
# exit 0; status pass
env -u EXO_MODEL_HOME -u EXO_CODEX_BASE_URL -u PI_OAUTH_CALLBACK_HOST node broker/exo-model.mjs leak-scan /tmp/exo-proto-live-codex-49b4c44-attempt2-83f74a4e evidence/ handoff/live-authoring.md
# run through a JSON-only capture wrapper; exit 0; 1,928 files, 79,259,825 bytes, 0 hits before this handoff append
```

The synthetic trial home resolved to `/private/tmp/exo-proto-live-syn-final-49b4c44-8174def4`. Its retained evidence in `evidence/live-authoring-synthetic-loopback.json` and `-scan.json` records acceptance `ok`, `first_pass_valid: false`, 2 rounds (invalid then valid), 4 model calls, 3 tool calls, 104 commit-candidate files scanned, and zero scenario and final-scan hits. Its v2 A2A and Temporal run passed. The synthetic histories under `evidence/live-authoring-synthetic-loopback/` were regenerated and kept.

The live trial home resolved to `/private/tmp/exo-proto-live-codex-49b4c44-attempt2-83f74a4e`. Evidence in `evidence/live-authoring-codex-subscription.json` and `-scan.json` records acceptance `ok`, `first_pass_valid: true`, 1 valid round, 4 model calls, and 3 tool calls within the default 4/12/24/600 budget. The model was `gpt-6-sol`, `live: true`, `provider: codex-subscription`, `billing: subscription`; originator acceptance was `success` with `originator: exomachina`. Auto approval published v2 on build `b-05b5760359e6`.

The A2A task completed on the provisioned instance identity and ran the v2 package on that build. Parent and child Temporal histories were exported under `evidence/live-authoring-codex-subscription/`; both completed and pinned to the build (14 and 44 events, respectively). The Director was labelled `fixture`, and the one release effect was `http-release (fixture)`. The scenario scan covered 2,011 files, including 107 commit candidates, with zero hits; its planted synthetic positive control detected both files. The final sidecar scan had zero hits and its evidence SHA-256 matched. Cleanup recorded no errors; the trial runner stopped. Total scenario time was 65.717 seconds.
