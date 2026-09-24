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
