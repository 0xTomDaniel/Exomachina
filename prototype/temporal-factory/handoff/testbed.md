# Testbed lane handoff

## Files changed

- `services/testbed.py` (new): detached six-service lifecycle, durable state, health, pinned metadata.
- `services/quality_server.py`: import the prototype `src/` harness and fixture.
- `services/release_server.py`: import the prototype `src/` fixture.
- `definitions/v1-template.json`: optional caller `question` input; `outcome_mode` unchanged.
- `tests/test_testbed.py` (new): metadata shapes and package validation.
- `evidence/testbed-smoke.json` (new): full live smoke results.
- `handoff/testbed.md` (this file).

## Commands and results

All commands ran from the repository root. `PY` below is the exact interpreter path `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python`; the listed commands used that path, not a shell variable.

Read-only inspection commands all exited 0: `cat prototype/temporal-factory/INTERFACES.md`; `cat prototype/temporal-factory/briefs/testbed.md`; `git status --short --branch` (branch `feat/temporal-factory-prototype`, initially clean); `rg --files prototype/temporal-factory/services prototype/temporal-factory/definitions prototype/temporal-factory/tests prototype/temporal-factory/src`; `cat prototype/temporal-factory/services/quality_server.py prototype/temporal-factory/services/release_server.py`; `cat prototype/temporal-factory/definitions/v1-template.json prototype/temporal-factory/src/harness_server.py prototype/temporal-factory/src/definition.py`; `cat prototype/temporal-factory/src/fixture.py prototype/temporal-factory/src/long_client.py`; `sed -n '1,260p' prototype/temporal-factory/src/harness_server.py`; `sed -n '1,240p' prototype/temporal-factory/src/definition.py`; `cat tools/spikes/2026-09-23/temporal-director-contract/author.py`; `rg -n 'quality_server|release_server|approved_bindings|contracts.json|quality_policy|testbed.py|input\"|output\"' prototype/temporal-factory tools/spikes/2026-09-23/temporal-director-contract -g '*.py' -g '*.json' -g '*.md'`; `sed -n '240,360p' prototype/temporal-factory/src/harness_server.py`; `sed -n '1,180p' prototype/temporal-factory/tests/test_contract.py`; `rg -n 'pids.json|start_new_session=True|def health|SIGTERM' prototype/temporal-factory tools/spikes/2026-09-23/temporal-director-contract/probe.py`.

Verification commands:

| Exact command | Exit and output |
| --- | --- |
| `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B prototype/temporal-factory/tests/test_testbed.py -v` | 0; 2 tests passed, `OK`. |
| `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B prototype/temporal-factory/services/testbed.py up --home /tmp/exo-proto-testbed-20260923-t1 --port-base 45100` | 0; six bindings and healthy services, incarnation 1. |
| `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B prototype/temporal-factory/services/testbed.py status --home /tmp/exo-proto-testbed-20260923-t1 --port-base 45100` | 0; six `healthy: true`. |
| A2A smoke heredoc below | 0; capability `source_evidence@1` artifact and Quality `accepted: true`, exact revision `r1`. |
| `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B prototype/temporal-factory/services/testbed.py up --home /tmp/exo-proto-testbed-20260923-t1 --port-base 45100` | 0; all PIDs and incarnations unchanged. |
| `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B prototype/temporal-factory/services/testbed.py down --home /tmp/exo-proto-testbed-20260923-t1 --port-base 45100` | 0; six `stopped`. |
| `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B prototype/temporal-factory/services/testbed.py up --home /tmp/exo-proto-testbed-20260923-t1 --port-base 45100` | 0; identities unchanged, six incarnations 2. |
| `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B prototype/temporal-factory/services/testbed.py down --home /tmp/exo-proto-testbed-20260923-t1 --port-base 45100` | 0; six `stopped`. |
| `git diff --check` | 0; no output. |
| `git status --short` | 0; own files listed alongside concurrent edits from other lanes. |
| `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B prototype/temporal-factory/services/testbed.py status --home /tmp/exo-proto-testbed-20260923-t1 --port-base 45100` | 0; six `healthy: false` after final down. |
| `ps -p 14571,14572,14573,14577,14579,14580 -o pid=,stat=,command=` | 1; no output, confirming final processes absent. This was a normal no-match exit, not a rejected command. |
| `git diff -- prototype/temporal-factory/services/quality_server.py prototype/temporal-factory/services/release_server.py prototype/temporal-factory/definitions/v1-template.json` | 0; showed only intended import and input changes. |
| Python syntax and JSON heredoc below | 0; `Python syntax and JSON: OK`. |

A2A smoke command:

```sh
/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B - <<'PY'
import json
import sys
sys.path.insert(0, 'prototype/temporal-factory/src')
from fixture import branch_brief, canonical, sha256_text
from long_client import send
capability = send('http://127.0.0.1:45100', {
    'op': 'assign', 'action_id': 'testbed-smoke:source_alpha',
    'run_id': 'testbed-smoke', 'definition_digest': 'smoke-definition',
    'brief': canonical(branch_brief('source_alpha', 'source_evidence')),
})
content = canonical({
    'kind': 'verified_research_candidate@1', 'revision': 'r1',
    'join': {'kind': 'evidence_join@1', 'route_status': 'clear', 'requires_scope': False},
    'objections_resolved': True,
})
artifact = {'revision': 'r1', 'sha256': sha256_text(content),
            'author': capability['harness_identity'], 'content': content}
quality = send('http://127.0.0.1:45104', {
    'op': 'review', 'action_id': 'testbed-smoke:quality',
    'run_id': 'testbed-smoke', 'definition_digest': 'smoke-definition',
    'artifact': artifact,
})
print(json.dumps({'capability': capability, 'quality': quality}, indent=2, sort_keys=True))
PY
```

Syntax and JSON check command:

```sh
/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B - <<'PY'
import ast
import json
from pathlib import Path
root = Path('prototype/temporal-factory')
for path in [root/'services/testbed.py', root/'services/quality_server.py', root/'services/release_server.py', root/'tests/test_testbed.py']:
    ast.parse(path.read_text(), filename=str(path))
for path in [root/'definitions/v1-template.json', root/'evidence/testbed-smoke.json']:
    json.loads(path.read_text())
print('Python syntax and JSON: OK')
PY
```

## Evidence and gaps

- Full smoke results: `evidence/testbed-smoke.json`.
- Preserved runtime state and logs: `/tmp/exo-proto-testbed-20260923-t1/`.
- Metadata test leaves `/tmp/exo-proto-testbed-test-*` trial directories, as deletion is prohibited.
- The participating release receiver retains its existing HTTP `/release` and `/receipts/{id}` behavior. It does not implement A2A `message/send`; the lane brief explicitly required preserving its behavior.
- No command or approval was rejected. No assigned work remains.
