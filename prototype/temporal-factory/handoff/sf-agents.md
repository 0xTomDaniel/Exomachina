# Contract questions

- The brief says `contracts.json` includes each agent's `agent_binding.pin()` and capability. I treated the HTTP release receiver as the stated exception: the four A2A entries have pins and capability, while `release` retains its HTTP fixture contract.
- The fixed failure status text is `Agent work failed.`. A cancelled worker leaves its committed Task `working` so startup can recover it.
- The explicit legacy `--delayed-agent` option selects the legacy testbed profile even when `--profile` is omitted. The ordinary default remains `report`.
- The pre-existing Python and Node suites hard-code legacy fixture ports outside this lane's assigned block (the Node suite uses 46110). I ran the explicitly required suites under the qualification lock; this test-port conflict needs an orchestrator decision before another cross-lane concurrent run. New lane-owned service tests use 45740–45759.

# Files changed

- `services/model_agent.py` — independent async A2A server; durable identity, Tasks, model-call audit and stimulus; three-call and 240-second Task budget; background Strands work; pinned card/contract.
- `services/testbed.py` — default five-service report profile, scripted/synthetic/live model selection, report metadata and pins; legacy profile retained.
- `broker/exo-model.mjs` — added the request session id to successful `stream` log events only.
- `tests/test_model_agent.py` — injected fake role tests for replay, conflict, pin, durable restart, session isolation, budget failure and stimulus.
- `tests/test_testbed.py` — report profile process and metadata test.
- This handoff file.

`services/agent_roles.py` is an untracked stub supplied by the orchestrator. I did not edit it. The orchestrator will replace it with lane R's final file.

# Commands and results

All Python test commands used the absolute qualification venv with `-B` and `/usr/bin/lockf -k /tmp/exo-qual-suite.lock`.

| Command | Result |
| --- | --- |
| `/usr/bin/lockf -k /tmp/exo-qual-suite.lock /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest discover -s tests -p test_model_agent.py -v` | Pass, 4 tests (first focused run). |
| `/usr/bin/lockf -k /tmp/exo-qual-suite.lock /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest discover -s tests -p test_testbed.py -v` | Pass, 3 tests. |
| `npm --prefix prototype/temporal-factory/broker ci --offline --cache /tmp/exomachina-pi-strands-debate/npm-cache` | Pass, 85 packages added, 0 vulnerabilities. |
| `npm --prefix prototype/temporal-factory/broker test` | First run failed 17/18: the existing provider-error log assertion at `broker.test.mjs:400` observed one event before the second asynchronous log write. No broker change was made for this timing failure. |
| `npm --prefix prototype/temporal-factory/broker test` | Rerun passed 18/18. |
| `/usr/bin/lockf -k /tmp/exo-qual-suite.lock /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest discover -s tests` | First full run passed 117 tests. |
| `/usr/bin/lockf -k /tmp/exo-qual-suite.lock /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest discover -s tests -p test_model_agent.py` | Pass, 6 tests after adding restart and next-run coverage. |
| `/usr/bin/lockf -k /tmp/exo-qual-suite.lock /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest discover -s tests` | Final full run passed 119 tests. |
| `/usr/bin/lockf -k /tmp/exo-qual-suite.lock /Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -m unittest discover -s tests -p test_testbed.py` | Pass, 3 tests after the legacy-option adjustment. |
| `git diff --check` | Pass. |
| `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -c 'import socket; ports=list(range(45740,45760))+list(range(46470,46490)); listeners=[]; [(lambda s,p: (listeners.append(p) if s.connect_ex(("127.0.0.1",p))==0 else None, s.close()))(socket.socket(),p) for p in ports]; print("remaining listeners:",listeners)'` | Pass: `remaining listeners: []`. |

The prior committed qualification reports **112** Python tests before this lane; I did not run a pre-edit baseline. The final suite has **119** tests: six new model-agent tests and one new testbed test. FastAPI emits an `on_event` deprecation warning; it does not affect the pass. The earlier testbed run emitted subprocess `ResourceWarning`s; the final focused run did not after child handles were retained and waited.

# Trial state and cleanup

Tests preserved their state under `/tmp/exo-sf-agents-unit-*` and `/tmp/exo-sf-agents-testbed-*`. The testbed process test shut down each process in cleanup. Final socket checks found no listeners in services 45740–45759 or mocks 46470–46489; a process-list check found no `model_agent.py`, `release_server.py` or `exo-model.mjs` process from this worktree.

# Gaps

- Role behaviour is exercised through injected fake `ROLES` only, as directed. The final lane R role implementation, live provider and loopback provider have not been exercised in this lane.
- The first Node test run exposed an existing asynchronous log timing assertion. The required rerun passed all 18 tests; this lane's broker edit was limited to the session field.
- The baseline count is from the committed qualification record, not a pre-edit test run in this worktree.
