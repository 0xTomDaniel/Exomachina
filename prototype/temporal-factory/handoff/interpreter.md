# Interpreter lane handoff

## Merged behavior

- Director contract lane: retained typed run inputs, owner claims, abort commands, child failure incidents, and propagation of child results. Start input now checks the exact shared key set and the pinned run-input digest. `question` flows through every assignment brief and typed join.
- Version binding lane: runs verify the pinned definition, bindings, contracts, Quality policy, and worker build before each node and each parallel assignment, and before a child starts. The workflow is PINNED on `exo-factory`; children use the same queue and closure. Status exposes `manifest_digest` and `interpreter_build`. Publications have labels and `get`/`list`; build IDs derive from the flat source digest.
- Quality reconciliation lane: assignment and release Activities use the durable outcome journal; Quality uses `quality_action_id` and checks the binding, assignment, attempt, task, and lookup evidence. Unresolved assignment, Quality, and release outcomes return terminal `incident` results. Public projection maps incidents to failed A2A tasks.
- Recovery scale baseline: retained the bounded declarative graph interpreter, typed join, repair limit, Director wait, independent Quality acceptance, release, and nested factory behavior. Removed fault, barrier, drop-ack, and spike-path controls from the product interpreter path.

## Files changed

- `src/factory.py`, `src/adapter.py`, `src/binding.py`, `src/buildinfo.py` (new), `src/worker.py`, `src/fixture.py`, `src/failure_projection.py`
- `tests/test_contract.py`, `tests/test_binding.py`, `tests/test_decisions.py`, `tests/test_interpreter.py` (new)

No other files were edited by this lane. No branch switch, commit, push, merge, deletion, or live Temporal start was performed.

## Commands and output

The required suite was run three times during integration; the final run was:

```sh
PY=/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python; cd prototype/temporal-factory && $PY -B -m unittest discover -s tests -v
```

Output: all listed tests `ok`; `Ran 43 tests in 0.109s` / `OK` (exit 0). The earlier runs also passed 43 tests (`0.128s` and `0.101s`). These include the ported contract, binding, and decision tests, the new interpreter tests, and the concurrently added authoring, runner, and testbed tests.

Offline Temporal workflow validation was run three times, including after the final changes:

```sh
/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B -c 'import sys; sys.path.insert(0, "src"); from factory import FactoryRun; from temporalio.worker import Replayer; Replayer(workflows=[FactoryRun], workflow_failure_exception_types=[ValueError]); print("Replayer workflow validation OK")'
```

Output: `Replayer workflow validation OK` (exit 0).

```sh
git diff --check -- prototype/temporal-factory/src/factory.py prototype/temporal-factory/src/adapter.py prototype/temporal-factory/src/binding.py prototype/temporal-factory/src/worker.py prototype/temporal-factory/src/definition.py prototype/temporal-factory/src/fixture.py prototype/temporal-factory/src/failure_projection.py prototype/temporal-factory/src/incident_projection.py prototype/temporal-factory/src/quality_authority.py prototype/temporal-factory/src/a2a_outcome.py prototype/temporal-factory/src/long_client.py prototype/temporal-factory/src/receiver_client.py prototype/temporal-factory/tests/test_contract.py prototype/temporal-factory/tests/test_binding.py prototype/temporal-factory/tests/test_decisions.py
```

Output: empty (exit 0).

Static scan of the owned product entry files for `faults|barrier|drop_ack|tools/spikes|EXO_TQ_OUTCOME_DB|EXO_TEMPORAL_ACTIVITY_LOG`: no matches (rg exit 1, expected).

## Evidence and gaps

The interpreter tests left state, without cleanup, at:

- `/tmp/exo-proto-interpreter-0886c64y`
- `/tmp/exo-proto-interpreter-4g3nx803`
- `/tmp/exo-proto-interpreter-5p4qggs0`
- `/tmp/exo-proto-interpreter-78wfh01u`
- `/tmp/exo-proto-interpreter-c2vhw2m_`
- `/tmp/exo-proto-interpreter-cp8by5p7`
- `/tmp/exo-proto-interpreter-ku_41vcq`
- `/tmp/exo-proto-interpreter-zkmjcn5s`

No live Temporal or A2A integration was run in this lane, as directed by the brief. There were no rejected commands or approvals.
