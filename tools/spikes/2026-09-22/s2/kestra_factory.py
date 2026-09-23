"""Kestra-backed implementation of the same owned Factory Module Interface.

Kestra owns task sequencing/Pause; this Module owns identity, assignment intent,
acceptance and command deduplication. Explicit refreshes are reconciliation reads,
not a second scheduler. The bounded contract has one native Pause per execution.
"""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any
from uuid import uuid4

from harness import Rejected, canonical
from kestra_adapter import KestraAdapter


class KestraFactory:
    def __init__(self, harness):
        self.harness = harness
        self.config = harness.kestra_config

    def _authorize(self, db, command, actor, mutation=True):
        self.harness._authorize(db, actor, mutation)
        if command.get('expected_incarnation', self.harness.incarnation) != self.harness.incarnation:
            raise Rejected('stale incarnation')

    @staticmethod
    def _save(db, run):
        db.execute('INSERT OR REPLACE INTO runs VALUES(?, ?)', (run['id'], canonical(run)))

    def _reserve(self, db, command, actor, run_id):
        if not command.get('key'):
            raise Rejected('mutation requires idempotency key')
        fingerprint = hashlib.sha256(canonical({'actor': actor, 'command': command}).encode()).hexdigest()
        found = db.execute('SELECT * FROM commands WHERE key=?', (command['key'],)).fetchone()
        if found:
            if found['fingerprint'] != fingerprint:
                raise Rejected('idempotency key conflict')
            return json.loads(found['result'])['id']
        db.execute('INSERT INTO commands VALUES(?, ?, ?)',
                   (command['key'], fingerprint, canonical({'id': run_id})))
        return run_id

    def _adapter(self, run):
        # Each run retains its execution target/version; credential location can rotate.
        return KestraAdapter(run['engine']['binding'] | {'auth_file': self.config['auth_file']})

    def execute(self, command: dict[str, Any], actor: str, delivery=None) -> dict[str, Any]:
        op = command['op']
        with self.harness.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, command, actor, op not in {'inspect', 'list'})
            if op == 'list':
                return {'runs': [json.loads(row[0]) for row in db.execute('SELECT body FROM runs')]}
            if op == 'start':
                proposed_id = str(uuid4())
                run_id = self._reserve(db, command, actor, proposed_id)
                if run_id == proposed_id:
                    binding = {key: self.config[key] for key in ('url', 'namespace', 'flow_id', 'revision')}
                    run = {'id': run_id, 'owner': self.harness.identity,
                        'organization': self.harness.organization, 'brief': command.get('brief', ''),
                        'definition': f"kestra:{binding['namespace']}.{binding['flow_id']}@{binding['revision']}",
                        'created_incarnation': self.harness.incarnation,
                        'state': 'working', 'accepted_output': None, 'acceptance': None,
                        'engine': {'kind': 'kestra', 'binding': binding, 'submission': 'intent',
                                   'execution_id': None, 'native_state': None, 'resume_requests': 0}}
                    self._save(db, run)
                run = self.harness._load(db, run_id)
            else:
                run_id = command['run_id']
                run = self.harness._load(db, run_id)
                if op not in {'inspect', 'review', 'decide'}:
                    raise Rejected('native command outside bounded S2 contract')
                if op != 'inspect':
                    self._reserve(db, command, actor, run_id)
            self.harness._bind(db, run_id, delivery)
        if op == 'start':
            self._create_once(run_id, command, actor)
        if op == 'review':
            self._review(run_id, command, actor)
        if op == 'decide':
            self._resume(run_id, command, actor)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                run = self._refresh(run_id, command, actor)
                if run['state'] in {'completed', 'failed', 'canceled'}:
                    return run
                time.sleep(.1)
        return self._refresh(run_id, command, actor)

    def _create_once(self, run_id, command, actor):
        with self.harness.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, command, actor)
            run = self.harness._load(db, run_id)
            if run['engine']['execution_id']:
                return
            if run['engine']['submission'] != 'intent':
                # A lost create response is not an idempotent retry opportunity.
                raise Rejected('native create outcome unresolved; reconciliation required')
            run['engine']['submission'] = 'inflight'
            self._save(db, run)
        execution = self._adapter(run).create(run)
        with self.harness.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, command, actor)
            run = self.harness._load(db, run_id)
            run['engine'].update(execution_id=execution['id'], submission='bound',
                                 native_state=execution['state']['current'])
            self._save(db, run)

    def _refresh(self, run_id, command, actor):
        with self.harness.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, command, actor, False)
            run = self.harness._load(db, run_id)
            if not run['engine']['execution_id']:
                return run
            adapter = self._adapter(run)
            execution = adapter.get(run['engine']['execution_id'])
            if execution.get('inputs', {}).get('assignment_id') != run_id:
                raise Rejected('native assignment correlation mismatch')
            if execution['flowRevision'] != run['engine']['binding']['revision']:
                raise Rejected('native definition revision mismatch')
            native_state = execution['state']['current']
            run['engine']['native_state'] = native_state
            run['candidate'] = adapter.task_output(execution, 'candidate').get('value')
            if native_state == 'PAUSED':
                run['state'] = 'input-required'
            elif native_state == 'SUCCESS':
                returned_digest = adapter.task_output(execution, 'after').get('value')
                if not run['acceptance'] or returned_digest != run['acceptance']['sha256']:
                    run['state'] = 'failed'
                    run['failure'] = 'native success lacks matching ledger acceptance'
                else:
                    run['state'] = 'completed'
                    run['accepted_output'] = run['acceptance']
            elif native_state in {'FAILED', 'WARNING'}:
                run['state'] = 'failed'
            elif native_state in {'KILLED', 'CANCELLED'}:
                run['state'] = 'canceled'
            else:
                run['state'] = 'working'
            self._save(db, run)
            return run

    def _review(self, run_id, command, actor):
        self._refresh(run_id, command, actor)
        with self.harness.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, command, actor)
            run = self.harness._load(db, run_id)
            if run['acceptance']:
                return
            if run['engine']['native_state'] != 'PAUSED':
                raise Rejected('native factory is not waiting for review')
            candidate = run['candidate']
            if candidate != 'candidate:' + run['brief']:
                raise Rejected('evaluator fixture rejected native candidate')
            execution = self._adapter(run).get(run['engine']['execution_id'])
            gates = [t for t in execution.get('taskRunList', []) if t['taskId'] == 'director_gate']
            if len(gates) != 1 or gates[0]['state']['current'] != 'PAUSED':
                raise Rejected('expected one native Director Pause')
            run['acceptance'] = {'text': candidate, 'sha256': hashlib.sha256(candidate.encode()).hexdigest(),
                'reviewer': 'fixed-native-candidate-evaluator', 'definition': run['definition'],
                'execution_id': run['engine']['execution_id'], 'pause_task_run_id': gates[0]['id']}
            self._save(db, run)

    def _resume(self, run_id, command, actor):
        with self.harness.connect() as db:
            # Single local owner serializes supported-path commands. This is a
            # bounded spike, not a high-throughput network transaction design.
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, command, actor)
            run = self.harness._load(db, run_id)
            if not run['acceptance']:
                raise Rejected('accepted artifact required before native resume')
            adapter = self._adapter(run)
            execution = adapter.get(run['engine']['execution_id'])
            if execution['state']['current'] == 'PAUSED':
                gate = next(t for t in execution['taskRunList'] if t['taskId'] == 'director_gate')
                if gate['id'] != run['acceptance']['pause_task_run_id']:
                    raise Rejected('stale decision for a different native Pause')
                response = adapter.resume(execution['id'], run['acceptance']['sha256'])
                if response.status_code not in {200, 409}:
                    response.raise_for_status()
                run['engine']['resume_requests'] += 1
                run['engine']['resume_http_status'] = response.status_code
                self._save(db, run)
