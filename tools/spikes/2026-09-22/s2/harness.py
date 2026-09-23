"""S2 harness proof. The model and workflow engine are explicit fixtures.

The persistent identity, fencing, command deduplication and A2A projection are
owned application code. They are not capabilities supplied by Strands memory.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any
from uuid import uuid4

from strands import Agent, tool
from strands.models import Model
from strands.plugins import Plugin


class Rejected(Exception):
    pass


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


class ToolCallingModelFixture(Model):
    """Deterministic provider fixture exercising the real Strands tool loop."""

    def update_config(self, **model_config: Any) -> None:
        pass

    def get_config(self) -> dict[str, Any]:
        return {'model_id': 'deterministic-tool-fixture', 'context_window_limit': 16000}

    async def structured_output(self, *args: Any, **kwargs: Any):
        raise NotImplementedError('Not part of this spike')
        yield  # pragma: no cover

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        last = messages[-1]['content']
        yield {'messageStart': {'role': 'assistant'}}
        if 'toolResult' not in last[0]:
            command = json.loads(last[0]['text'])
            yield {'contentBlockStart': {'start': {'toolUse': {
                'toolUseId': 'fixture-call', 'name': tool_specs[0]['name']}}}}
            yield {'contentBlockDelta': {'delta': {'toolUse': {
                'input': canonical({'command_json': canonical(command)})}}}}
            yield {'contentBlockStop': {}}
            yield {'messageStop': {'stopReason': 'tool_use'}}
        else:
            blocks = last[0]['toolResult']['content']
            value = blocks[0].get('text')
            if value is None:
                value = canonical(blocks[0]['json'])
            yield {'contentBlockStart': {'start': {}}}
            yield {'contentBlockDelta': {'delta': {'text': value}}}
            yield {'contentBlockStop': {}}
            yield {'messageStop': {'stopReason': 'end_turn'}}


class FactoryPlugin(Plugin):
    name = 'exomachina-factory'

    def __init__(self, harness: Harness, actor: str, delivery: tuple[str, str] | None = None):
        self.harness = harness
        self.actor = actor
        self.delivery = delivery
        super().__init__()

    @tool
    def factory_command(self, command_json: str) -> str:
        """Perform an authorized factory/capability command.

        Args:
            command_json: JSON command supported by the durable factory interface.
        """
        try:
            command = json.loads(command_json)
            if command['op'].startswith('child_'):
                from child_adapter import ChildAdapter
                result = ChildAdapter(self.harness, self.actor).execute(command)
                if self.delivery:
                    self.harness.bind_task(self.delivery[0], result['id'], self.delivery[1])
                return canonical(result)
            return canonical(self.harness.command(command, actor=self.actor, delivery=self.delivery))
        except Rejected as exc:
            return canonical({'error': str(exc)})


class Harness:
    """One reusable package, configured as a capability or a factory Director.

    SQLite is the single authoritative local ledger. The engine fixture has one
    fixed checkpoint; it is intentionally not a scheduler or definition DSL.
    """

    def __init__(self, directory: Path, role: str, organization: str = 'org-fixture'):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        (self.directory / 'workspace').mkdir(exist_ok=True)
        self.path = self.directory / 'state.sqlite3'
        self.role = role
        self.organization = organization
        engine_file = self.directory / 'kestra.json'
        self.kestra_config = json.loads(engine_file.read_text()) if engine_file.exists() else None
        if self.kestra_config and role != 'factory':
            raise Rejected('Kestra binding requires factory role')
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, incarnation INTEGER NOT NULL,
                    role TEXT NOT NULL, organization TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, body TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS commands (
                    key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, result TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS aliases (
                    task_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, context_id TEXT NOT NULL);
            ''')
            db.execute('BEGIN IMMEDIATE')
            existing = db.execute('SELECT * FROM identity WHERE singleton=1').fetchone()
            if existing:
                if existing['role'] != role or existing['organization'] != organization:
                    raise Rejected('identity configuration mismatch')
                db.execute('UPDATE identity SET incarnation=incarnation+1 WHERE singleton=1')
            else:
                db.execute('INSERT INTO identity VALUES(1, ?, 1, ?, ?)',
                           (str(uuid4()), role, organization))
            current = db.execute('SELECT * FROM identity').fetchone()
            self.identity = current['id']
            self.incarnation = current['incarnation']
        config = {'identity': self.identity, 'role': role, 'organization': organization,
                  'engine': 'kestra' if self.kestra_config else 'fixed-checkpoint-fixture',
                  'model': 'tool-calling-fixture'}
        (self.directory / 'config.json').write_text(json.dumps(config, indent=2) + '\n')

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA synchronous=FULL')
        return db

    def _authorize(self, db, actor: str, mutation: bool = True) -> None:
        current = db.execute('SELECT incarnation FROM identity').fetchone()[0]
        if current != self.incarnation:
            raise Rejected('stale incarnation')
        if actor not in {'director', 'observer'} or (mutation and actor != 'director'):
            raise Rejected('unauthorized command')

    def command(self, command: dict[str, Any], actor: str = 'director',
                delivery: tuple[str, str] | None = None) -> dict[str, Any]:
        if self.kestra_config:
            from kestra_factory import KestraFactory
            return KestraFactory(self).execute(command, actor, delivery)
        op = command['op']
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, actor, op not in {'inspect', 'list'})
            if command.get('expected_incarnation', self.incarnation) != self.incarnation:
                raise Rejected('stale incarnation')
            if op == 'list':
                return {'runs': [json.loads(r[0]) for r in db.execute('SELECT body FROM runs')]}
            if op == 'inspect':
                result = self._load(db, command['run_id'])
                self._bind(db, result['id'], delivery)
                return result
            key = command.get('key')
            if not key:
                raise Rejected('mutation requires idempotency key')
            fingerprint = hashlib.sha256(canonical({'actor': actor, 'command': command}).encode()).hexdigest()
            cached = db.execute('SELECT * FROM commands WHERE key=?', (key,)).fetchone()
            if cached:
                if cached['fingerprint'] != fingerprint:
                    raise Rejected('idempotency key conflict')
                # Return current authoritative state, not the original snapshot.
                previous = json.loads(cached['result'])
                self._bind(db, previous['id'], delivery)
                return self._load(db, previous['id'])
            if op == 'start':
                run = {'id': str(uuid4()), 'owner': self.identity, 'organization': self.organization,
                       'definition': f'fixture.{self.role}@1', 'brief': command.get('brief', ''),
                       'state': 'input-required' if self.role == 'factory' else 'completed',
                       'accepted_output': None, 'created_incarnation': self.incarnation}
                if self.role == 'capability':
                    run['accepted_output'] = self._evaluate(run)
            else:
                run = self._load(db, command['run_id'])
                if op == 'decide':
                    if run['state'] != 'input-required':
                        raise Rejected('run is not waiting for a decision')
                    if run.get('child') and run['child'].get('state') != 'completed':
                        raise Rejected('child has not produced accepted output')
                    # Director authorizes continuation; evaluator owns acceptance.
                    run['accepted_output'] = self._evaluate(run)
                    run['state'] = 'completed'
                elif op == 'cancel':
                    if run['state'] in {'completed', 'canceled'}:
                        raise Rejected('run is terminal')
                    run['state'] = 'canceled'
                elif op == 'prepare_child':
                    if run.get('child') and run['child'] != command['child']:
                        raise Rejected('child already assigned')
                    run['child'] = command['child']
                elif op == 'link_child':
                    existing = run['child'].get('run_id')
                    if existing and existing != command['child_run_id']:
                        raise Rejected('child correlation conflict')
                    run['child'].setdefault('task_id', command['task_id'])
                    run['child']['run_id'] = command['child_run_id']
                elif op == 'observe_child':
                    run['child']['state'] = command['state']
                    run['child']['accepted_output'] = command.get('accepted_output')
                elif op == 'request_cancel':
                    run['cancel_requested'] = True
                elif op == 'authorize_child':
                    pass
                else:
                    raise Rejected('unsupported command')
            db.execute('INSERT OR REPLACE INTO runs VALUES(?, ?)', (run['id'], canonical(run)))
            db.execute('INSERT INTO commands VALUES(?, ?, ?)', (key, fingerprint, canonical(run)))
            self._bind(db, run['id'], delivery)
            return run

    @staticmethod
    def _bind(db, run_id: str, delivery: tuple[str, str] | None) -> None:
        if delivery:
            db.execute('INSERT OR IGNORE INTO aliases VALUES(?, ?, ?)', (delivery[0], run_id, delivery[1]))

    @staticmethod
    def _load(db, run_id: str) -> dict[str, Any]:
        row = db.execute('SELECT body FROM runs WHERE id=?', (run_id,)).fetchone()
        if not row:
            raise Rejected('unknown run')
        return json.loads(row[0])

    @staticmethod
    def _evaluate(run: dict[str, Any]) -> dict[str, Any]:
        """Independent evaluator fixture: never claim real output-quality proof."""
        text = 'fixture-result:' + run['brief']
        return {'text': text, 'sha256': hashlib.sha256(text.encode()).hexdigest(),
                'reviewer': 'fixed-evaluator-fixture', 'definition': run['definition']}

    async def invoke(self, command: dict[str, Any], actor: str = 'director',
                     delivery: tuple[str, str] | None = None) -> dict[str, Any]:
        # Fresh conversation deliberately proves ledger recovery without chat memory.
        agent = Agent(name=f'Exomachina {self.role}', model=ToolCallingModelFixture(),
                      plugins=[FactoryPlugin(self, actor, delivery)], callback_handler=None)
        result = await agent.invoke_async(canonical(command))
        return json.loads(str(result))

    def bind_task(self, task_id: str, run_id: str, context_id: str) -> None:
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, 'director')
            db.execute('INSERT OR IGNORE INTO aliases VALUES(?, ?, ?)', (task_id, run_id, context_id))

    def task_record(self, task_id: str) -> tuple[dict[str, Any], str] | None:
        with self.connect() as db:
            self._authorize(db, 'observer', False)
            alias = db.execute('SELECT * FROM aliases WHERE task_id=?', (task_id,)).fetchone()
            if alias is None:
                return None
            return self._load(db, alias['run_id']), alias['context_id']
