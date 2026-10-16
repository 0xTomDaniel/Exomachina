"""Reusable A2A adapter for both configured roles; local test credentials only."""
from __future__ import annotations

import argparse
import json
from contextvars import ContextVar
from pathlib import Path
from uuid import uuid4

import uvicorn
from fastapi.responses import JSONResponse
from a2a.server.agent_execution import AgentExecutor
from a2a.server.apps import A2AFastAPIApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import TaskStore
from a2a.types import AgentCard, AgentCapabilities, AgentSkill, Artifact, DataPart, Message, Part, Task, TaskStatus

from harness import Harness, Rejected


ACTOR = ContextVar('verified_fixture_actor', default='unauthenticated')


class LedgerTaskStore(TaskStore):
    """A2A tasks are projections, not a second source of execution authority."""

    def __init__(self, harness):
        self.harness = harness

    async def get(self, task_id, context=None):
        record = self.harness.task_record(task_id)
        if record is None:
            return None
        run, context_id = record
        if self.harness.kestra_config:
            # Native A2A task polling reconciles engine state without a model turn.
            run = self.harness.command({'op': 'inspect', 'run_id': run['id']}, actor='observer')
        artifacts = None
        if run['state'] == 'completed' and run['accepted_output']:
            artifacts = [Artifact(artifact_id=run['accepted_output']['sha256'],
                                  parts=[Part(root=DataPart(data=run['accepted_output']))])]
        return Task(id=task_id, context_id=context_id, status=TaskStatus(state=run['state']),
                    artifacts=artifacts, metadata={'run_id': run['id'], 'owner': run['owner'],
                    'organization': run['organization'], 'definition': run['definition'],
                    'child': run.get('child'),
                    'engine': ({key: run['engine'].get(key) for key in
                        ('kind', 'execution_id', 'native_state', 'resume_requests')}
                        if run.get('engine') else None),
                    'acceptance_sha256': (run.get('acceptance') or {}).get('sha256')})

    async def save(self, task, context=None):
        # The command committed the assignment and delivery alias atomically.
        # DefaultRequestHandler cannot turn a chat completion into factory success.
        if self.harness.task_record(task.id) is None:
            raise Rejected('task has no authoritative factory record')

    async def delete(self, task_id, context=None):
        raise NotImplementedError('Deletion is outside S2')


class HarnessExecutor(AgentExecutor):
    def __init__(self, harness, store):
        self.harness = harness
        self.store = store

    async def execute(self, context, event_queue):
        try:
            parts = context.message.parts
            command = next((part.root.data for part in parts if isinstance(part.root, DataPart)), None)
            if command is None:
                command = json.loads(context.get_user_input())
            result = await self.harness.invoke(command, actor=ACTOR.get(),
                         delivery=(context.task_id, context.context_id))
            if 'error' in result or 'id' not in result:
                await event_queue.enqueue_event(Message(message_id=str(uuid4()), role='agent',
                    parts=[Part(root=DataPart(data=result))]))
            else:
                await event_queue.enqueue_event(await self.store.get(context.task_id))
        except Rejected as exc:
            await event_queue.enqueue_event(Message(message_id=str(uuid4()), role='agent',
                parts=[Part(root=DataPart(data={'error': str(exc)}))]))

    async def cancel(self, context, event_queue):
        run, _ = self.harness.task_record(context.task_id)
        self.harness.command({'op': 'cancel', 'key': 'a2a-cancel:' + context.task_id,
                              'run_id': run['id']}, actor=ACTOR.get())
        await event_queue.enqueue_event(await self.store.get(context.task_id))


def create_app(state: Path, role: str, port: int):
    harness = Harness(state, role)
    store = LedgerTaskStore(harness)
    card = AgentCard(name=f'Exomachina S2 {role}', description='Fixture-backed harness proof',
        url=f'http://127.0.0.1:{port}/', version='0.0.1', protocol_version='0.3.0',
        default_input_modes=['application/json'], default_output_modes=['application/json'],
        capabilities=AgentCapabilities(streaming=False),
        skills=[AgentSkill(id=role, name=role, description='S2 fixed-checkpoint fixture', tags=['s2', 'fixture'])],
        security_schemes={'fixtureBearer': {'type': 'http', 'scheme': 'bearer'}}, security=[{'fixtureBearer': []}])
    handler = DefaultRequestHandler(HarnessExecutor(harness, store), store)
    app = A2AFastAPIApplication(card, handler).build()

    @app.middleware('http')
    async def fixture_auth(request, call_next):
        if request.url.path in {'/health', '/.well-known/agent-card.json'}:
            return await call_next(request)
        actors = {'Bearer director-test-token': 'director', 'Bearer observer-test-token': 'observer'}
        actor = actors.get(request.headers.get('authorization'))
        if actor is None:
            return JSONResponse({'error': 'fixture authentication required'}, status_code=401)
        token = ACTOR.set(actor)
        try:
            return await call_next(request)
        finally:
            ACTOR.reset(token)

    @app.get('/health')
    def health():
        return {'identity': harness.identity, 'incarnation': harness.incarnation,
                'role': role, 'organization': harness.organization}

    return app


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--role', choices=['factory', 'capability'], required=True)
    parser.add_argument('--port', type=int, required=True)
    args = parser.parse_args()
    uvicorn.run(create_app(args.state, args.role, args.port), host='127.0.0.1', port=args.port,
                log_level='warning')
