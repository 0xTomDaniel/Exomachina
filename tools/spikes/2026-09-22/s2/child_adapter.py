"""Bounded reusable A2A child adapter; explicit reconciliation, no scheduler.

Its credentials and engine are fixtures. This exercises correlation and basic
cancel observation, not a production outbox, deadline, budget, or remote fencing.
"""
from uuid import uuid4

import httpx


class ChildAdapter:
    def __init__(self, harness, actor):
        self.harness = harness
        self.actor = actor

    def command(self, body):
        return self.harness.command(body, actor=self.actor)

    @staticmethod
    def rpc(url, method, params):
        result = httpx.post(url + '/', timeout=20,
            headers={'Authorization': 'Bearer director-test-token'},
            json={'jsonrpc': '2.0', 'id': str(uuid4()), 'method': method, 'params': params})
        result.raise_for_status()
        body = result.json()
        if 'error' in body:
            raise RuntimeError(body['error'])
        return body['result']

    def execute(self, request):
        run = self.command({'op': 'inspect', 'run_id': request['run_id']})
        base = self.harness.identity + ':' + run['id']
        self.command({'op': 'authorize_child', 'key': base + ':authorize:' + str(uuid4()),
                      'run_id': run['id']})
        if request['op'] == 'child_start':
            if not run.get('child'):
                run = self.command({'op': 'prepare_child', 'key': base + ':prepare', 'run_id': run['id'],
                    'child': {'url': request['url'], 'key': base + ':child-assignment'}})
            child = run['child']
            if not child.get('task_id'):
                task = self.rpc(child['url'], 'message/send', {'message': {'role': 'user',
                    'messageId': str(uuid4()), 'parts': [{'kind': 'data', 'data': {
                        'op': 'start', 'key': child['key'], 'brief': 'child of ' + run['id']}}]}})
                run = self.command({'op': 'link_child', 'key': base + ':link:' + task['id'],
                    'run_id': run['id'], 'task_id': task['id'], 'child_run_id': task['metadata']['run_id']})
        child = run['child']
        if request['op'] == 'child_cancel':
            self.command({'op': 'request_cancel', 'key': base + ':request-cancel', 'run_id': run['id']})
            task = self.rpc(child['url'], 'tasks/get', {'id': child['task_id']})
            if task['status']['state'] not in {'completed', 'canceled', 'failed'}:
                self.rpc(child['url'], 'tasks/cancel', {'id': child['task_id']})
        task = self.rpc(child['url'], 'tasks/get', {'id': child['task_id']})
        output = task['artifacts'][0]['parts'][0]['data'] if task.get('artifacts') else None
        run = self.command({'op': 'observe_child', 'key': base + ':observe:' + str(uuid4()),
            'run_id': run['id'], 'state': task['status']['state'], 'accepted_output': output})
        if request['op'] == 'child_cancel' and run['state'] != 'canceled':
            run = self.command({'op': 'cancel', 'key': base + ':canceled', 'run_id': run['id']})
        return run
