"""External Kestra 2.0.3 HTTP Adapter; contains no graph scheduler."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx


class KestraAdapter:
    def __init__(self, config: dict[str, Any]):
        self.config = config

    def request(self, method: str, path: str, **kwargs) -> httpx.Response:
        auth = json.loads(Path(self.config['auth_file']).read_text())
        with httpx.Client(base_url=self.config['url'].rstrip('/') + '/api/v1/main',
                          auth=(auth['username'], auth['password']), timeout=20) as client:
            return client.request(method, path, **kwargs)

    def create(self, assignment: dict[str, Any]) -> dict[str, Any]:
        response = self.request('POST', f"/executions/{self.config['namespace']}/{self.config['flow_id']}",
            params=[('revision', str(self.config['revision'])),
                    ('labels', 'exomachina-assignment:' + assignment['id']),
                    ('labels', 'exomachina-instance:' + assignment['owner'])],
            files={'assignment_id': (None, assignment['id']), 'brief': (None, assignment['brief'])})
        response.raise_for_status()
        return response.json()

    def get(self, execution_id: str) -> dict[str, Any]:
        response = self.request('GET', '/executions/' + execution_id)
        response.raise_for_status()
        return response.json()

    def resume(self, execution_id: str, acceptance_sha256: str) -> httpx.Response:
        return self.request('POST', f'/executions/{execution_id}/actions/resume',
                            files={'acceptance_sha256': (None, acceptance_sha256)})

    def task_output(self, execution: dict[str, Any], task_id: str) -> dict[str, Any]:
        tasks = [task for task in execution.get('taskRunList', [])
                 if task['taskId'] == task_id and task['state']['current'] == 'SUCCESS']
        if not tasks:
            return {}
        if len(tasks) != 1:
            raise ValueError('Bounded S2 contract expects one task run for each task')
        response = self.request('GET', f"/outputs/tasks/{execution['id']}/{tasks[0]['id']}")
        response.raise_for_status()
        return response.json()
