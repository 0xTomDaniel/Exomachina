import json
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import httpx


ROOT = Path(__file__).parent
DIRECTOR = {'Authorization': 'Bearer director-test-token'}
OBSERVER = {'Authorization': 'Bearer observer-test-token'}


def unused_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def launch(state, role, port):
    state.mkdir(parents=True, exist_ok=True)
    output = (state / f'server-{time.time_ns()}.log').open('w')
    process = subprocess.Popen([sys.executable, str(ROOT / 'server.py'), '--state', str(state),
                                '--role', role, '--port', str(port)], cwd=ROOT, stdout=output, stderr=output)
    output.close()
    url = f'http://127.0.0.1:{port}'
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError('server startup failed; see server log')
        try:
            response = httpx.get(url + '/health', timeout=.4)
            if response.status_code == 200:
                return process, url, response.json()
        except httpx.HTTPError:
            pass
        time.sleep(.05)
    process.kill()
    raise AssertionError('startup timeout')


def rpc(url, method, params, headers=DIRECTOR):
    response = httpx.post(url + '/', headers=headers, timeout=20,
                          json={'jsonrpc': '2.0', 'id': str(uuid4()), 'method': method, 'params': params})
    response.raise_for_status()
    data = response.json()
    assert 'error' not in data, data
    return data['result']


def send(url, command, task_id=None, headers=DIRECTOR):
    message = {'role': 'user', 'messageId': str(uuid4()), 'parts': [{'kind': 'data', 'data': command}]}
    if task_id:
        message['taskId'] = task_id
    return rpc(url, 'message/send', {'message': message}, headers)


def test_real_a2a_process_kill_restart_fencing_and_completion(tmp_path):
    processes = []
    try:
        cap, cap_url, cap_info = launch(tmp_path / 'capability', 'capability', unused_port())
        processes.append(cap)
        port = unused_port()
        first, url, info1 = launch(tmp_path / 'factory', 'factory', port)
        processes.append(first)
        card = httpx.get(url + '/.well-known/agent-card.json').json()
        assert card['protocolVersion'] == '0.3.0'
        assert httpx.post(url + '/', json={}).status_code == 401
        cap_task = send(cap_url, {'op': 'start', 'key': 'c1', 'brief': 'hello'})
        start = {'op': 'start', 'key': 'f1', 'brief': 'report'}
        task = send(url, start)
        assert task['status']['state'] == 'input-required'
        assert not task.get('artifacts')
        run_id = task['metadata']['run_id']
        denied = send(url, {'op': 'decide', 'key': 'bad', 'run_id': run_id}, headers=OBSERVER)
        assert denied['parts'][0]['data']['error'] == 'unauthorized command'
        with ThreadPoolExecutor(max_workers=8) as pool:
            duplicate_tasks = list(pool.map(lambda _: send(url, start), range(16)))
        assert {t['metadata']['run_id'] for t in duplicate_tasks} == {run_id}
        first.kill()
        first.wait(timeout=5)
        second, url, info2 = launch(tmp_path / 'factory', 'factory', port)
        processes.append(second)
        assert info2['identity'] == info1['identity']
        assert info2['incarnation'] == info1['incarnation'] + 1
        restored = rpc(url, 'tasks/get', {'id': task['id']})
        assert restored['status']['state'] == 'input-required'
        assert restored['metadata']['run_id'] == run_id
        stale = send(url, {'op': 'decide', 'key': 'stale', 'run_id': run_id,
                           'expected_incarnation': info1['incarnation']})
        assert stale['parts'][0]['data']['error'] == 'stale incarnation'
        completed = send(url, {'op': 'decide', 'key': 'good', 'run_id': run_id}, task['id'])
        assert completed['status']['state'] == 'completed'
        assert completed['artifacts'][0]['parts'][0]['data']['reviewer'] == 'fixed-evaluator-fixture'
        assert rpc(url, 'tasks/get', {'id': duplicate_tasks[0]['id']})['status']['state'] == 'completed'
        assert rpc(cap_url, 'tasks/get', {'id': cap_task['id']}) == cap_task
        assert httpx.get(cap_url + '/health').json() == cap_info
        # A still-running old process is fenced when another process takes ownership.
        third, successor_url, info3 = launch(tmp_path / 'factory', 'factory', unused_port())
        processes.append(third)
        stale_owner = send(url, {'op': 'start', 'key': 'must-not-run', 'brief': 'bad'})
        assert stale_owner['parts'][0]['data']['error'] == 'stale incarnation'
        assert info3['incarnation'] == info2['incarnation'] + 1
        assert send(successor_url, {'op': 'start', 'key': 'new-run', 'brief': 'next'})['kind'] == 'task'
        (tmp_path / 'evidence.json').write_text(json.dumps({
            'capability': cap_info, 'first': info1, 'restart': info2, 'successor': info3,
            'card': card, 'waiting': task, 'restored': restored, 'completed': completed,
            'duplicate_deliveries': len(duplicate_tasks), 'stale_rejection': stale,
            'stale_live_owner_rejection': stale_owner, 'unauthorized_rejection': denied,
        }, indent=2))
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)


def test_parent_child_over_a2a_with_durable_link_result_and_cancel(tmp_path):
    processes = []
    try:
        child_process, child_url, child_info = launch(tmp_path / 'child', 'factory', unused_port())
        processes.append(child_process)
        parent_port = unused_port()
        parent, parent_url, parent_info = launch(tmp_path / 'parent', 'factory', parent_port)
        processes.append(parent)
        task = send(parent_url, {'op': 'start', 'key': 'parent', 'brief': 'parent'})
        parent_id = task['metadata']['run_id']
        delegated = send(parent_url, {'op': 'child_start', 'run_id': parent_id, 'url': child_url})
        link = delegated['metadata']['child']
        repeated = send(parent_url, {'op': 'child_start', 'run_id': parent_id, 'url': child_url})
        assert repeated['metadata']['child']['run_id'] == link['run_id']
        parent.kill()
        parent.wait(timeout=5)
        restarted, parent_url, _ = launch(tmp_path / 'parent', 'factory', parent_port)
        processes.append(restarted)
        restored = rpc(parent_url, 'tasks/get', {'id': task['id']})
        assert restored['metadata']['child']['task_id'] == link['task_id']
        premature = send(parent_url, {'op': 'decide', 'key': 'premature', 'run_id': parent_id})
        assert premature['parts'][0]['data']['error'] == 'child has not produced accepted output'
        child_done = send(child_url, {'op': 'decide', 'key': 'child-accept', 'run_id': link['run_id']}, link['task_id'])
        observed = send(parent_url, {'op': 'child_get', 'run_id': parent_id})
        assert observed['metadata']['child']['accepted_output'] == child_done['artifacts'][0]['parts'][0]['data']
        assert send(parent_url, {'op': 'decide', 'key': 'parent-accept', 'run_id': parent_id})['status']['state'] == 'completed'
        second = send(parent_url, {'op': 'start', 'key': 'parent-cancel', 'brief': 'cancel'})
        second_id = second['metadata']['run_id']
        second = send(parent_url, {'op': 'child_start', 'run_id': second_id, 'url': child_url})
        canceled = send(parent_url, {'op': 'child_cancel', 'run_id': second_id})
        assert canceled['status']['state'] == 'canceled'
        child_canceled = rpc(child_url, 'tasks/get', {'id': second['metadata']['child']['task_id']})
        assert child_canceled['status']['state'] == 'canceled'
        assert child_info['identity'] != parent_info['identity']
        (tmp_path / 'evidence.json').write_text(json.dumps({'parent': parent_info, 'child': child_info,
            'delegated': delegated, 'restored': restored, 'child_output': child_done,
            'parent_observation': observed, 'parent_canceled': canceled, 'child_canceled': child_canceled}, indent=2))
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
