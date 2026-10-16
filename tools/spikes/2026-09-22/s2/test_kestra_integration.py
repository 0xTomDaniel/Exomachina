"""Live contract proof. Mutates only a unique S2 flow/run on parent's S1 server."""
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from test_a2a_process import launch, unused_port, rpc, send, OBSERVER


ROOT = Path(__file__).parent
pytestmark = pytest.mark.skipif(os.getenv('EXOMACHINA_S2_KESTRA_LIVE') != '1', reason='Explicit live Kestra opt-in required')


def test_kestra_factory_native_pause_restart_accept_resume(tmp_path):
    credentials = json.loads(Path('/tmp/exomachina-spikes/s1/auth.json').read_text())
    native = httpx.Client(base_url='http://127.0.0.1:28081/api/v1/main',
        auth=(credentials['username'], credentials['password']), timeout=20)
    flow_id = 'exomachina_s2_factory_' + uuid4().hex[:10]
    flow_source = (ROOT / 'kestra-flow.yaml').read_text().replace('__FLOW_ID__', flow_id)
    (tmp_path / 'published-flow.yaml').write_text(flow_source)
    publication = native.post('/flows', content=flow_source, headers={'Content-Type': 'application/x-yaml'})
    publication.raise_for_status()
    flow = publication.json()
    state = tmp_path / 'director'
    state.mkdir()
    config = {'url': 'http://127.0.0.1:28081', 'auth_file': '/tmp/exomachina-spikes/s1/auth.json',
              'namespace': 'exomachina.spikes', 'flow_id': flow_id, 'revision': flow['revision']}
    (state / 'kestra.json').write_text(json.dumps(config, indent=2))
    processes = []
    evidence = {'flow': flow, 'flow_id': flow_id, 'config': config}
    try:
        capability, cap_url, cap_info = launch(tmp_path / 'capability', 'capability', unused_port())
        processes.append(capability)
        cap_task = send(cap_url, {'op': 'start', 'key': 'independent-capability', 'brief': 'unchanged'})
        port = unused_port()
        director, url, identity1 = launch(state, 'factory', port)
        processes.append(director)
        start = {'op': 'start', 'key': 'native-assignment', 'brief': 'verified native candidate'}
        initial = send(url, start)
        evidence['initial'] = initial
        assert initial['metadata']['engine']['kind'] == 'kestra'
        run_id = initial['metadata']['run_id']
        execution_id = initial['metadata']['engine']['execution_id']
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            waiting = rpc(url, 'tasks/get', {'id': initial['id']})
            if waiting['status']['state'] == 'input-required':
                break
            time.sleep(.1)
        assert waiting['status']['state'] == 'input-required'
        paused = native.get('/executions/' + execution_id).json()
        assert paused['state']['current'] == 'PAUSED'
        assert paused['flowRevision'] == flow['revision']
        assert paused['inputs']['assignment_id'] == run_id
        with ThreadPoolExecutor(max_workers=4) as pool:
            duplicate_starts = list(pool.map(lambda _: send(url, start), range(8)))
        assert {t['metadata']['engine']['execution_id'] for t in duplicate_starts} == {execution_id}
        director.kill()
        director.wait(timeout=5)
        successor, url, identity2 = launch(state, 'factory', port)
        processes.append(successor)
        restored = rpc(url, 'tasks/get', {'id': initial['id']})
        assert restored['metadata']['engine']['execution_id'] == execution_id
        assert restored['status']['state'] == 'input-required'
        assert identity2['identity'] == identity1['identity']
        assert identity2['incarnation'] == identity1['incarnation'] + 1
        no_review = send(url, {'op': 'decide', 'key': 'without-review', 'run_id': run_id})
        assert no_review['parts'][0]['data']['error'] == 'accepted artifact required before native resume'
        denied = send(url, {'op': 'review', 'key': 'observer-review', 'run_id': run_id}, headers=OBSERVER)
        assert denied['parts'][0]['data']['error'] == 'unauthorized command'
        reviewed = send(url, {'op': 'review', 'key': 'review-1', 'run_id': run_id})
        evidence['review_response'] = reviewed
        assert reviewed['status']['state'] == 'input-required'
        digest = reviewed['metadata']['acceptance_sha256']
        assert digest and not reviewed.get('artifacts')
        assert native.get('/executions/' + execution_id).json()['state']['current'] == 'PAUSED'
        # Acceptance survives independently before any native resume is attempted.
        successor.kill()
        successor.wait(timeout=5)
        final_owner, url, identity3 = launch(state, 'factory', port)
        processes.append(final_owner)
        accepted_restored = rpc(url, 'tasks/get', {'id': initial['id']})
        assert accepted_restored['metadata']['acceptance_sha256'] == digest
        stale = send(url, {'op': 'decide', 'key': 'stale-decision', 'run_id': run_id,
                           'expected_incarnation': identity2['incarnation']})
        assert stale['parts'][0]['data']['error'] == 'stale incarnation'
        decision = {'op': 'decide', 'key': 'resume-1', 'run_id': run_id}
        with ThreadPoolExecutor(max_workers=4) as pool:
            responses = list(pool.map(lambda _: send(url, decision), range(8)))
        final = responses[0]
        assert all(t['status']['state'] == 'completed' for t in responses)
        assert final['artifacts'][0]['parts'][0]['data']['sha256'] == digest
        assert final['metadata']['engine']['resume_requests'] == 1
        executed = native.get('/executions/' + execution_id).json()
        assert executed['state']['current'] == 'SUCCESS'
        after = [t for t in executed['taskRunList'] if t['taskId'] == 'after']
        assert len(after) == 1 and after[0]['state']['current'] == 'SUCCESS'
        after_output = native.get('/outputs/tasks/' + execution_id + '/' + after[0]['id']).json()
        assert after_output['value'] == digest
        executions = native.get('/executions', params={'namespace': 'exomachina.spikes', 'flowId': flow_id}).json()
        assert executions['total'] == 1
        assert rpc(cap_url, 'tasks/get', {'id': cap_task['id']}) == cap_task
        assert httpx.get(cap_url + '/health').json() == cap_info
        assert identity3['identity'] != cap_info['identity']
        assert rpc(url, 'tasks/get', {'id': initial['id']})['status']['state'] == 'completed'
        evidence.update({'initial': initial, 'waiting': waiting, 'paused_native': paused,
            'restored': restored, 'reviewed': reviewed, 'acceptance_restored': accepted_restored,
            'identity_before': identity1, 'identity_restart': identity2, 'identity_after_acceptance': identity3,
            'separate_capability': cap_info, 'stale_rejection': stale, 'unauthorized_rejection': denied,
            'unreviewed_rejection': no_review, 'final': final, 'native_final': executed,
            'after_task_output': after_output,
            'execution_count': executions['total'], 'duplicate_start_deliveries': 8,
            'duplicate_resume_deliveries': 8, 'downstream_after_task_runs': len(after)})
    finally:
        (tmp_path / 'kestra-evidence.json').write_text(json.dumps(evidence, indent=2))
        native.close()
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
