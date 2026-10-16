"""Native Kestra API probe. Requires S0 clearance and the isolated S3 server."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EVIDENCE = ROOT / 'evidence'
BASE = 'http://127.0.0.1:28084/api/v1/main'
AUTH_HEADER = ''
TRANSCRIPT = []


def save(name: str, value: object) -> None:
    (EVIDENCE / (name + '.json')).write_text(json.dumps(value, indent=2) + '\n')


def request(method: str, path: str, data: bytes | None = None,
            content_type: str | None = None, label: str | None = None):
    headers = {'Authorization': AUTH_HEADER, 'Accept': 'application/json'}
    if content_type:
        headers['Content-Type'] = content_type
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            status, raw = response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        status, raw = error.code, error.read().decode()
    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        result = {'raw': raw}
    TRANSCRIPT.append({'method': method, 'path': path, 'status': status, 'label': label})
    if label:
        save(label, {'status': status, 'body': result})
    save('api-transcript', TRANSCRIPT)
    if status >= 400:
        raise RuntimeError(f'{method} {path}: HTTP {status}: {result}')
    return result


def multipart(fields: dict | None = None) -> tuple[bytes, str]:
    boundary = 's3-native-probe-boundary'
    body = b''
    for key, value in (fields or {}).items():
        body += (f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n').encode()
    body += f'--{boundary}--\r\n'.encode()
    return body, 'multipart/form-data; boundary=' + boundary


def yaml_call(method: str, path: str, fixture: str, label: str):
    return request(method, path, (ROOT / 'fixtures' / fixture).read_bytes(), 'application/x-yaml', label)


def execute(flow: str, label: str) -> str:
    body, kind = multipart()
    result = request('POST', '/executions/exomachina.s3/' + flow, body, kind, label)
    return result['id']


def resume(execution: str, label: str, fields: dict | None = None):
    body, kind = multipart(fields)
    return request('POST', '/executions/' + execution + '/actions/resume', body, kind, label)


def wait_for(execution: str, state: str, label: str, pause_task: str | None = None):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        result = request('GET', '/executions/' + execution)
        observed = result['state']['current']
        has_pause = pause_task is None or any(task['taskId'] == pause_task and task['state']['current'] == 'PAUSED'
                                             for task in result.get('taskRunList', []))
        if observed == state and has_pause:
            save(label, result)
            return result
        if observed in {'FAILED', 'KILLED', 'CANCELLED'}:
            save(label + '-unexpected-terminal', result)
            raise RuntimeError(f'{execution}: expected {state}, observed {observed}')
        time.sleep(0.25)
    save(label + '-timeout', result)
    raise RuntimeError(f'{execution}: timed out waiting for {state}/{pause_task}')


def main() -> None:
    # Server readiness is read-only. The caller has already received S0 clearance.
    deadline = time.monotonic() + 40
    while True:
        try:
            request('GET', '/flows/exomachina.s3', label='readiness')
            break
        except (urllib.error.URLError, RuntimeError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.5)

    for fixture in ['child-v1.yaml', 'child-v2.yaml', 'parent-pinned-v1.yaml', 'parent-pinned-v2.yaml',
                    'parent-latest.yaml', 'reference-v1.yaml', 'reference-v2.yaml', 'review-bypass.yaml']:
        yaml_call('POST', '/flows/validate', fixture, 'validate-' + fixture[:-5])

    child1 = yaml_call('POST', '/flows', 'child-v1.yaml', 'publish-child-v1')
    yaml_call('POST', '/flows', 'parent-pinned-v1.yaml', 'publish-parent-v1')
    yaml_call('POST', '/flows', 'parent-latest.yaml', 'publish-parent-latest')
    yaml_call('POST', '/flows', 'reference-v1.yaml', 'publish-reference-v1')
    yaml_call('POST', '/flows', 'review-bypass.yaml', 'publish-review-bypass')

    pinned = execute('s3_parent_pinned', 'admit-pinned-v1')
    latest = execute('s3_parent_latest', 'admit-latest-v1')
    reference = execute('s3_reference', 'admit-reference-v1')
    for key, execution in [('pinned', pinned), ('latest', latest), ('reference', reference)]:
        wait_for(execution, 'PAUSED', key + '-publication-paused', 'publication_wait')

    # Intentionally submits revision: 1 with changed content; observe actual assigned revision.
    child2 = yaml_call('PUT', '/flows/exomachina.s3/s3_child', 'child-v2.yaml', 'attempt-child-revision-1-mutation')
    parent2 = yaml_call('PUT', '/flows/exomachina.s3/s3_parent_pinned', 'parent-pinned-v2.yaml', 'publish-parent-v2')
    yaml_call('PUT', '/flows/exomachina.s3/s3_reference', 'reference-v2.yaml', 'publish-reference-v2')
    retained_child1 = request('GET', '/flows/exomachina.s3/s3_child?revision=1', label='retained-child-v1')
    request('GET', '/flows/exomachina.s3/s3_parent_pinned?revision=1', label='retained-parent-v1')

    resume(pinned, 'resume-pinned-v1')
    pinned_result = wait_for(pinned, 'SUCCESS', 'pinned-v1-result')
    resume(latest, 'resume-latest-v1')
    latest_result = wait_for(latest, 'SUCCESS', 'latest-v1-result')
    new = execute('s3_parent_pinned', 'admit-pinned-v2')
    wait_for(new, 'PAUSED', 'pinned-v2-paused', 'publication_wait')
    resume(new, 'resume-pinned-v2')
    new_result = wait_for(new, 'SUCCESS', 'pinned-v2-result')

    resume(reference, 'resume-reference-publication')
    reference_wait = wait_for(reference, 'PAUSED', 'reference-director-paused', 'director_wait')
    resume(reference, 'resume-reference-director', {'approved': 'true'})
    reference_result = wait_for(reference, 'SUCCESS', 'reference-result')
    finalize()


def finalize() -> None:
    global TRANSCRIPT
    TRANSCRIPT = json.loads((EVIDENCE / 'api-transcript.json').read_text())
    def load(name: str):
        return json.loads((EVIDENCE / (name + '.json')).read_text())
    child1 = load('publish-child-v1')['body']
    child2 = load('attempt-child-revision-1-mutation')['body']
    parent2 = load('publish-parent-v2')['body']
    retained_child1 = load('retained-child-v1')['body']
    pinned_result = load('pinned-v1-result')
    latest_result = load('latest-v1-result')
    new_result = load('pinned-v2-result')
    reference_result = load('reference-result')
    reference_wait = load('reference-director-paused')
    observed = {}
    for key, execution in [('pinned-v1', pinned_result), ('latest-v1', latest_result), ('pinned-v2', new_result)]:
        output = request('POST', '/executions/' + execution['id'] + '/actions/eval',
                         b'{{ outputs.observed.value }}', 'text/plain', key + '-observed-output')
        observed[key] = output['result']
    for task in ['research_a', 'research_b']:
        output = request('POST', '/executions/' + reference_result['id'] + '/actions/eval',
                         ('{{ outputs.' + task + '.executionId }}').encode(), 'text/plain', 'reference-' + task + '-id')
        request('GET', '/executions/' + output['result'], label='reference-' + task)

    assert child1['revision'] == 1 and child2['revision'] == 2
    assert retained_child1['revision'] == 1 and retained_child1['tasks'][0]['format'] == 'child-v1'
    assert observed == {'pinned-v1': 'child-v1', 'latest-v1': 'child-v2', 'pinned-v2': 'child-v2'}
    assert pinned_result['flowRevision'] == 1 and latest_result['flowRevision'] == 1 and new_result['flowRevision'] == 2
    assert reference_result['state']['current'] == 'SUCCESS'
    assert sum(task['taskId'] == 'repair' for task in reference_result['taskRunList']) == 1

    summary = {
        'candidate': 'Kestra OSS v2.0.3', 'source_commit': '269e8d0d01c27f6117a667758112d2b8d77cc4a3',
        'server': BASE, 'postgres': 'Operator-supplied isolated PostgreSQL; see run configuration',
        'child_initial_revision': child1.get('revision'), 'changed_child_assigned_revision': child2.get('revision'),
        'parent_new_revision': parent2.get('revision'),
        'retained_child_revision': retained_child1.get('revision'),
        'pinned_parent_v1_flow_revision': pinned_result.get('flowRevision'),
        'pinned_parent_v1_observed': observed['pinned-v1'],
        'unpinned_parent_v1_flow_revision': latest_result.get('flowRevision'),
        'unpinned_parent_v1_observed': observed['latest-v1'],
        'new_parent_flow_revision': new_result.get('flowRevision'),
        'new_parent_observed': observed['pinned-v2'],
        'reference_state': reference_result['state']['current'],
        'reference_tasks': [{'task': task['taskId'], 'state': task['state']['current']}
                            for task in reference_result['taskRunList']],
        'reference_director_pause_observed': reference_wait['state']['current'],
        'bypass_published': True,
        'limits': ['Synthetic core Return tasks, no real agents, Quality authorization or delivery effect',
                   'Native engine API only; no Factory Module policy gate',
                   'No workload/capacity claim or restart in S3', 'No plugin update or capability revision test'],
    }
    save('runtime-summary', summary)
    inventory = [{'file': str(path.relative_to(ROOT)), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
                 for path in sorted((ROOT / 'fixtures').glob('*.yaml')) if path.is_file()]
    save('artifact-inventory', inventory)
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default=BASE, help='API base for an isolated local Kestra v2.0.3 instance')
    parser.add_argument('--auth-file', type=Path, required=True, help='External JSON file containing username and password')
    parser.add_argument('--evidence-dir', type=Path, required=True, help='Output directory outside this repository')
    parser.add_argument('--finish', action='store_true', help='Only re-read previously recorded executions and their outputs')
    args = parser.parse_args()
    repository = next((p for p in ROOT.parents if (p / '.git').exists()), ROOT)
    evidence_dir = args.evidence_dir.expanduser().resolve()
    auth_file = args.auth_file.expanduser().resolve()
    if evidence_dir.is_relative_to(repository) or auth_file.is_relative_to(repository):
        parser.error('Credentials and generated evidence must be outside the repository')
    if not args.base_url.startswith(('http://127.0.0.1:', 'http://localhost:')):
        parser.error('This bounded probe requires an isolated loopback HTTP endpoint')
    auth = json.loads(auth_file.read_text())
    if not all(isinstance(auth.get(key), str) and auth[key] for key in ('username', 'password')):
        parser.error('Credential file must contain nonempty username and password strings')
    AUTH_HEADER = 'Basic ' + base64.b64encode((auth['username'] + ':' + auth['password']).encode()).decode()
    EVIDENCE = evidence_dir
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    BASE = args.base_url.rstrip('/')
    try:
        if args.finish:
            finalize()
        else:
            main()
    except Exception as error:
        save('probe-error', {'error_type': type(error).__name__, 'message': str(error)})
        raise
