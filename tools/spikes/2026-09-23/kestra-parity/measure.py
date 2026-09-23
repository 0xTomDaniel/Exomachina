"""Bounded resource driver. It is not part of the frozen graph executor inventory."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from api import KestraAPI

ROOT = Path(__file__).resolve().parents[4]
STATE = Path('/tmp/exomachina-kestra-parity-state')
EVIDENCE = Path(__file__).resolve().parent / 'evidence'
S2 = ROOT / 'tools/spikes/2026-09-22/s2'
COMMON = ROOT / 'tools/spikes/2026-09-22/decision-round/common'
ARBITRATION = ROOT / 'tools/spikes/2026-09-22/arbitration/common'
sys.path.insert(0, str(ROOT / 'tools/spikes/2026-09-22/arbitration/wait-scaling'))
from footprint_probe import sample  # noqa: E402


def save(name, value):
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / name).write_text(json.dumps(value, indent=2) + '\n')


def find_engine_pids():
    pg = int((STATE / 'postgres/postmaster.pid').read_text().splitlines()[0])
    rows = subprocess.check_output(['ps', '-axo', 'pid=,ppid=,command='], text=True)
    matches = []
    for line in rows.splitlines():
        pieces = line.strip().split(None, 2)
        if len(pieces) == 3 and str(STATE / 'config.yml') in pieces[2] and 'kestra-2.0.3' in pieces[2]:
            matches.append((int(pieces[0]), int(pieces[1])))
    if len(matches) != 1:
        raise RuntimeError(f'expected exactly one Kestra process, found {matches}')
    java, launcher = matches[0]
    return {java: 'kestra_jvm', pg: 'postgres_main', launcher: 'kestra_launcher'}


def wait_health(port, process):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f'fixture process {port} exited {process.returncode}')
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{port}/health', timeout=.5) as response:
                if response.status == 200:
                    return
        except Exception:
            pass
        time.sleep(.1)
    raise RuntimeError(f'fixture port {port} not ready')


def start_services():
    python = str(S2 / '.venv/bin/python')
    services = [
        ('capability_a', COMMON / 'harness_server.py', 28644, ['--role', 'capability']),
        ('capability_b', COMMON / 'harness_server.py', 28645, ['--role', 'capability']),
        ('quality', ARBITRATION / 'quality_server.py', 28646, []),
        ('release_participating', ARBITRATION / 'release_server.py', 28647, ['--mode', 'participating']),
        ('release_opaque', ARBITRATION / 'release_server.py', 28648, ['--mode', 'opaque']),
        ('director', S2 / 'server.py', 28649, ['--role', 'factory']),
    ]
    password = (STATE / 'http.pass').read_text().strip()
    auth_file = STATE / 'auth.json'
    auth_file.write_text(json.dumps({'username': 'parity@example.invalid', 'password': password}))
    auth_file.chmod(0o600)
    director_state = STATE / 'fixture-director'
    director_state.mkdir(exist_ok=True)
    (director_state / 'kestra.json').write_text(json.dumps({
        'url': 'http://127.0.0.1:28643', 'auth_file': str(auth_file),
        'namespace': 'exomachina.kestra_parity', 'flow_id': 'parity_parent', 'revision': 3,
    }))
    children = []
    for name, source, port, extra in services:
        state = director_state if name == 'director' else STATE / ('fixture-' + name)
        state.mkdir(exist_ok=True)
        log = (STATE / (name + '.log')).open('w')
        process = subprocess.Popen([python, str(source), '--state', str(state), '--port', str(port), *extra],
                                   stdout=log, stderr=log)
        log.close()
        children.append((name, port, process))
    for name, port, process in children:
        wait_health(port, process)
    return children


def child_runs(client):
    result = client.call('GET', '/executions?namespace=exomachina.kestra_parity&flowId=parity_child&size=100')
    if result['status'] != 200:
        raise RuntimeError(result)
    return result['body']['results']


def pause_count(client, baseline_ids, expected, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        paused = [run for run in child_runs(client)
                  if run['id'] not in baseline_ids and run['flowRevision'] == 3
                  and run['state']['current'] == 'PAUSED']
        if len(paused) >= expected:
            return paused
        time.sleep(.4)
    raise RuntimeError(f'only {len(paused)}/{expected} new child waits')


def main():
    client = KestraAPI(28643, STATE / 'api.netrc')
    roots = find_engine_pids()
    children = start_services()
    roles = {os.getpid(): 'measurement_driver', **{p.pid: name for name, _, p in children}}
    started = []
    try:
        baseline_ids = {run['id'] for run in child_runs(client)}
        snapshots = {}
        snapshots['0'] = sample(os.getpid(), STATE, roles, roots)
        for stage, count in [('2', 2), ('10', 8)]:
            for _ in range(count):
                result = client.execute('exomachina.kestra_parity', 'parity_parent', {'outcome': 'exhausted'}, 3)
                if result['status'] != 200:
                    raise RuntimeError(result)
                started.append(result['body']['id'])
            paused = pause_count(client, baseline_ids, len(started))
            time.sleep(2)
            snapshots[stage] = sample(os.getpid(), STATE, roles, roots)
            snapshots[stage]['parent_ids'] = started[:]
            snapshots[stage]['child_ids'] = sorted(run['id'] for run in paused)
        save('footprint.json', snapshots)
        print(json.dumps({stage: {'bytes': row['physical_footprint_bytes_deduplicated'],
                                  'pids': row['pid_count'], 'waits': len(row.get('child_ids', []))}
                          for stage, row in snapshots.items()}, indent=2))
        for run in pause_count(client, baseline_ids, 10):
            result = client.resume(run['id'], {'abort': 'true'})
            if result['status'] != 200:
                raise RuntimeError(result)
        finals = [client.wait(execution_id, 'SUCCESS', timeout=90) for execution_id in started]
        save('footprint-completed.json', [{'id': started[i], 'state': row['body']['state']['current']}
                                          for i, row in enumerate(finals)])
    finally:
        for _, _, process in reversed(children):
            if process.poll() is None:
                process.terminate()
        for _, _, process in reversed(children):
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


if __name__ == '__main__':
    main()
