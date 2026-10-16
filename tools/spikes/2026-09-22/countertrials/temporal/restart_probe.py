#!/usr/bin/env python3
"""Crash/restart the isolated non-development Temporal+PostgreSQL helpers."""
import json
import os
import signal
import socket
import subprocess
import time
from pathlib import Path

ROOT = Path('/tmp/exomachina-countertrials/temporal')
STATE = ROOT / 'state'
RUNTIME = ROOT / 'runtime'
CLI = Path('/tmp/exomachina-temporal-evaluation/temporal')
OUTPUT = Path(__file__).with_name('restart-results.json')
BASE = [str(CLI), '--disable-config-env', '--disable-config-file', '--address', '127.0.0.1:27233', '--output', 'json']


def listening_pid(port):
    result = subprocess.run(['lsof', '-tiTCP:' + str(port), '-sTCP:LISTEN'], capture_output=True, text=True)
    pids = [int(s) for s in result.stdout.splitlines() if s.isdigit()]
    return pids[0] if pids else None


def port_open(port):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=.2):
            return True
    except OSError:
        return False


def until(check, seconds=45):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(.2)
    raise TimeoutError('condition not met')


def cli(*args, timeout=20):
    p = subprocess.run(BASE + list(args), capture_output=True, text=True, timeout=timeout)
    if p.returncode:
        raise RuntimeError(f'Temporal CLI {args[0]} failed: {p.stderr[-500:]}')
    return json.loads(p.stdout) if p.stdout.strip() else None


def pgctl(*args):
    env = dict(os.environ, LC_ALL='C.UTF-8')
    p = subprocess.run([str(RUNTIME / 'pg/bin/pg_ctl'), '-D', str(STATE / 'pgdata'), *args], capture_output=True, text=True, env=env, timeout=30)
    if p.returncode:
        raise RuntimeError(f'pg_ctl failed: {p.stderr[-700:]} {p.stdout[-700:]}')


def stop_owned():
    for port in (27301, 27305, 27310, 27233):
        pid = listening_pid(port)
        if pid:
            try: os.kill(pid, signal.SIGTERM)
            except ProcessLookupError: pass
    if port_open(25545):
        pgctl('-m', 'fast', 'stop')


def main():
    record = {'topology': 'Temporal Server v1.32.0 binary + PostgreSQL 16.15 persistence/visibility + Zigflow v0.15.2 workers', 'mode': 'non-development server; local trust/no-auth spike configuration; not product hardened'}
    server_process = None
    try:
        before = cli('workflow', 'describe', '--workflow-id', 'countertrial-restart')
        record['beforeStatus'] = before['workflowExecutionInfo']['status']
        record['beforeVersion'] = before['workflowExecutionInfo'].get('versioningInfo', {}).get('version')
        if record['beforeStatus'] != 'WORKFLOW_EXECUTION_STATUS_RUNNING':
            raise RuntimeError('waiting execution not running before crash')
        old_server = listening_pid(27233)
        if not old_server or not port_open(25545):
            raise RuntimeError('expected server and PostgreSQL listeners absent')
        os.kill(old_server, signal.SIGKILL)
        until(lambda: not port_open(27233), 15)
        pgctl('-m', 'immediate', 'stop')
        until(lambda: not port_open(25545), 15)
        started = time.monotonic()
        pg_started = time.monotonic()
        pgctl('-l', str(STATE / 'postgres-restart.log'), '-o', '-p 25545 -h 127.0.0.1 -k ' + str(STATE / 'pgsocket'), 'start')
        record['postgresRestartSeconds'] = round(time.monotonic() - pg_started, 3)
        log = open(STATE / 'temporal-restart.log', 'w')
        server_started = time.monotonic()
        server_process = subprocess.Popen([str(RUNTIME / 'temporal-server'), '--config-file', str(STATE / 'server.yaml'), '--allow-no-auth', 'start'], cwd=RUNTIME, stdout=log, stderr=log, start_new_session=True)
        log.close()
        until(lambda: port_open(27233) or server_process.poll() is not None, 45)
        if server_process.poll() is not None:
            raise RuntimeError('Temporal server failed to restart')
        until(lambda: subprocess.run(BASE + ['operator','namespace','describe','--namespace','default'], capture_output=True, timeout=4).returncode == 0, 45)
        record['temporalReadySeconds'] = round(time.monotonic() - server_started, 3)
        record['helpersReadySeconds'] = round(time.monotonic() - started, 3)
        record['workersStillListening'] = bool(listening_pid(27301) and listening_pid(27305))
        cli('workflow', 'signal', '--workflow-id', 'countertrial-restart', '--name', 'approve', '--input', '{"approved":true}')
        outcome = cli('workflow', 'result', '--workflow-id', 'countertrial-restart', timeout=30)
        record['afterResult'] = outcome.get('result') if isinstance(outcome, dict) else outcome
        record['sameExecutionCompleted'] = record['afterResult'] == {'revision': '2.0.0'}
        subprocess.run(['python3', str(Path(__file__).with_name('measure.py')), 'after-helper-crash-restart'], check=True)
        record['status'] = 'passed' if record['sameExecutionCompleted'] else 'incomplete'
    except Exception as exc:
        record['status'] = 'failed'
        record['error'] = str(exc)
        raise
    finally:
        try:
            stop_owned()
            record['ownedListenersStopped'] = all(not port_open(p) for p in (25545, 27233, 27301, 27305, 27310))
        except Exception as exc:
            record['cleanupError'] = str(exc)
        OUTPUT.write_text(json.dumps(record, indent=2) + '\n')
        print(json.dumps(record, indent=2))


if __name__ == '__main__':
    main()
