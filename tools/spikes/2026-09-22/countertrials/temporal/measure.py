#!/usr/bin/env python3
"""Read-only resource sample for the isolated Temporal countertrial."""
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path('/tmp/exomachina-countertrials/temporal')
OUTPUT = Path(__file__).with_name('measurements.json')


def command(*args):
    return subprocess.run(args, text=True, capture_output=True, check=True).stdout


def listening_pid(port):
    result = subprocess.run(['lsof', '-tiTCP:' + str(port), '-sTCP:LISTEN'], text=True, capture_output=True)
    return [int(line) for line in result.stdout.splitlines() if line.strip().isdigit()]


def process_table():
    table = {}
    for line in command('ps', '-axo', 'pid=,ppid=,rss=,%cpu=,comm=').splitlines():
        fields = line.strip().split(None, 4)
        if len(fields) < 5:
            continue
        pid, ppid, rss, cpu, name = fields
        if pid.isdigit() and ppid.isdigit():
            table[int(pid)] = {'pid': int(pid), 'ppid': int(ppid), 'rssKiB': int(rss), 'cpuPercentSample': float(cpu), 'name': name}
    return table


def du_kib(path):
    if not path.exists():
        return None
    return int(command('du', '-sk', str(path)).split()[0])


def footprint_bytes(pids):
    if not pids:
        return None
    args = ['footprint', '--noCategories', '-f', 'bytes']
    for pid in pids:
        args.extend(['-p', str(pid)])
    result = subprocess.run(args, text=True, capture_output=True)
    match = re.search(r'Summary Footprint:\s+(\d+) B', result.stdout)
    return int(match.group(1)) if match else None


def sample(phase):
    table = process_table()
    pgpid_path = ROOT / 'state/pgdata/postmaster.pid'
    pgpid = int(pgpid_path.read_text().splitlines()[0]) if pgpid_path.exists() else None
    pids = []
    for port in (27233, 27301, 27305):
        pids.extend(listening_pid(port))
    if pgpid:
        pids.append(pgpid)
        pids.extend(pid for pid, info in table.items() if info['ppid'] == pgpid)
    pids = sorted(set(pid for pid in pids if pid in table))
    processes = [table[pid] for pid in pids]
    return {
        'phase': phase,
        'timeUtc': datetime.now(timezone.utc).isoformat(),
        'processCount': len(processes),
        'rssSumKiB': sum(p['rssKiB'] for p in processes),
        'macOSFootprintGroupBytes': footprint_bytes(pids),
        'macOSFootprintMethod': 'footprint --noCategories -f bytes with all listed PIDs; a separate kernel footprint metric, not RSS/PSS',
        'processes': processes,
        'diskKiB': {
            'serverBinary': du_kib(ROOT / 'runtime/temporal-server'),
            'sqlTool': du_kib(ROOT / 'runtime/temporal-sql-tool'),
            'zigflowWorkerBinary': du_kib(ROOT / 'runtime/zigflow'),
            'postgresBundle': du_kib(ROOT / 'runtime/pg'),
            'postgresState': du_kib(ROOT / 'state/pgdata'),
            'completeExtractedTemporalRelease': du_kib(ROOT / 'runtime'),
        },
    }


def main():
    phase = sys.argv[1]
    data = json.loads(OUTPUT.read_text()) if OUTPUT.exists() else {
        'host': 'macOS 26.5.2 arm64; other countertrials active concurrently',
        'temporal': 'v1.32.0 non-development temporal-server binary',
        'postgres': '16.15 copied host-local bundle; no Docker',
        'zigflow': 'v0.15.2',
        'caution': 'Concurrent host activity prevents controlled whole-machine or exact cross-candidate ranking. RSS sums can double-count shared pages.',
        'samples': [],
    }
    observation = sample(phase)
    data['samples'].append(observation)
    OUTPUT.write_text(json.dumps(data, indent=2) + '\n')
    print(json.dumps({k: observation[k] for k in ('phase', 'processCount', 'rssSumKiB', 'macOSFootprintGroupBytes', 'diskKiB')}, indent=2))


if __name__ == '__main__':
    main()
