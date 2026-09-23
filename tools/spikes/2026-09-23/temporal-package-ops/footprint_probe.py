"""One de-duplicated Darwin footprint method for both frozen bundles."""
from __future__ import annotations

from pathlib import Path
import os
import re
import subprocess
import time


def _process_table() -> dict[int, tuple[int, str]]:
    table = {}
    output = subprocess.check_output(["ps", "-axo", "pid=,ppid=,command="], text=True)
    for line in output.splitlines():
        fields = line.strip().split(None, 2)
        if len(fields) == 3 and fields[0].isdigit() and fields[1].isdigit():
            table[int(fields[0])] = (int(fields[1]), fields[2])
    return table


def sample(supervisor_pid: int, state: Path, roles: dict[int, str],
           extra_roots: dict[int, str] | None = None) -> dict:
    """Select the supervisor tree, state-bound detached runs, and explicit store tree."""
    failures = []
    for attempt in range(3):
        try:
            table = _process_table()
            roots = {supervisor_pid, *(extra_roots or {})}
            if any(pid not in table for pid in roots):
                raise RuntimeError(f"missing process root: {sorted(roots - table.keys())}")
            selected = set(roots)
            while True:
                added = {pid for pid, (ppid, _) in table.items() if ppid in selected}
                if added <= selected:
                    break
                selected |= added
            selected |= {pid for pid, (_, command) in table.items()
                         if str(state) in command and pid != os.getpid()}
            # Detached Dagu run commands include the state path even after they
            # leave the supervisor's process tree.
            selected = {pid for pid in selected if pid in table}
            command = ["footprint", "-f", "bytes", "--noCategories"]
            for pid in sorted(selected):
                command += ["-p", str(pid)]
            run = subprocess.run(command, capture_output=True, text=True, timeout=60)
            if run.returncode:
                raise RuntimeError(f"footprint exited {run.returncode}: {run.stderr[-500:].strip()}")
            match = re.search(r"Summary Footprint:\s*([0-9,]+) B", run.stdout)
            if match is None and len(selected) == 1:
                match = re.search(r"Footprint:\s*([0-9,]+) B", run.stdout)
            if match is None:
                raise RuntimeError("footprint summary missing")
            processes = []
            for pid in sorted(selected):
                ppid, argv = table[pid]
                role = roles.get(pid) or (extra_roots or {}).get(pid)
                if role is None:
                    if "ops.py nested" in argv:
                        role = "parent_nested_adapter"
                    elif "ops.py" in argv:
                        role = "dagu_step_adapter"
                    elif "dagu-parent-exit-watcher" in argv:
                        role = "dagu_parent_exit_watcher"
                    elif "dagu start" in argv or argv.endswith("(dagu)"):
                        role = "dagu_run"
                    elif (extra_roots and _descends_from(pid, table, set(extra_roots))):
                        role = "postgres_child"
                    else:
                        role = "bundle_child"
                processes.append({"pid": pid, "ppid": ppid, "role": role,
                                  "command": argv[:320]})
            return {"sampled_unix_seconds": time.time(),
                    "physical_footprint_bytes_deduplicated": int(match[1].replace(",", "")),
                    "pid_count": len(processes), "processes": processes,
                    "selection": "supervisor descendants plus state-bound detached processes plus explicit store descendants",
                    "footprint_command": "footprint -f bytes --noCategories -p <each selected PID>",
                    "footprint_exit_code": 0,
                    "sampling_retries": failures}
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            failures.append(str(error))
            if attempt == 2:
                raise
            time.sleep(0.5)
    raise AssertionError("unreachable")


def _descends_from(pid: int, table: dict[int, tuple[int, str]], roots: set[int]) -> bool:
    seen = set()
    while pid in table and pid not in seen:
        if pid in roots:
            return True
        seen.add(pid)
        pid = table[pid][0]
    return False
