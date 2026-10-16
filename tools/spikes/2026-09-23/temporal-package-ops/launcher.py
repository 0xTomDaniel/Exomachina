"""Single-command native bundle lifecycle and cold consistent state backup."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
PREFIX = HERE.parent
STATE = PREFIX / "state"


def running() -> int | None:
    marker = STATE / "supervisor-ready"
    if not marker.exists():
        return None
    pid = int(marker.read_text())
    try:
        os.kill(pid, 0)
        return pid
    except ProcessLookupError:
        return None


def main() -> None:
    os.environ.setdefault("LC_ALL", "C")
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["start", "stop", "status", "backup", "restore", "schema-update"])
    parser.add_argument("--archive", type=Path)
    args = parser.parse_args()
    if args.command == "start":
        if running():
            raise RuntimeError("already running")
        import supervisor
        supervisor.run(STATE)
    elif args.command == "stop":
        pid = running()
        if pid is None:
            raise RuntimeError("not running")
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + 60
        while running() and time.monotonic() < deadline:
            time.sleep(0.2)
        if running():
            raise TimeoutError("graceful stop did not complete")
        print(json.dumps({"stopped": pid}))
    elif args.command == "status":
        print(json.dumps({"running_pid": running(), "prefix": str(PREFIX)}))
    elif args.command == "backup":
        if running():
            raise RuntimeError("stop gracefully before cold backup")
        if args.archive is None:
            raise ValueError("--archive required")
        if args.archive.exists():
            raise FileExistsError(args.archive)
        started = time.monotonic()
        shutil.copytree(STATE, args.archive, ignore=shutil.ignore_patterns("supervisor-ready", "supervisor.lock"))
        print(json.dumps({"archive": str(args.archive), "seconds": time.monotonic() - started}))
    elif args.command == "restore":
        if running():
            raise RuntimeError("stop before restore")
        if args.archive is None or not args.archive.is_dir():
            raise ValueError("--archive directory required")
        if (STATE / "pgdata").exists():
            raise RuntimeError("restore requires a fresh installed state")
        shutil.rmtree(STATE)
        shutil.copytree(args.archive, STATE)
        print(json.dumps({"restored": str(args.archive), "prefix": str(PREFIX)}))
    else:
        if not running():
            raise RuntimeError("start before schema-update")
        import runtime
        runtime.STATE = STATE
        for name, directory in (("temporal", "temporal"), ("temporal_visibility", "visibility")):
            runtime.sql_command(name, "update-schema", "--schema-dir", str(runtime.SCHEMA / directory / "versioned"))
        print(json.dumps({"schema_update": "complete"}))


if __name__ == "__main__":
    main()
