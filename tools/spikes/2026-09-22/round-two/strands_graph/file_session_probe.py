"""Run the existing pinned Graph fixture with the documented file manager."""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

from strands.session import FileSessionManager


PRIOR = Path(__file__).resolve().parents[2] / "countertrials" / "strands_graph" / "runner.py"
SPEC = importlib.util.spec_from_file_location("prior_strands_runner", PRIOR)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def documented_file_manager(*, session_id: str, storage: object, **kwargs: object) -> FileSessionManager:
    """Adapt the old fixture's snapshot constructor to the file manager API."""
    if kwargs != {"multi_agent_save_latest_on": "node"}:
        raise ValueError(f"unexpected snapshot settings: {kwargs}")
    storage_dir = getattr(storage, "_base_dir")
    return FileSessionManager(session_id=session_id, storage_dir=storage_dir)


runner.SnapshotSessionManager = documented_file_manager


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["scenario", "resume", "exercise", "measure"])
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--hold", action="store_true")
    args = parser.parse_args()
    if args.action == "scenario":
        runner.scenario(args.root, hold=args.hold)
    elif args.action == "resume":
        if args.run_id is None:
            parser.error("--run-id is required for resume")
        print(json.dumps(runner.resume(args.root, args.run_id, "approve"), sort_keys=True))
    elif args.action == "exercise":
        def invoke(*command: str) -> dict:
            done = subprocess.run([sys.executable, __file__, *command, "--root", str(args.root)], text=True, capture_output=True, check=True, timeout=30)
            return json.loads(done.stdout)

        started = invoke("scenario")
        resumed = {run_id: invoke("resume", "--run-id", run_id) for run_id in ("run-v1", "run-v2")}
        assert started["v1"]["status"] == started["v2"]["status"] == "interrupted"
        assert resumed["run-v1"]["status"] == resumed["run-v2"]["status"] == "completed"
        assert "verify" not in resumed["run-v1"]["execution_order"]
        assert "verify" in resumed["run-v2"]["execution_order"]
        outcome = {"started": started, "resumed": resumed, "fresh_process_per_command": True}
        if args.output is not None:
            args.output.write_text(json.dumps(outcome, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"status": "passed", "v1": resumed["run-v1"]["status"], "v2": resumed["run-v2"]["status"]}))
    else:
        process = subprocess.Popen([sys.executable, __file__, "scenario", "--root", str(args.root), "--hold"], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            assert process.stdout is not None
            ready = json.loads(process.stdout.readline())
            assert process.poll() is None
            rss = int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(process.pid)], text=True).strip())
            state = int(subprocess.check_output(["du", "-sk", str(args.root)], text=True).split()[0])
            outcome = {"paused_runs": 2, "rss_kib": rss, "state_kib": state, "ready_statuses": [ready["v1"]["status"], ready["v2"]["status"]]}
            if args.output is not None:
                args.output.write_text(json.dumps(outcome, indent=2, sort_keys=True) + "\n")
            print(json.dumps(outcome, sort_keys=True))
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)


if __name__ == "__main__":
    main()
