"""Probe two unfenced processes resuming the same Graph checkpoint."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path


RUNNER = Path(__file__).parent / "runner.py"


def event_counts(root: Path, run_id: str) -> dict[str, int]:
    events = [json.loads(line) for line in (root / f"{run_id}.events.jsonl").read_text().splitlines()]
    return dict(Counter(event["node"] for event in events))


def wait_for(path: Path, children: list[subprocess.Popen]) -> None:
    deadline = time.monotonic() + 15
    while not path.exists():
        for child in children:
            if child.poll() is not None:
                raise RuntimeError(f"resume owner exited before rendezvous: {child.returncode}")
        if time.monotonic() > deadline:
            raise TimeoutError(f"rendezvous did not appear: {path.name}")
        time.sleep(0.02)


def run(root: Path) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    setup = subprocess.run(
        [sys.executable, str(RUNNER), "scenario", "--root", str(root)],
        text=True,
        capture_output=True,
        check=True,
        timeout=30,
    )
    ready = json.loads(setup.stdout)
    assert ready["v1"]["status"] == "interrupted"
    gate = root / "rendezvous"
    gate.mkdir()
    children: list[subprocess.Popen] = []
    try:
        for owner in ("a", "b"):
            env = os.environ.copy()
            env["EXO_RENDEZVOUS_DIR"] = str(gate)
            env["EXO_OWNER_ID"] = owner
            child = subprocess.Popen(
                [sys.executable, str(RUNNER), "resume", "--root", str(root), "--run-id", "run-v1"],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
            )
            children.append(child)
        wait_for(gate / "ready-a", children)
        wait_for(gate / "ready-b", children)
        before_release = event_counts(root, "run-v1")
        (gate / "release-a").touch()
        out_a, err_a = children[0].communicate(timeout=15)
        after_a = event_counts(root, "run-v1")
        # Owner B restored the old checkpoint before A completed and remained
        # alive with stale in-memory graph state throughout A's completion.
        (gate / "release-b").touch()
        out_b, err_b = children[1].communicate(timeout=15)
        after_b = event_counts(root, "run-v1")
        return {
            "sdk": "strands-agents==1.57.0",
            "same_logical_run": "run-v1",
            "both_owners_reached_restored_interrupt_before_release": True,
            "owner_b_released_after_owner_a_completed": True,
            "before_release": before_release,
            "after_owner_a": after_a,
            "after_owner_b": after_b,
            "owner_a": {"exit_code": children[0].returncode, "result": json.loads(out_a) if out_a else None, "stderr": err_a[-1000:]},
            "owner_b": {"exit_code": children[1].returncode, "result": json.loads(out_b) if out_b else None, "stderr": err_b[-1000:]},
            "duplicate_deliveries": after_b.get("deliver", 0) > 1,
        }
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=10)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = run(args.root)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "duplicate_deliveries": result["duplicate_deliveries"],
        "deliveries_after_owner_a": result["after_owner_a"].get("deliver", 0),
        "deliveries_after_owner_b": result["after_owner_b"].get("deliver", 0),
        "owner_exit_codes": [result["owner_a"]["exit_code"], result["owner_b"]["exit_code"]],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
