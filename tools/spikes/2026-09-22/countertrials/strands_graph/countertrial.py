"""Run the bounded Graph process-kill and two-harness resource countertrial."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path


RUNNER = Path(__file__).parent / "runner.py"


def invoke(*args: str) -> dict:
    result = subprocess.run(
        [sys.executable, str(RUNNER), *args],
        check=True,
        text=True,
        capture_output=True,
        timeout=30,
    )
    return json.loads(result.stdout)


def launch(root: Path) -> tuple[subprocess.Popen, dict, float]:
    started = time.monotonic()
    process = subprocess.Popen(
        [sys.executable, str(RUNNER), "scenario", "--root", str(root), "--hold"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdout is not None
    ready = json.loads(process.stdout.readline())
    elapsed = time.monotonic() - started
    assert process.poll() is None
    assert ready["v1"]["status"] == ready["v2"]["status"] == "interrupted"
    assert "verify" not in ready["v1"]["execution_order"]
    assert "verify" in ready["v2"]["execution_order"]
    assert "deliver" not in ready["v1"]["execution_order"]
    assert "deliver" not in ready["v2"]["execution_order"]
    return process, ready, elapsed


def rss_kib(pid: int) -> int:
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True).strip())


def disk_kib(path: Path) -> int:
    return int(subprocess.check_output(["du", "-sk", str(path)], text=True).split()[0])


def event_counts(root: Path, run_id: str) -> dict[str, int]:
    events = [json.loads(line) for line in (root / f"{run_id}.events.jsonl").read_text().splitlines()]
    return dict(Counter(event["node"] for event in events))


def run(root: Path) -> dict:
    roots = [root / "harness-one", root / "harness-two"]
    children: list[subprocess.Popen] = []
    try:
        one, ready_one, startup_one = launch(roots[0])
        children.append(one)
        time.sleep(1)
        one_rss_alone = rss_kib(one.pid)
        two, ready_two, startup_two = launch(roots[1])
        children.append(two)
        time.sleep(1)
        one_rss_shared = rss_kib(one.pid)
        two_rss_shared = rss_kib(two.pid)
        before = {
            str(index): {run_id: event_counts(path, run_id) for run_id in ("run-v1", "run-v2")}
            for index, path in enumerate(roots, 1)
        }
        assert ready_one["harness_id"] != ready_two["harness_id"]
        assert one_rss_alone > 0 and one_rss_shared > 0 and two_rss_shared > 0
        for child in children:
            child.kill()
            assert child.wait(timeout=10) == -9
        children.clear()
        resumed: dict[str, dict] = {}
        for index, path in enumerate(roots, 1):
            resumed[str(index)] = {}
            for run_id in ("run-v1", "run-v2"):
                outcome = invoke("resume", "--root", str(path), "--run-id", run_id)
                assert outcome["status"] == "completed"
                assert outcome["revision"] == (1 if run_id == "run-v1" else 2)
                assert ("verify" in outcome["execution_order"]) == (run_id == "run-v2")
                resumed[str(index)][run_id] = outcome
        after = {
            str(index): {run_id: event_counts(path, run_id) for run_id in ("run-v1", "run-v2")}
            for index, path in enumerate(roots, 1)
        }
        for index in ("1", "2"):
            for run_id in ("run-v1", "run-v2"):
                assert after[index][run_id].get("deliver") == 1
                for node, count in before[index][run_id].items():
                    assert after[index][run_id][node] == count == 1
        bypass = invoke("validate-bypass", "--root", str(roots[0]))
        assert bypass["rejected"] == "review must be the sole delivery predecessor"
        return {
            "platform": sys.platform,
            "python": sys.version.split()[0],
            "strands_agents": importlib.metadata.version("strands-agents"),
            "provider_calls": 0,
            "harnesses": 2,
            "runs_per_harness": 2,
            "same_process_publication": True,
            "sigkill_then_new_process_resume": True,
            "distinct_harness_identities": True,
            "review_bypass_rejected_by_fixture_validator": True,
            "memory": {
                "unit": "KiB RSS per process; sums double-count shared pages",
                "one_harness_alone": one_rss_alone,
                "harness_one_with_two_alive": one_rss_shared,
                "harness_two_with_two_alive": two_rss_shared,
                "two_process_rss_sum": one_rss_shared + two_rss_shared,
            },
            "startup_to_two_paused_runs_seconds": {"harness_one": round(startup_one, 3), "harness_two": round(startup_two, 3)},
            "disk_kib": {
                "harness_one_state": disk_kib(roots[0]),
                "harness_two_state": disk_kib(roots[1]),
                "environment": disk_kib(Path(sys.prefix)),
            },
            "first_harness": ready_one,
            "second_harness": ready_two,
            "events_before_restart": before,
            "events_after_restart": after,
            "resumed": resumed,
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
    args.root.mkdir(parents=True, exist_ok=True)
    result = run(args.root)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "status": "passed",
        "memory": result["memory"],
        "startup_to_two_paused_runs_seconds": result["startup_to_two_paused_runs_seconds"],
        "disk_kib": result["disk_kib"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
