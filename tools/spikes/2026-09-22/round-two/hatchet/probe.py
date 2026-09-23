"""Run the v1/v2 static-interpreter Hatchet recovery trial."""

import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parent
STATE = Path(tempfile.mkdtemp(prefix="exomachina-hatchet-r2-"))
COMMANDS = STATE / "commands.jsonl"
LOG = STATE / "service.log"


def markers() -> list[dict]:
    if not LOG.exists():
        return []
    found = []
    for line in LOG.read_text(errors="replace").splitlines():
        if line.startswith("SPIKE_JSON "):
            found.append(json.loads(line.removeprefix("SPIKE_JSON ")))
    return found


def wait_for(predicate, description: str, timeout: int = 90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        observed = predicate()
        if observed:
            return observed
        errors = [x for x in markers() if x["kind"] == "command_error"]
        if errors:
            raise RuntimeError(f"{description}: {errors}")
        time.sleep(0.25)
    raise TimeoutError(f"{description}; service log: {LOG.read_text(errors='replace')[-4000:]}")


def publish(version: int) -> str:
    doc = json.loads((ROOT / "fixtures" / f"v{version}.json").read_text())
    expected = ["research_a", "research_b", "join"]
    if version == 2:
        expected.append("verify")
    expected.extend(["review", "director", "delivery"])
    if doc["version"] != version or doc["steps"] != expected:
        raise ValueError("publication policy rejected missing or reordered acceptance gate")
    raw = json.dumps(doc, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(raw).hexdigest()
    (STATE / "catalog").mkdir(exist_ok=True)
    destination = STATE / "catalog" / f"{digest}.json"
    destination.write_bytes(raw)
    (STATE / "catalog" / "latest").write_text(digest)
    return digest


def send(command: dict) -> None:
    with COMMANDS.open("a") as stream:
        stream.write(json.dumps(command, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def step_rows() -> list[dict]:
    path = STATE / "observed.sqlite3"
    if not path.exists():
        return []
    with sqlite3.connect(path, timeout=5) as db:
        rows = db.execute("SELECT harness_id,run_id,step FROM steps ORDER BY harness_id,run_id,step").fetchall()
    return [dict(zip(("harness_id", "run_id", "step"), x)) for x in rows]


def has_step(run_id: str, step: str) -> bool:
    return any(x["run_id"] == run_id and x["step"] == step for x in step_rows())


def memory_sample(root_pid: int) -> list[dict]:
    output = subprocess.check_output(["ps", "-axo", "pid=,ppid=,pgid=,rss=,comm="], text=True)
    all_rows = []
    for line in output.splitlines():
        parts = line.split(maxsplit=4)
        if len(parts) == 5 and all(x.isdigit() for x in parts[:4]):
            all_rows.append(dict(pid=int(parts[0]), ppid=int(parts[1]), pgid=int(parts[2]), rss_kib=int(parts[3]), command=parts[4]))
    selected = {root_pid}
    # Embedded Postgres daemonizes and is reparented to PID 1, so it is not
    # always in the service's process group after startup.
    postmaster = STATE / "postgres" / "data" / "postmaster.pid"
    if postmaster.exists():
        selected.add(int(postmaster.read_text().splitlines()[0]))
    for _ in range(10):
        selected.update(row["pid"] for row in all_rows if row["ppid"] in selected or row["pgid"] == root_pid)
    return [row for row in all_rows if row["pid"] in selected]


def launch() -> subprocess.Popen:
    COMMANDS.write_text("")
    env = os.environ.copy()
    env["HATCHET_SPIKE_STATE"] = str(STATE)
    env["PYTHONUNBUFFERED"] = "1"
    with LOG.open("a") as output:
        process = subprocess.Popen(
            [sys.executable, str(ROOT / "service.py")], cwd=ROOT,
            env=env, stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
        )
    wait_for(lambda: next((x for x in markers() if x["kind"] == "ready" and x["pid"] == process.pid), None), "engine ready", 90)
    return process


def kill(process: subprocess.Popen) -> None:
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=15)
    # The bundled postmaster can outlive a SIGKILLed embedded owner. Stop only
    # this trial's exact data directory before a replacement owner starts.
    pg_ctl = STATE / "postgres" / "runtime" / "bin" / "pg_ctl"
    pg_data = STATE / "postgres" / "data"
    if pg_ctl.exists() and pg_data.exists():
        subprocess.run(
            [str(pg_ctl), "stop", "-D", str(pg_data), "-m", "immediate", "-w"],
            timeout=15, capture_output=True, text=True, check=False,
        )
    time.sleep(2)


def main() -> None:
    process = None
    try:
        d1 = publish(1)
        process = launch()
        source = next(x["source_sha256"] for x in markers() if x["kind"] == "ready")
        send(dict(op="start", digest=d1, harness_id="harness-a", run_id="a-v1"))
        wait_for(lambda: has_step("a-v1", "review"), "v1 reached review")
        assert not has_step("a-v1", "delivery")
        d2 = publish(2)
        send(dict(op="start", digest=d2, harness_id="harness-b", run_id="b-v2"))
        wait_for(lambda: has_step("b-v2", "review"), "v2 reached review")
        assert has_step("b-v2", "verify")
        assert not has_step("a-v1", "verify")
        assert not has_step("b-v2", "delivery")
        before_restart = memory_sample(process.pid)
        kill(process)
        process = launch()
        after_restart = next(x["source_sha256"] for x in markers() if x["kind"] == "ready" and x["pid"] == process.pid)
        assert after_restart == source
        send(dict(op="decide", run_id="a-v1", approved=True, copies=2))
        send(dict(op="decide", run_id="b-v2", approved=True, copies=1))
        wait_for(lambda: has_step("a-v1", "delivery") and has_step("b-v2", "delivery"), "both deliveries", 120)
        time.sleep(2)
        rows = step_rows()
        assert sum(x["run_id"] == "a-v1" and x["step"] == "delivery" for x in rows) == 1
        assert sum(x["run_id"] == "b-v2" and x["step"] == "delivery" for x in rows) == 1
        trace = {
            "state_dir": str(STATE),
            "sidecar_version": "v0.107.0",
            "sdk_version": "1.41.0",
            "digests": {"v1": d1, "v2": d2},
            "unchanged_source_sha256": source,
            "markers": markers(),
            "step_rows": rows,
            "paused_processes": before_restart,
            "paused_total_rss_kib": sum(p["rss_kib"] for p in before_restart),
        }
        (ROOT / "observed.json").write_text(json.dumps(trace, indent=2) + "\n")
        print(json.dumps({"status": "passed", "state_dir": str(STATE), "v1": d1, "v2": d2,
                          "paused_rss_kib": trace["paused_total_rss_kib"]}, indent=2))
    finally:
        if process is not None:
            kill(process)


if __name__ == "__main__":
    main()
