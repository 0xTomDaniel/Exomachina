"""Kestra native pause/restart/duplicate-resume litmus; invoke in two phases."""

import argparse
import base64
import concurrent.futures
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).parent
API = "/api/v1/main"
AUTH = None


def call(base, method, path, data=None, content_type="application/json"):
    payload = data.encode() if isinstance(data, str) else data
    headers = {"Content-Type": content_type}
    if AUTH is not None:
        encoded = base64.b64encode(f"{AUTH['username']}:{AUTH['password']}".encode()).decode()
        headers["Authorization"] = "Basic " + encoded
    req = urllib.request.Request(base + API + path, data=payload, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        raw = error.read()
        return error.code, json.loads(raw) if raw.startswith(b"{") else raw.decode(errors="replace")


def state(execution):
    item = execution.get("state", {})
    if isinstance(item, dict):
        return item.get("current")
    return item


def wait_for(base, execution_id, allowed, seconds=90):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        code, last = call(base, "GET", "/executions/" + execution_id)
        assert code == 200, (code, last)
        if state(last) in allowed:
            return last
        time.sleep(0.4)
    raise AssertionError(f"Execution never reached {allowed}: {state(last)}")


def create(base):
    code, flow = call(base, "POST", "/flows", (ROOT / "pause.yaml").read_text(), "application/x-yaml")
    assert code in (200, 201, 409), (code, flow)
    code, execution = call(
        base,
        "POST",
        "/executions/exomachina.spikes/exomachina_s1_pause",
        b"--empty--\r\n",
        "multipart/form-data; boundary=empty",
    )
    assert code in (200, 201), (code, execution)
    execution_id = execution["id"]
    paused = wait_for(base, execution_id, {"PAUSED"})
    (ROOT / "execution-id.txt").write_text(execution_id + "\n")
    print(json.dumps({"phase": "before_restart", "id": execution_id, "flow_revision": flow.get("revision"), "state": state(paused)}, indent=2))


def check(base):
    execution_id = (ROOT / "execution-id.txt").read_text().strip()
    execution = wait_for(base, execution_id, {"PAUSED"}, seconds=20)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(
            lambda _: call(
                base, "POST", f"/executions/{execution_id}/actions/resume",
                b"--empty--\r\n", "multipart/form-data; boundary=empty",
            ),
            range(8),
        ))
    after = wait_for(base, execution_id, {"SUCCESS", "FAILED", "KILLED"})
    runs = after.get("taskRunList") or after.get("taskRuns") or []
    observed_after = [{"taskId": task.get("taskId"), "state": task.get("state")} for task in runs if task.get("taskId") == "after"]
    result = {"phase": "after_restart", "id": execution_id, "state_before_resume": state(execution), "resume_codes": [r[0] for r in results], "final_state": state(after), "after_task_runs": observed_after, "task_run_fields": [key for key in after if "task" in key.lower()]}
    (ROOT / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    assert state(after) == "SUCCESS", result
    assert len(observed_after) == 1, "Need exactly one downstream task run; inspect raw execution if field differs"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["create", "check"])
    parser.add_argument("--url", required=True)
    args = parser.parse_args()
    AUTH = json.loads((ROOT / "auth.json").read_text())
    if args.phase == "create":
        create(args.url.rstrip("/"))
    else:
        check(args.url.rstrip("/"))
