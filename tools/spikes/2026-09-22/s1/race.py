"""One bounded resume-vs-kill contention batch on an isolated Kestra instance."""

import concurrent.futures
import json
import threading
import time
from pathlib import Path

import probe

ROOT = Path(__file__).parent
BASE = "http://127.0.0.1:28081"
probe.AUTH = json.loads((ROOT / "auth.json").read_text())


def one(index):
    code, execution = probe.call(
        BASE, "POST", "/executions/exomachina.spikes/exomachina_s1_pause",
        b"--empty--\r\n", "multipart/form-data; boundary=empty",
    )
    assert code in (200, 201), (code, execution)
    execution_id = execution["id"]
    probe.wait_for(BASE, execution_id, {"PAUSED"})
    barrier = threading.Barrier(2)

    def resume():
        barrier.wait()
        code, _ = probe.call(
            BASE, "POST", f"/executions/{execution_id}/actions/resume",
            b"--empty--\r\n", "multipart/form-data; boundary=empty",
        )
        return code

    def kill():
        barrier.wait()
        code, _ = probe.call(BASE, "DELETE", f"/executions/{execution_id}/actions/kill")
        return code

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        a = pool.submit(resume)
        b = pool.submit(kill)
        resume_code, kill_code = a.result(), b.result()
    final = probe.wait_for(BASE, execution_id, {"SUCCESS", "KILLED", "FAILED"})
    time.sleep(0.5)
    code, again = probe.call(BASE, "GET", f"/executions/{execution_id}")
    assert code == 200 and probe.state(again) == probe.state(final)
    after = [r for r in again.get("taskRunList", []) if r.get("taskId") == "after"]
    assert probe.state(final) in {"SUCCESS", "KILLED"}
    assert len(after) <= 1, "One run generated duplicate downstream tasks"
    if probe.state(final) == "SUCCESS":
        assert len(after) == 1 and after[0]["state"]["current"] == "SUCCESS"
    return {
        "index": index, "id": execution_id, "resume_http": resume_code,
        "kill_http": kill_code, "terminal": probe.state(final),
        "downstream_runs": len(after),
        "downstream_states": [r["state"]["current"] for r in after],
    }


if __name__ == "__main__":
    outcomes = [one(i) for i in range(5)]
    (ROOT / "race-results.json").write_text(json.dumps(outcomes, indent=2) + "\n")
    print(json.dumps(outcomes, indent=2))
