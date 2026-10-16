"""Exercise one unchanged Effect FactoryRun helper across publication and SIGKILL."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


HERE = Path(__file__).resolve().parent
ROOT = Path(tempfile.mkdtemp(prefix="exomachina-round2-effect-"))
OBSERVED = HERE / "observed.json"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


PORT = free_port()
URL = f"http://127.0.0.1:{PORT}"


def request(route, value=None):
    data = None if value is None else json.dumps(value).encode()
    req = Request(URL + route, data=data, headers={"content-type": "application/json"})
    try:
        with urlopen(req, timeout=20) as response:
            return {"status": response.status, "body": json.load(response)}
    except HTTPError as error:
        return {"status": error.code, "body": json.load(error)}


def until_ready(process, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"helper exited during boot: {process.returncode}")
        try:
            response = request("/health")
            if response["status"] == 200 and response["body"]["pid"] == process.pid:
                return
        except (URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(0.1)
    raise TimeoutError("helper did not become ready")


def start(log):
    env = os.environ.copy()
    env["EFFECT_ROUND2_ROOT"] = str(ROOT)
    env["EFFECT_ROUND2_PORT"] = str(PORT)
    process = subprocess.Popen(["node", "helper.mjs"], cwd=HERE, env=env,
                               stdout=log, stderr=subprocess.STDOUT)
    until_ready(process)
    return process


def stop(process, hard=False):
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGKILL if hard else signal.SIGTERM)
    process.wait(timeout=15)


def until_state(run_id, expected, seconds=30):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        last = request("/poll", {"id": run_id})
        if last["status"] == 200 and last["body"]["state"] == expected:
            return last["body"]
        time.sleep(0.1)
    raise TimeoutError(f"{run_id} never reached {expected}: {last}")


def rss_kib(process):
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(process.pid)], text=True))


def disk_kib(path):
    return int(subprocess.check_output(["du", "-sk", str(path)], text=True).split()[0])


def main():
    evidence = {"runtime_root": str(ROOT), "port": PORT,
                "node": subprocess.check_output(["node", "--version"], text=True).strip(),
                "package_lock_sha256": hashlib.sha256((HERE / "package-lock.json").read_bytes()).hexdigest()}
    helper = None
    with (ROOT / "helper.log").open("a") as log:
        try:
            helper = start(log)
            evidence["initial_pid"] = helper.pid
            evidence["warm_rss_kib"] = rss_kib(helper)
            doc1 = json.loads((HERE / "fixtures/v1.json").read_text())
            doc2 = json.loads((HERE / "fixtures/v2.json").read_text())
            digest1 = request("/publish", doc1)
            assert digest1["status"] == 200, digest1
            evidence["publish_v1"] = digest1
            start1 = request("/start", {"id": "harness-v1", "digest": digest1["body"]["digest"]})
            assert start1["status"] == 200, start1
            evidence["start_v1"] = start1
            evidence["wait_v1"] = until_state("harness-v1", "Suspended")

            # A publication policy rejection must happen before the document reaches Effect.
            bypass = {**doc2, "steps": [step for step in doc2["steps"] if step != "review"]}
            evidence["review_bypass"] = request("/publish", bypass)
            assert evidence["review_bypass"]["status"] == 400
            digest2 = request("/publish", doc2)
            assert digest2["status"] == 200, digest2
            evidence["publish_v2"] = digest2
            evidence["same_helper_at_v2_publish"] = helper.poll() is None and helper.pid == evidence["initial_pid"]
            assert evidence["same_helper_at_v2_publish"]
            start2 = request("/start", {"id": "harness-v2", "digest": digest2["body"]["digest"]})
            assert start2["status"] == 200, start2
            evidence["start_v2"] = start2
            evidence["wait_v2"] = until_state("harness-v2", "Suspended")
            evidence["wrong_binding_start"] = request("/start", {"id": "harness-v1", "digest": digest2["body"]["digest"]})
            assert evidence["wrong_binding_start"]["status"] == 409
            evidence["paused_two_rss_kib"] = rss_kib(helper)

            stop(helper, hard=True)
            evidence["killed_pid"] = helper.pid
            evidence["killed_exit_code"] = helper.returncode
            helper = start(log)
            evidence["restart_pid"] = helper.pid
            evidence["restart_rss_kib"] = rss_kib(helper)
            evidence["wrong_binding_decision"] = request("/approve", {"id": "harness-v1", "digest": digest2["body"]["digest"]})
            assert evidence["wrong_binding_decision"]["status"] == 409

            with ThreadPoolExecutor(max_workers=3) as pool:
                futures = [pool.submit(request, "/approve", {"id": run_id, "digest": digest})
                           for run_id, digest in (("harness-v1", digest1["body"]["digest"]),
                                                  ("harness-v1", digest1["body"]["digest"]),
                                                  ("harness-v2", digest2["body"]["digest"]))]
                evidence["concurrent_decisions"] = [future.result() for future in futures]
            evidence["result_v1"] = until_state("harness-v1", "Complete")
            evidence["result_v2"] = until_state("harness-v2", "Complete")

            rows = [json.loads(line) for line in (ROOT / "events.jsonl").read_text().splitlines()]
            counts = Counter((row["id"], row["node"]) for row in rows)
            evidence["event_counts"] = {f"{run_id}:{node}": count for (run_id, node), count in sorted(counts.items())}
            evidence["events"] = rows
            evidence["installed_node_modules_kib"] = disk_kib(HERE / "node_modules")
            evidence["runtime_state_kib"] = disk_kib(ROOT)
            evidence["sqlite_files_bytes_live"] = {
                item.name: item.stat().st_size for item in sorted(ROOT.glob("state.sqlite*"))
            }
            evidence["owned_code_lines"] = {name: len((HERE / name).read_text().splitlines())
                                            for name in ("helper.mjs", "probe.py")}
            assert evidence["result_v1"]["exit"] == evidence["result_v2"]["exit"] == "Success"
            assert evidence["result_v1"]["value"]["digest"] == digest1["body"]["digest"]
            assert evidence["result_v2"]["value"]["digest"] == digest2["body"]["digest"]
            assert evidence["result_v1"]["value"]["version"] == 1
            assert evidence["result_v2"]["value"]["version"] == 2
            assert counts[("harness-v1", "verify")] == 0
            assert counts[("harness-v2", "verify")] == 1
            assert counts[("harness-v1", "deliver")] == counts[("harness-v2", "deliver")] == 1
            assert all(item["status"] == 200 for item in evidence["concurrent_decisions"])
            evidence["passed"] = True
        except Exception as error:
            evidence["passed"] = False
            evidence["error"] = repr(error)
            raise
        finally:
            stop(helper)
            OBSERVED.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
            print(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
