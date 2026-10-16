"""Run the Effect candidate against two real Strands/A2A fixture processes."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

HERE = Path(__file__).resolve().parent
COMMON = HERE.parent / "common" / "harness_server.py"
PYTHON = HERE.parents[1] / "s2" / ".venv" / "bin" / "python"
ROOT = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(tempfile.mkdtemp(prefix="exomachina-effect-product-"))
ROOT.mkdir(parents=True, exist_ok=True)
OBSERVED = HERE / "observed.json"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(url, route, value=None, timeout=10):
    data = None if value is None else json.dumps(value).encode()
    req = Request(url + route, data=data, headers={"content-type": "application/json",
              "authorization": "Bearer fixture-token"})
    try:
        with urlopen(req, timeout=timeout) as response:
            return {"status": response.status, "body": json.load(response)}
    except HTTPError as error:
        return {"status": error.code, "body": json.load(error)}


def until_ready(process, url, seconds=40):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"process {process.pid} exited during boot with {process.returncode}")
        try:
            result = request(url, "/health")
            if result["status"] == 200:
                return result["body"]
        except (URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(.1)
    raise TimeoutError(f"process {process.pid} did not become ready")


def launch_harness(role, port, log):
    url = f"http://127.0.0.1:{port}"
    process = subprocess.Popen([str(PYTHON), str(COMMON), "--state", str(ROOT / role),
                                "--role", role, "--port", str(port)],
                               stdout=log, stderr=subprocess.STDOUT)
    return process, url, until_ready(process, url)


def launch_effect(port, log):
    url = f"http://127.0.0.1:{port}"
    env = os.environ.copy()
    env["EFFECT_DECISION_ROOT"] = str(ROOT / "effect")
    env["EFFECT_DECISION_PORT"] = str(port)
    process = subprocess.Popen(["node", "helper.mjs"], cwd=HERE, env=env,
                               stdout=log, stderr=subprocess.STDOUT)
    return process, url, until_ready(process, url)


def stop(process, hard=False):
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGKILL if hard else signal.SIGTERM)
    process.wait(timeout=10)


def until_exit(process, seconds=20):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return process.returncode
        time.sleep(.1)
    raise TimeoutError(f"process {process.pid} did not exit")


def until_state(url, run_id, expected, seconds=60):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        last = request(url, "/poll", {"id": run_id})
        if last["status"] == 200 and last["body"]["state"] == expected:
            return last["body"]
        time.sleep(.15)
    raise TimeoutError(f"{run_id} never reached {expected}: {last}")


def rss_kib(process):
    if process is None or process.poll() is not None:
        return None
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(process.pid)], text=True))


def disk_kib(path):
    return int(subprocess.check_output(["du", "-sk", str(path)], text=True).split()[0])


def main():
    evidence = {"runtime_root": str(ROOT),
                "node": subprocess.check_output(["node", "--version"], text=True).strip(),
                "python": subprocess.check_output([str(PYTHON), "--version"], text=True).strip(),
                "package_lock_sha256": hashlib.sha256((HERE / "package-lock.json").read_bytes()).hexdigest()}
    processes = []
    with (ROOT / "worker.log").open("a") as worker_log, \
         (ROOT / "quality.log").open("a") as quality_log, \
         (ROOT / "effect.log").open("a") as effect_log:
        try:
            worker_port, quality_port, effect_port = free_port(), free_port(), free_port()
            worker, worker_url, worker_info = launch_harness("capability", worker_port, worker_log)
            processes.append(worker)
            quality, quality_url, quality_info = launch_harness("quality", quality_port, quality_log)
            processes.append(quality)
            effect, effect_url, effect_info = launch_effect(effect_port, effect_log)
            processes.append(effect)
            evidence["initial_pids"] = {"worker": worker.pid, "quality": quality.pid, "effect": effect.pid}
            evidence["harnesses"] = {"worker": worker_info, "quality": quality_info}
            assert worker_info["identity"] != quality_info["identity"]
            assert worker_info["a2a_protocol"] == quality_info["a2a_protocol"] == "0.3.0"
            capabilities = {
                "worker": {"identity": worker_info["identity"], "url": worker_url, "version": "v1"},
                "quality": {"identity": quality_info["identity"], "url": quality_url, "version": "v1"}}
            v1 = {"name": "verified-research", "version": 1, "capabilities": capabilities,
                  "steps": ["assign", "review", "director_wait", "deliver"]}
            v2 = {"name": "verified-research", "version": 2,
                  "capabilities": {"worker": {**capabilities["worker"], "version": "v2"},
                                   "quality": {**capabilities["quality"], "version": "v2"}},
                  "steps": ["assign", "verify", "review", "director_wait", "deliver"]}
            pub1 = request(effect_url, "/publish", v1)
            assert pub1["status"] == 200, pub1
            d1 = pub1["body"]["digest"]
            evidence["publish_v1"] = pub1
            start1 = request(effect_url, "/start", {"id": "run-v1", "digest": d1})
            assert start1["status"] == 200, start1
            evidence["start_v1"] = start1
            evidence["receiver_exit_code"] = until_exit(worker)
            assert evidence["receiver_exit_code"] == 23
            # Crash the Effect owner while the remote action is committed but
            # its Activity has not yet reconciled a dropped acknowledgement.
            stop(effect, hard=True)
            evidence["effect_killed_during_unknown"] = effect.pid
            worker2, _, worker2_info = launch_harness("capability", worker_port, worker_log)
            processes.append(worker2)
            evidence["worker_restart"] = worker2_info
            assert worker2_info["identity"] == worker_info["identity"]
            effect, effect_url, effect_unknown_info = launch_effect(effect_port, effect_log)
            processes.append(effect)
            evidence["effect_unknown_restart"] = effect_unknown_info
            evidence["wait_v1"] = until_state(effect_url, "run-v1", "Suspended")
            bypass = {**v2, "steps": ["assign", "verify", "director_wait", "deliver"]}
            evidence["bypass_publish"] = request(effect_url, "/publish", bypass)
            assert evidence["bypass_publish"]["status"] == 400
            pub2 = request(effect_url, "/publish", v2)
            assert pub2["status"] == 200, pub2
            d2 = pub2["body"]["digest"]
            evidence["publish_v2"] = pub2
            evidence["same_effect_pid_at_publish"] = effect.poll() is None and effect.pid == effect_unknown_info["pid"]
            assert evidence["same_effect_pid_at_publish"]
            start2 = request(effect_url, "/start", {"id": "run-v2", "digest": d2})
            assert start2["status"] == 200, start2
            evidence["start_v2"] = start2
            evidence["wait_v2"] = until_state(effect_url, "run-v2", "Suspended")
            evidence["wrong_binding"] = request(effect_url, "/start", {"id": "run-v1", "digest": d2})
            assert evidence["wrong_binding"]["status"] == 409
            evidence["rss_kib_paused"] = {"effect": rss_kib(effect), "worker": rss_kib(worker2),
                                           "quality": rss_kib(quality)}
            stop(effect, hard=True)
            evidence["effect_killed_pid"] = effect.pid
            effect2, effect_url, effect2_info = launch_effect(effect_port, effect_log)
            processes.append(effect2)
            evidence["effect_restart"] = effect2_info
            assert effect2.pid != effect.pid
            assert until_state(effect_url, "run-v1", "Suspended")["state"] == "Suspended"
            assert until_state(effect_url, "run-v2", "Suspended")["state"] == "Suspended"
            l1 = request(effect_url, "/ledger", {"id": "run-v1"})["body"]["run"]
            l2 = request(effect_url, "/ledger", {"id": "run-v2"})["body"]["run"]
            evidence["observed_revisions"] = {"run-v1": l1["revision"], "run-v2": l2["revision"]}
            assert l1["author"] == l2["author"] == worker_info["identity"]
            assert l1["reviewer"] == l2["reviewer"] == quality_info["identity"]
            assert l1["quality_accepted"] == l2["quality_accepted"] == 1
            base1 = {"id": "run-v1", "digest": d1, "revision": l1["revision"],
                     "sha256": l1["sha256"], "actor": l1["reviewer"]}
            base2 = {"id": "run-v2", "digest": d2, "revision": l2["revision"],
                     "sha256": l2["sha256"], "actor": l2["reviewer"]}
            evidence["stale_accept"] = request(effect_url, "/accept", {**base1, "revision": "r1"})
            evidence["self_accept"] = request(effect_url, "/accept", {**base1, "actor": l1["author"]})
            evidence["wrong_digest_accept"] = request(effect_url, "/accept", {**base1, "digest": d2})
            evidence["wrong_run_accept"] = request(effect_url, "/accept", {**base1, "id": "unrelated"})
            evidence["unaccepted_flush"] = request(effect_url, "/flush", {"id": "run-v1", "digest": d1})
            assert evidence["stale_accept"]["status"] == 409
            assert evidence["self_accept"]["status"] == 403
            assert evidence["wrong_digest_accept"]["status"] == 409
            assert evidence["wrong_run_accept"]["status"] == 404
            assert evidence["unaccepted_flush"]["status"] == 409
            evidence["accepted_v1"] = request(effect_url, "/accept", base1)
            evidence["accepted_v2"] = request(effect_url, "/accept", base2)
            evidence["duplicate_accept"] = request(effect_url, "/accept", base1)
            assert evidence["accepted_v1"]["status"] == evidence["accepted_v2"]["status"] == 200
            assert evidence["duplicate_accept"]["status"] == 409
            stop(effect2, hard=True)
            evidence["effect_killed_after_accept"] = effect2.pid
            effect2, effect_url, effect_after_accept_info = launch_effect(effect_port, effect_log)
            processes.append(effect2)
            evidence["effect_after_accept_restart"] = effect_after_accept_info
            assert request(effect_url, "/ledger", {"id": "run-v1"})["body"]["outbox"]["state"] == "pending"
            try:
                request(effect_url, "/flush", {"id": "run-v1", "digest": d1, "drop_ack": True})
                raise AssertionError("flush should have killed helper before its HTTP reply")
            except (URLError, ConnectionError, TimeoutError) as error:
                evidence["lost_continuation_ack"] = type(error).__name__
            evidence["effect_bridge_exit_code"] = until_exit(effect2)
            effect3, effect_url, effect3_info = launch_effect(effect_port, effect_log)
            processes.append(effect3)
            evidence["effect_bridge_restart"] = effect3_info
            evidence["outbox_after_lost_ack"] = request(effect_url, "/ledger", {"id": "run-v1"})["body"]["outbox"]
            evidence["replayed_flush"] = request(effect_url, "/flush", {"id": "run-v1", "digest": d1})
            evidence["result_v1"] = until_state(effect_url, "run-v1", "Complete")
            assert evidence["result_v1"]["exit"] == "Success"

            # Deliberately try two live Effect owners against one run/state.
            # The result will be reported separately from the single-owner proof.
            other_port = free_port()
            try:
                effect4, other_url, other_info = launch_effect(other_port, effect_log)
                processes.append(effect4)
                evidence["second_owner"] = {"started": True, "pid": effect4.pid,
                                            "rss_kib": rss_kib(effect4)}
                race_start_actions = [(effect_url if index % 2 == 0 else other_url,
                                       "/start", {"id": "run-race", "digest": d2})
                                      for index in range(8)]
                with ThreadPoolExecutor(max_workers=8) as pool:
                    futures = [pool.submit(request, *item) for item in race_start_actions]
                    evidence["race_start_responses"] = [future.result() for future in futures]
                evidence["race_wait"] = until_state(effect_url, "run-race", "Suspended")
                race_row = request(effect_url, "/ledger", {"id": "run-race"})["body"]["run"]
                race_accept_body = {
                    "id": "run-race", "digest": d2, "revision": race_row["revision"],
                    "sha256": race_row["sha256"], "actor": race_row["reviewer"]}
                race_accept_actions = [(effect_url if index % 2 == 0 else other_url,
                                        "/accept", race_accept_body) for index in range(8)]
                with ThreadPoolExecutor(max_workers=8) as pool:
                    futures = [pool.submit(request, *item) for item in race_accept_actions]
                    evidence["race_accept_responses"] = [future.result() for future in futures]
                assert sorted(item["status"] for item in evidence["race_accept_responses"]) == [200] + [409] * 7
                race_flush_actions = [(effect_url if index % 2 == 0 else other_url,
                                       "/flush", {"id": "run-race", "digest": d2})
                                      for index in range(8)]
                with ThreadPoolExecutor(max_workers=8) as pool:
                    futures = [pool.submit(request, *item) for item in race_flush_actions]
                    evidence["race_flush_responses"] = [future.result() for future in futures]
                evidence["race_result"] = until_state(effect_url, "run-race", "Complete")
                assert evidence["race_result"]["exit"] == "Success"
                actions = [(effect_url, "/start", {"id": "run-v2", "digest": d2}),
                           (other_url, "/start", {"id": "run-v2", "digest": d2})] * 2 + \
                          [(effect_url, "/flush", {"id": "run-v2", "digest": d2}),
                           (other_url, "/flush", {"id": "run-v2", "digest": d2})] * 2
                with ThreadPoolExecutor(max_workers=8) as pool:
                    futures = [pool.submit(request, *item) for item in actions]
                    evidence["owner_race_responses"] = [future.result() for future in futures]
            except Exception as error:
                evidence["second_owner"] = {"started": False, "error": repr(error)}
            if request(effect_url, "/poll", {"id": "run-v2"})["body"]["state"] != "Complete":
                evidence["fallback_flush"] = request(effect_url, "/flush", {"id": "run-v2", "digest": d2})
            evidence["result_v2"] = until_state(effect_url, "run-v2", "Complete")
            assert evidence["result_v2"]["exit"] == "Success"
            receipts = {}
            runs = ("run-v1", "run-v2", "run-race") if evidence["second_owner"]["started"] else ("run-v1", "run-v2")
            for run in runs:
                receipts[run] = {
                    "assign": request(worker_url, "/fixture/actions/" + run + "-assign")["body"],
                    "review": request(quality_url, "/fixture/actions/" + run + "-review")["body"]}
                assert receipts[run]["assign"]["accepted_count"] == 1
                assert receipts[run]["review"]["accepted_count"] == 1
            evidence["receipts"] = receipts
            evidence["a2a_task_get"] = {}
            for role, url, action in (("capability", worker_url, receipts["run-v1"]["assign"]),
                                      ("quality", quality_url, receipts["run-v1"]["review"])):
                result = request(url, "/", {"jsonrpc": "2.0", "id": role + "-get",
                                            "method": "tasks/get", "params": {"id": action["task_id"]}})
                assert result["status"] == 200 and result["body"]["result"]["status"]["state"] == "completed"
                task = result["body"]["result"]
                assert task["artifacts"][0]["parts"][0]["data"] == action["artifact"]
                evidence["a2a_task_get"][role] = {"task_id": task["id"],
                                                    "status": task["status"]["state"],
                                                    "artifact": task["artifacts"][0]["parts"][0]["data"]}
            evidence["final_ledgers"] = {run: request(effect_url, "/ledger", {"id": run})["body"]
                                         for run in runs}
            assert all(value["delivery"] for value in evidence["final_ledgers"].values())
            assert all(value["outbox"] and value["outbox"]["state"] == "signaled"
                       for value in evidence["final_ledgers"].values())
            evidence["installed_node_modules_kib"] = disk_kib(HERE / "node_modules")
            evidence["runtime_state_kib"] = disk_kib(ROOT)
            evidence["owned_code_lines"] = {name: len((HERE / name).read_text().splitlines())
                                            for name in ("helper.mjs", "probe.py")}
            evidence["passed"] = True
        except Exception as error:
            evidence["passed"] = False
            evidence["error"] = repr(error)
            raise
        finally:
            for process in processes:
                stop(process)
            OBSERVED.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
            print(json.dumps({key: value for key, value in evidence.items() if key != "receipts"},
                             indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
