"""Post-freeze integrated Strands Director Task and lost release reply probe."""
from __future__ import annotations

import copy
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

import httpx

HERE = Path(__file__).resolve().parent
OLD = HERE.parents[1] / "2026-09-22"
COMMON = OLD / "arbitration" / "common"
sys.path.insert(0, str(HERE))
import definition  # noqa: E402
from probe import api, free_port, until  # noqa: E402
sys.path.insert(0, str(COMMON))
import service_probe  # noqa: E402

TOKEN = {"authorization": "Bearer director-test-token"}


def rpc(url, method, params):
    response = httpx.post(url + "/", headers=TOKEN, timeout=30,
                          json={"jsonrpc": "2.0", "id": str(uuid4()),
                                "method": method, "params": params})
    response.raise_for_status()
    body = response.json()
    if "error" in body:
        raise RuntimeError(body["error"])
    return body["result"]


def send(url, command, task_id=None):
    message = {"role": "user", "messageId": str(uuid4()),
               "parts": [{"kind": "data", "data": command}]}
    if task_id:
        message["taskId"] = task_id
    return rpc(url, "message/send", {"message": message})


def task_until(url, task_id, state, seconds=50):
    def matching():
        task = rpc(url, "tasks/get", {"id": task_id})
        return task if task["status"]["state"] == state else None
    return until(matching,
                 seconds=seconds, label=f"original Task {task_id} {state}")


class LostReplyProxy:
    """Test-network shim: forward one release commit, then drop its HTTP reply."""
    def __init__(self, target):
        self.target = target
        self.forwarded_releases = 0
        self.dropped_replies = 0
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                self.forward()

            def do_POST(self):
                self.forward()

            def forward(self):
                length = int(self.headers.get("Content-Length", "0"))
                payload = self.rfile.read(length) if length else None
                req = Request(owner.target + self.path, data=payload, method=self.command,
                              headers={"Authorization": self.headers.get("Authorization", ""),
                                       "Content-Type": self.headers.get("Content-Type", "application/json")})
                try:
                    with urlopen(req, timeout=20) as upstream:
                        body = upstream.read()
                        status = upstream.status
                except HTTPError as upstream:
                    body = upstream.read()
                    status = upstream.code
                if self.command == "POST" and self.path == "/release":
                    owner.forwarded_releases += 1
                    if owner.dropped_replies == 0:
                        owner.dropped_replies += 1
                        self.close_connection = True
                        try: self.connection.shutdown(socket.SHUT_RDWR)
                        except OSError: pass
                        self.connection.close()
                        return
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def main():
    base = Path(tempfile.mkdtemp(prefix="exo-effect-director-parity-"))
    processes = []
    proxy = None
    evidence = {"base": str(base), "scope": "post-freeze Director A2A integration and participating release lost reply"}
    effect_port, director_port = free_port(), free_port()
    effect_url, director_url = f"http://127.0.0.1:{effect_port}", f"http://127.0.0.1:{director_port}"

    def service(name, script, args):
        process, endpoint, health, logfile = service_probe.launch(base, name, script, args)
        processes.append(process)
        return process, endpoint, health

    def launch(name, argv, url, env=None):
        log = (base / f"{name}.log").open("a")
        process = subprocess.Popen(argv, cwd=HERE, env=env, stdout=log, stderr=subprocess.STDOUT)
        log.close()
        processes.append(process)
        def ready():
            if process.poll() is not None:
                raise RuntimeError(f"{name} exited during startup: {process.returncode}")
            try:
                response = httpx.get(url + "/health", timeout=.5)
                return response.json() if response.status_code == 200 else None
            except httpx.HTTPError:
                return None
        info = until(ready, label=f"{name} ready")
        return process, info

    def effect_start():
        env = os.environ.copy()
        env.update(EFFECT_PARITY_ROOT=str(base / "effect"),
                   EFFECT_PARITY_APPROVED=str(base / "approved.json"),
                   EFFECT_PARITY_PORT=str(effect_port), EFFECT_PARITY_PYTHON=sys.executable)
        return launch("effect", ["node", str(HERE / "helper.mjs")], effect_url, env)

    def director_start():
        return launch("director", [sys.executable, str(HERE / "director_server.py"),
            "--state", str(base / "director"), "--effect", effect_url,
            "--port", str(director_port)], director_url)

    def child(run_id):
        parent = api(effect_url, "/poll", {"id": run_id})[1]["run"]
        return api(effect_url, "/poll", {"id": parent["child_id"]})[1]["run"] if parent["child_id"] else None

    try:
        harness = OLD / "decision-round" / "common" / "harness_server.py"
        _, source_url, source_info = service("source", harness, ["--role", "capability"])
        _, counter_url, counter_info = service("counter", harness, ["--role", "capability"])
        _, quality_url, quality_info = service("quality", COMMON / "quality_server.py", [])
        _, release_url, release_info = service("release", COMMON / "release_server.py",
                                               ["--mode", "participating"])
        proxy = LostReplyProxy(release_url)
        def binding(role, endpoint, info):
            return {"role": role, "url": endpoint,
                    "identity": info["identity"], "approved": True}
        bindings = {
            "source": binding("capability", source_url, source_info),
            "counter": binding("capability", counter_url, counter_info),
            "quality": binding("quality", quality_url, quality_info),
            "release": binding("release", proxy.url, release_info),
        }
        (base / "approved.json").write_text(json.dumps(bindings))
        helper, effect_info = effect_start()
        director, director_info = director_start()
        assert len({source_info["identity"], counter_info["identity"],
                    quality_info["identity"], director_info["identity"]}) == 4
        template = json.loads((HERE / "definitions" / "visible-v3.json").read_text())
        child_digest = definition.digest(template["child"])
        template["root"]["nodes"]["invoke_child"]["child_digest"] = child_digest
        package = {"schema": 1, "root": template["root"],
                   "children": {child_digest: template["child"]}, "bindings": bindings}
        definition.validate(package, bindings)
        (base / "native-v3-package.json").write_text(json.dumps(package, indent=2, sort_keys=True) + "\n")
        code, published = api(effect_url, "/publish", package)
        assert code == 200, published
        package_digest = published["package_digest"]

        success_command = {"op": "start", "run_id": "director-effect-success",
                           "key": "director-effect-success-start",
                           "package_digest": package_digest, "resolution_after_repairs": 1}
        started = send(director_url, success_command)
        assert started["kind"] == "task"
        success_task_id = started["id"]
        completed = task_until(director_url, success_task_id, "completed")
        output = completed["artifacts"][0]["parts"][0]["data"]
        assert output["revision"] == "r2" and output["release_receipt"]["accepted_effect_count"] == 1
        success_child = child(success_command["run_id"])
        success_ledger = api(effect_url, "/ledger", {"id": success_child["id"]})[1]["run"]
        assert json.loads(success_ledger["acceptance_json"])["revision"] == "r2"
        assert proxy.forwarded_releases == proxy.dropped_replies == 1
        assert output["release_receipt"]["attempts"] == 1
        evidence["accepted_task"] = {"original_task_id": success_task_id,
            "revision": output["revision"], "artifact_sha256": output["sha256"],
            "child_id": success_child["id"], "quality_task_id": json.loads(
                success_ledger["acceptance_json"])["quality_task_id"],
            "release_receiver_attempts": output["release_receipt"]["attempts"],
            "release_effects": output["release_receipt"]["accepted_effect_count"]}
        evidence["post_commit_lost_release_reply"] = {"proxy_forwarded": proxy.forwarded_releases,
            "proxy_dropped": proxy.dropped_replies, "receiver_attempts": 1,
            "receiver_effects": 1, "task_completed": True}

        exhausted_command = {"op": "start", "run_id": "director-effect-exhausted",
                             "key": "director-effect-exhausted-start",
                             "package_digest": package_digest, "resolution_after_repairs": 3}
        started_exhausted = send(director_url, exhausted_command)
        assert started_exhausted["kind"] == "task"
        original_task_id = started_exhausted["id"]
        waiting = task_until(director_url, original_task_id, "input-required")
        waiting_child = child(exhausted_command["run_id"])
        assert waiting_child["revision"] == "r3" and waiting_child["repair_count"] == 2
        evidence["waiting_task"] = {"original_task_id": original_task_id,
            "child_id": waiting_child["id"], "revision": "r3",
            "definition_digest": waiting_child["digest"]}

        director.kill()
        director.wait(timeout=10)
        director, director_restart_info = director_start()
        assert director_restart_info["identity"] == director_info["identity"]
        assert director_restart_info["incarnation"] == director_info["incarnation"] + 1
        restored = rpc(director_url, "tasks/get", {"id": original_task_id})
        assert restored["status"]["state"] == "input-required"
        helper.kill()
        helper.wait(timeout=10)
        helper, _ = effect_start()
        restored_after_helper = rpc(director_url, "tasks/get", {"id": original_task_id})
        assert restored_after_helper["status"]["state"] == "input-required"
        assert child(exhausted_command["run_id"])["id"] == waiting_child["id"]
        evidence["original_task_restored"] = {"same_task_id": original_task_id,
            "director_identity_pinned": True, "helper_restart": True,
            "child_id_pinned": True}

        decision = {"op": "decide", "run_id": exhausted_command["run_id"],
                    "package_digest": package_digest, "child_id": waiting_child["id"],
                    "child_digest": waiting_child["digest"],
                    "revision": waiting_child["revision"], "sha256": waiting_child["sha256"]}
        stale = send(director_url, {**decision, "revision": "r1"}, original_task_id)
        assert stale["kind"] == "message" and "error" in stale["parts"][0]["data"]
        assert rpc(director_url, "tasks/get", {"id": original_task_id})["status"]["state"] == "input-required"
        decided = send(director_url, decision, original_task_id)
        assert decided["kind"] == "task"
        aborted_task = task_until(director_url, original_task_id, "completed")
        assert not aborted_task.get("artifacts")
        parent_result = api(effect_url, "/poll", {"id": exhausted_command["run_id"]})[1]["value"]
        assert parent_result["status"] == "aborted"
        child_ledger = api(effect_url, "/ledger", {"id": waiting_child["id"]})[1]
        assert child_ledger["run"]["acceptance_json"] is None
        assert not any(item["kind"] == "release" for item in child_ledger["events"])
        evidence["authorized_exact_r3_abort"] = {"stale_rejected": True,
            "original_task_completed": True, "child_revision": "r3",
            "acceptances": 0, "releases": 0}
        evidence["freeze_verified"] = json.loads(subprocess.check_output([
            sys.executable, str(COMMON / "freeze.py"), "verify", str(HERE / "freeze.json")], text=True))
        evidence["passed"] = True
    except Exception as error:
        evidence["passed"] = False
        evidence["error"] = repr(error)
        raise
    finally:
        if proxy: proxy.close()
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try: process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        (HERE / "director-observed.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
        print(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
