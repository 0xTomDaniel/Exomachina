"""De-duplicated macOS footprint of the post-freeze Effect A2A bundle at 0/2/10 waits."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
OLD = HERE.parents[1] / "2026-09-22"
COMMON = OLD / "arbitration" / "common"
sys.path.insert(0, str(HERE))
import definition  # noqa: E402
from director_probe import send, task_until  # noqa: E402
from probe import api, free_port, until  # noqa: E402
sys.path.insert(0, str(COMMON))
import service_probe  # noqa: E402


def footprint(processes):
    pids = [process.pid for process in processes]
    command = ["footprint", "-f", "bytes", "--noCategories"]
    for pid in pids:
        command += ["-p", str(pid)]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    found = re.search(r"Summary Footprint: (\d+) B", result.stdout)
    if not found:
        found = re.search(r"Footprint: (\d+) B", result.stdout)
    if not found:
        raise ValueError("footprint summary missing")
    return {"bytes": int(found[1]), "pid_count": len(pids),
            "method": "footprint -f bytes --noCategories with each bundle PID"}


def main():
    base = Path(tempfile.mkdtemp(prefix="exo-effect-resources-"))
    processes = []
    evidence = {"base": str(base), "scope": "post-freeze full local bundle, 0/2/10 nested waits",
                "topology": "Effect helper, Strands Director, two Strands capabilities, independent Strands Quality, participating release receiver; no production supervisor or opaque receiver"}

    def service(name, script, args):
        process, endpoint, health, logfile = service_probe.launch(base, name, script, args)
        processes.append(process)
        return endpoint, health

    def launch(name, command, url, env=None):
        log = (base / f"{name}.log").open("a")
        process = subprocess.Popen(command, cwd=HERE, env=env, stdout=log, stderr=subprocess.STDOUT)
        log.close()
        processes.append(process)
        def ready():
            if process.poll() is not None:
                raise RuntimeError(f"{name} exited: {process.returncode}")
            try:
                return api(url, "/health")[1]
            except Exception:
                return None
        return until(ready, label=f"{name} ready")

    try:
        cap = OLD / "decision-round" / "common" / "harness_server.py"
        source_url, source = service("source", cap, ["--role", "capability"])
        counter_url, counter = service("counter", cap, ["--role", "capability"])
        quality_url, quality = service("quality", COMMON / "quality_server.py", [])
        release_url, release = service("release", COMMON / "release_server.py",
                                        ["--mode", "participating"])
        def binding(role, url, info):
            return {"role": role, "url": url, "identity": info["identity"], "approved": True}
        bindings = {"source": binding("capability", source_url, source),
                    "counter": binding("capability", counter_url, counter),
                    "quality": binding("quality", quality_url, quality),
                    "release": binding("release", release_url, release)}
        (base / "approved.json").write_text(json.dumps(bindings))
        effect_port, director_port = free_port(), free_port()
        effect_url, director_url = f"http://127.0.0.1:{effect_port}", f"http://127.0.0.1:{director_port}"
        env = os.environ.copy()
        env.update(EFFECT_PARITY_ROOT=str(base / "effect"),
                   EFFECT_PARITY_APPROVED=str(base / "approved.json"),
                   EFFECT_PARITY_PORT=str(effect_port), EFFECT_PARITY_PYTHON=sys.executable)
        launch("effect", ["node", str(HERE / "helper.mjs")], effect_url, env)
        launch("director", [sys.executable, str(HERE / "director_server.py"),
            "--state", str(base / "director"), "--effect", effect_url,
            "--port", str(director_port)], director_url)
        template = json.loads((HERE / "definitions" / "visible-v3.json").read_text())
        child_digest = definition.digest(template["child"])
        template["root"]["nodes"]["invoke_child"]["child_digest"] = child_digest
        package = {"schema": 1, "root": template["root"],
                   "children": {child_digest: template["child"]}, "bindings": bindings}
        definition.validate(package, bindings)
        code, published = api(effect_url, "/publish", package)
        assert code == 200, published
        digest = published["package_digest"]

        evidence["zero_waits"] = footprint(processes)
        tasks = []
        for index in range(1, 11):
            run_id = f"resource-wait-{index}"
            started = send(director_url, {"op": "start", "run_id": run_id,
                "key": f"resource-start-{index}", "package_digest": digest,
                "resolution_after_repairs": 3})
            assert started["kind"] == "task"
            tasks.append((run_id, started["id"]))
            if index == 2:
                for _, task_id in tasks:
                    task_until(director_url, task_id, "input-required")
                evidence["two_waits"] = footprint(processes)
        for _, task_id in tasks:
            task_until(director_url, task_id, "input-required")
        evidence["ten_waits"] = footprint(processes)
        evidence["distinct_original_task_ids"] = len({task for _, task in tasks})
        evidence["package_digest"] = digest
        evidence["node_modules_kib"] = int(subprocess.check_output([
            "du", "-sk", str((HERE / "node_modules").resolve())], text=True).split()[0])
        evidence["runtime_state_kib"] = int(subprocess.check_output([
            "du", "-sk", str(base)], text=True).split()[0])
        evidence["freeze_verified"] = json.loads(subprocess.check_output([
            sys.executable, str(COMMON / "freeze.py"), "verify", str(HERE / "freeze.json")], text=True))
        evidence["passed"] = True
    except Exception as error:
        evidence["passed"] = False
        evidence["error"] = repr(error)
        raise
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try: process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        (HERE / "resources-observed.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
        print(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
