"""Assemble a host-local Dagu/Strands bundle, then run it from copied paths."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.request


HERE = Path(__file__).resolve().parent
S2 = HERE.parents[1] / "s2"
COMMON = HERE.parent / "common"
PINNED_DAGU = Path("/tmp/exomachina-countertrials/dagu/dagu")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1 << 20):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle", type=Path)
    args = parser.parse_args()
    bundle = args.bundle.resolve()
    if bundle.exists() and any(bundle.iterdir()):
        raise SystemExit(f"bundle target must be empty: {bundle}")
    bundle.mkdir(parents=True, exist_ok=True)
    if not PINNED_DAGU.exists():
        raise SystemExit("pinned Dagu binary must already be prepared")
    (bundle / "bin").mkdir()
    (bundle / "dagu").mkdir()
    (bundle / "common").mkdir()
    (bundle / "licenses" / "dagu").mkdir(parents=True)
    (bundle / "s2_project").mkdir()
    shutil.copy2(PINNED_DAGU, bundle / "bin" / "dagu")
    for name in ("LICENSE", "LICENSING.md"):
        shutil.copy2(PINNED_DAGU.parent / name, bundle / "licenses" / "dagu" / name)
    for name in ("pyproject.toml", "uv.lock", ".python-version"):
        shutil.copy2(S2 / name, bundle / "s2_project" / name)
    subprocess.run(["uv", "python", "install", "3.13.2", "--install-dir", str(bundle / "python")],
                   check=True)
    candidates = list((bundle / "python").glob("cpython-3.13.2-*/bin/python3.13"))
    if len(candidates) != 1:
        raise RuntimeError("expected exactly one bundled uv-managed CPython 3.13.2")
    managed_python = candidates[0]
    env = {**os.environ, "UV_PROJECT_ENVIRONMENT": str(bundle / "venv")}
    subprocess.run(["uv", "sync", "--frozen", "--python", str(managed_python)],
                   cwd=bundle / "s2_project", env=env, check=True)
    shutil.copytree(HERE / "definitions", bundle / "dagu" / "definitions")
    for name in ("adapter.py", "publisher.py", "director_server.py", "supervisor.py", "bundle_probe.py"):
        shutil.copy2(HERE / name, bundle / "dagu" / name)
    for name in ("client.py", "harness_server.py"):
        shutil.copy2(COMMON / name, bundle / "common" / name)
    inventory = {"dagu_sha256": sha256(bundle / "bin" / "dagu"),
                 "managed_python_sha256": sha256(managed_python),
                 "s2_lock_sha256": sha256(bundle / "s2_project" / "uv.lock"),
                 "dagu_license_sha256": sha256(bundle / "licenses" / "dagu" / "LICENSE"),
                 "dagu_licensing_sha256": sha256(bundle / "licenses" / "dagu" / "LICENSING.md"),
                 "source_files": sorted(str(p.relative_to(bundle)) for p in
                     (bundle / "dagu").rglob("*") if p.is_file()),
                 "python_venv_base": (bundle / "venv" / "pyvenv.cfg").read_text(),
                 "python_symlink": str((bundle / "venv" / "bin" / "python").readlink()),
                 "managed_python_path": str(managed_python)}
    (bundle / "inventory.json").write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n")
    run = bundle / "run"
    run.write_text("#!/bin/sh\nset -eu\nbase=$(CDPATH= cd -- \"$(dirname -- \"$0\")\" && pwd)\n"
                   "exec \"$base/venv/bin/python\" \"$base/dagu/supervisor.py\" \"${1:-$base/state}\"\n")
    run.chmod(0o755)
    command = [str(run)]
    with (bundle / "supervisor.log").open("ab") as output:
        supervisor = subprocess.Popen(command, stdout=output, stderr=output)
    try:
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            if supervisor.poll() is not None:
                raise RuntimeError(f"supervisor exited during startup: {supervisor.returncode}")
            registry_path = bundle / "state" / "endpoints.json"
            if registry_path.exists():
                try:
                    info = json.loads(registry_path.read_text())
                    urls = info["urls"]
                    for name in ("dagu", "capability", "quality", "director_a", "director_b"):
                        health = urls[name] + ("/api/v1/dags" if name == "dagu" else "/health")
                        with urllib.request.urlopen(health, timeout=.5):
                            pass
                    break
                except (OSError, KeyError, json.JSONDecodeError):
                    pass
            time.sleep(.1)
        else:
            raise TimeoutError("supervisor startup timed out")
        completed = subprocess.run([str(bundle / "venv" / "bin" / "python"),
                                    str(bundle / "dagu" / "bundle_probe.py"),
                                    str(bundle / "state")], text=True)
        if completed.returncode:
            raise SystemExit(completed.returncode)
    finally:
        if supervisor.poll() is None:
            supervisor.terminate()
            try:
                supervisor.wait(timeout=10)
            except subprocess.TimeoutExpired:
                supervisor.kill()
                supervisor.wait(timeout=5)
    size = subprocess.check_output(["du", "-sk", str(bundle)], text=True).strip()
    print(json.dumps({"bundle": str(bundle), "disk_kib": int(size.split()[0]),
                      "runtime_command": command, "python_symlink": inventory["python_symlink"]},
                     sort_keys=True))


if __name__ == "__main__":
    main()
