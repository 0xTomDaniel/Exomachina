"""Build a host-local copied Effect/Strands bundle in /tmp.

Build prerequisites are explicit: a verified official Node tarball, a uv-managed
Python runtime, uv, and the pinned source environments. The resulting launcher
does not read source files from this repository.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import platform
import shutil
import subprocess
import tarfile

HERE = Path(__file__).resolve().parent
S2 = HERE.parents[1] / "s2"
COMMON = HERE.parent / "common"
NODE_SHA256 = "880cf6f35eb9dea84b2373adba13b6023b50cc0decbad47b57824d146373265a"
NODE_MEMBER = "node-v26.0.0-darwin-arm64/bin/node"


def sha256(path: Path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--node-archive", type=Path, required=True)
    parser.add_argument("--python-runtime", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if platform.system() != "Darwin" or platform.machine() != "arm64":
        raise SystemExit("this bundle trial is pinned to macOS arm64")
    if output.exists():
        raise SystemExit(f"fresh output required: {output}")
    if sha256(args.node_archive) != NODE_SHA256:
        raise SystemExit("official Node archive SHA-256 mismatch")
    if not (args.python_runtime / "bin" / "python3.13").exists():
        raise SystemExit("uv-managed Python 3.13 runtime required")
    output.mkdir(parents=True)
    (output / "bin").mkdir()
    with tarfile.open(args.node_archive, "r:xz") as archive:
        source = archive.extractfile(NODE_MEMBER)
        if source is None:
            raise SystemExit("Node executable not found in archive")
        with (output / "bin" / "node").open("wb") as target:
            shutil.copyfileobj(source, target)
    (output / "bin" / "node").chmod(0o755)
    shutil.copytree(args.python_runtime, output / "python", symlinks=True)
    app = output / "app"
    (app / "effect").mkdir(parents=True)
    (app / "s2").mkdir()
    (app / "common").mkdir()
    effect_files = ("helper.mjs", "director_server.py", "package.json", "package-lock.json")
    s2_files = ("harness.py", "server.py", "child_adapter.py", "pyproject.toml", "uv.lock")
    for name in effect_files:
        shutil.copy2(HERE / name, app / "effect" / name)
    for name in s2_files:
        shutil.copy2(S2 / name, app / "s2" / name)
    shutil.copy2(COMMON / "harness_server.py", app / "common" / "harness_server.py")
    shutil.copytree(HERE / "node_modules", app / "effect" / "node_modules", symlinks=True)
    shutil.copy2(HERE / "bundle_launcher.py", output / "launcher.py")
    run = output / "run"
    run.write_text('#!/bin/sh\nset -eu\nBUNDLE_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)\n'
                   'exec "$BUNDLE_ROOT/venv/bin/python" "$BUNDLE_ROOT/launcher.py" "$@"\n')
    run.chmod(0o755)
    subprocess.run(["uv", "venv", "--python", str(output / "python" / "bin" / "python3.13"),
                    str(output / "venv")], check=True)
    env = {"UV_PROJECT_ENVIRONMENT": str(output / "venv")}
    import os
    subprocess.run(["uv", "sync", "--frozen", "--no-dev", "--python",
                    str(output / "python" / "bin" / "python3.13")],
                   cwd=app / "s2", env={**os.environ, **env}, check=True)
    python = output / "venv" / "bin" / "python"
    subprocess.run([str(python), "-c", "import strands,a2a,uvicorn; print('strands/a2a ready')"], check=True)
    subprocess.run([str(output / "bin" / "node"), "--check", str(app / "effect" / "helper.mjs")], check=True)
    manifest = {
        "node_archive_sha256": NODE_SHA256,
        "node_version": subprocess.check_output([str(output / "bin" / "node"), "--version"], text=True).strip(),
        "python_version": subprocess.check_output([str(python), "--version"], text=True).strip(),
        "package_lock_sha256": sha256(app / "effect" / "package-lock.json"),
        "uv_lock_sha256": sha256(app / "s2" / "uv.lock"),
        "source_sha256": {name: sha256(app / "effect" / name) for name in effect_files[:2]},
        "runtime_bundle": str(output),
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
