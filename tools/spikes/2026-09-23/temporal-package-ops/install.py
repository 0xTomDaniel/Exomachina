#!/usr/bin/env python3
"""Install the bounded native trial into a fresh /tmp/exo-tq-package-* prefix."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import secrets
import shutil
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
TEMPORAL_SOURCE = Path("/tmp/exomachina-countertrials/temporal/runtime")
PG_SOURCE = Path("/opt/homebrew/opt/postgresql@16").resolve()
PY_SOURCE = Path("/Users/tomdaniel/.local/share/uv/python/cpython-3.12.9-macos-aarch64-none")
VENV_SOURCE = REPO / "tools/spikes/2026-09-22/arbitration/temporal/.venv"
CLI_SOURCE = Path("/tmp/exomachina-temporal-evaluation/temporal")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def install(prefix: Path) -> dict:
    if not str(prefix).startswith("/tmp/exo-tq-package-"):
        raise ValueError("prefix must start /tmp/exo-tq-package-")
    if prefix.exists():
        raise FileExistsError(prefix)
    old_umask = os.umask(0o077)
    try:
        prefix.mkdir(mode=0o700)
        (prefix / "bin").mkdir()
        (prefix / "temporal").mkdir()
        for name in ("temporal-server", "temporal-sql-tool"):
            shutil.copy2(TEMPORAL_SOURCE / name, prefix / "temporal" / name)
        shutil.copytree(TEMPORAL_SOURCE / "temporal-1.32.0", prefix / "temporal/temporal-1.32.0")
        shutil.copytree(TEMPORAL_SOURCE / "config", prefix / "temporal/config")
        shutil.copytree(PG_SOURCE, prefix / "pg", symlinks=True)
        shutil.copytree(PY_SOURCE, prefix / "python", symlinks=True)
        shutil.copytree(VENV_SOURCE, prefix / "venv", symlinks=True)
        (prefix / "venv/bin/python").unlink()
        (prefix / "venv/bin/python").symlink_to(prefix / "python/bin/python3.12")
        config = prefix / "venv/pyvenv.cfg"
        config.write_text(config.read_text().replace(str(PY_SOURCE / "bin"), str(prefix / "python/bin")))
        shutil.copy2(CLI_SOURCE, prefix / "bin/temporal")
        shutil.copytree(HERE, prefix / "app", ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "observed.json", "result.md"))
        launcher = prefix / "bin/exo-factory"
        launcher.write_text("#!/bin/sh\nexec '" + str(prefix / "venv/bin/python") + "' '" + str(prefix / "app/launcher.py") + "' \"$@\"\n")
        launcher.chmod(0o700)
        state = prefix / "state"
        state.mkdir(mode=0o700)
        sd = state / "secrets"
        sd.mkdir(mode=0o700)
        for name in ("postgres-password", "temporal-password", "director-token"):
            target = sd / name
            target.write_text(secrets.token_hex(24) + "\n")
            target.chmod(0o600)
        dependencies = subprocess.check_output([str(prefix / "venv/bin/python"), "-c",
            "import importlib.metadata as m; print('\\n'.join(sorted(d.metadata['Name']+'=='+d.version for d in m.distributions())))"], text=True)
        (prefix / "dependencies.lock").write_text(dependencies)
        files = {str(p.relative_to(prefix)): sha(p) for p in
                 (prefix / "temporal/temporal-server", prefix / "temporal/temporal-sql-tool",
                  prefix / "pg/bin/postgres", prefix / "python/bin/python3.12", prefix / "bin/temporal")}
        manifest = {"schema": "exomachina.temporal.package-prototype/1", "architecture": "macos-arm64",
                    "temporal": "1.32.0", "postgres": "16.15", "python": "3.12.9",
                    "temporalio": "1.33.0", "prefix": str(prefix), "binaries_sha256": files,
                    "source": "existing pinned local binaries; no download"}
        (prefix / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        return manifest
    finally:
        os.umask(old_umask)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(install(args.prefix), sort_keys=True))
