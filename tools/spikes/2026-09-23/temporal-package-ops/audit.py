"""Offline installed-prefix dependency, absolute-reference and size audit."""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from pathlib import Path

MACHO = {bytes.fromhex(x) for x in ("feedface", "feedfacf", "cefaedfe", "cffaedfe",
                                   "cafebabe", "bebafeca", "cafebabf", "bfbafeca")}


def audit(prefix: Path) -> dict:
    prefix = prefix.resolve()
    binaries = {}
    external = {}
    symlinks = {}
    shebangs = {}
    for path in prefix.rglob("*"):
        rel = str(path.relative_to(prefix))
        if path.is_symlink():
            target = path.resolve()
            if not target.is_relative_to(prefix):
                symlinks[rel] = str(target)
            continue
        if not path.is_file():
            continue
        with path.open("rb") as f:
            head = f.read(256)
        if head.startswith(b"#!"):
            line = head.split(b"\n", 1)[0].decode(errors="replace")
            if "/opt/homebrew" in line or "/Users/tomdaniel" in line or "/tmp/" in line:
                if str(prefix) not in line:
                    shebangs[rel] = line
        if head[:4] not in MACHO:
            continue
        result = subprocess.run(["otool", "-L", str(path)], capture_output=True, text=True)
        deps = []
        if result.returncode == 0:
            deps = [line.strip().split(" (", 1)[0] for line in result.stdout.splitlines()[1:]
                    if line.startswith("\t")]
        binaries[rel] = {"otool_exit": result.returncode, "dependencies": deps}
        bad = [dep for dep in deps if dep.startswith("/") and not dep.startswith(str(prefix))
               and not dep.startswith("/usr/lib/") and not dep.startswith("/System/Library/")]
        if bad:
            external[rel] = bad
    du = lambda path: int(subprocess.check_output(["du", "-sk", str(path)], text=True).split()[0])
    secrets = {str(p.relative_to(prefix)): oct(p.stat().st_mode & 0o777)
               for p in (prefix / "state/secrets").iterdir()}
    return {"schema": "exomachina.temporal.package-audit/1", "prefix": str(prefix),
        "bundle_disk_kib": du(prefix), "state_disk_kib": du(prefix / "state"),
        "binary_count": len(binaries), "binaries": binaries,
        "external_dylib_references": external, "external_symlinks": symlinks,
        "external_shebangs": shebangs, "secret_modes": secrets,
        "state_mode": oct((prefix / "state").stat().st_mode & 0o777),
        "socket_dir_mode": oct((prefix / "state/pgsocket").stat().st_mode & 0o777)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("prefix", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.prefix)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({k: result[k] for k in ("bundle_disk_kib", "state_disk_kib", "binary_count")}, sort_keys=True))
