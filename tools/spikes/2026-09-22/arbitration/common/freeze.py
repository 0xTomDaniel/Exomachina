"""Record and later verify implementation bytes before a withheld composition.

The evaluator must receive the manifest before disclosing the additional case;
a local timestamp/hash alone cannot attest to that ordering.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[5]
SKIP_DIRS = {".git", ".venv", "node_modules", "__pycache__"}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError as error:
        raise ValueError(f"path is outside Exomachina: {path}") from error


def inventory(targets: list[Path], output: Path | None = None):
    seen: dict[str, dict] = {}
    for target in targets:
        if not target.exists():
            raise ValueError(f"missing path: {target}")
        files = [target] if target.is_file() else target.rglob("*")
        for item in files:
            if any(part in SKIP_DIRS for part in item.parts) or not item.is_file():
                continue
            if item.is_symlink():
                raise ValueError(f"symlink in freeze inventory: {item}")
            if output and item.resolve() == output.resolve():
                continue
            name = relative(item)
            data = item.read_bytes()
            seen[name] = {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
    if not seen:
        raise ValueError("no implementation files selected")
    return dict(sorted(seen.items()))


def capture(targets: list[Path], output: Path):
    roots = sorted({relative(path) for path in targets})
    files = inventory(targets, output)
    body = {"schema": "exomachina.arbitration.freeze/1", "created_utc":
            datetime.now(timezone.utc).isoformat(), "roots": roots, "files": files}
    body["inventory_sha256"] = hashlib.sha256(canonical(files).encode()).hexdigest()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")
    return body


def verify(manifest: Path):
    body = json.loads(manifest.read_text())
    if body.get("schema") != "exomachina.arbitration.freeze/1":
        raise ValueError("unknown freeze schema")
    expected = body["files"]
    roots = [ROOT / name for name in body["roots"]]
    actual = inventory(roots, manifest)
    if actual != expected:
        removed = sorted(set(expected) - set(actual))
        added = sorted(set(actual) - set(expected))
        changed = sorted(name for name in set(actual) & set(expected)
                         if actual[name] != expected[name])
        raise ValueError(f"freeze changed: added={added}, removed={removed}, changed={changed}")
    digest = hashlib.sha256(canonical(actual).encode()).hexdigest()
    if digest != body["inventory_sha256"]:
        raise ValueError("freeze digest mismatch")
    return {"verified": True, "inventory_sha256": digest, "files": len(actual)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="action", required=True)
    save = sub.add_parser("capture")
    save.add_argument("--out", type=Path, required=True)
    save.add_argument("paths", nargs="+", type=Path)
    check = sub.add_parser("verify")
    check.add_argument("manifest", type=Path)
    args = parser.parse_args()
    value = capture(args.paths, args.out) if args.action == "capture" else verify(args.manifest)
    print(json.dumps(value, indent=2, sort_keys=True))
