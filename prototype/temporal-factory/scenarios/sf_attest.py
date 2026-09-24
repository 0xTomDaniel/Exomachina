"""Post-write G-4 attestation of the exact delivered single-factory evidence."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import subprocess
from pathlib import Path
from uuid import uuid4

from common import ROOT
from live_authoring import candidate_files, positive_control, scan_paths

REPO = ROOT.parents[1]
SENSITIVE = {"token", "actor", "authorized_actor", "owner_epoch", "epoch",
             "package", "closure", "bindings", "input"}
BEARER = re.compile(r"\bBearer\s+[^\s\"']+", re.I)
JWT = re.compile(r"\beyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def recorded(path: Path) -> str:
    path = path.resolve()
    try:
        return str(path.relative_to(REPO))
    except ValueError:
        return str(path)


def resolve(path: str) -> Path:
    value = Path(path)
    return value if value.is_absolute() else REPO / value


def referenced_files(evidence: dict) -> set[Path]:
    """Resolve every exported report/history and other file path in the record."""
    found: set[Path] = set()
    def walk(value: object) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if key in {"report_path", "history_path", "store_path"} and isinstance(child, str):
                    found.add(resolve(child))
                else:
                    walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
    walk(evidence)
    return found


def exported_files(evidence_path: Path) -> list[Path]:
    stem = evidence_path.stem
    return sorted(path for path in evidence_path.parent.rglob("*") if path.is_file() and
        not path.name.endswith("-attestation.json") and
        (path == evidence_path or path.name.startswith(stem + "-") or
         path.relative_to(evidence_path.parent).parts[0].startswith(stem + "-")))


def verify_redaction(paths: list[Path]) -> dict:
    """Inspect raw JSON and recursively decoded base64 JSON, without retaining values."""
    counts = {"files_checked": 0, "decoded_payloads_checked": 0,
              "redaction_objects_checked": 0, "violations": 0, "pass": False}
    def walk(value: object, depth: int = 0) -> None:
        if depth > 20:
            counts["violations"] += 1
            return
        if isinstance(value, dict):
            for key, child in value.items():
                if key in SENSITIVE:
                    if (isinstance(child, dict) and set(child) ==
                            {"redacted_sha256", "decoded_payload_sha256"} and
                            isinstance(child["redacted_sha256"], str) and
                            re.fullmatch(r"[0-9a-f]{64}", child["redacted_sha256"]) and
                            isinstance(child["decoded_payload_sha256"], list) and
                            all(isinstance(x, str) and re.fullmatch(r"[0-9a-f]{64}", x)
                                for x in child["decoded_payload_sha256"])):
                        counts["redaction_objects_checked"] += 1
                    else:
                        counts["violations"] += 1
                else:
                    walk(child, depth + 1)
        elif isinstance(value, list):
            for child in value:
                walk(child, depth + 1)
        elif isinstance(value, str):
            if BEARER.search(value) or JWT.search(value):
                counts["violations"] += 1
            try:
                decoded = base64.b64decode(value, validate=True)
                nested = json.loads(decoded)
            except (ValueError, UnicodeDecodeError):
                return
            if isinstance(nested, (dict, list)):
                counts["decoded_payloads_checked"] += 1
                walk(nested, depth + 1)
    for path in paths:
        counts["files_checked"] += 1
        raw = path.read_bytes()
        if BEARER.search(raw.decode("utf-8", errors="ignore")) or JWT.search(raw.decode("utf-8", errors="ignore")):
            counts["violations"] += 1
        if path.suffix == ".json":
            try:
                walk(json.loads(raw))
            except (ValueError, UnicodeDecodeError):
                counts["violations"] += 1
    counts["pass"] = counts["files_checked"] == len(paths) and counts["violations"] == 0
    return counts


def packet_provenance(home: Path) -> dict:
    packet_path = home / "instances" / "report-factory" / "evidence_packet.json"
    if not packet_path.is_file():
        return {"observed": False, "items_checked": 0, "matched": 0}
    packet = json.loads(packet_path.read_text())
    source = subprocess.check_output(["git", "show",
        "2d609e3:prototype/temporal-factory/QUALIFICATION.md"], cwd=REPO, text=True)
    lines = source.splitlines(keepends=True)
    checked = matched = trimmed = 0
    for item in packet.get("items") or []:
        source_ref = item.get("source") or ""
        match = re.fullmatch(r"prototype/temporal-factory/QUALIFICATION\.md#L(\d+)-L(\d+)", source_ref)
        checked += 1
        if not match:
            continue
        start, end = map(int, match.groups())
        excerpt = "".join(lines[start - 1:end])
        text = item.get("text")
        if isinstance(text, str) and text.rstrip("\r\n") == excerpt.rstrip("\r\n"):
            matched += 1
            trimmed += text != excerpt
    return {"observed": True, "items_checked": checked, "matched": matched,
            "terminal_newline_trimmed": trimmed}


def attest(evidence_path: Path, home: Path, label: str, console: Path | None = None) -> dict:
    evidence_path = evidence_path.resolve()
    home = home.resolve()
    console = console.resolve() if console is not None else None
    evidence = json.loads(evidence_path.read_text())
    evidence_hash = digest(evidence_path)
    model_home = (home / "model" if evidence.get("provider") == "scripted" else
                  Path(__import__("model_broker").DEFAULT_HOME))
    exports = exported_files(evidence_path)
    references = referenced_files(evidence)
    output_path = evidence_path.with_name(evidence_path.stem + "-attestation.json")
    candidates = {path.resolve() for path in
        (set(candidate_files()) | set(exports) | references | {evidence_path})} - {output_path}
    if console is not None:
        candidates.add(console)
    existing = sorted(path for path in candidates if path.is_file())
    manifest = [{"path": recorded(path), "sha256": digest(path)} for path in existing]
    lookup = {row["path"]: row["sha256"] for row in manifest}
    required = references | {evidence_path} | set(exports)
    if console is not None:
        required.add(console)
    manifest_bound = (all(path.is_file() and lookup.get(recorded(path)) == digest(path)
        for path in required) and lookup.get(recorded(evidence_path)) == evidence_hash)
    control = positive_control(home.with_name(home.name + "-attest-" + uuid4().hex))
    paths = [home, evidence_path.parent, model_home, *existing]
    scan = scan_paths(paths)
    stable = all(digest(path) == lookup[recorded(path)] for path in existing)
    redaction = verify_redaction(exports)
    terms = {"positive_control_detected": bool((control.get("scan") or {}).get("hits")),
             "zero_real_hits": scan.get("hits") == [],
             "scan_coverage": isinstance(scan.get("files_scanned"), int) and
                scan["files_scanned"] >= len(manifest),
             "manifest_bound": manifest_bound and stable,
             "redaction": redaction["pass"],
             "checker_bound": evidence.get("checker_sha256") == digest(ROOT / "scenarios" / "single_factory.py")}
    result = {"attempt_label": label, "evidence_sha256": evidence_hash,
        "checker_sha256": digest(ROOT / "scenarios" / "single_factory.py"),
        "attest_sha256": digest(Path(__file__)), "manifest": manifest,
        "scan_summary": {"files_scanned": scan.get("files_scanned"),
                         "hit_count": len(scan.get("hits") or [])},
        "positive_control": {"detected": terms["positive_control_detected"]},
        "redaction": redaction, "packet_provenance": packet_provenance(home),
        "g4_final": {"pass": all(terms.values()), "terms": terms}}
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--home", required=True, type=Path)
    parser.add_argument("--console", type=Path)
    parser.add_argument("--attempt-label", required=True)
    args = parser.parse_args()
    result = attest(args.evidence, args.home, args.attempt_label, args.console)
    path = args.evidence.with_name(args.evidence.stem + "-attestation.json")
    path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"attestation": str(path), "g4_final": result["g4_final"]}))
    return 0 if result["g4_final"]["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
