"""Pure, content-addressed run closure and atomic publication pointer.

The closure values travel in Temporal start history. Verification is pure so a
Workflow can repeat it before scheduling each Activity and child Workflow.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
from pathlib import Path

from definition import validate


DEPLOYMENT = "exo-factory"
QUEUE = "exo-factory"
NAMESPACE = "exomachina"
INTERPRETER_FILES = (
    "worker.py", "factory.py", "adapter.py", "binding.py", "buildinfo.py",
    "definition.py", "report_contract.py", "fixture.py", "failure_projection.py",
    "incident_projection.py", "quality_authority.py", "a2a_outcome.py",
    "long_client.py", "agent_binding.py", "receiver_client.py",
)


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def source_digest(directory: Path) -> str:
    files = {name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
             for name in INTERPRETER_FILES}
    return digest(files)


def build_id_for(code_digest: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}", code_digest):
        raise ValueError("invalid worker source digest")
    return "b-" + code_digest[:12]


def _verify_report_capabilities(package: dict, contracts: dict) -> None:
    expected = {}
    for child in package["children"].values():
        for node in child["nodes"].values():
            if node["type"] == "parallel":
                for branch in node["branches"].values():
                    expected[branch["service"]] = branch["capability"]
            elif node["type"] == "synthesize":
                expected[node["service"]] = "report_synthesis@1"
    for name, binding in package["bindings"].items():
        if binding["role"] == "quality":
            expected[name] = "report_quality_review@1"
    for name, capability in expected.items():
        if contracts.get(name, {}).get("capability") != capability:
            raise ValueError(f"service {name} lacks pinned {capability} capability")


def make_manifest(package: dict, contracts: dict, quality_policy: dict,
                  *, build_id: str, code_digest: str, python: str,
                  temporalio: str) -> dict:
    validate(package)
    if build_id != build_id_for(code_digest):
        raise ValueError("invalid immutable worker build ID")
    if set(contracts) != set(package["bindings"]):
        raise ValueError("one contract per bound service required")
    _verify_report_capabilities(package, contracts)
    if not isinstance(quality_policy, dict) or not quality_policy:
        raise ValueError("Quality policy required")
    if not re.fullmatch(r"[0-9a-f]{64}", code_digest):
        raise ValueError("code digest required")
    return {
        "schema": 1,
        "package_digest": digest(package),
        "root_digest": digest(package["root"]),
        "child_digests": sorted(package["children"]),
        "services": {name: {"binding_digest": digest(package["bindings"][name]),
                             "contract_digest": digest(contracts[name])}
                     for name in sorted(contracts)},
        "quality_policy_digest": digest(quality_policy),
        "interpreter": {"deployment": DEPLOYMENT, "build_id": build_id,
                        "source_digest": code_digest, "python": python,
                        "temporalio": temporalio},
    }


def verify_closure(closure: dict, package: dict, *, build_id: str,
                   definition_digest: str, document: dict | None = None) -> str:
    """Return manifest digest or fail closed on any changed dependency."""
    if set(closure) != {"manifest", "manifest_digest", "contracts", "quality_policy"}:
        raise ValueError("incomplete run closure")
    manifest = closure["manifest"]
    if digest(manifest) != closure["manifest_digest"]:
        raise ValueError("manifest digest mismatch")
    if validate(package) != manifest["package_digest"]:
        raise ValueError("package changed")
    if definition_digest not in {manifest["root_digest"], *manifest["child_digests"]}:
        raise ValueError("definition outside closure")
    if document is not None:
        expected_document = (package["root"] if definition_digest == manifest["root_digest"]
                             else package["children"].get(definition_digest))
        if digest(document) != definition_digest or document != expected_document:
            raise ValueError("document differs from pinned definition")
    interpreter = manifest["interpreter"]
    if (interpreter["deployment"] != DEPLOYMENT
            or interpreter["build_id"] != build_id
            or build_id_for(interpreter["source_digest"]) != build_id):
        raise ValueError("wrong interpreter build")
    if set(closure["contracts"]) != set(manifest["services"]):
        raise ValueError("service contract set changed")
    _verify_report_capabilities(package, closure["contracts"])
    for name, entry in manifest["services"].items():
        if digest(package["bindings"][name]) != entry["binding_digest"]:
            raise ValueError("service binding changed")
        if digest(closure["contracts"][name]) != entry["contract_digest"]:
            raise ValueError("service contract changed")
    if digest(closure["quality_policy"]) != manifest["quality_policy_digest"]:
        raise ValueError("Quality policy changed")
    return closure["manifest_digest"]


def _atomic_json(path: Path, value: dict) -> None:
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    with tmp.open("xb") as stream:
        stream.write(canonical(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class PublicationStore:
    def __init__(self, catalog: Path):
        self.catalog = catalog
        catalog.mkdir(parents=True, exist_ok=True)

    def publish(self, package: dict, closure: dict, *, label: str) -> str:
        if not isinstance(label, str) or not label.strip():
            raise ValueError("publication label required")
        manifest = closure["manifest"]
        key = verify_closure(closure, package,
                             build_id=manifest["interpreter"]["build_id"],
                             definition_digest=manifest["root_digest"])
        path = self.catalog / f"publication-{key}.json"
        record = {"manifest_digest": key, "package_digest": manifest["package_digest"],
                  "build_id": manifest["interpreter"]["build_id"],
                  "label": label,
                  "closure": closure}
        with (self.catalog / "publication.lock").open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            for prior_path in self.catalog.glob("publication-*.json"):
                prior = json.loads(prior_path.read_text())
                if (prior["build_id"] == record["build_id"] and
                        prior["closure"]["manifest"]["interpreter"]["source_digest"] !=
                        manifest["interpreter"]["source_digest"]):
                    raise ValueError("worker build ID already bound to different source")
            if path.exists():
                if json.loads(path.read_text()) != record:
                    raise ValueError("immutable publication conflict")
            else:
                with path.open("xb") as stream:
                    stream.write(canonical(record))
                    stream.flush()
                    os.fsync(stream.fileno())
            fcntl.flock(lock, fcntl.LOCK_UN)
        return key

    def activate(self, manifest_digest: str, *, registered_version: str,
                 registered_source_digest: str) -> dict:
        path = self.catalog / f"publication-{manifest_digest}.json"
        record = json.loads(path.read_text())
        if record["manifest_digest"] != manifest_digest:
            raise ValueError("publication key mismatch")
        expected_version = f"{DEPLOYMENT}.{record['build_id']}"
        if registered_version != expected_version:
            raise ValueError("target worker version not registered")
        if registered_source_digest != record["closure"]["manifest"]["interpreter"]["source_digest"]:
            raise ValueError("target worker code differs from publication")
        with (self.catalog / "activation.lock").open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            _atomic_json(self.catalog / "active-publication.json",
                         {key: record[key] for key in ("manifest_digest", "package_digest", "build_id")})
            fcntl.flock(lock, fcntl.LOCK_UN)
        return record

    def active(self) -> dict:
        pointer = json.loads((self.catalog / "active-publication.json").read_text())
        record = self.get(pointer["manifest_digest"])
        if {key: record[key] for key in pointer} != pointer:
            raise ValueError("activation pointer conflict")
        return record

    def get(self, manifest_digest: str) -> dict:
        record = json.loads((self.catalog / f"publication-{manifest_digest}.json").read_text())
        if record["manifest_digest"] != manifest_digest:
            raise ValueError("publication key mismatch")
        return record

    def list(self) -> list[dict]:
        return [self.get(path.name.removeprefix("publication-").removesuffix(".json"))
                for path in sorted(self.catalog.glob("publication-*.json"))]


def may_retire(build_id: str, active_build: str, open_pinned_builds: list[str],
               deployment_description: dict) -> bool:
    """Require both local run drain and Temporal's observed Drained status."""
    if build_id == active_build or build_id in open_pinned_builds:
        return False
    summaries = deployment_description.get("versionSummaries", [])
    matches = [entry for entry in summaries if entry.get("BuildID") == build_id]
    return len(matches) == 1 and matches[0].get("drainageStatus") == "drained"
