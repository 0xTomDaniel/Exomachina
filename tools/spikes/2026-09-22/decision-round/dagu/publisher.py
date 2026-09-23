"""Small product-owned Dagu publication policy for the decision trial.

This intentionally accepts only the demonstrated vocabulary. Dagu's own YAML
validator checks syntax; this module checks the transitive child closure and the
review/Director gate before immutable-name publication.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

import yaml


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def closure(source: Path, version: str) -> tuple[dict[str, bytes], dict[str, object]]:
    root = f"exo_decision_factory_{version}"
    found: dict[str, bytes] = {}
    parsed: dict[str, dict] = {}
    visiting: set[str] = set()

    def visit(name: str) -> None:
        if name in visiting:
            raise ValueError(f"child reference cycle: {name}")
        if name in found:
            return
        if not name.startswith("exo_decision_") or not name.endswith("_" + version):
            raise ValueError(f"unpinned or cross-version child: {name}")
        path = source / f"{name}.yaml"
        if not path.is_file():
            raise ValueError(f"missing child: {name}")
        raw = path.read_bytes()
        document = yaml.safe_load(raw)
        if not isinstance(document, dict) or set(document) != {"steps"}:
            raise ValueError(f"unexpected top-level fields: {name}")
        steps = document["steps"]
        if not isinstance(steps, list) or not steps:
            raise ValueError(f"empty or invalid steps: {name}")
        ids: set[str] = set()
        children: list[str] = []
        for step in steps:
            if not isinstance(step, dict) or not isinstance(step.get("id"), str):
                raise ValueError(f"invalid step in {name}")
            if set(step) - {"id", "depends", "action", "with", "run"}:
                raise ValueError(f"unsupported step fields in {name}:{step['id']}")
            if step["id"] in ids:
                raise ValueError(f"duplicate step id in {name}")
            ids.add(step["id"])
            action = step.get("action")
            command = step.get("run")
            if action == "dag.run":
                child = (step.get("with") or {}).get("dag")
                if not isinstance(child, str) or set(step.get("with") or {}) != {"dag"}:
                    raise ValueError(f"invalid child reference in {name}")
                children.append(child)
            elif action == "human.task":
                if step["id"] not in {"publication_gate", "director"}:
                    raise ValueError(f"unapproved human task in {name}")
                options = step.get("with")
                form = options.get("form") if isinstance(options, dict) else None
                if (not isinstance(options.get("prompt"), str) if isinstance(options, dict) else True) or (
                    not isinstance(form, dict) or form.get("required") != ["accepted"]
                    or form.get("properties", {}).get("accepted") != {"type": "boolean", "enum": [True]}
                ):
                    raise ValueError(f"human gate must require true acceptance in {name}")
            elif isinstance(command, str) and action is None:
                expected = ('exo-dagu-adapter '
                            f'{step["id"]} --version {version}')
                if command != expected:
                    raise ValueError(f"unapproved command in {name}:{step['id']}")
            else:
                raise ValueError(f"unsupported action in {name}:{step['id']}")
        for step in steps:
            dependencies = step.get("depends", [])
            if isinstance(dependencies, str):
                dependencies = [dependencies]
            if not isinstance(dependencies, list) or any(dep not in ids for dep in dependencies):
                raise ValueError(f"invalid dependency in {name}:{step['id']}")
        visiting.add(name)
        found[name] = raw
        parsed[name] = document
        for child in children:
            visit(child)
        visiting.remove(name)

    visit(root)
    root_steps = parsed[root]["steps"]
    root_by_id = {step["id"]: step for step in root_steps}
    if set(root_by_id) != ({"publication_gate", "capability", "review", "director", "deliver"}
                           | ({"verify"} if version == "v2" else set())):
        raise ValueError("root must contain approved assignment, Quality, Director and delivery steps")
    if root_by_id["capability"].get("depends") != "publication_gate":
        raise ValueError("assignment must depend on publication gate")
    if root_by_id["director"].get("depends") != "review":
        raise ValueError("Director must wait for Quality")
    if root_by_id["deliver"].get("depends") != "director":
        raise ValueError("delivery must wait for Director")
    if root_by_id["review"].get("depends") != ("verify" if version == "v2" else "capability"):
        raise ValueError("Quality must wait for capability")
    if version == "v2" and root_by_id["verify"].get("depends") != "capability":
        raise ValueError("v2 verification must wait for capability")
    if root_by_id["capability"].get("with", {}).get("dag") != f"exo_decision_capability_{version}":
        raise ValueError("root does not reference the required capability closure")
    capability_steps = parsed[f"exo_decision_capability_{version}"]["steps"]
    leaf_steps = parsed[f"exo_decision_leaf_{version}"]["steps"]
    if (len(capability_steps) != 1 or capability_steps[0].get("id") != "worker"
        or capability_steps[0].get("action") != "dag.run"
        or capability_steps[0].get("with") != {"dag": f"exo_decision_leaf_{version}"}
        or len(leaf_steps) != 1 or leaf_steps[0].get("id") != "assign"):
        raise ValueError("capability must bind the pinned assignment leaf")
    files = {f"{name}.yaml": digest(raw) for name, raw in sorted(found.items())}
    identity = digest(json.dumps(files, sort_keys=True, separators=(",", ":")).encode())
    return found, {"root": root, "version": version, "files": files, "closure_sha256": identity}


def publish(source: Path, home: Path, version: str, dagu: Path) -> dict[str, object]:
    definitions, manifest = closure(source, version)
    destination = home / "dags"
    manifests = home / "manifests"
    destination.mkdir(parents=True, exist_ok=True)
    manifests.mkdir(parents=True, exist_ok=True)
    for name, raw in definitions.items():
        subprocess.run([str(dagu), "validate", str(source / f"{name}.yaml")], check=True,
                       env={**os.environ, "DAGU_HOME": str(home), "DAGU_AUTH_MODE": "none"},
                       capture_output=True, text=True)
        target = destination / f"{name}.yaml"
        if target.exists() and target.read_bytes() != raw:
            raise ValueError(f"immutable definition conflict: {target.name}")
    # Dependencies first; the root is the publication commit marker.
    for name in sorted(definitions, key=lambda key: (key.startswith("exo_decision_factory_"), key)):
        target = destination / f"{name}.yaml"
        if not target.exists():
            with target.open("xb") as stream:
                stream.write(definitions[name])
                stream.flush()
                os.fsync(stream.fileno())
    manifest_path = manifests / f"{version}.json"
    payload = json.dumps(manifest, sort_keys=True, indent=2).encode() + b"\n"
    if manifest_path.exists() and manifest_path.read_bytes() != payload:
        raise ValueError("immutable closure manifest conflict")
    if not manifest_path.exists():
        with manifest_path.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("version", choices=["v1", "v2"])
    parser.add_argument("--source", type=Path, default=Path(__file__).parent / "definitions")
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--dagu", type=Path)
    args = parser.parse_args()
    dagu = args.dagu or (Path(__file__).resolve().parent.parent / "bin" / "dagu")
    if not dagu.is_file():
        raise SystemExit("pass --dagu or use a bundle containing bin/dagu")
    print(json.dumps(publish(args.source, args.home, args.version, dagu), sort_keys=True))
