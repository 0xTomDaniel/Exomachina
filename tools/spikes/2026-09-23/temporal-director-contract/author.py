"""Materialize reviewable native JSON templates with pinned child/service closure."""
from __future__ import annotations

import copy
import json
from pathlib import Path

from definition import digest, validate


HERE = Path(__file__).resolve().parent


def materialize(template: dict, bindings: dict) -> dict:
    source = copy.deepcopy(template)
    if set(source) not in ({"schema", "root", "child"},
                           {"schema", "root", "child", "run_inputs"}):
        raise ValueError("authoring template needs schema, root, child")
    child = source["child"]
    child_digest = digest(child)
    root = source["root"]
    for node in root["nodes"].values():
        if node["type"] == "nested_factory" and node["child_digest"] == "@child":
            node["child_digest"] = child_digest
    package = {"schema": source["schema"], "root": root,
               "children": {child_digest: child}, "bindings": copy.deepcopy(bindings),
               "run_inputs": copy.deepcopy(source.get("run_inputs", {}))}
    validate(package)
    return package


def template(name: str) -> dict:
    return json.loads((HERE / "definitions" / name).read_text())
