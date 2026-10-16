"""Immutable, bounded factory documents and publication validation.

The node table is data. No submitted field is evaluated as Python or a URL.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path


ALLOWED = {
    "parallel": {"type", "branches", "next"},
    "join": {"type", "branches", "next"},
    "route": {"type", "field", "cases"},
    "synthesize": {"type", "resolved", "next"},
    "quality": {"type", "next"},
    "repair": {"type", "max_repairs", "next", "exhausted"},
    "director_wait": {"type", "reason", "next"},
    "abort": {"type"},
    "release": {"type", "service", "next"},
    "complete": {"type"},
    "nested_factory": {"type", "child", "child_digest", "next"},
}
RESULT_TYPES = {"source_evidence": "source_evidence@1",
                "counter_evidence": "counter_evidence@1"}
ROUTE_VALUES = {"join.route_status": {"requires_scope", "clear"},
                "join.requires_scope": {"true", "false"},
                "verdict.accepted": {"true", "false"}}
INPUT_TYPES = {"string", "integer", "number", "boolean"}
INPUT_SOURCES = {"caller", "director", "verified_artifact"}


def validate_input_schema(schema: dict) -> None:
    if not isinstance(schema, dict) or len(schema) > 16:
        raise ValueError("run input schema must have at most 16 fields")
    for name, spec in schema.items():
        if not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", name):
            raise ValueError("invalid run input name")
        if not isinstance(spec, dict) or not {"type", "required", "source", "may_affect_acceptance"} <= set(spec) or not set(spec) <= {"type", "required", "enum", "default", "source", "may_affect_acceptance", "allowed_actors", "artifact_contract"}:
            raise ValueError(f"invalid run input declaration: {name}")
        if (not isinstance(spec["type"], str) or spec["type"] not in INPUT_TYPES
                or type(spec["required"]) is not bool
                or type(spec["may_affect_acceptance"]) is not bool
                or not isinstance(spec["source"], str)
                or spec["source"] not in INPUT_SOURCES):
            raise ValueError(f"invalid run input type or requirement: {name}")
        if spec["source"] == "caller":
            actors = spec.get("allowed_actors")
            if (not isinstance(actors, list) or not actors
                    or any(not isinstance(actor, str) or not actor for actor in actors)
                    or len(actors) != len(set(actors))
                    or "artifact_contract" in spec):
                raise ValueError(f"caller input needs pinned allowed actors: {name}")
        elif "allowed_actors" in spec:
            raise ValueError(f"noncaller input cannot list callers: {name}")
        if spec["source"] == "verified_artifact":
            if not isinstance(spec.get("artifact_contract"), str) or not spec["artifact_contract"]:
                raise ValueError(f"verified artifact contract required: {name}")
        elif "artifact_contract" in spec:
            raise ValueError(f"artifact contract on nonartifact input: {name}")
        if spec["required"] and "default" in spec:
            raise ValueError(f"required input has default: {name}")
        if "enum" in spec and (not isinstance(spec["enum"], list) or not spec["enum"]
                               or len(spec["enum"]) != len({canonical(v) for v in spec["enum"]})):
            raise ValueError(f"invalid run input enum: {name}")
        for value in spec.get("enum", []) + ([spec["default"]] if "default" in spec else []):
            if not input_type_matches(spec["type"], value):
                raise ValueError(f"run input value type mismatch: {name}")
        if "default" in spec and "enum" in spec and spec["default"] not in spec["enum"]:
            raise ValueError(f"default outside enum: {name}")


def input_type_matches(kind: str, value: object) -> bool:
    return {"string": lambda: isinstance(value, str),
            "integer": lambda: type(value) is int,
            "number": lambda: type(value) in {int, float} and math.isfinite(value),
            "boolean": lambda: type(value) is bool}[kind]()


def validate_run_inputs(schema: dict, submitted: dict) -> dict:
    validate_input_schema(schema)
    if not isinstance(submitted, dict) or set(submitted) - set(schema):
        raise ValueError("unknown or malformed run input")
    result = {}
    for name, spec in schema.items():
        if name in submitted:
            value = submitted[name]
        elif "default" in spec:
            value = spec["default"]
        elif spec["required"]:
            raise ValueError(f"missing required run input: {name}")
        else:
            continue
        if not input_type_matches(spec["type"], value):
            raise ValueError(f"invalid run input type: {name}")
        if "enum" in spec and value not in spec["enum"]:
            raise ValueError(f"invalid run input enum: {name}")
        result[name] = value
    return result


def authorize_run_inputs(schema: dict, submitted: dict, actor: str,
                         *, director_values: dict | None = None,
                         verified_artifacts: dict | None = None) -> tuple[dict, dict]:
    """Resolve pinned provenance; trusted sources are never accepted in A2A data."""
    validate_input_schema(schema)
    if not isinstance(actor, str) or not actor:
        raise ValueError("authenticated input actor required")
    if not isinstance(submitted, dict) or set(submitted) - set(schema):
        raise ValueError("unknown or malformed run input")
    director_values = director_values or {}
    verified_artifacts = verified_artifacts or {}
    values = {}
    authority = {}
    for name, spec in schema.items():
        source = spec["source"]
        if source == "caller":
            if name in submitted:
                if actor not in spec["allowed_actors"]:
                    raise ValueError(f"actor not authorized for run input: {name}")
                values[name] = submitted[name]
                authority[name] = {"source": source, "actor": actor}
            elif "default" in spec:
                authority[name] = {"source": "package_default", "actor": "publisher"}
        elif name in submitted:
            raise ValueError(f"run input cannot be caller supplied: {name}")
        elif source == "director" and name in director_values:
            values[name] = director_values[name]
            authority[name] = {"source": source, "actor": "director"}
        elif source == "verified_artifact" and name in verified_artifacts:
            artifact = verified_artifacts[name]
            if (not isinstance(artifact, dict)
                    or artifact.get("contract") != spec["artifact_contract"]
                    or not isinstance(artifact.get("verified_by"), str)
                    or not artifact["verified_by"]
                    or not isinstance(artifact.get("sha256"), str)
                    or artifact["sha256"] != digest(artifact.get("value"))):
                raise ValueError(f"unverified run input artifact: {name}")
            values[name] = artifact["value"]
            authority[name] = {"source": source, "actor": artifact["verified_by"],
                               "artifact_digest": artifact["sha256"]}
        elif "default" in spec:
            authority[name] = {"source": "package_default", "actor": "publisher"}
    validated = validate_run_inputs(schema, values)
    return validated, authority


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode("utf-8")


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def _keys(value: dict, expected: set[str], where: str) -> None:
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError(f"{where}: expected fields {sorted(expected)}")


def _definition(document: dict, children: dict, bindings: dict, *, parent: bool) -> None:
    _keys(document, {"name", "revision", "start", "nodes"}, "definition")
    nodes = document["nodes"]
    if not isinstance(nodes, dict) or not nodes or len(nodes) > 32:
        raise ValueError("definition needs 1..32 nodes")
    if document["start"] not in nodes:
        raise ValueError("start node missing")
    for name, node in nodes.items():
        if not isinstance(name, str) or not name or not isinstance(node, dict):
            raise ValueError("invalid node")
        kind = node.get("type")
        if kind not in ALLOWED:
            raise ValueError("arbitrary code or unsupported block")
        _keys(node, ALLOWED[kind], name)
        targets = []
        if "next" in node:
            targets.append(node["next"])
        if "exhausted" in node:
            targets.append(node["exhausted"])
        if kind == "route":
            if node["field"] not in ROUTE_VALUES:
                raise ValueError("route field not a declared typed enum/boolean")
            cases = node["cases"]
            if not isinstance(cases, dict) or set(cases) != ROUTE_VALUES[node["field"]]:
                raise ValueError("route must cover each typed value")
            targets += list(cases.values())
        if any(target not in nodes for target in targets):
            raise ValueError(f"{name}: edge targets missing node")
        if kind == "parallel":
            branches = node["branches"]
            if not isinstance(branches, dict) or not 2 <= len(branches) <= 4:
                raise ValueError("parallel fan-out must be 2..4")
            if any(not isinstance(instance, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", instance)
                   for instance in branches):
                raise ValueError("invalid branch instance")
            for instance, branch in branches.items():
                _keys(branch, {"result_type", "capability", "service", "scope_status"},
                      f"branch {instance}")
                result_type = branch["result_type"]
                if result_type not in RESULT_TYPES:
                    raise ValueError("unapproved capability result type")
                if branch["capability"] != RESULT_TYPES[result_type]:
                    raise ValueError("unapproved capability revision")
                binding = bindings.get(branch["service"])
                if not isinstance(binding, dict) or binding.get("role") != "capability" or not binding.get("approved"):
                    raise ValueError("unapproved capability service")
                scope = branch["scope_status"]
                if result_type == "counter_evidence":
                    if scope not in {"requires_scope", "clear"}:
                        raise ValueError("invalid counter scope enum")
                elif scope is not None:
                    raise ValueError("source branch cannot set counter scope")
        elif kind == "join":
            if not isinstance(node["branches"], list) or len(node["branches"]) < 2:
                raise ValueError("join needs named typed predecessors")
        elif kind == "synthesize":
            if node["resolved"] not in (True, False, "after_repair", "from_run"):
                raise ValueError("synthesis resolution is not approved")
        elif kind == "repair":
            if type(node["max_repairs"]) is not int or not 1 <= node["max_repairs"] <= 2:
                raise ValueError("repair bound must be one or two")
        elif kind == "director_wait":
            if node["reason"] != "repair_exhausted" or nodes[node["next"]]["type"] != "abort":
                raise ValueError("Director wait may only authorize abort after exhaustion")
        elif kind == "release":
            binding = bindings.get(node["service"])
            if not isinstance(binding, dict) or binding.get("role") != "release" or not binding.get("approved"):
                raise ValueError("unapproved release receiver")
        elif kind == "nested_factory":
            if not parent:
                raise ValueError("recursive child nesting is outside this trial")
            child = children.get(node["child_digest"])
            if child is None or child.get("name") != node["child"] or digest(child) != node["child_digest"]:
                raise ValueError("missing or mutated pinned child closure")
        if parent and kind not in {"nested_factory", "complete"}:
            raise ValueError("parent may use only pinned nested factory and completion")
        if not parent and kind == "nested_factory":
            raise ValueError("nested child must not call an unpinned grandchild")

    # Bounded abstract execution checks *every* route and verdict outcome.
    # Only repair may revisit a node; the state includes its bounded count.
    visiting = set()
    done = set()
    terminal = set()
    def walk(at: str, *, branches: tuple = (), joined: bool = False,
             candidate: bool = False, verdict: str = "none", accepted: bool = False,
             repairs: int = 0, waited: bool = False, exhausted: bool = False,
             released: bool = False) -> None:
        state = (at, branches, joined, candidate, verdict, accepted, repairs,
                 waited, exhausted, released)
        if state in visiting:
            raise ValueError("unbounded cycle outside bounded repair")
        if state in done:
            return
        visiting.add(state)
        node = nodes[at]
        kind = node["type"]
        next_state = dict(branches=branches, joined=joined, candidate=candidate,
                          verdict=verdict, accepted=accepted, repairs=repairs,
                          waited=waited, exhausted=exhausted, released=released)
        if kind == "parallel":
            if branches:
                raise ValueError("multiple assignment groups without distinct lineage")
            declarations = tuple(sorted((key, value["result_type"])
                                        for key, value in node["branches"].items()))
            next_state["branches"] = declarations
        elif kind == "join":
            declared = dict(branches)
            if set(node["branches"]) != set(declared) or set(declared.values()) != set(RESULT_TYPES):
                raise ValueError("typed join mismatch or missing predecessor")
            next_state["joined"] = True
        elif kind == "synthesize":
            if not joined:
                raise ValueError("candidate synthesis bypasses typed join")
            next_state.update(candidate=True, verdict="none", accepted=False)
        elif kind == "quality":
            if not candidate:
                raise ValueError("Quality bypasses candidate")
            for outcome in ("true", "false"):
                walk(node["next"], **{**next_state, "verdict": outcome,
                                      "accepted": outcome == "true"})
            visiting.remove(state)
            done.add(state)
            return
        elif kind == "route":
            field = node["field"]
            if field.startswith("join.") and not joined:
                raise ValueError("typed route bypasses join")
            if field == "verdict.accepted":
                if verdict == "none":
                    raise ValueError("verdict route bypasses Quality")
                outcomes = (verdict,)
            else:
                outcomes = tuple(ROUTE_VALUES[field])
            for outcome in outcomes:
                walk(node["cases"][outcome], **next_state)
            visiting.remove(state)
            done.add(state)
            return
        elif kind == "repair":
            if verdict != "false":
                raise ValueError("repair without rejected current revision")
            if repairs < node["max_repairs"]:
                next_state.update(repairs=repairs + 1, candidate=False,
                                  verdict="none", accepted=False)
                walk(node["next"], **next_state)
            else:
                walk(node["exhausted"], **{**next_state, "exhausted": True})
            visiting.remove(state)
            done.add(state)
            return
        elif kind == "director_wait":
            if verdict != "false" or not exhausted or repairs > 2:
                raise ValueError("Director wait before exhausted rejection")
            next_state["waited"] = True
        elif kind == "abort":
            if not waited or accepted:
                raise ValueError("abort path lacks exhausted Director wait")
            terminal.add("aborted")
            visiting.remove(state)
            done.add(state)
            return
        elif kind == "release":
            if not accepted or verdict != "true":
                raise ValueError("release bypasses current independent Quality acceptance")
            next_state["released"] = True
        elif kind == "nested_factory":
            next_state.update(accepted=True, released=True)
        elif kind == "complete":
            if not accepted or not released:
                raise ValueError("completion bypasses accepted release or child")
            terminal.add("accepted")
            visiting.remove(state)
            done.add(state)
            return
        walk(node["next"], **next_state)
        visiting.remove(state)
        done.add(state)
    walk(document["start"])
    if not terminal:
        raise ValueError("definition has no terminal outcome")


def validate(package: dict, approved_bindings: dict | None = None) -> str:
    _keys(package, {"schema", "root", "children", "bindings", "run_inputs"}, "package")
    if package["schema"] != 1:
        raise ValueError("unsupported package schema")
    validate_input_schema(package["run_inputs"])
    bindings = package["bindings"]
    if not isinstance(bindings, dict):
        raise ValueError("missing approved bindings")
    for name, binding in bindings.items():
        _keys(binding, {"role", "url", "identity", "approved"}, f"binding {name}")
        if (binding["role"] not in {"capability", "quality", "release"}
                or not isinstance(binding["url"], str)
                or not binding["url"].startswith("http://127.0.0.1:")
                or not isinstance(binding["identity"], str) or not binding["identity"]
                or binding["approved"] is not True):
            raise ValueError("unapproved service binding")
        if approved_bindings is not None and approved_bindings.get(name) != binding:
            raise ValueError("unapproved service identity, contract, or endpoint")
    if approved_bindings is not None and not set(bindings) <= set(approved_bindings):
        raise ValueError("unapproved service binding name")
    if sum(b["role"] == "quality" for b in bindings.values()) != 1:
        raise ValueError("one independent Quality binding required")
    if sum(b["role"] == "release" for b in bindings.values()) < 1:
        raise ValueError("a release receiver binding is required")
    children = package["children"]
    if not isinstance(children, dict) or not children:
        raise ValueError("missing child definition closure")
    for key, child in children.items():
        if key != digest(child):
            raise ValueError("active child closure mutation or digest mismatch")
        _definition(child, children, bindings, parent=False)
    _definition(package["root"], children, bindings, parent=True)
    if any(node["type"] == "synthesize" and node["resolved"] == "from_run"
           for child in children.values() for node in child["nodes"].values()):
        outcome = package["run_inputs"].get("outcome_mode")
        if (not isinstance(outcome, dict)
                or outcome.get("type") != "string" or outcome.get("required") is not True
                or outcome.get("enum") != ["after_first_repair", "never"]
                or outcome.get("may_affect_acceptance") is not True):
            raise ValueError("from_run requires the pinned outcome_mode enum")
    return digest(package)


def publish(package: dict, catalog: Path, approved_bindings: dict) -> str:
    content_digest = validate(package, approved_bindings)
    catalog.mkdir(parents=True, exist_ok=True)
    path = catalog / f"{content_digest}.json"
    with path.open("xb") as stream:
        stream.write(canonical(package))
        stream.flush()
        import os
        os.fsync(stream.fileno())
    return content_digest
