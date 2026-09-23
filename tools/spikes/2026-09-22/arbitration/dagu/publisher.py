"""Product publication boundary for a bounded, native Dagu authoring profile.

The graph is not a template. Authors may rearrange approved typed nodes and
routes; publication checks their transitive safety properties and pins bytes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess

import yaml


NAME = re.compile(r"exo_arb_[a-z][a-z0-9_]*_v([0-9]+)$")
IDENT = re.compile(r"[a-z][a-z0-9_]*$")
OPS = {"assign", "join", "synthesize", "quality", "repair", "accept", "release",
       "abort", "nested", "parent-result", "route-value"}
ALLOWED_ROUTES = {"accepted", "rejected", "clear", "requires_scope", "true", "false"}
STEP_KEYS = {"id", "depends", "run", "action", "with", "output", "retry_policy"}


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def command(step: dict) -> tuple[str, dict[str, str]] | None:
    raw = step.get("run")
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError("command must be a single literal string")
    words = shlex.split(raw)
    if len(words) < 2 or words[0] != "exo-arb" or words[1] not in OPS:
        raise ValueError("arbitrary command or unapproved operation")
    args: dict[str, str] = {}
    tail = words[2:]
    if len(tail) % 2:
        raise ValueError("command arguments must be literal --key value pairs")
    for key, value in zip(tail[::2], tail[1::2]):
        if not key.startswith("--") or not IDENT.fullmatch(key[2:].replace("-", "_")):
            raise ValueError("invalid command argument")
        key = key[2:].replace("-", "_")
        if key in args or any(ch in value for ch in "$`;&|<>\\\n"):
            raise ValueError("duplicate or executable command argument")
        args[key] = value
    return words[1], args


def deps(step: dict) -> set[str]:
    value = step.get("depends", [])
    if isinstance(value, str):
        return {value}
    if not isinstance(value, list) or any(not isinstance(v, str) for v in value):
        raise ValueError("depends must name steps")
    if len(value) != len(set(value)):
        raise ValueError("duplicate dependency")
    return set(value)


def validate_graph(name: str, document: dict) -> set[str]:
    if not isinstance(document, dict) or set(document) != {"steps"}:
        raise ValueError(f"{name}: only native steps are allowed")
    steps = document["steps"]
    if not isinstance(steps, list) or not steps:
        raise ValueError(f"{name}: steps required")
    by_id: dict[str, dict] = {}
    parsed: dict[str, tuple[str, dict[str, str]] | None] = {}
    children: set[str] = set()
    for step in steps:
        if not isinstance(step, dict) or set(step) - STEP_KEYS:
            raise ValueError(f"{name}: unsupported step fields")
        sid = step.get("id")
        if not isinstance(sid, str) or not IDENT.fullmatch(sid) or sid in by_id:
            raise ValueError(f"{name}: invalid or duplicate step id")
        by_id[sid] = step
        parsed[sid] = command(step)
        if bool(step.get("run")) == bool(step.get("action")):
            raise ValueError(f"{name}:{sid}: exactly one action or run required")
        retry = step.get("retry_policy")
        if retry is not None and (not isinstance(retry, dict) or set(retry) - {"limit", "interval_sec"}
                              or not isinstance(retry.get("limit"), int) or not 0 <= retry["limit"] <= 3
                              or retry.get("interval_sec") != 1):
            raise ValueError("unbounded or unsupported retry policy")
        if parsed[sid]:
            op, args = parsed[sid]
            allowed = {
                "assign": {"instance", "type", "scope", "drop_ack"},
                "join": {"branches"}, "synthesize": {"revision", "resolved"},
                "quality": {"revision"}, "repair": {"from", "to", "resolved"},
                "accept": {"revision"}, "release": {"revision"},
                "abort": {"revision"}, "nested": {"child"},
                "parent-result": set(), "route-value": {"field"},
            }[op]
            if set(args) - allowed:
                raise ValueError(f"{name}:{sid}: unsupported {op} arguments")
            if op == "nested":
                if set(args) != {"child"}:
                    raise ValueError("nested child must be pinned")
                children.add(args["child"])
            if op == "assign":
                if args.get("type") not in {"source_evidence", "counter_evidence"} or not IDENT.fullmatch(args.get("instance", "")):
                    raise ValueError("unapproved capability revision or instance")
                if args.get("scope", "requires_scope") not in {"requires_scope", "clear"}:
                    raise ValueError("unapproved branch scope")
            if op == "repair":
                if args.get("from") not in {"r1", "r2"} or args.get("to") != f"r{int(args['from'][1:])+1}":
                    raise ValueError("invalid repair bound")
            if op in {"synthesize", "quality", "accept", "release", "abort"} and args.get("revision") not in {"r1", "r2", "r3"}:
                raise ValueError("invalid revision")
            if op in {"synthesize", "repair"} and args.get("resolved") not in {"true", "false", "input"}:
                raise ValueError("candidate resolution must be typed boolean")
            if op == "route-value" and args.get("field") not in {"route_status", "requires_scope"}:
                raise ValueError("route input must be typed")
            if op == "quality" and not isinstance(step.get("output"), str):
                raise ValueError("Quality verdict must be captured for typed routing")
        elif step["action"] == "dag.run":
            opts = step.get("with")
            if not isinstance(opts, dict) or set(opts) != {"dag"} or not isinstance(opts["dag"], str):
                raise ValueError("dag.run must pin one child")
            children.add(opts["dag"])
        elif step["action"] == "human.task":
            opts = step.get("with")
            form = opts.get("form") if isinstance(opts, dict) else None
            if (sid not in {"old_wait", "director_wait"} or not isinstance(opts.get("prompt"), str)
                or not isinstance(form, dict) or form.get("type") != "object"
                or form.get("additionalProperties") is not False):
                raise ValueError("unapproved human task")
            if sid == "director_wait":
                if form.get("properties") != {"decision": {"type": "string", "enum": ["abort"]},
                                              "revision": {"type": "string", "enum": ["r3"]}} or form.get("required") != ["decision", "revision"]:
                    raise ValueError("Director wait form must require bound abort")
        elif step["action"] == "router.route":
            opts = step.get("with")
            if not isinstance(opts, dict) or set(opts) != {"value", "routes"}:
                raise ValueError("invalid router")
            if not isinstance(opts["value"], str) or not re.fullmatch(r"\$\{[A-Z][A-Z0-9_]*\}", opts["value"]):
                raise ValueError("router must consume captured typed output")
            routes = opts["routes"]
            if not isinstance(routes, dict) or not routes or set(routes) - ALLOWED_ROUTES:
                raise ValueError("route uses unapproved predicate")
            if any(not isinstance(v, list) or not v or any(not isinstance(x, str) for x in v) for v in routes.values()):
                raise ValueError("invalid route targets")
        else:
            raise ValueError("unapproved Dagu action")

    # Include router edges as the native engine does, then reject cycles and
    # paths around the required gates. Runtime checks repeat every invariant.
    predecessors = {sid: deps(step) for sid, step in by_id.items()}
    route_owner: dict[str, tuple[str, str]] = {}
    outputs = {step.get("output"): sid for sid, step in by_id.items() if step.get("output")}
    for sid, step in by_id.items():
        if step.get("action") == "router.route":
            source = outputs.get(step["with"]["value"][2:-1])
            if source is None or parsed[source] is None or parsed[source][0] not in {"quality", "route-value"}:
                raise ValueError("router must consume verified enum/boolean output")
            if source not in predecessors[sid]:
                raise ValueError("router must depend on its typed source")
            for label, targets in step["with"]["routes"].items():
                for target in targets:
                    if target in route_owner:
                        raise ValueError("a step cannot be targeted by two routes")
                    route_owner[target] = (sid, label)
                    predecessors.setdefault(target, set()).add(sid)
    for sid, step in by_id.items():
        for dependency in deps(step):
            if dependency in route_owner and route_owner.get(sid) != route_owner[dependency]:
                # Dagu v2.17 does not skip a dependent when its predecessor is
                # skipped by a router. Each conditional descendant must carry
                # the same route predicate until the next router boundary.
                raise ValueError("conditional descendant missing matching router target")
    if any(not targets <= by_id.keys() or sid in targets for sid, targets in predecessors.items()):
        raise ValueError("unknown or self dependency")
    order: list[str] = []
    pending = {sid: set(values) for sid, values in predecessors.items()}
    while pending:
        ready = sorted(sid for sid, values in pending.items() if not values)
        if not ready:
            raise ValueError("dependency or route cycle")
        for sid in ready:
            order.append(sid)
            del pending[sid]
        for values in pending.values():
            values.difference_update(ready)

    def ancestors(sid: str) -> set[str]:
        result: set[str] = set()
        stack = list(predecessors[sid])
        while stack:
            item = stack.pop()
            if item not in result:
                result.add(item)
                stack.extend(predecessors[item])
        return result

    assign = {sid: args for sid, value in parsed.items() if value and value[0] == "assign" for args in [value[1]]}
    joins = [sid for sid, value in parsed.items() if value and value[0] == "join"]
    if len({args["instance"] for args in assign.values()}) != len(assign):
        raise ValueError("duplicate assignment instance")
    for sid in joins:
        names = parsed[sid][1].get("branches", "").split(",")
        known = {args["instance"]: (aid, args["type"]) for aid, args in assign.items()}
        if not names or len(names) != len(set(names)) or not set(names) <= set(known):
            raise ValueError("join declarations mismatch branch instances")
        if not {known[name][0] for name in names} <= ancestors(sid):
            raise ValueError("join omits a declared branch dependency")
        if {known[name][1] for name in names} != {"source_evidence", "counter_evidence"}:
            raise ValueError("join requires both useful result types")
    for sid, value in parsed.items():
        if not value:
            continue
        op, args = value
        rev = args.get("revision")
        prior = ancestors(sid)
        roles = [parsed[p] for p in prior if parsed[p]]
        if op == "synthesize" and not any(join_id in prior for join_id in joins):
            raise ValueError("synthesis needs typed join")
        if op == "quality" and not any(p[0] in {"synthesize", "repair"} and p[1].get("revision", p[1].get("to")) == rev for p in roles):
            raise ValueError("Quality lacks exact candidate predecessor")
        if op == "accept":
            if not any(p[0] == "quality" and p[1].get("revision") == rev for p in roles):
                raise ValueError("review bypass")
            owner = route_owner.get(sid)
            if owner is None or owner[1] != "accepted":
                raise ValueError("accept must follow positive Quality route")
        if op == "repair":
            owner = route_owner.get(sid)
            if owner is None or owner[1] != "rejected" or not any(p[0] == "quality" and p[1].get("revision") == args["from"] for p in roles):
                raise ValueError("repair must follow rejected exact revision")
        if op == "release" and not any(p[0] == "accept" and p[1].get("revision") == rev for p in roles):
            raise ValueError("review or acceptance bypass")
        if op == "abort" and ("director_wait" not in prior or rev != "r3"):
            raise ValueError("abort must follow exhausted Director wait")
    if any(parsed[sid] and parsed[sid][0] == "release" and "director_wait" in ancestors(sid) for sid in by_id):
        raise ValueError("Director abort path cannot release")
    if assign and (not joins or not any(parsed[sid] and parsed[sid][0] == "synthesize" for sid in by_id)):
        raise ValueError("capability branches require join and synthesis")
    repair_depth: dict[str, int] = {}
    for sid in order:
        repair_depth[sid] = max((repair_depth[parent] for parent in predecessors[sid]), default=0) + int(
            bool(parsed[sid] and parsed[sid][0] == "repair"))
    if max(repair_depth.values(), default=0) > 2:
        raise ValueError("repair bound exceeds two")
    if "director_wait" in by_id and not any(parsed[sid] and parsed[sid][0] == "abort" for sid in by_id):
        raise ValueError("Director wait lacks abort outcome")
    if any(parsed[sid] and parsed[sid][0] == "nested" for sid in by_id) and not any(parsed[sid] and parsed[sid][0] == "parent-result" for sid in by_id):
        raise ValueError("parent lacks public child result")
    return children


def closure(source: Path, root: str) -> tuple[dict[str, bytes], dict]:
    match = NAME.fullmatch(root)
    if not match:
        raise ValueError("root must be a versioned approved DAG name")
    version = match[1]
    files: dict[str, bytes] = {}
    visiting: set[str] = set()

    def visit(name: str) -> None:
        if name in visiting:
            raise ValueError("child reference cycle")
        if name in files:
            return
        m = NAME.fullmatch(name)
        if not m or m[1] != version:
            raise ValueError("unpinned or cross-version child")
        path = source / f"{name}.yaml"
        if not path.is_file():
            raise ValueError(f"missing child: {name}")
        raw = path.read_bytes()
        children = validate_graph(name, yaml.safe_load(raw))
        visiting.add(name)
        files[name] = raw
        for child in children:
            visit(child)
        visiting.remove(name)

    visit(root)
    hashes = {f"{name}.yaml": sha(raw) for name, raw in sorted(files.items())}
    manifest = {"root": root, "version": version, "files": hashes,
                "closure_sha256": sha(canonical(hashes).encode())}
    return files, manifest


def publish(source: Path, home: Path, root: str, dagu: Path) -> dict:
    files, manifest = closure(source, root)
    dags, manifests = home / "dags", home / "manifests"
    dags.mkdir(parents=True, exist_ok=True)
    manifests.mkdir(parents=True, exist_ok=True)
    for name, raw in files.items():
        path = dags / f"{name}.yaml"
        if path.exists() and path.read_bytes() != raw:
            raise ValueError("active closure mutation forbidden: " + path.name)
        subprocess.run([str(dagu), "validate", str(source / f"{name}.yaml")], check=True,
                       env={**os.environ, "DAGU_HOME": str(home), "DAGU_AUTH_MODE": "none"},
                       capture_output=True, text=True)
    # Children first. Root plus manifest are written with create-exclusive
    # semantics, and consumers require the manifest and all matching hashes.
    for name in sorted(files, key=lambda n: (n == root, n)):
        path = dags / f"{name}.yaml"
        if not path.exists():
            with path.open("xb") as stream:
                stream.write(files[name]); stream.flush(); os.fsync(stream.fileno())
    target = manifests / f"{root}.json"
    payload = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    if target.exists() and target.read_bytes() != payload:
        raise ValueError("immutable closure manifest conflict")
    if not target.exists():
        with target.open("xb") as stream:
            stream.write(payload); stream.flush(); os.fsync(stream.fileno())
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--dagu", type=Path, required=True)
    args = parser.parse_args()
    print(canonical(publish(args.source, args.home, args.root, args.dagu)))
