"""Operator CLI for one harness instance: provision, publish, and agent-authored publication.

None of these commands starts the local runner; a factory instance starts it
lazily on first factory work or recovery of unfinished runs.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

SRC = Path(__file__).resolve().parent
sys.path.insert(0, str(SRC))


def _instance(instance_dir: Path):
    from harness import Director, load_config
    return Director(instance_dir, load_config(instance_dir), claim=False)


def provision(instance_dir: Path, *, name: str, port: int, home: Path, testbed: Path,
              mode: str = "factory", wait_seconds: int = 900) -> dict:
    """Create the instance and pin the test services that stand in for the directory."""
    from harness import init_instance
    config = init_instance(instance_dir, name=name, mode=mode, port=port, home=home,
                           wait_seconds=wait_seconds)
    catalog = instance_dir / "catalog"
    catalog.mkdir(parents=True, exist_ok=True)
    for file in ("approved_bindings.json", "contracts.json", "quality_policy.json"):
        source = json.loads((testbed / file).read_text())
        target = catalog / file
        if target.exists() and json.loads(target.read_text()) != source:
            raise ValueError(f"pinned {file} differs from testbed; refusing to re-pin silently")
        target.write_text(json.dumps(source, indent=2, sort_keys=True) + "\n")
    return config


def publish_template(instance_dir: Path, template_path: Path, *, label: str,
                     approver: str) -> dict:
    from authoring import approve, materialize
    director = _instance(instance_dir)
    package = materialize(json.loads(template_path.read_text()), director.module.approved())
    approval = {**approve(package, approver=approver,
                          policy={"mode": "human", "approved_bindings": director.module.approved()}),
                "status": "approved", "decision": "operator reviewed hand-written template"}
    return {"publication": director.module.publish(package, label=label, approval=approval),
            "approval": approval, "source": str(template_path)}


def author_and_publish(instance_dir: Path, brief_path: Path, *, label: str,
                       base_template: Path | None, allow_scripted: bool,
                       interpreter_source: Path = SRC) -> dict:
    from authoring import (AuthoringSession, ScriptedAuthoringModel, StrandsGraphAuthor,
                           approve, model_from_environment)
    director = _instance(instance_dir)
    model, reason = model_from_environment()
    live_status = {"live_model_available": model is not None, "reason": reason}
    if model is None:
        if not allow_scripted:
            return {"status": "untested", "live": live_status}
        model = ScriptedAuthoringModel()
    session = AuthoringSession(StrandsGraphAuthor(model),
                               approved_bindings=director.module.approved())
    base = json.loads(base_template.read_text()) if base_template else None
    started = time.time()
    outcome = session.run(brief_path.read_text(), base_template=base)
    record = {"live": live_status, "outcome": _jsonable(outcome),
              "seconds": round(time.time() - started, 3)}
    if outcome.status != "approved":
        return {**record, "status": outcome.status}
    approval = approve(outcome.package, approver=f"director:{director.identity}",
                       policy={"mode": "auto", "approved_bindings": director.module.approved()})
    publication = director.module.publish(outcome.package, label=label, approval=approval,
                                          interpreter_source=interpreter_source)
    return {**record, "status": "published", "approval": approval, "publication": publication}


def _jsonable(outcome) -> dict:
    if hasattr(outcome, "as_dict"):
        return outcome.as_dict()
    if hasattr(outcome, "__dataclass_fields__"):
        from dataclasses import asdict
        return asdict(outcome)
    return dict(outcome)


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("provision")
    p.add_argument("--instance-dir", type=Path, required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--home", type=Path, required=True)
    p.add_argument("--testbed", type=Path, required=True)
    p.add_argument("--mode", choices=["agent", "factory"], default="factory")
    p.add_argument("--wait-seconds", type=int, default=900)
    p = sub.add_parser("publish-template")
    p.add_argument("--instance-dir", type=Path, required=True)
    p.add_argument("--template", type=Path, required=True)
    p.add_argument("--label", required=True)
    p.add_argument("--approver", default="operator:fixture-operator")
    p = sub.add_parser("author")
    p.add_argument("--instance-dir", type=Path, required=True)
    p.add_argument("--brief", type=Path, required=True)
    p.add_argument("--label", required=True)
    p.add_argument("--base-template", type=Path)
    p.add_argument("--allow-scripted", action="store_true")
    p.add_argument("--interpreter-source", type=Path, default=SRC,
                   help="immutable interpreter source to snapshot as this publication's build")
    p = sub.add_parser("publications")
    p.add_argument("--instance-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "provision":
        value = provision(args.instance_dir, name=args.name, port=args.port, home=args.home,
                          testbed=args.testbed, mode=args.mode, wait_seconds=args.wait_seconds)
    elif args.command == "publish-template":
        value = publish_template(args.instance_dir, args.template, label=args.label,
                                 approver=args.approver)
    elif args.command == "author":
        value = author_and_publish(args.instance_dir, args.brief, label=args.label,
                                   base_template=args.base_template,
                                   allow_scripted=args.allow_scripted,
                                   interpreter_source=args.interpreter_source)
    else:
        from binding import PublicationStore
        store = PublicationStore(args.instance_dir / "catalog")
        value = {"active": store.active(), "all": [
            {k: r[k] for k in ("label", "manifest_digest", "package_digest", "build_id")}
            for r in store.list()]}
        value["active"] = {k: value["active"][k] for k in ("label", "manifest_digest", "build_id")}
    print(json.dumps(value, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
