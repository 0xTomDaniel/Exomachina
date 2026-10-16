"""Retain Temporal histories and replay them with their pinned interpreter builds."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import io
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace

NAMESPACE = "exomachina"


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--home", type=Path, help="EXO_HOME install root")
    cli.add_argument("--address", help="Temporal frontend address")
    cli.add_argument("--out", type=Path, help="directory for retained histories and summary")
    cli.add_argument("--workflow-id", action="append", default=[], metavar="ID")
    cli.add_argument("--self-test", action="store_true", help="check imports and CLI without a server")
    cli.add_argument("--_replay-build", type=Path, help=argparse.SUPPRESS)
    return cli


def validate_args(args: argparse.Namespace, cli: argparse.ArgumentParser) -> None:
    if args.self_test:
        return
    if args._replay_build:
        if args.home is None:
            cli.error("--_replay-build requires --home")
        return
    missing = [flag for flag, value in (("--home", args.home),
                                        ("--address", args.address),
                                        ("--out", args.out)) if value is None]
    if missing:
        cli.error("required arguments: " + ", ".join(missing))
    if not args.address.strip() or any(not item for item in args.workflow_id):
        cli.error("address and workflow IDs must be nonempty")


def pinned_build(description) -> str:
    info = description.raw_description.workflow_execution_info.versioning_info
    override = info.versioning_override.pinned.version.build_id
    deployed = info.deployment_version.build_id
    return override or deployed


def history_name(workflow_id: str, used: set[str]) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]", "_", workflow_id).strip(".")
    if not name:
        name = "workflow"
    if name in used:
        name += "-" + hashlib.sha256(workflow_id.encode()).hexdigest()[:12]
    if name in used:
        raise ValueError(f"history filename collision for {workflow_id!r}")
    used.add(name)
    return name + ".json"


def builds_at(home: Path) -> list[str]:
    root = home / "runner" / "builds"
    if not root.is_dir():
        return []
    return sorted(path.name for path in root.iterdir()
                  if path.is_dir() and (path / "build.json").is_file())


async def collect(address: str, workflow_ids: list[str], out: Path) -> list[dict]:
    from temporalio.client import Client

    client = await Client.connect(address, namespace=NAMESPACE)
    if not workflow_ids:
        workflow_ids = [item.id async for item in client.list_workflows()]
    rows: list[dict] = []
    used: set[str] = set()
    for workflow_id in dict.fromkeys(workflow_ids):
        row: dict = {"workflow_id": workflow_id}
        rows.append(row)
        try:
            handle = client.get_workflow_handle(workflow_id)
            history = await handle.fetch_history()
            path = out / history_name(workflow_id, used)
            path.write_text(history.to_json() + "\n")
            row["history"] = str(path)
            description = await handle.describe()
            row["build_id"] = pinned_build(description)
            if not row["build_id"]:
                row["replay"] = {"status": "fail", "error": "no pinned build in describe versioning_info"}
        except Exception as exc:
            row["replay"] = {"status": "fail", "error": f"{type(exc).__name__}: {exc}"}
    return rows


RESULT_PREFIX = "EXO_REPLAY_RESULT "


def manifest_for(build_dir: Path) -> dict:
    if not re.fullmatch(r"b-[0-9a-f]{12}", build_dir.name):
        raise ValueError(f"invalid build ID: {build_dir.name}")
    manifest = json.loads((build_dir / "build.json").read_text())
    if (manifest.get("build_id") != build_dir.name
            or not isinstance(manifest.get("source_digest"), str)
            or not manifest["source_digest"]):
        raise ValueError(f"invalid build manifest: {build_dir / 'build.json'}")
    if not (build_dir / "factory.py").is_file():
        raise ValueError(f"missing factory.py in {build_dir}")
    return manifest


def replay_batch(home: Path, build_id: str, jobs: list[dict]) -> dict[str, dict]:
    build_dir = home / "runner" / "builds" / build_id
    try:
        manifest = manifest_for(build_dir)
        env = os.environ.copy()
        env.pop("PYTHONPATH", None)
        env.update(EXO_WORKER_BUILD_ID=build_id,
                   EXO_WORKER_SOURCE_DIGEST=manifest["source_digest"],
                   PYTHONNOUSERSITE="1")
        result = subprocess.run(
            [sys.executable, "-B", str(Path(__file__).resolve()),
             "--home", str(home), "--_replay-build", str(build_dir)],
            input=json.dumps(jobs), text=True, capture_output=True,
            cwd=build_dir, env=env, check=False)
        if result.returncode:
            raise RuntimeError(f"replay subprocess exited {result.returncode}: "
                               f"{(result.stderr or result.stdout).strip()}")
        # The SDK core may log to stdout (e.g. nondeterminism warnings); read only
        # the sentinel result line.
        lines = [line for line in result.stdout.splitlines() if line.startswith(RESULT_PREFIX)]
        if len(lines) != 1:
            raise ValueError("replay subprocess produced no result line")
        outcomes = json.loads(lines[0][len(RESULT_PREFIX):])
        if set(outcomes) != {job["key"] for job in jobs}:
            raise ValueError("replay subprocess returned incomplete results")
        return outcomes
    except Exception as exc:
        failure = {"status": "fail", "error": f"{type(exc).__name__}: {exc}"}
        return {job["key"]: failure.copy() for job in jobs}


async def replay_in_build(build_dir: Path, jobs: list[dict]) -> dict[str, dict]:
    from temporalio.client import WorkflowHistory
    from temporalio.worker import Replayer

    # The script path is normally sys.path[0]. Replace it before importing any
    # snapshot module, so flat imports resolve inside this immutable build.
    sys.path[0] = str(build_dir)
    from binding import source_digest
    from factory import FactoryRun

    manifest = manifest_for(build_dir)
    if Path(sys.modules["factory"].__file__).resolve() != (build_dir / "factory.py").resolve():
        raise RuntimeError("factory import did not resolve inside the pinned build")
    if source_digest(build_dir) != manifest["source_digest"]:
        raise ValueError(f"build content differs from manifest: {build_dir}")
    replayer = Replayer(workflows=[FactoryRun], workflow_failure_exception_types=[ValueError])
    outcomes: dict[str, dict] = {}
    for job in jobs:
        try:
            history = WorkflowHistory.from_json(job["workflow_id"], Path(job["history"]).read_text())
            result = await replayer.replay_workflow(history, raise_on_replay_failure=False)
            failure = result.replay_failure
            outcomes[job["key"]] = ({"status": "fail", "error": f"{type(failure).__name__}: {failure}"}
                                    if failure else {"status": "pass"})
        except Exception as exc:
            outcomes[job["key"]] = {"status": "fail", "error": f"{type(exc).__name__}: {exc}"}
    return outcomes


def run_self_test() -> dict:
    from temporalio.api.workflowservice.v1 import request_response_pb2
    from temporalio.client import Client, WorkflowHistory
    from temporalio.worker import Replayer

    cli = parser()
    valid = cli.parse_args(["--home", "/tmp/exo-proto-replay-check",
                            "--address", "127.0.0.1:44002", "--out", "evidence/histories",
                            "--workflow-id", "first", "--workflow-id", "second"])
    validate_args(valid, cli)
    assert valid.workflow_id == ["first", "second"]
    assert [Client.__name__, WorkflowHistory.__name__, Replayer.__name__] == [
        "Client", "WorkflowHistory", "Replayer"]
    assert history_name("a/b", set()) == "a_b.json"
    raw = request_response_pb2.DescribeWorkflowExecutionResponse()
    versioning = raw.workflow_execution_info.versioning_info
    versioning.deployment_version.build_id = "b-111111111111"
    description = SimpleNamespace(raw_description=raw)
    assert pinned_build(description) == "b-111111111111"
    versioning.versioning_override.pinned.version.build_id = "b-222222222222"
    assert pinned_build(description) == "b-222222222222"
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            validate_args(cli.parse_args(["--home", "/tmp/x"]), cli)
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("missing required arguments were accepted")
    return {"self_test": "pass", "checks": ["Temporal imports", "CLI arguments",
                                               "history names", "pinned build metadata"]}


def main() -> int:
    cli = parser()
    args = cli.parse_args()
    validate_args(args, cli)
    if args.self_test:
        print(json.dumps(run_self_test(), sort_keys=True))
        return 0
    if args._replay_build:
        jobs = json.load(sys.stdin)
        outcomes = asyncio.run(replay_in_build(args._replay_build.resolve(), jobs))
        print(RESULT_PREFIX + json.dumps(outcomes, sort_keys=True), flush=True)
        return 0

    home = args.home.resolve()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    summary: dict = {"namespace": NAMESPACE, "address": args.address,
                     "home": str(home), "out": str(out), "workflows": [], "errors": []}
    try:
        rows = asyncio.run(collect(args.address, args.workflow_id, out))
        summary["workflows"] = rows
        builds = builds_at(home)
        jobs_by_build: dict[str, list[dict]] = defaultdict(list)
        for index, row in enumerate(rows):
            if "history" not in row or not row.get("build_id"):
                row.setdefault("negative_control", "not-applicable (history or pinned build unavailable)")
                continue
            build_id = row["build_id"]
            jobs_by_build[build_id].append({"key": f"{index}:replay",
                                            "workflow_id": row["workflow_id"],
                                            "history": row["history"]})
            alternatives = [item for item in builds if item != build_id]
            if alternatives:
                other = alternatives[0]
                row["negative_control"] = {"build_id": other}
                jobs_by_build[other].append({"key": f"{index}:negative_control",
                                             "workflow_id": row["workflow_id"],
                                             "history": row["history"]})
            else:
                row["negative_control"] = ("not-applicable (single build)" if len(builds) == 1
                                           else "not-applicable (no other build)")
        outcomes = {key: value for build_id, jobs in jobs_by_build.items()
                    for key, value in replay_batch(home, build_id, jobs).items()}
        for index, row in enumerate(rows):
            if "history" in row and row.get("build_id"):
                row["replay"] = outcomes[f"{index}:replay"]
                if isinstance(row["negative_control"], dict):
                    row["negative_control"].update(outcomes[f"{index}:negative_control"])
    except Exception as exc:
        summary["errors"].append(f"{type(exc).__name__}: {exc}")
    rows = summary["workflows"]
    summary["counts"] = {"histories": sum("history" in row for row in rows),
                         "replay_pass": sum(row.get("replay", {}).get("status") == "pass" for row in rows),
                         "replay_fail": sum(row.get("replay", {}).get("status") == "fail" for row in rows)}
    serialized = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    (out / "replay-summary.json").write_text(serialized)
    print(serialized, end="")
    return 1 if summary["errors"] or summary["counts"]["replay_fail"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
