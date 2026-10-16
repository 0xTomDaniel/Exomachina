#!/usr/bin/env python3
"""Replay only histories from ordinary Director runs, with no live server."""
from __future__ import annotations

import asyncio
import glob
import hashlib
import json
import sys
from pathlib import Path

from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

import runtime
from factory import FactoryRun
from probe import SERVICE_PORTS

HERE = Path(__file__).resolve().parent


async def main() -> None:
    ports = list(runtime.PORTS.values()) + list(SERVICE_PORTS.values()) + [41468, 41469]
    open_ports = [port for port in ports if runtime.port_open(port)]
    state = glob.glob("/tmp/exo-tq-director-*")
    if open_ports or state:
        raise RuntimeError(f"replay requires stopped trial: ports={open_ports}, state={state}")
    live = json.loads((HERE / "trial_observed.json").read_text())
    if live["status"] != "passed" or not live["cleanup"]["state_removed"]:
        raise RuntimeError("ordinary trial has no clean passing history set")
    result = {"status": "running", "offline": True,
              "workflow_failure_exception_types": ["ValueError"],
              "open_owned_ports_before": open_ports,
              "remaining_owned_state_before": state, "histories": {}}
    replayer = Replayer(workflows=[FactoryRun], workflow_failure_exception_types=[ValueError])
    try:
        for label, metadata in sorted(live["histories"].items()):
            path = HERE / metadata["file"]
            content = path.read_text()
            sha256 = hashlib.sha256(content.encode()).hexdigest()
            if sha256 != metadata["sha256"]:
                raise RuntimeError(f"history digest changed: {label}")
            history = WorkflowHistory.from_json(metadata["workflow_id"], content)
            replay = await replayer.replay_workflow(history)
            result["histories"][label] = {"workflow_id": history.workflow_id,
                "events": len(history.events), "sha256": sha256,
                "replay_failure": str(replay.replay_failure) if replay.replay_failure else None}
        result["status"] = "passed"
    except Exception as error:
        result["status"] = "failed"
        result["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        (HERE / "replay_observed.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"status": result["status"], "histories": len(result["histories"])}))


if __name__ == "__main__":
    asyncio.run(main())
