"""Replay a recorded ordinary child history in a separate, offline process."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", choices=("b1", "b2"), required=True)
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--workflow-id", required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parent / f"build_{args.build}"))
    from factory import FactoryRun

    history = WorkflowHistory.from_json(args.workflow_id, args.history.read_text())
    try:
        await Replayer(workflows=[FactoryRun]).replay_workflow(history)
        print(json.dumps({"build": args.build, "replay": "pass"}))
    except Exception as error:
        print(json.dumps({"build": args.build, "replay": "fail",
                          "error_type": type(error).__name__, "error": str(error)[:500]}))
        raise


if __name__ == "__main__":
    asyncio.run(main())
