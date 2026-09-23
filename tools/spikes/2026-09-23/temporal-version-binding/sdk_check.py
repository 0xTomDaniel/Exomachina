"""Record the installed SDK and CLI versioning surface without a live server."""
from __future__ import annotations

import inspect
import json
import subprocess
from importlib.metadata import version
from pathlib import Path

from temporalio.client import Client
from temporalio.common import (PinnedVersioningOverride, VersioningBehavior,
                               WorkerDeploymentVersion)
from temporalio.worker import Worker, WorkerDeploymentConfig

from binding import DEPLOYMENT


HERE = Path(__file__).resolve().parent
CLI = Path("/tmp/exomachina-temporal-evaluation/temporal")


def main() -> None:
    override = PinnedVersioningOverride(WorkerDeploymentVersion(DEPLOYMENT, "b1"))
    config = WorkerDeploymentConfig(WorkerDeploymentVersion(DEPLOYMENT, "b1"),
        use_worker_versioning=True, default_versioning_behavior=VersioningBehavior.PINNED)
    commands = {}
    for command in ("set-current-version", "set-ramping-version", "describe", "describe-version"):
        completed = subprocess.run([str(CLI), "worker", "deployment", command, "--help"],
                                   capture_output=True, text=True, check=True)
        commands[command] = {"available": completed.returncode == 0,
                             "usage_line": next((line for line in completed.stdout.splitlines()
                                                 if line.startswith("Usage:")), "")}
    observed = {"sdk": version("temporalio"),
        "start_has_versioning_override": "versioning_override" in inspect.signature(Client.start_workflow).parameters,
        "worker_has_deployment_config": "deployment_config" in inspect.signature(Worker).parameters,
        "pinned_override_proto": {"behavior": int(override._to_proto().behavior),
                                  "pinned_version": override._to_proto().pinned_version},
        "worker_config": {"build_id": config.version.build_id,
                          "deployment": config.version.deployment_name,
                          "pinned_behavior": config.default_versioning_behavior.name},
        "cli": commands,
        "python_client_set_current_api": hasattr(Client, "set_current_worker_deployment_version")}
    assert observed["start_has_versioning_override"]
    assert observed["worker_has_deployment_config"]
    assert observed["pinned_override_proto"]["pinned_version"] == f"{DEPLOYMENT}.b1"
    assert all(value["available"] for value in commands.values())
    (HERE / "sdk_observed.json").write_text(json.dumps(observed, indent=2, sort_keys=True))
    print(json.dumps(observed, sort_keys=True))


if __name__ == "__main__":
    main()
