"""Read final Hatchet run status after the probe's second clean restart."""

import json
from pathlib import Path

from hatchet_sdk import ClientConfig, EmbeddedHatchetConfig, Hatchet


ROOT = Path(__file__).resolve().parent
observed = json.loads((ROOT / "observed.json").read_text())
client = Hatchet.from_embedded(
    ClientConfig(
        embedded=EmbeddedHatchetConfig(
            version="v0.107.0",
            binary_path=str(Path.home() / ".hatchet/embedded/v0.107.0/hatchet-embedded-sidecar_darwin_arm64"),
            checksum="3903d6c7057ee2d1bc4d3809981287191e3106a1f3be812ae78f7d77855b98ff",
            postgres_data_dir=observed["state_dir"] + "/postgres",
            start_api=True,
        )
    )
)
try:
    statuses = {
        event["run_id"]: {
            "workflow_run_id": event["workflow_run_id"],
            "status": client.runs.get_status(event["workflow_run_id"]).value,
        }
        for event in observed["markers"] if event["kind"] == "started"
    }
    (ROOT / "status-check.json").write_text(json.dumps(statuses, indent=2) + "\n")
    print(json.dumps(statuses, indent=2))
finally:
    client.stop_embedded()
