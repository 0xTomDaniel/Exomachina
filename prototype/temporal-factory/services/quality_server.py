"""Independent deterministic Quality service for the arbitration fixture.

It reuses the decision-round Strands Agent, A2A executor, and durable task store.
It is an ordinary A2A agent: the brief is the first text Part and the draft
under review arrives as the next Part, named by its own revision and the
sha256 this service computes over the received text (older fixture callers
may still name ``{"artifact": {revision, sha256, content}}`` in the brief). A
resent ``messageId`` returns the original Task; only JSON-RPC and the Agent
Card are served. The verdict is the artifact's single text Part.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import uvicorn

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
import harness_server as prior  # noqa: E402
from fixture import quality_decision  # noqa: E402


class QualityHarness(prior.Harness):
    def __init__(self, state: Path):
        super().__init__(state, "quality")

    def verdict(self, brief: str, inputs: list) -> dict:
        source = prior.input_candidate(inputs)
        if source is None:
            try:
                source = json.loads(brief).get("artifact")
            except (ValueError, AttributeError):
                source = None
        if not isinstance(source, dict):
            raise prior.Rejected("missing artifact")
        for field in ("revision", "sha256", "content"):
            if not isinstance(source.get(field), str) or not source[field]:
                raise prior.Rejected("missing artifact " + field)
        # Author/reviewer independence is the factory's policy, not this agent's.
        source = {key: source[key] for key in ("revision", "sha256", "content")}
        accepted, reason = quality_decision(source, self.identity)
        return {"accepted": accepted, "revision": source["revision"],
                "sha256": source["sha256"], "reason": reason}


def create_app(state: Path, port: int):
    harness = QualityHarness(state)
    card = prior.fixture_card("Arbitration Quality", "Deterministic independent Quality fixture",
                              "quality", port, ["fixture", "quality"])
    return prior.fixture_app(harness, card)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    uvicorn.run(create_app(args.state, args.port), host="127.0.0.1",
                port=args.port, log_level="warning")
