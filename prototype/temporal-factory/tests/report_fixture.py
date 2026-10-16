"""Small packet and package fixtures for report interpreter unit tests."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def packet() -> dict:
    return {"kind": "evidence_packet@1", "packet_id": "unit-packet", "title": "Unit packet",
            "default_question": "What happened?", "items": [
                {"id": f"E{i}", "source": f"prototype/temporal-factory/QUALIFICATION.md#L{i}-L{i}", "text": f"Evidence {i}."}
                for i in range(1, 9)]}


def template() -> dict:
    return json.loads((ROOT / "definitions" / "report-template.json").read_text())


def bindings(port_base: int = 45720) -> dict:
    names = ("research_findings", "research_risks", "synthesizer", "quality", "release")
    return {name: {"role": "quality" if name == "quality" else "release" if name == "release" else "capability",
                   "url": f"http://127.0.0.1:{port_base+i}", "identity": f"unit-{name}", "approved": True}
            for i, name in enumerate(names)}
