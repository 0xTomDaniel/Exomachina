"""Role prompts, output validation, and scripted responses for report agents."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass


class RoleOutputError(ValueError):
    """A role returned content outside its assignment contract."""


RUBRIC = {"kind": "report-quality@1", "blocking": ["factual contradiction of the packet", "uncited or unsupported claim", "fixture described as live", "missing required section"], "minor": ["style issues"]}
RUBRIC_DIGEST = hashlib.sha256(json.dumps(RUBRIC, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
REPORT_SECTIONS = ("Live-proven", "Fixture-only", "Remaining gaps", "Next priority")


@dataclass(frozen=True)
class Role:
    name: str

    def system_prompt(self, capability: str) -> str:
        raise NotImplementedError

    def user_prompt(self, brief: dict) -> str:
        raise NotImplementedError

    def parse(self, text: str, brief: dict, identity: str) -> dict:
        raise NotImplementedError

    def precheck(self, brief: dict, identity: str) -> dict | None:
        return None

    def scripted_reply(self, brief: dict, identity: str) -> str:
        raise NotImplementedError


ROLES: dict[str, Role] = {name: Role(name) for name in ("research", "synthesis", "quality")}


def usefulness_check(content: dict, packet: dict) -> bool:
    raise NotImplementedError
