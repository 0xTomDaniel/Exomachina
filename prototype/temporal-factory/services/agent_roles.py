"""Role prompts, output validation, and scripted responses for report agents."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path


class RoleOutputError(ValueError):
    """A role returned content outside its assignment contract."""


RUBRIC = {"kind": "report-quality@1", "blocking": ["factual contradiction of the packet", "uncited or unsupported claim", "fixture described as live", "missing required section"], "minor": ["style issues"]}
RUBRIC_DIGEST = hashlib.sha256(json.dumps(RUBRIC, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
REPORT_SECTIONS = ("Live-proven", "Fixture-only", "Remaining gaps", "Next priority")
_IDS = {"packet_findings@1": "F", "packet_risks@1": "R"}


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _digest(value: object) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RoleOutputError(message)


def _keys(value: object, expected: set[str], label: str) -> dict:
    _require(isinstance(value, dict) and set(value) == expected, f"{label}: expected {sorted(expected)}")
    return value


def _string(value: object, label: str, *, maximum: int | None = None) -> str:
    _require(isinstance(value, str) and bool(value.strip()), f"{label}: non-empty string required")
    if maximum is not None:
        _require(len(value) <= maximum, f"{label}: exceeds {maximum} characters")
    return value


def _packet_ids(brief: dict) -> set[str]:
    _require(isinstance(brief, dict), "brief: object required")
    packet = brief.get("packet")
    _require(isinstance(packet, dict) and isinstance(packet.get("items"), list), "brief: packet required")
    raw_ids = [item.get("id") for item in packet["items"] if isinstance(item, dict)]
    _require(all(isinstance(item, str) for item in raw_ids), "brief: invalid packet ids")
    ids = set(raw_ids)
    _require(all(isinstance(item, str) and re.fullmatch(r"E[1-9][0-9]*", item) for item in ids), "brief: invalid packet ids")
    _require(len(ids) == len(packet["items"]), "brief: duplicate packet ids")
    _require(_digest(packet) == brief.get("packet_digest"), "brief: packet digest mismatch")
    return ids


def _evidence(value: object, ids: set[str], label: str, *, nonempty: bool = True) -> list[str]:
    _require(isinstance(value, list) and (not nonempty or bool(value)), f"{label}: evidence list required")
    _require(all(isinstance(item, str) and item in ids for item in value), f"{label}: unknown evidence id")
    _require(len(value) == len(set(value)), f"{label}: duplicate evidence id")
    return value


def _load(text: str) -> dict:
    _require(isinstance(text, str), "reply: JSON text required")
    raw = text.strip()
    if raw.startswith("```"):
        match = re.fullmatch(r"```(?:json)?\s*\n([\s\S]*?)\n```", raw, re.IGNORECASE)
        _require(match is not None, "reply: expected one fenced JSON block")
        raw = match.group(1)

    def unique(pairs: list[tuple[str, object]]) -> dict:
        result = {}
        for key, value in pairs:
            _require(key not in result, f"reply: duplicate key {key}")
            result[key] = value
        return result

    try:
        value = json.loads(raw, object_pairs_hook=unique)
    except (ValueError, TypeError) as exc:
        raise RoleOutputError("reply: invalid JSON") from exc
    _require(isinstance(value, dict), "reply: JSON object required")
    return value


def _report(value: dict, brief: dict) -> dict:
    ids = _packet_ids(brief)
    _keys(value, {"kind", "revision", "packet_digest", "question", "title", "markdown", "claims"}, "report")
    _require(value["kind"] == "verified_report@1", "report: invalid kind")
    _require(isinstance(value["revision"], str) and value["revision"] == brief.get("revision") and
             re.fullmatch(r"r[1-9][0-9]*", value["revision"]) is not None,
             "report: revision mismatch")
    _require(value["packet_digest"] == brief["packet_digest"], "report: packet digest mismatch")
    _require(value["question"] == brief.get("question"), "report: question mismatch")
    _string(value["title"], "report.title")
    markdown = _string(value["markdown"], "report.markdown", maximum=12000)
    for section in REPORT_SECTIONS:
        pattern = rf"(?m)^\s*#{{1,6}}\s+{re.escape(section)}\s*#*\s*$"
        _require(re.search(pattern, markdown) is not None, f"report: missing {section} heading")
    claims = value["claims"]
    _require(isinstance(claims, list) and len(claims) >= 3, "report: at least 3 claims required")
    seen = set()
    for claim in claims:
        _keys(claim, {"id", "text", "evidence"}, "claim")
        claim_id = claim["id"]
        _require(isinstance(claim_id, str) and re.fullmatch(r"C[1-9][0-9]*", claim_id) is not None,
                 "claim: invalid id")
        _require(claim_id not in seen, "claim: duplicate id")
        seen.add(claim_id)
        _string(claim["text"], "claim.text")
        _evidence(claim["evidence"], ids, "claim")
    if brief.get("mode") == "repair":
        prior = brief.get("prior")
        _require(isinstance(prior, dict) and isinstance(prior.get("content"), (str, dict)),
                 "repair: prior content required")
        prior_content = _load(prior["content"]) if isinstance(prior["content"], str) else prior["content"]
        _require(isinstance(prior_content.get("claims"), list), "repair: prior claims required")
        rejected = {finding.get("claim_id") for finding in brief.get("quality_findings") or []
                    if isinstance(finding, dict) and finding.get("severity") == "blocking"}
        rejected_texts = {claim.get("text") for claim in prior_content["claims"]
                          if isinstance(claim, dict) and claim.get("id") in rejected}
        for rejected_text in rejected_texts:
            if isinstance(rejected_text, str) and rejected_text:
                _require(all(rejected_text != claim["text"] for claim in claims) and
                         rejected_text not in markdown, "repair: rejected claim reintroduced")
    return value


def _findings(value: object, ids: set[str], claim_ids: set[str] | None = None) -> list[dict]:
    _require(isinstance(value, list), "findings: list required")
    for finding in value:
        _keys(finding, {"claim_id", "severity", "problem", "evidence"}, "finding")
        claim_id = finding["claim_id"]
        _require(claim_id is None or
                 (isinstance(claim_id, str) and re.fullmatch(r"C[1-9][0-9]*", claim_id) is not None and
                  (claim_ids is None or claim_id in claim_ids)), "finding: invalid claim id")
        _require(finding["severity"] in ("blocking", "minor"), "finding: invalid severity")
        _string(finding["problem"], "finding.problem")
        _evidence(finding["evidence"], ids, "finding", nonempty=False)
    return value


def _candidate(brief: dict) -> dict:
    candidate = _keys(brief.get("candidate"), {"revision", "sha256", "author", "content"}, "candidate")
    _require(candidate["revision"] == brief.get("revision"), "candidate: revision mismatch")
    _string(candidate["author"], "candidate.author")
    _require(isinstance(candidate["sha256"], str) and
             re.fullmatch(r"[0-9a-f]{64}", candidate["sha256"]) is not None, "candidate: invalid sha256")
    _require(isinstance(candidate["content"], str), "candidate: content text required")
    _require(hashlib.sha256(candidate["content"].encode("utf-8")).hexdigest() == candidate["sha256"],
             "candidate: sha256 mismatch")
    content = _report(_load(candidate["content"]), brief)
    return content


def _verdict(brief: dict, identity: str, accepted: bool, findings: list[dict], decided_by: str) -> dict:
    candidate = brief.get("candidate") if isinstance(brief.get("candidate"), dict) else {}
    return {"kind": "quality_verdict@1",
            "candidate": {key: candidate.get(key, "") for key in ("revision", "sha256", "author")},
            "reviewer": identity, "accepted": accepted, "decided_by": decided_by,
            "findings": findings, "rubric": RUBRIC["kind"], "rubric_digest": RUBRIC_DIGEST}


@dataclass(frozen=True)
class Role:
    name: str

    def system_prompt(self, capability: str) -> str:
        if self.name == "research":
            _require(capability in _IDS, "research: unsupported capability")
            prefix = _IDS[capability]
            extra = 'Include severity "high", "medium", or "low" on each risk.' if prefix == "R" else "Do not include severity on findings."
            return ("You are an independent evidence researcher. Use only the assigned packet. "
                    f"Return a JSON object only, with kind {capability} and packet_digest equal to the assignment, "
                    f'and 2 to 8 items: {{"id":"{prefix}1","statement":"...","evidence":["E1"]}}. '
                    f"Use distinct {prefix} numbers and real packet IDs. {extra} Distinguish live proof from fixtures.")
        if self.name == "synthesis":
            _require(capability == "report_synthesis@1", "synthesis: unsupported capability")
            return ("You write a verified report from the packet and research join. Return one JSON object only with "
                    "exact keys kind, revision, packet_digest, question, title, markdown, claims. "
                    "kind is verified_report@1; echo the assigned revision, digest and question. "
                    "Write markdown with these exact headings: " + ", ".join("## " + name for name in REPORT_SECTIONS) + ". "
                    "Answer the question's live/fixture status, remaining gaps and next priority. "
                    "Use at least three distinct claims, each {id:C1, text:string, evidence:[E-id,...]} citing packet IDs. "
                    "Keep markdown at most 12000 characters. Do not turn fixture evidence into live proof. "
                    "In repair mode address every blocking and minor Quality finding, remove rejected claims, "
                    "and do not reintroduce their factual assertions under new words or IDs.")
        _require(self.name == "quality" and capability == "report_quality_review@1", "quality: unsupported capability")
        return ("You are the independent Quality reviewer. Review the candidate against the packet, not its author. "
                "Return one JSON object only with accepted (boolean) and findings (array). Each finding has "
                "claim_id (C-id or null), severity (blocking or minor), problem (specific text), and evidence (packet IDs). "
                "A factual contradiction, unsupported claim, fixture called live, or missing required report section is blocking. "
                "Style issues may be minor. Inspect every claim and the markdown. "
                "accepted must be false exactly when any finding is blocking. Rubric: " + _json(RUBRIC))

    def user_prompt(self, brief: dict) -> str:
        _require(isinstance(brief, dict), "brief: object required")
        if self.name == "research":
            _require(brief.get("kind") == "research_assignment@1", "research: invalid brief")
        elif self.name == "synthesis":
            _require(brief.get("kind") == "synthesis_assignment@1" and brief.get("mode") in ("draft", "repair"),
                     "synthesis: invalid brief")
        else:
            _require(brief.get("kind") == "quality_review_request@1", "quality: invalid brief")
        _packet_ids(brief)
        return "Assignment (canonical JSON):\n" + _json(brief)

    def parse(self, text: str, brief: dict, identity: str) -> dict:
        _string(identity, "identity")
        value = _load(text)
        ids = _packet_ids(brief)
        if self.name == "research":
            capability = brief.get("capability")
            _require(brief.get("kind") == "research_assignment@1" and capability in _IDS and
                     brief.get("revision") == "r1", "research: invalid brief")
            _keys(value, {"kind", "packet_digest", "items"}, "research")
            _require(value["kind"] == capability and value["packet_digest"] == brief["packet_digest"],
                     "research: kind or packet digest mismatch")
            items = value["items"]
            _require(isinstance(items, list) and 2 <= len(items) <= 8, "research: 2 to 8 items required")
            seen = set()
            for item in items:
                _keys(item, {"id", "statement", "evidence", "severity"} if capability == "packet_risks@1"
                      else {"id", "statement", "evidence"}, "research item")
                item_id = item["id"]
                _require(isinstance(item_id, str) and
                         re.fullmatch(_IDS[capability] + r"[1-9][0-9]*", item_id) is not None and
                         item_id not in seen, "research: invalid or duplicate item id")
                seen.add(item_id)
                _string(item["statement"], "research statement")
                _evidence(item["evidence"], ids, "research item")
                if capability == "packet_risks@1":
                    _require(item["severity"] in ("high", "medium", "low"), "risk: invalid severity")
            return value
        if self.name == "synthesis":
            _require(brief.get("kind") == "synthesis_assignment@1" and brief.get("mode") in ("draft", "repair"),
                     "synthesis: invalid brief")
            return _report(value, brief)
        _require(self.name == "quality" and brief.get("kind") == "quality_review_request@1", "quality: invalid brief")
        _require(isinstance(brief.get("policy_digest"), str) and
                 re.fullmatch(r"[0-9a-f]{64}", brief["policy_digest"]) is not None,
                 "quality: invalid policy digest")
        content = _candidate(brief)
        if set(value) == {"accepted", "findings"}:
            pass
        else:
            _keys(value, {"kind", "candidate", "reviewer", "accepted", "decided_by", "findings",
                          "rubric", "rubric_digest"}, "quality")
            expected = _verdict(brief, identity, value["accepted"], value["findings"], "model")
            for key in ("kind", "candidate", "reviewer", "decided_by", "rubric", "rubric_digest"):
                _require(value[key] == expected[key], f"quality: {key} mismatch")
        _require(type(value["accepted"]) is bool, "quality: accepted must be boolean")
        findings = _findings(value["findings"], ids, {claim["id"] for claim in content["claims"]})
        _require(value["accepted"] == (not any(f["severity"] == "blocking" for f in findings)),
                 "quality: accepted contradicts findings")
        return _verdict(brief, identity, value["accepted"], findings, "model")

    def precheck(self, brief: dict, identity: str) -> dict | None:
        if self.name != "quality":
            return None
        try:
            _string(identity, "identity")
            _require(brief.get("kind") == "quality_review_request@1", "quality: invalid brief")
            _packet_ids(brief)
            _require(isinstance(brief.get("policy_digest"), str) and
                     re.fullmatch(r"[0-9a-f]{64}", brief["policy_digest"]) is not None,
                     "quality: invalid policy digest")
            _require(brief.get("candidate", {}).get("author") != identity, "candidate: author is reviewer")
            _candidate(brief)
        except (RoleOutputError, AttributeError, TypeError) as exc:
            finding = {"claim_id": None, "severity": "blocking", "problem": str(exc), "evidence": []}
            return _verdict(brief, identity, False, [finding], "deterministic-precheck")
        return None

    def scripted_reply(self, brief: dict, identity: str) -> str:
        ids = _packet_ids(brief)
        if self.name == "research":
            capability = brief["capability"]
            _require(capability in _IDS, "research: invalid capability")
            if capability == "packet_findings@1":
                rows = [("The earlier qualification proved a live broker-backed Director, while the A and B Directors were fixtures.", ["E3"]),
                        ("The prior release receiver and capability content were fixtures.", ["E4", "E7"]),
                        ("The live C run reached a Director wait and completed with an abort and no release.", ["E9"])]
            else:
                rows = [("Quality, capability content, and the HTTP release receiver remained fixtures.", ["E7"], "high"),
                        ("Live Director coverage was limited to one account, one model, and abort.", ["E6"], "medium"),
                        ("Remote attestation and operational hardening were unbuilt.", ["E7"], "medium")]
            items = []
            for index, row in enumerate(rows, 1):
                item = {"id": f"{_IDS[capability]}{index}", "statement": row[0],
                        "evidence": [item_id for item_id in row[1] if item_id in ids]}
                if capability == "packet_risks@1":
                    item["severity"] = row[2]
                items.append(item)
            return _json({"kind": capability, "packet_digest": brief["packet_digest"], "items": items})
        if self.name == "synthesis":
            available = [
                ("The prior live C run proved a broker-backed Director for the abort route.", ["E3", "E9"]),
                ("The earlier Quality and capability content were fixtures, as was the HTTP release receiver.", ["E7"]),
                ("The live Director proof covered one account, one model, and abort only.", ["E6"]),
                ("Remote attestation and operational hardening remain unbuilt gaps.", ["E7"]),
                ("The next priority is to exercise real research, synthesis, and Quality agents while keeping release labeled as a fixture.", ["E6", "E7"]),
            ]
            rejected_ids = set()
            rejected_texts = set()
            if brief.get("mode") == "repair":
                prior = brief.get("prior") or {}
                prior_content = prior.get("content")
                if isinstance(prior_content, str):
                    prior_content = _load(prior_content)
                if isinstance(prior_content, dict):
                    prior_claims = prior_content.get("claims", [])
                    for finding in brief.get("quality_findings") or []:
                        if finding.get("severity") == "blocking":
                            rejected_ids.add(finding.get("claim_id"))
                            rejected_texts.update(c.get("text") for c in prior_claims
                                                  if c.get("id") == finding.get("claim_id"))
                    available = [(c["text"], c["evidence"]) for c in prior_claims
                                 if c.get("id") not in rejected_ids and c.get("text") not in rejected_texts]
                    available += [row for row in [
                        ("The earlier live C run reached an abort with zero releases.", ["E9"]),
                        ("Prior Quality and release were fixtures.", ["E7"]),
                        ("The next priority is independent model-backed report review.", ["E6", "E7"]),
                        ("Unbuilt remote attestation is a remaining gap.", ["E7"])]
                        if row[0] not in rejected_texts]
            claims = []
            seen = set()
            for statement, evidence in available:
                if statement in seen or statement in rejected_texts:
                    continue
                selected = [item_id for item_id in evidence if item_id in ids]
                if not selected:
                    continue
                seen.add(statement)
                claims.append({"id": f"C{len(claims) + 1}", "text": statement, "evidence": selected})
            _require(len(claims) >= 3, "scripted synthesis: too few safe claims")
            lines = ["## Live-proven", claims[0]["text"], "", "## Fixture-only",
                     claims[1]["text"], "", "## Remaining gaps", claims[2]["text"], "",
                     "## Next priority", claims[-1]["text"]]
            return _json({"kind": "verified_report@1", "revision": brief["revision"],
                          "packet_digest": brief["packet_digest"], "question": brief["question"],
                          "title": "Qualification report", "markdown": "\n".join(lines), "claims": claims})
        _require(self.name == "quality", "role: unsupported")
        content = _candidate(brief)
        planted = json.loads((Path(__file__).resolve().parents[1] / "scenarios" / "sf_stimuli.json").read_text())
        planted_texts = {route["append_claim"]["text"] for route in planted.values()}
        findings = [{"claim_id": claim["id"], "severity": "blocking",
                     "problem": "The claim contradicts packet E7: " + claim["text"], "evidence": ["E7"]}
                    for claim in content["claims"] if claim["text"] in planted_texts]
        return _json({"accepted": not findings, "findings": findings})


ROLES: dict[str, Role] = {name: Role(name) for name in ("research", "synthesis", "quality")}


def usefulness_check(content: dict, packet: dict) -> dict:
    """Check report readability and citation coverage without making a model decision."""
    reasons = []
    ids = {item.get("id") for item in packet.get("items", []) if isinstance(item, dict)} if isinstance(packet, dict) else set()
    claims = content.get("claims") if isinstance(content, dict) else None
    if not isinstance(claims, list) or len(claims) < 3:
        reasons.append("at least three claims required")
    if isinstance(claims, list):
        for index, claim in enumerate(claims, 1):
            cited = claim.get("evidence") if isinstance(claim, dict) else None
            if not isinstance(cited, list) or not cited or any(item not in ids for item in cited):
                reasons.append(f"claim {index} lacks valid packet evidence")
    markdown = content.get("markdown") if isinstance(content, dict) else None
    if not isinstance(markdown, str) or not markdown.strip():
        reasons.append("markdown is empty")
    else:
        for section in REPORT_SECTIONS:
            if re.search(rf"(?m)^\s*#{{1,6}}\s+{re.escape(section)}\s*#*\s*$", markdown) is None:
                reasons.append(f"missing {section} heading")
    return {"ok": not reasons, "reasons": reasons}
