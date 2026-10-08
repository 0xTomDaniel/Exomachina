"""Pure, strict wire contracts for packet-backed report agents."""
from __future__ import annotations

import hashlib
import json
import re


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


# The report factory's acceptance criteria for independent Quality review. The
# factory owns them, sends their content in the Quality brief, and keeps only
# their digest (``rubric_digest``) in its pinned quality policy as evidence.
REPORT_ACCEPTANCE_CRITERIA = {
    "kind": "report-quality@1",
    "blocking": ["factual contradiction of the packet", "uncited or unsupported claim",
                 "fixture described as live", "missing required section"],
    "minor": ["style issues"],
}


def validate_acceptance_criteria(value: object) -> dict:
    value = _keys(value, {"kind", "blocking", "minor"})
    if (not _nonempty(value["kind"])
            or not all(isinstance(value[k], list) and all(_nonempty(x) for x in value[k])
                       for k in ("blocking", "minor"))
            or not value["blocking"]):
        raise ValueError("invalid acceptance criteria")
    return value


def _keys(value: object, required: set[str], optional: set[str] = frozenset()) -> dict:
    if not isinstance(value, dict) or not required <= set(value) or set(value) - required - optional:
        raise ValueError(f"expected fields {sorted(required)}")
    return value


def _nonempty(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def validate_packet(packet: object) -> dict:
    packet = _keys(packet, {"kind", "packet_id", "title", "default_question", "items"})
    if (packet["kind"] != "evidence_packet@1" or not all(_nonempty(packet[k]) for k in
            ("packet_id", "title", "default_question")) or not isinstance(packet["items"], list)
            or not 8 <= len(packet["items"]) <= 14):
        raise ValueError("invalid evidence packet")
    ids = set()
    for item in packet["items"]:
        _keys(item, {"id", "source", "text"})
        if (not isinstance(item["id"], str) or not re.fullmatch(r"E[1-9][0-9]*", item["id"])
                or item["id"] in ids or not isinstance(item["source"], str)
                or not re.fullmatch(r"prototype/temporal-factory/QUALIFICATION\.md#L[1-9][0-9]*-L[1-9][0-9]*", item["source"])
                or not _nonempty(item["text"])):
            raise ValueError("invalid evidence packet item")
        ids.add(item["id"])
    if len(canonical(packet).encode()) > 12 * 1024:
        raise ValueError("evidence packet exceeds 12 KB")
    return packet


def packet_digest(packet: dict) -> str:
    return digest(validate_packet(packet))


def _evidence(value: object, packet: dict) -> None:
    ids = {item["id"] for item in validate_packet(packet)["items"]}
    if not isinstance(value, list) or not value or any(x not in ids for x in value):
        raise ValueError("unknown or empty evidence ids")


def research_assignment(capability: str, question: str, packet: dict) -> dict:
    if capability not in {"packet_findings@1", "packet_risks@1"} or not _nonempty(question):
        raise ValueError("invalid research assignment")
    return {"kind": "research_assignment@1", "capability": capability, "revision": "r1",
            "question": question, "packet": validate_packet(packet), "packet_digest": packet_digest(packet)}


def validate_research_result(value: object, capability: str, packet: dict) -> dict:
    value = _keys(value, {"kind", "packet_digest", "items"})
    if (capability not in {"packet_findings@1", "packet_risks@1"} or value["kind"] != capability
            or value["packet_digest"] != packet_digest(packet) or not isinstance(value["items"], list)
            or not 2 <= len(value["items"]) <= 8):
        raise ValueError("invalid research result")
    prefix = "F" if capability == "packet_findings@1" else "R"
    ids = set()
    for item in value["items"]:
        _keys(item, {"id", "statement", "evidence"} | ({"severity"} if prefix == "R" else set()))
        if (not isinstance(item["id"], str) or not re.fullmatch(prefix + r"[1-9][0-9]*", item["id"])
                or item["id"] in ids or not _nonempty(item["statement"])):
            raise ValueError("invalid research item")
        if prefix == "R" and item["severity"] not in {"high", "medium", "low"}:
            raise ValueError("invalid risk severity")
        _evidence(item["evidence"], packet)
        ids.add(item["id"])
    return value


def packet_evidence_join(results: dict[str, dict], packet: dict) -> dict:
    if set(results) != {"research_findings", "research_risks"}:
        raise ValueError("join needs both report branches")
    findings = validate_research_result(results["research_findings"]["content"], "packet_findings@1", packet)
    risks = validate_research_result(results["research_risks"]["content"], "packet_risks@1", packet)
    return {"kind": "packet_evidence_join@1", "packet_digest": packet_digest(packet),
            "findings": findings["items"], "risks": risks["items"],
            "branch_artifact_sha256": {name: results[name]["sha256"] for name in sorted(results)}}


def synthesis_assignment(revision: str, question: str, packet: dict) -> dict:
    """The synthesis brief: factory-own fields only.

    Research results and, on repair, the rejected draft and Quality's verdict
    (its findings) are consumed hand-off items that travel as their own Parts
    after the brief (decisions 5 and 9); the brief never embeds them.
    """
    if revision not in {"r1", "r2", "r3"} or not _nonempty(question):
        raise ValueError("invalid synthesis revision or question")
    mode = "draft" if revision == "r1" else "repair"
    return {"kind": "synthesis_assignment@1", "mode": mode, "revision": revision,
            "question": question, "packet": validate_packet(packet), "packet_digest": packet_digest(packet)}


def validate_report(value: object, revision: str, question: str, packet: dict) -> dict:
    value = _keys(value, {"kind", "revision", "packet_digest", "question", "title", "markdown", "claims"})
    if (value["kind"] != "verified_report@1" or value["revision"] != revision
            or value["packet_digest"] != packet_digest(packet) or value["question"] != question
            or not _nonempty(value["title"]) or not _nonempty(value["markdown"])
            or len(value["markdown"]) > 12000 or not isinstance(value["claims"], list)
            or len(value["claims"]) < 3):
        raise ValueError("invalid verified report")
    ids = set()
    for claim in value["claims"]:
        _keys(claim, {"id", "text", "evidence"})
        if (not isinstance(claim["id"], str) or not re.fullmatch(r"C[1-9][0-9]*", claim["id"])
                or claim["id"] in ids or not _nonempty(claim["text"])):
            raise ValueError("invalid report claim")
        _evidence(claim["evidence"], packet)
        ids.add(claim["id"])
    return value


def quality_review_request(candidate: dict, question: str, packet: dict,
                           acceptance_criteria: dict, rubric_digest: str | None = None) -> dict:
    """The Quality brief. The candidate draft travels as its own Part; the
    factory validates it here but never embeds it in the brief. The acceptance
    criteria travel as content; their digest stays factory-side evidence."""
    if not isinstance(candidate, dict) or not {"revision", "sha256", "author", "content"} <= set(candidate):
        raise ValueError("invalid candidate")
    if candidate["sha256"] != hashlib.sha256(candidate["content"].encode()).hexdigest():
        raise ValueError("invalid candidate")
    criteria = validate_acceptance_criteria(acceptance_criteria)
    if rubric_digest is not None and digest(criteria) != rubric_digest:
        raise ValueError("acceptance criteria differ from the pinned quality policy digest")
    validate_report(json.loads(candidate["content"]), candidate["revision"], question, packet)
    return {"kind": "quality_review_request@1", "revision": candidate["revision"],
            "question": question, "packet": validate_packet(packet), "packet_digest": packet_digest(packet),
            "acceptance_criteria": criteria}


def validate_verdict(value: object, candidate: dict, reviewer: str, *,
                     packet: dict | None = None, rubric_digest: str | None = None) -> dict:
    """Validate a verdict against the factory's accepted candidate.

    The verdict names its candidate by ``{revision, sha256}`` as the agent
    computed them over the Part it received. Author/reviewer independence is
    the factory's policy over its own bindings (``reviewer`` is the pinned
    Quality identity, ``candidate["author"]`` the pinned synthesis identity).
    """
    value = _keys(value, {"kind", "candidate", "accepted", "decided_by", "findings", "rubric", "rubric_digest"})
    _keys(value["candidate"], {"revision", "sha256"})
    if (value["kind"] != "quality_verdict@1" or value["candidate"] != {k: candidate[k] for k in ("revision", "sha256")}
            or not _nonempty(reviewer) or reviewer == candidate.get("author")
            or type(value["accepted"]) is not bool or value["decided_by"] not in {"model", "deterministic-precheck"}
            or value["rubric"] != "report-quality@1" or not _nonempty(value["rubric_digest"])
            or (rubric_digest is not None and value["rubric_digest"] != rubric_digest)
            or not isinstance(value["findings"], list)):
        raise ValueError("invalid quality verdict")
    for finding in value["findings"]:
        _keys(finding, {"claim_id", "severity", "problem", "evidence"})
        if (finding["claim_id"] is not None and not _nonempty(finding["claim_id"])) or finding["severity"] not in {"blocking", "minor"} or not _nonempty(finding["problem"]) or not isinstance(finding["evidence"], list):
            raise ValueError("invalid quality finding")
        if packet is not None:
            ids = {item["id"] for item in validate_packet(packet)["items"]}
            if any(evidence_id not in ids for evidence_id in finding["evidence"]):
                raise ValueError("unknown quality evidence id")
        if finding["claim_id"] is not None:
            claims = {claim["id"] for claim in json.loads(candidate["content"])["claims"]}
            if finding["claim_id"] not in claims:
                raise ValueError("unknown quality claim id")
    if value["accepted"] == any(x["severity"] == "blocking" for x in value["findings"]):
        raise ValueError("acceptance disagrees with blocking findings")
    return value
