"""Check normalized arbitration evidence without prescribing an engine DSL.

The oracle cross-checks identities, types, revisions and counters. Native engine
history, process logs, and fault-controller records remain necessary evidence
for claims that a candidate actually produced the normalized observation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from fixture import BRIEFS, canonical, quality_decision, sha256_text, typed_join


NEGATIVES = {"review_bypass", "wrong_join_type", "bad_repair_bound",
             "mutable_active_closure", "unapproved_capability", "unsafe_arbitrary_code"}
FAULTS = {"in_flight_assignment", "quality_verdict_gap", "acceptance_ack_gap",
          "repair_or_nested_wait", "conflicting_stale_owner"}
HERE = Path(__file__).resolve().parent
PRIOR = HERE.parents[1] / "decision-round" / "common"
SOURCE_FILES = [HERE / name for name in ("fixture.py", "quality_server.py",
    "release_server.py", "receiver_client.py", "service_probe.py", "oracle.py")]
SOURCE_FILES += [PRIOR / "harness_server.py", PRIOR / "client.py"]


class OracleFailure(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise OracleFailure(message)


def field(record, key):
    require(isinstance(record, dict) and key in record, f"missing {key}")
    return record[key]


def verify_service(value):
    require(value.get("fixture_only") is True, "service record must be labeled fixture_only")
    ids = field(value, "identities")
    names = [field(ids, name)["identity"] for name in ("source", "counter", "quality")]
    require(len(set(names)) == 3, "Strands identities must be distinct")
    branches = field(value, "branch_receipts")
    sample = branches["source_evidence"]
    run_id, digest = sample["run_id"], sample["definition_digest"]
    require(typed_join(branches, run_id=run_id, definition_digest=digest)
            == field(value, "typed_join"), "typed join mismatch")
    intervals = field(value, "branch_intervals_ns")
    require(set(intervals) == set(BRIEFS), "service branch intervals missing")
    starts, ends = [], []
    for name in BRIEFS:
        start, end = field(intervals[name], "start"), field(intervals[name], "end")
        require(isinstance(start, int) and isinstance(end, int) and start < end,
                "invalid service branch interval")
        starts.append(start)
        ends.append(end)
    require(max(starts) < min(ends), "service branches did not overlap")
    artifacts = field(value, "candidate_artifacts")
    for label, expected in (("quality_rejected", False), ("quality_approved", True)):
        quality = field(value, label)
        artifact = artifacts["r1" if not expected else "r2"]
        verdict = quality["artifact"]
        require(quality["harness_identity"] == ids["quality"]["identity"]
                == verdict["reviewer"], "Quality identity mismatch")
        require(quality["harness_role"] == "quality", "wrong Quality role")
        require(quality["run_id"] == run_id and quality["definition_digest"] == digest,
                "Quality run/definition mismatch")
        require(verdict["accepted"] is expected and verdict["revision"] == artifact["revision"]
                and verdict["sha256"] == artifact["sha256"], "Quality exact verdict mismatch")
        require(quality_decision(artifact, ids["quality"]["identity"])[0] is expected,
                "Quality policy mismatch")
    exhausted = field(value, "quality_exhausted")
    require(len(exhausted) == 3, "exhaustion needs r1/r2/r3 Quality verdicts")
    for number, quality in enumerate(exhausted, 1):
        verdict = quality["artifact"]
        require(verdict["accepted"] is False and verdict["revision"] == f"r{number}",
                "exhaustion Quality sequence mismatch")
        require(quality["harness_identity"] == ids["quality"]["identity"]
                == verdict["reviewer"], "exhaustion reviewer mismatch")
    rejected_action = field(value, "quality_rejected_receiver_action")
    require(rejected_action["accepted_count"] == 1
            and rejected_action["artifact"]["accepted"] is False,
            "rejected Quality receiver action was conflated with acceptance")
    lost = field(value, "receiver_lost_ack")
    require(lost["accepted_count"] == 1 and lost["attempts"] == 1,
            "receiver lost-ack counts mismatch")
    delivered = field(value, "release_receipt")
    require(delivered["accepted_effect_count"] == 1 and delivered["attempts"] == 2,
            "release dedup counts mismatch")
    require(delivered["revision"] == "r2" and delivered["sha256"] == artifacts["r2"]["sha256"],
            "release exact artifact mismatch")
    require(value["opaque_effects_evaluator_only"] == 1,
            "opaque evaluator effect count mismatch")
    return {"verified": True, "kind": "service-fixture", "strands_identities": 3,
            "quality_verdicts": 5, "receiver_effects": 1, "release_effects": 1}


def source_hashes():
    return {path.relative_to(HERE.parents[1]).as_posix():
            hashlib.sha256(path.read_bytes()).hexdigest() for path in SOURCE_FILES}


def summarize_service(value):
    """Keep a stable, sanitized observation after temporary state is removed."""
    verdict = verify_service(value)
    quality = [value["quality_rejected"]["artifact"]["accepted"],
               value["quality_approved"]["artifact"]["accepted"]]
    quality += [item["artifact"]["accepted"] for item in value["quality_exhausted"]]
    return {
        "schema": "exomachina.arbitration.common-observation/1",
        "fixture_only": True,
        "a2a_protocol": value["protocol"],
        "observed": {
            "distinct_strands_a2a_identities": verdict["strands_identities"],
            "typed_branch_join": sorted(BRIEFS),
            "branch_assignments_overlapped": True,
            "quality_verdicts_in_order": quality,
            "rejected_quality_receiver_action_count":
                value["quality_rejected_receiver_action"]["accepted_count"],
            "lost_assignment_reply": {"receiver_effects":
                value["receiver_lost_ack"]["accepted_count"],
                "submission_attempts": value["receiver_lost_ack"]["attempts"]},
            "lost_release_reply": {"receiver_effects":
                value["release_receipt"]["accepted_effect_count"],
                "submission_attempts_after_repeat": value["release_receipt"]["attempts"]},
            "opaque_reply": {"caller_outcome": "unresolved", "lookup_available": False,
                "evaluator_only_effects": value["opaque_effects_evaluator_only"]},
        },
        "oracle_verdict": verdict,
        "source_sha256": source_hashes(),
    }


def verify_summary(value):
    require(value.get("schema") == "exomachina.arbitration.common-observation/1"
            and value.get("fixture_only") is True, "unknown common summary schema")
    require(value.get("a2a_protocol") == "A2A 0.3.0", "A2A protocol mismatch")
    require(value.get("source_sha256") == source_hashes(),
            "retained observation does not match fixture source bytes")
    expected_verdict = {"verified": True, "kind": "service-fixture",
        "strands_identities": 3, "quality_verdicts": 5,
        "receiver_effects": 1, "release_effects": 1}
    require(value.get("oracle_verdict") == expected_verdict, "retained oracle verdict mismatch")
    observed = field(value, "observed")
    require(observed.get("distinct_strands_a2a_identities") == 3
            and observed.get("typed_branch_join") == sorted(BRIEFS)
            and observed.get("branch_assignments_overlapped") is True,
            "retained branch evidence mismatch")
    require(observed.get("quality_verdicts_in_order") == [False, True, False, False, False]
            and observed.get("rejected_quality_receiver_action_count") == 1,
            "retained Quality evidence mismatch")
    require(observed.get("lost_assignment_reply") == {"receiver_effects": 1,
            "submission_attempts": 1}, "retained assignment evidence mismatch")
    require(observed.get("lost_release_reply") == {"receiver_effects": 1,
            "submission_attempts_after_repeat": 2}, "retained release evidence mismatch")
    require(observed.get("opaque_reply") == {"caller_outcome": "unresolved",
            "lookup_available": False, "evaluator_only_effects": 1},
            "retained opaque evidence mismatch")
    return {"verified": True, "kind": "retained-common-observation",
            "source_files": len(SOURCE_FILES), "fixture_only": True}


def verify_run(value, outcome):
    """Check one visible v3 child and its public parent result."""
    require(outcome in {"success", "exhaustion"}, "unknown run outcome")
    run_id = field(value, "run_id")
    digest = field(value, "definition_digest")
    quality_identity = field(value, "quality_identity")
    require(all(isinstance(item, str) and item for item in (run_id, digest, quality_identity)),
            "missing run/definition/Quality identity")
    branches = field(value, "branch_receipts")
    require(set(branches) == set(BRIEFS), "visible v3 requires both branch roles")
    join = typed_join(branches, run_id=run_id, definition_digest=digest)
    require(join == field(value, "join"), "joined data or branch digests differ")
    intervals = field(value, "branch_intervals_ns")
    require(set(intervals) == set(BRIEFS), "missing branch timing")
    starts, ends = [], []
    for name in BRIEFS:
        interval = intervals[name]
        start, end = field(interval, "start"), field(interval, "end")
        require(isinstance(start, int) and isinstance(end, int) and start < end,
                "invalid branch interval")
        starts.append(start)
        ends.append(end)
    require(max(starts) < min(ends), "two capability assignments did not overlap")

    revisions = field(value, "revisions")
    expected = [False, True] if outcome == "success" else [False, False, False]
    require(len(revisions) == len(expected), "wrong revision count")
    require(field(value, "repair_count") == len(expected) - 1,
            "repair count must be one or the exhausted cap of two")
    quality_actions = set()
    for number, (record, accepted) in enumerate(zip(revisions, expected), 1):
        artifact = field(record, "artifact")
        quality = field(record, "quality")
        verdict = field(quality, "artifact")
        require(artifact.get("revision") == f"r{number}", "revision lineage mismatch")
        require(artifact.get("sha256") == sha256_text(field(artifact, "content")),
                "candidate digest mismatch")
        body = json.loads(artifact["content"])
        require(body.get("join") == join, "candidate was not made from current typed join")
        require(quality_decision(artifact, quality_identity)[0] is accepted,
                "Quality fixture policy disagrees with observed verdict")
        require(quality.get("run_id") == run_id and quality.get("definition_digest") == digest,
                "Quality run/definition binding mismatch")
        require(quality.get("harness_role") == "quality"
                and quality.get("harness_identity") == quality_identity
                and verdict.get("reviewer") == quality_identity,
                "Quality identity was not verified")
        require(verdict.get("revision") == artifact["revision"]
                and verdict.get("sha256") == artifact["sha256"]
                and verdict.get("accepted") is accepted,
                "Quality verdict is not exact current artifact")
        action = field(quality, "action_id")
        require(action not in quality_actions, "reused Quality action across revisions")
        quality_actions.add(action)

    acceptance = value.get("acceptance")
    delivery = value.get("release_receipt")
    child = field(value, "public_child_result")
    parent = field(value, "public_parent_result")
    if outcome == "success":
        final = revisions[-1]
        artifact, quality = final["artifact"], final["quality"]
        require(isinstance(acceptance, dict) and acceptance.get("count") == 1,
                "one authoritative acceptance required")
        require(acceptance.get("run_id") == run_id
                and acceptance.get("definition_digest") == digest
                and acceptance.get("revision") == artifact["revision"]
                and acceptance.get("sha256") == artifact["sha256"]
                and acceptance.get("quality_task_id") == quality["task_id"],
                "acceptance is not bound to current Quality result")
        require(isinstance(delivery, dict) and delivery.get("run_id") == run_id
                and delivery.get("definition_digest") == digest
                and delivery.get("revision") == artifact["revision"]
                and delivery.get("sha256") == artifact["sha256"]
                and delivery.get("accepted_effect_count") == 1,
                "release receipt is not one exact effect")
        for result in (child, parent):
            require(result.get("status") == "accepted"
                    and result.get("revision") == artifact["revision"]
                    and result.get("sha256") == artifact["sha256"]
                    and result.get("release_id") == delivery["release_id"],
                    "nested public accepted result mismatch")
        require(value.get("director_wait") is None, "success must not enter Director wait")
    else:
        require(acceptance is None and delivery is None,
                "exhausted child cannot accept or release")
        wait = field(value, "director_wait")
        command = field(wait, "command")
        require(wait.get("scope") == "nested_child"
                and wait.get("current_revision") == "r3"
                and wait.get("persisted_after_restart") is True,
                "exhausted repair did not persist a nested Director wait")
        require(command.get("decision") == "abort" and command.get("authorized") is True
                and command.get("revision") == "r3" and command.get("accepted") is True,
                "current authorized abort missing")
        require(child.get("status") == "aborted" and parent.get("status") == "aborted",
                "nested abort result mismatch")
    return {"verified": True, "kind": f"visible-v3-{outcome}",
            "run_id": run_id, "quality_verdicts": len(revisions),
            "repairs": len(revisions) - 1, "acceptances": 1 if outcome == "success" else 0,
            "release_effects": 1 if outcome == "success" else 0}


def verify_candidate(value, require_a2=False):
    require(value.get("schema") == "exomachina.arbitration.observation/1",
            "unknown candidate observation schema")
    require(isinstance(value.get("candidate"), str) and value["candidate"],
            "candidate name missing")
    a1 = field(value, "a1")
    versions = field(a1, "versions")
    old, new = field(versions, "old"), field(versions, "new")
    require(old.get("version") == "v2" and new.get("version") == "v3",
            "expected v2 waiting across v3 publication")
    require(old.get("wait_started_ns", 0) < new.get("published_ns", 0),
            "v2 was not waiting before v3 published")
    require(old.get("definition_digest") != new.get("definition_digest"),
            "v3 is not a new definition")
    for key in ("closure_digest", "child_binding"):
        require(old.get(key + "_before") and old.get(key + "_before") == old.get(key + "_after"),
                f"v2 {key} changed while active")
    require(versions.get("worker_code_sha256_before")
            == versions.get("worker_code_sha256_after")
            and versions.get("worker_code_sha256_before"),
            "new definition required worker code replacement")
    success = verify_run(field(a1, "success"), "success")
    exhaustion = verify_run(field(a1, "exhaustion"), "exhaustion")
    require(a1["success"]["definition_digest"] == new["definition_digest"]
            == a1["exhaustion"]["definition_digest"],
            "v3 runs did not bind published definition")

    a3 = field(value, "a3")
    negatives = field(a3, "publication_negatives")
    require(set(negatives) == NEGATIVES, "missing six publication negatives")
    for name, record in negatives.items():
        require(record.get("status") == "rejected" and record.get("reason")
                and record.get("catalog_digest_before") == record.get("catalog_digest_after")
                and record.get("catalog_digest_before"),
                f"publication negative did not fail closed: {name}")
    runtime = field(a3, "authority_negatives")
    require(set(runtime) == {"forged_quality_identity", "stale_revision"},
            "missing authority negatives")
    for name, record in runtime.items():
        require(record.get("status") == "rejected" and record.get("acceptance_count_after")
                == record.get("acceptance_count_before") and record.get("reason"),
                f"authority negative advanced acceptance: {name}")

    a4 = field(value, "a4")
    require(set(a4) == FAULTS, "missing separate A4 fault points")
    for name, record in a4.items():
        require(record.get("automatic_recovery") is True
                and record.get("driver_retry_or_resume") is False
                and record.get("original_run_id") == record.get("recovered_run_id")
                and record.get("original_task_id") == record.get("recovered_task_id")
                and record.get("evidence_ref"),
                f"autonomous same-task recovery not evidenced: {name}")
    require(a4["conflicting_stale_owner"].get("stale_command") == "rejected",
            "stale owner was not fenced")

    a5 = field(value, "a5")
    opaque = field(a5, "opaque_receiver")
    require(opaque.get("state") == "durable_unresolved"
            and opaque.get("resubmissions") == 0
            and opaque.get("retained_after_restart") is True,
            "opaque lost reply did not remain durably ambiguous")
    counts = field(a5, "separate_counters")
    require(set(counts) >= {"remote_submission_attempts", "receiver_effects",
            "quality_negative_verdicts", "quality_positive_verdicts",
            "product_acceptances", "release_submission_attempts", "release_effects"},
            "separate effect/verdict/acceptance counters missing")
    owned = field(value, "product_owned_code")
    require(set(owned) >= {"validator_publisher", "executor_interpreter",
            "reconciler", "supervisor_lifecycle", "durable_scheduler_or_queue"},
            "product-owned responsibility accounting missing")

    a2 = value.get("a2")
    if require_a2:
        require(isinstance(a2, dict), "A2 withheld-composition result missing")
    if a2 is not None:
        require(a2.get("freeze_received_before_reveal") is True
                and a2.get("code_inventory_before") == a2.get("code_inventory_after")
                and a2.get("new_definition_digest")
                and a2.get("published_and_executed") is True,
                "A2 freeze/publication evidence mismatch")
    return {"verified": True, "kind": "candidate-normalized-observation",
            "candidate": value["candidate"], "a1": [success, exhaustion],
            "a2": "checked" if a2 else "pending", "a3_negatives": len(negatives),
            "a4_faults": len(a4), "a5_opaque": "durable_unresolved"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="action", required=True)
    service = sub.add_parser("verify-service")
    service.add_argument("result", type=Path)
    summary = sub.add_parser("verify-summary")
    summary.add_argument("result", type=Path)
    run = sub.add_parser("verify-run")
    run.add_argument("outcome", choices=["success", "exhaustion"])
    run.add_argument("result", type=Path)
    candidate = sub.add_parser("verify-candidate")
    candidate.add_argument("result", type=Path)
    candidate.add_argument("--require-a2", action="store_true")
    args = parser.parse_args()
    value = json.loads(args.result.read_text())
    if args.action == "verify-service":
        result = verify_service(value)
    elif args.action == "verify-summary":
        result = verify_summary(value)
    elif args.action == "verify-run":
        result = verify_run(value, args.outcome)
    else:
        result = verify_candidate(value, args.require_a2)
    print(json.dumps(result, indent=2, sort_keys=True))
