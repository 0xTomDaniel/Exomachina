"""Focused validation tests for the nested-factory command protocol."""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from supplier_protocol import (  # noqa: E402
    CHILD_FIELDS,
    MAX_CANONICAL_INPUT_BYTES,
    MAX_INPUT_NESTING,
    PARENT_FIELDS,
    SUPPLIER_ECHO_FIELDS,
    NestedFactoryEnvelope,
    SupplierEnvelopeError,
    nested_factory_fingerprint,
    parse_nested_factory_envelope,
    project_supplier_echo,
    supplier_assignment_echo_declaration,
    validate_supplier_echo,
)


def command_fixture() -> dict:
    return {
        "op": "nested_factory",
        "action_id": "test:child-action/1",
        "run_id": "test.child.run-1",
        "definition_digest": "a" * 64,
        "parent_task_id": "test:parent-task/1",
        "parent_run_id": "test.parent.run-1",
        "parent_definition_digest": "b" * 64,
        "parent_assignment_id": "test:parent-assignment/1",
        "parent_attempt_id": "test:parent-attempt/1",
        "assignment_id": "test:child-assignment/1",
        "attempt_id": "test:child-attempt/1",
        "payload": {"inputs": {"fixture": "synthetic", "nested": [1, True, None]}},
    }


class SupplierProtocolTests(unittest.TestCase):
    def test_declaration_is_exact_and_fresh(self):
        declaration = supplier_assignment_echo_declaration()
        self.assertEqual(declaration, {
            "version": 1,
            "fields": sorted((*PARENT_FIELDS, *CHILD_FIELDS)),
        })
        self.assertEqual(tuple(declaration["fields"]), SUPPLIER_ECHO_FIELDS)
        declaration["fields"].clear()
        self.assertEqual(len(supplier_assignment_echo_declaration()["fields"]), 10)

    def test_parse_returns_detached_binding_and_inputs(self):
        command = command_fixture()
        parsed = parse_nested_factory_envelope(command)
        self.assertIsInstance(parsed, NestedFactoryEnvelope)
        self.assertEqual(parsed.echo_tuple(), {
            field: command[field] for field in SUPPLIER_ECHO_FIELDS
        })
        self.assertEqual(parsed.inputs, command["payload"]["inputs"])
        parsed.inputs["fixture"] = "changed locally"
        self.assertEqual(command["payload"]["inputs"]["fixture"], "synthetic")
        self.assertEqual(parsed.to_command()["payload"]["inputs"]["fixture"],
                         "changed locally")

    def test_fingerprint_is_canonical_and_binds_all_facts(self):
        command = command_fixture()
        reordered = dict(reversed(list(command.items())))
        self.assertEqual(nested_factory_fingerprint(command),
                         nested_factory_fingerprint(reordered))
        changed_parent = deepcopy(command)
        changed_parent["parent_attempt_id"] = "test:parent-attempt/2"
        changed_input = deepcopy(command)
        changed_input["payload"]["inputs"]["fixture"] = "other synthetic fixture"
        self.assertNotEqual(nested_factory_fingerprint(command),
                            nested_factory_fingerprint(changed_parent))
        self.assertNotEqual(nested_factory_fingerprint(command),
                            nested_factory_fingerprint(changed_input))
        parsed = parse_nested_factory_envelope(command)
        self.assertEqual(nested_factory_fingerprint(parsed),
                         nested_factory_fingerprint(command))

    def test_exact_command_and_payload_shapes_are_required(self):
        command = command_fixture()
        for mutate in (
            lambda value: value.pop("attempt_id"),
            lambda value: value.update(unexpected="field"),
            lambda value: value.update(op="start"),
            lambda value: value.update(payload={"inputs": {}, "extra": True}),
            lambda value: value.update(payload={"input": {}}),
            lambda value: value.update(payload={"inputs": []}),
        ):
            malformed = deepcopy(command)
            mutate(malformed)
            with self.subTest(malformed=malformed):
                with self.assertRaises(SupplierEnvelopeError):
                    parse_nested_factory_envelope(malformed)

    def test_all_opaque_ids_and_both_digests_are_validated(self):
        command = command_fixture()
        id_fields = ("parent_task_id", "parent_run_id", "parent_assignment_id",
                     "parent_attempt_id", "action_id", "run_id", "assignment_id",
                     "attempt_id")
        for field in id_fields:
            for invalid in ("", " leading", "has space", "é", "x" * 257):
                malformed = deepcopy(command)
                malformed[field] = invalid
                with self.subTest(field=field, invalid=invalid):
                    with self.assertRaises(SupplierEnvelopeError):
                        parse_nested_factory_envelope(malformed)
        for field in ("definition_digest", "parent_definition_digest"):
            for invalid in ("a" * 63, "A" * 64, "g" * 64):
                malformed = deepcopy(command)
                malformed[field] = invalid
                with self.subTest(field=field, invalid=invalid):
                    with self.assertRaises(SupplierEnvelopeError):
                        parse_nested_factory_envelope(malformed)

    def test_inputs_reject_non_json_and_non_finite_values(self):
        for invalid_inputs in (
            {"number": float("nan")},
            {"number": float("inf")},
            {"nested": (1, 2)},
            {1: "non-string key"},
            {"value": object()},
        ):
            malformed = command_fixture()
            malformed["payload"]["inputs"] = invalid_inputs
            with self.subTest(invalid=invalid_inputs):
                with self.assertRaises(SupplierEnvelopeError):
                    parse_nested_factory_envelope(malformed)

    def test_canonical_input_byte_limit_accepts_boundary_and_rejects_overflow(self):
        # {"x":"..."} is eight canonical UTF-8 bytes beyond the string.
        at_limit = command_fixture()
        at_limit["payload"]["inputs"] = {
            "x": "a" * (MAX_CANONICAL_INPUT_BYTES - 8),
        }
        self.assertEqual(
            len(json.dumps(
                at_limit["payload"]["inputs"], sort_keys=True,
                separators=(",", ":"), ensure_ascii=False).encode("utf-8")),
            MAX_CANONICAL_INPUT_BYTES,
        )
        self.assertEqual(parse_nested_factory_envelope(at_limit).inputs,
                         at_limit["payload"]["inputs"])

        over_limit = command_fixture()
        over_limit["payload"]["inputs"] = {
            "x": "a" * (MAX_CANONICAL_INPUT_BYTES - 7),
        }
        with self.assertRaises(SupplierEnvelopeError):
            parse_nested_factory_envelope(over_limit)

        # The cap counts UTF-8 bytes, not Python code points.
        emoji_count = (MAX_CANONICAL_INPUT_BYTES - 8) // 4
        multibyte = command_fixture()
        multibyte["payload"]["inputs"] = {"x": "😀" * emoji_count}
        self.assertEqual(parse_nested_factory_envelope(multibyte).inputs,
                         multibyte["payload"]["inputs"])
        multibyte["payload"]["inputs"]["x"] += "😀"
        with self.assertRaises(SupplierEnvelopeError):
            parse_nested_factory_envelope(multibyte)

    def test_input_container_depth_boundary_and_fingerprint_recheck(self):
        def nested(depth):
            value = None
            for _ in range(depth):
                value = {"x": value}
            return value

        at_limit = command_fixture()
        at_limit["payload"]["inputs"] = nested(MAX_INPUT_NESTING)
        envelope = parse_nested_factory_envelope(at_limit)
        self.assertEqual(envelope.inputs, nested(MAX_INPUT_NESTING))
        self.assertEqual(len(nested_factory_fingerprint(at_limit)), 64)

        over_limit = command_fixture()
        over_limit["payload"]["inputs"] = nested(MAX_INPUT_NESTING + 1)
        with self.assertRaises(SupplierEnvelopeError):
            parse_nested_factory_envelope(over_limit)

        # The frozen envelope's nested JSON tree can be edited by a caller, so
        # both command projection and fingerprinting re-enforce the same bound.
        envelope.inputs["x"] = "a" * MAX_CANONICAL_INPUT_BYTES
        with self.assertRaises(SupplierEnvelopeError):
            envelope.to_command()
        with self.assertRaises(SupplierEnvelopeError):
            nested_factory_fingerprint(envelope)

        envelope.inputs.clear()
        envelope.inputs["x"] = nested(MAX_INPUT_NESTING + 1)
        with self.assertRaises(SupplierEnvelopeError):
            nested_factory_fingerprint(envelope)

    def test_echo_projection_is_safe_and_requires_exact_bindings(self):
        command = command_fixture()
        envelope = parse_nested_factory_envelope(command)
        echo = envelope.echo_tuple()
        echo["provider_extra"] = "not projected"
        self.assertEqual(project_supplier_echo(echo), envelope.echo_tuple())
        self.assertEqual(validate_supplier_echo(envelope, echo), envelope.echo_tuple())
        for field in SUPPLIER_ECHO_FIELDS:
            wrong = envelope.echo_tuple()
            wrong[field] = "c" * 64 if field.endswith("definition_digest") else "wrong"
            with self.subTest(field=field):
                with self.assertRaises(SupplierEnvelopeError):
                    validate_supplier_echo(envelope, wrong)
        incomplete = envelope.echo_tuple()
        incomplete.pop("parent_task_id")
        with self.assertRaises(SupplierEnvelopeError):
            project_supplier_echo(incomplete)


if __name__ == "__main__":
    unittest.main()
