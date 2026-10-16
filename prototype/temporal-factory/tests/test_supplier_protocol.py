"""Focused validation tests for plain A2A nested-supplier requests.

A factory acting as an agent service accepts an ordinary Message: its single
brief Part is the run inputs object. Nothing Exomachina-specific is required
(A2A decisions 7 and 9).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import supplier_protocol  # noqa: E402
from supplier_protocol import (  # noqa: E402
    MAX_CANONICAL_INPUT_BYTES,
    MAX_INPUT_NESTING,
    NestedRequest,
    SupplierRequestError,
    is_nested_request,
    parse_nested_request,
)

INPUTS = {"question": "synthetic", "nested": [1, True, None]}


def data(value) -> list[dict]:
    return [{"data": value, "mediaType": "application/json"}]


def json_text(value) -> list[dict]:
    return [{"text": json.dumps(value), "mediaType": "application/json"}]


class SupplierProtocolTests(unittest.TestCase):
    def test_a_data_or_json_text_brief_is_the_run_inputs(self):
        for parts in (data(INPUTS), json_text(INPUTS),
                      [{"text": json.dumps(INPUTS), "mediaType": "application/json; charset=utf-8"}]):
            with self.subTest(parts=parts):
                self.assertTrue(is_nested_request(parts))
                request = parse_nested_request(parts)
                self.assertIsInstance(request, NestedRequest)
                self.assertEqual(request.inputs, INPUTS)
        # Detached: editing the received Part does not change the request.
        parts = data({"question": "q"})
        request = parse_nested_request(parts)
        parts[0]["data"]["question"] = "changed"
        self.assertEqual(request.inputs, {"question": "q"})

    def test_plain_text_briefs_stay_with_the_director(self):
        for parts in ([{"text": "Please write a report."}],
                      [{"text": "{}", "mediaType": "text/plain"}], [], [{"url": "https://x"}]):
            with self.subTest(parts=parts):
                self.assertFalse(is_nested_request(parts))

    def test_no_factory_identifier_or_extension_is_part_of_the_protocol(self):
        source = (ROOT / "src" / "supplier_protocol.py").read_text()
        for retired in ("parent_task_id", "parent_run_id", "assignment_id", "attempt_id",
                        "definition_digest", "action_id", "urn:exomachina", "echo"):
            self.assertNotIn(retired, source)
        for name in ("parse_nested_factory_envelope", "nested_factory_fingerprint",
                     "project_supplier_echo", "supplier_assignment_echo_declaration"):
            self.assertFalse(hasattr(supplier_protocol, name), name)
        # Caller-chosen identifiers are ordinary run inputs, never bindings: the
        # accepting factory's run-input schema rejects unknown names.
        request = parse_nested_request(data({"run_id": "caller-run", "question": "q"}))
        self.assertEqual(request.inputs, {"run_id": "caller-run", "question": "q"})

    def test_shape_is_one_object_brief_part(self):
        for parts, message in (
                ([*data(INPUTS), *data(INPUTS)], "exactly one brief Part"),
                (data([1, 2]), "JSON object"),
                (data("text"), "JSON object"),
                ([{"text": "not json", "mediaType": "application/json"}], "not JSON"),
                ([{"text": "[]", "mediaType": "application/json"}], "JSON object"),
                ([{"text": "{}"}], "data Part or an application/json text Part"),
                ("not-a-list", "list")):
            with self.subTest(message=message), self.assertRaisesRegex(SupplierRequestError, message):
                parse_nested_request(parts)

    def test_fingerprint_is_canonical_and_content_bound(self):
        first = parse_nested_request(data({"b": 1, "a": [1, 2]}))
        reordered = parse_nested_request(json_text({"a": [1, 2], "b": 1}))
        changed = parse_nested_request(data({"b": 2, "a": [1, 2]}))
        self.assertEqual(first.fingerprint(), reordered.fingerprint())
        self.assertNotEqual(first.fingerprint(), changed.fingerprint())
        self.assertRegex(first.fingerprint(), r"^[0-9a-f]{64}$")

    def test_inputs_reject_non_json_and_non_finite_values(self):
        for invalid_inputs in ({"number": float("nan")}, {"number": float("inf")},
                               {"nested": (1, 2)}, {1: "non-string key"}, {"value": object()}):
            with self.subTest(invalid=invalid_inputs), self.assertRaises(SupplierRequestError):
                parse_nested_request(data(invalid_inputs))

    def test_canonical_input_byte_limit_accepts_boundary_and_rejects_overflow(self):
        # {"x":"..."} is eight canonical UTF-8 bytes beyond the string.
        at_limit = {"x": "a" * (MAX_CANONICAL_INPUT_BYTES - 8)}
        self.assertEqual(len(json.dumps(at_limit, sort_keys=True, separators=(",", ":"),
                                        ensure_ascii=False).encode("utf-8")),
                         MAX_CANONICAL_INPUT_BYTES)
        self.assertEqual(parse_nested_request(data(at_limit)).inputs, at_limit)
        with self.assertRaises(SupplierRequestError):
            parse_nested_request(data({"x": "a" * (MAX_CANONICAL_INPUT_BYTES - 7)}))
        # The cap counts UTF-8 bytes, not Python code points.
        emoji = {"x": "😀" * ((MAX_CANONICAL_INPUT_BYTES - 8) // 4)}
        self.assertEqual(parse_nested_request(data(emoji)).inputs, emoji)
        emoji["x"] += "😀"
        with self.assertRaises(SupplierRequestError):
            parse_nested_request(data(emoji))
        with self.assertRaises(SupplierRequestError):
            parse_nested_request([{"text": " " * (3 * MAX_CANONICAL_INPUT_BYTES),
                                   "mediaType": "application/json"}])

    def test_input_container_depth_boundary(self):
        def nested(depth):
            value = None
            for _ in range(depth):
                value = {"x": value}
            return value
        self.assertEqual(parse_nested_request(data(nested(MAX_INPUT_NESTING))).inputs,
                         nested(MAX_INPUT_NESTING))
        with self.assertRaises(SupplierRequestError):
            parse_nested_request(data(nested(MAX_INPUT_NESTING + 1)))


if __name__ == "__main__":
    unittest.main()
