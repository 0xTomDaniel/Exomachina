"""Public-interface replay tests for durable, non-financial usage facts."""
from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from model_usage import (MeasurementConflict, ModelUsageJournal,  # noqa: E402
                         normalize_provider_usage, unavailable_usage)


class UsageReplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Keep trial state under /tmp; this project intentionally leaves it behind.
        cls.trial_dir = Path(tempfile.mkdtemp(prefix="exo-proto-usage-replay-", dir="/tmp"))

    def setUp(self):
        self.database = self.trial_dir / f"{self._testMethodName}.sqlite3"
        self.journal = ModelUsageJournal(self.database)

    @staticmethod
    def facts(*, call_id: str, assignment_id: str | None = "assignment-synthetic-7",
              attempt_id: str | None = "attempt-synthetic-2") -> dict:
        return {
            "model_call_id": call_id,
            "provider": "synthetic-provider-test-only",
            "model_id": "synthetic-model-test-only-v1",
            "reasoning_effort": "xhigh",
            "call_scope": "assignment_call",
            "task_id": "task-synthetic-9",
            "run_id": "run-synthetic-4",
            "definition_digest": "definition-digest-synthetic-3",
            "assignment_id": assignment_id,
            "attempt_id": attempt_id,
        }

    def test_replay_after_restart_is_idempotent_and_conflicting_facts_fail(self):
        facts = self.facts(call_id="synthetic-model-call-replay-1")
        usage = normalize_provider_usage({
            "input": 17,
            "output": 6,
            "cacheRead": 0,
            "totalTokens": 23,
        })

        first = self.journal.record(**facts, usage=usage)
        restarted = ModelUsageJournal(self.database)
        replay = restarted.record(**facts, usage=usage)

        self.assertEqual(replay, first)
        self.assertEqual(restarted.get(facts["model_call_id"]), first)
        rows = restarted.list_measurements(run_id="run-synthetic-4")
        self.assertEqual(rows, [first])
        self.assertEqual(rows[0]["recorded_at"], first["recorded_at"])

        with self.assertRaises(MeasurementConflict):
            restarted.record(**{**facts, "attempt_id": "attempt-conflict"}, usage=usage)
        self.assertEqual(restarted.list_measurements(model_call_id=facts["model_call_id"]), [first])

    def test_nullable_assignment_and_attempt_bindings_stay_independent(self):
        assignment_only = self.journal.record(
            **self.facts(call_id="synthetic-assignment-only-1", attempt_id=None),
            usage=unavailable_usage(),
        )
        attempt_only = self.journal.record(
            **self.facts(call_id="synthetic-attempt-only-1", assignment_id=None),
            usage=unavailable_usage(),
        )
        restarted = ModelUsageJournal(self.database)

        rows = restarted.list_measurements(task_id="task-synthetic-9")
        by_call = {row["model_call_id"]: row for row in rows}
        self.assertIsNone(by_call["synthetic-assignment-only-1"]["attempt_id"])
        self.assertEqual(by_call["synthetic-assignment-only-1"]["assignment_id"],
                         "assignment-synthetic-7")
        self.assertIsNone(by_call["synthetic-attempt-only-1"]["assignment_id"])
        self.assertEqual(by_call["synthetic-attempt-only-1"]["attempt_id"],
                         "attempt-synthetic-2")
        for row in (assignment_only, attempt_only):
            self.assertNotIn("cost", row)
            self.assertNotIn("charge", row)
            self.assertNotIn("customer_price", row)
            self.assertEqual(row["evidence_status"], "unknown")

    def test_reported_token_facts_do_not_create_cost_fields(self):
        row = self.journal.record(
            **self.facts(call_id="synthetic-token-only-1"),
            usage=normalize_provider_usage({"input": 0, "output": 5, "totalTokens": 5}),
        )
        self.assertEqual(row["usage"]["input_tokens"], {"value": 0, "status": "reported"})
        self.assertEqual(row["usage"]["output_tokens"], {"value": 5, "status": "reported"})
        self.assertNotIn("cost", row)
        self.assertNotIn("inference_cost", row)
        self.assertNotIn("hosting_cost", row)
        self.assertNotIn("charge", row)
        self.assertNotIn("customer_price", row)

    def test_public_reads_close_their_database_connections(self):
        facts = self.facts(call_id="synthetic-connection-close-1")
        self.journal.record(**facts, usage=unavailable_usage())

        real_connect = self.journal._connect
        opened = []

        class TrackedConnection:
            def __init__(self, connection):
                self.connection = connection
                self.closed = False

            def __enter__(self):
                self.connection.__enter__()
                return self

            def __exit__(self, *args):
                return self.connection.__exit__(*args)

            def close(self):
                self.closed = True
                self.connection.close()

            def __getattr__(self, name):
                return getattr(self.connection, name)

        def tracked_connect():
            connection = TrackedConnection(real_connect())
            opened.append(connection)
            return connection

        self.journal._connect = tracked_connect
        self.assertIsNotNone(self.journal.get(facts["model_call_id"]))
        self.journal.list_measurements(task_id="task-synthetic-9")
        self.assertEqual(len(opened), 2)
        self.assertTrue(all(connection.closed for connection in opened))


if __name__ == "__main__":
    unittest.main()
