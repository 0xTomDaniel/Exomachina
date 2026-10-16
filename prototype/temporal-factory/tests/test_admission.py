"""Durable factory admission queue tests, including separate-process races."""
from __future__ import annotations

import multiprocessing
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from admission import (AdmissionConfigurationConflict, AdmissionError,
                       AdmissionIdempotencyConflict, AdmissionQueue, SlotNotHeld)


def _enqueue_process(database, factory_id, capacity, request_id, task_id, barrier, results):
    try:
        queue = AdmissionQueue(database, factory_id=factory_id, capacity=capacity)
        barrier.wait(timeout=30)
        results.put(("ok", queue.enqueue(request_id, task_id=task_id)))
    except BaseException as error:
        results.put(("error", type(error).__name__, str(error)))


def _release_process(database, factory_id, capacity, request_id, release_id, barrier, results):
    try:
        queue = AdmissionQueue(database, factory_id=factory_id, capacity=capacity)
        barrier.wait(timeout=30)
        results.put(("ok", queue.release(request_id, release_id=release_id)))
    except BaseException as error:
        results.put(("error", type(error).__name__, str(error)))


def _snapshot_process(database, factory_id, capacity, barrier, results):
    try:
        queue = AdmissionQueue(database, factory_id=factory_id, capacity=capacity)
        barrier.wait(timeout=30)
        snapshots = []
        deadline = time.monotonic() + 0.3
        while time.monotonic() < deadline:
            snapshots.append(queue.read_snapshot())
        results.put(("snapshots", snapshots))
    except BaseException as error:
        results.put(("error", type(error).__name__, str(error)))


class AdmissionQueueTests(unittest.TestCase):
    def setUp(self):
        # Leave test state in /tmp for inspection; the repository contract forbids cleanup.
        self.directory = Path(tempfile.mkdtemp(prefix="exo-proto-admission-", dir="/tmp"))
        self.database = self.directory / "admission.sqlite3"

    def _run_processes(self, operations):
        context = multiprocessing.get_context("spawn")
        barrier = context.Barrier(len(operations) + 1)
        results = context.Queue()
        processes = [context.Process(target=target, args=(*args, barrier, results))
                     for target, args in operations]
        for process in processes:
            process.start()
        barrier.wait(timeout=30)
        observed = []
        try:
            for _ in processes:
                observed.append(results.get(timeout=45))
        finally:
            for process in processes:
                process.join(timeout=45)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
        self.assertTrue(all(process.exitcode == 0 for process in processes),
                        [process.exitcode for process in processes])
        self.assertFalse([item for item in observed if item[0] == "error"], observed)
        return observed

    def test_explicit_capacity_validation_and_restart_persistence(self):
        with self.assertRaises(TypeError):
            AdmissionQueue(self.database, factory_id="factory-a")
        for invalid in (True, -1, 1.5, "2"):
            with self.subTest(capacity=invalid), self.assertRaises(AdmissionError):
                AdmissionQueue(self.database, factory_id="factory-a", capacity=invalid)

        queue = AdmissionQueue(self.database, factory_id="factory-a", capacity=2)
        first = queue.enqueue("command-1", task_id="task-1")
        second = queue.enqueue("command-2", task_id="task-2")
        waiting = queue.enqueue("command-3", task_id="task-3")
        self.assertEqual([first["state"], second["state"], waiting["state"]],
                         ["admitted", "admitted", "queued"])
        self.assertEqual(waiting["queue_position"], 1)
        self.assertEqual(queue.capacity_view(), {
            "factory_id": "factory-a", "capacity": 2, "admitted_count": 2,
            "queued_count": 1, "released_count": 0, "available_slots": 0,
        })

        restarted = AdmissionQueue(self.database, factory_id="factory-a", capacity=2)
        self.assertEqual(restarted.list_requests(), [first, second, waiting])
        with self.assertRaises(AdmissionConfigurationConflict):
            AdmissionQueue(self.database, factory_id="factory-a", capacity=3)

        paused = AdmissionQueue(self.database, factory_id="factory-paused", capacity=0)
        self.assertEqual(paused.enqueue("command-paused", task_id="task-paused")["state"], "queued")

    def test_idempotent_enqueue_release_and_fifo_promotion_survive_restart(self):
        queue = AdmissionQueue(self.database, factory_id="factory-a", capacity=1)
        first = queue.enqueue("command-1", task_id="task-1")
        queued = queue.enqueue("command-2", task_id="task-2")
        self.assertEqual(queue.enqueue("command-1", task_id="task-1"), first)
        with self.assertRaises(AdmissionIdempotencyConflict):
            queue.enqueue("command-1", task_id="task-other")
        with self.assertRaises(AdmissionIdempotencyConflict):
            queue.enqueue("command-other", task_id="task-1")

        result = queue.release("command-1", release_id="terminal-1")
        self.assertEqual(result["released"]["state"], "released")
        self.assertEqual([row["request_id"] for row in result["admitted"]], ["command-2"])
        self.assertEqual(queue.capacity_view(), {
            "factory_id": "factory-a", "capacity": 1, "admitted_count": 1,
            "queued_count": 0, "released_count": 1, "available_slots": 0,
        })
        self.assertEqual(result, queue.release("command-1", release_id="terminal-1"))
        with self.assertRaises(AdmissionIdempotencyConflict):
            queue.release("command-2", release_id="terminal-1")
        with self.assertRaises(SlotNotHeld):
            queue.release("command-1", release_id="different-terminal")

        restarted = AdmissionQueue(self.database, factory_id="factory-a", capacity=1)
        self.assertEqual(restarted.get("command-1")["state"], "released")
        self.assertEqual(restarted.get("command-2")["state"], "admitted")
        self.assertEqual(restarted.enqueue("command-1", task_id="task-1"),
                         restarted.get("command-1"))

    def test_repeated_public_calls_explicitly_close_short_lived_connections(self):
        class TrackingConnection(sqlite3.Connection):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.explicitly_closed = False

            def close(self):
                self.explicitly_closed = True
                super().close()

        queue = AdmissionQueue(self.database, factory_id="factory-close", capacity=1)
        opened = []

        def tracked_connect():
            db = sqlite3.connect(self.database, timeout=30.0, isolation_level=None,
                                  factory=TrackingConnection)
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA busy_timeout=30000")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA foreign_keys=ON")
            opened.append(db)
            return db

        with patch.object(queue, "_connect", side_effect=tracked_connect):
            queue.enqueue("request-close", task_id="task-close")
            queue.get("request-close")
            queue.list_requests()
            queue.release("request-close", release_id="release-close")

        self.assertGreaterEqual(len(opened), 4)
        self.assertTrue(all(db.explicitly_closed for db in opened))

    def test_separate_process_enqueue_admit_and_release_race_respects_capacity(self):
        factory_id, capacity, process_count = "factory-race", 3, 10
        queue = AdmissionQueue(self.database, factory_id=factory_id, capacity=capacity)
        initial_operations = [(_enqueue_process,
            (str(self.database), factory_id, capacity, f"request-{index}", f"task-{index}"))
            for index in range(process_count)]
        initial_results = self._run_processes(initial_operations)
        self.assertEqual(len(initial_results), process_count)
        self.assertEqual(sum(result[1]["state"] == "admitted" for result in initial_results), capacity)

        initial_admitted = queue.list_requests(state="admitted")
        self.assertEqual(len(initial_admitted), capacity)
        releases = [(_release_process,
            (str(self.database), factory_id, capacity, row["request_id"],
             f"terminal-{row['request_id']}")) for row in initial_admitted[:2]]
        more_enqueues = [(_enqueue_process,
            (str(self.database), factory_id, capacity, f"later-{index}", f"later-task-{index}"))
            for index in range(4)]
        race_results = self._run_processes(releases + more_enqueues)
        self.assertEqual(len(race_results), len(releases) + len(more_enqueues))

        final_rows = queue.list_requests()
        admitted = [row for row in final_rows if row["state"] == "admitted"]
        released = [row for row in final_rows if row["state"] == "released"]
        self.assertEqual(len(admitted), capacity)
        self.assertEqual(len(released), 2)
        self.assertLessEqual(len(admitted), capacity)
        self.assertEqual(len(final_rows), process_count + len(more_enqueues))
        self.assertEqual(queue.admit_waiting(), [])
        counts = queue.capacity_view()
        self.assertEqual(counts["admitted_count"], capacity)
        self.assertEqual(counts["released_count"], 2)
        self.assertEqual(counts["queued_count"], len(final_rows) - capacity - 2)
        self.assertEqual(counts["available_slots"], 0)

    def test_concurrent_release_enqueue_snapshots_have_coherent_counts_and_rows(self):
        factory_id, capacity = "factory-snapshot", 2
        queue = AdmissionQueue(self.database, factory_id=factory_id, capacity=capacity)
        queue.enqueue("initial-1", task_id="task-1")
        queue.enqueue("initial-2", task_id="task-2")
        queue.enqueue("initial-3", task_id="task-3")
        operations = [
            (_release_process,
             (str(self.database), factory_id, capacity, "initial-1", "release-initial-1")),
            *[(_enqueue_process,
               (str(self.database), factory_id, capacity, f"racing-{index}", f"task-racing-{index}"))
              for index in range(6)],
            (_snapshot_process, (str(self.database), factory_id, capacity)),
        ]
        results = self._run_processes(operations)
        snapshots = next(result[1] for result in results if result[0] == "snapshots")
        self.assertTrue(snapshots)
        for snapshot in snapshots:
            states = {state: sum(row["state"] == state for row in snapshot["requests"])
                      for state in ("admitted", "queued", "released")}
            self.assertEqual(snapshot["admitted_count"], states["admitted"])
            self.assertEqual(snapshot["queued_count"], states["queued"])
            self.assertEqual(snapshot["released_count"], states["released"])
            self.assertEqual(snapshot["available_slots"], capacity - states["admitted"])
            self.assertLessEqual(states["admitted"], capacity)


if __name__ == "__main__":
    unittest.main()
