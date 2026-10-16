"""Focused tests for the local commercial ledger and adapter declarations."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import multiprocessing
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from commercial import (  # noqa: E402
    AuthorizationRejected,
    BudgetExceeded,
    CommercialError,
    CommercialLedger,
    IdempotencyConflict,
    PAYMENT_ADAPTER_DECLARATIONS,
    PaymentAdapterCatalog,
    ReconciliationConflict,
    UnsupportedAdapter,
)


def _reserve_worker(database: str, offer_digest: str, barrier, results, purchase_id: str) -> None:
    ledger = CommercialLedger(database)
    barrier.wait(timeout=15)
    try:
        ledger.reserve_purchase(
            purchase_id=purchase_id,
            idempotency_key=f"key-{purchase_id}",
            authorization_id="auth-shared",
            offer_digest=offer_digest,
            run_id="run-shared",
            assignment_id="assignment-shared",
            attempt_id="attempt-shared",
            service_identity="service.synthetic-runtime",
            currency="TST",
            atomic_scale=6,
            ceiling_units=8,
        )
        results.put((purchase_id, "reserved"))
    except BudgetExceeded:
        results.put((purchase_id, "budget-exceeded"))
    except Exception as error:
        results.put((purchase_id, f"error:{type(error).__name__}:{error}"))


def _fixture_terms(*, offer_id: str = "fixture-offer", required_units=None,
                   rates=None, charge_base=None, markup_base=None,
                   markup_bps: int | None = 2_500, late_usage: bool = False) -> dict:
    if required_units is None:
        required_units = ["input_units", "output_units", "host_seconds"]
    if rates is None:
        rates = [
            {"unit": "input_units", "component": "inference_cost", "numerator": 3,
             "denominator": 2, "evidence_state": "provider_reported",
             "source_ref": "test-only://synthetic/input"},
            {"unit": "output_units", "component": "inference_cost", "numerator": 2,
             "denominator": 1, "evidence_state": "provider_reported",
             "source_ref": "test-only://synthetic/output"},
            {"unit": "host_seconds", "component": "hosting_cost", "numerator": 1,
             "denominator": 1, "evidence_state": "provider_reported",
             "source_ref": "test-only://synthetic/hosting"},
        ]
    if charge_base is None:
        charge_base = ["inference_cost", "hosting_cost"]
    if markup_base is None:
        markup_base = ["inference_cost"]
    terms = {
        "supplier_id": "supplier.synthetic",
        "offer_id": offer_id,
        "version": "fixture-v1",
        "currency": "TST",
        "atomic_scale": 6,
        "price_basis": "usage",
        "payment_trigger": "incremental_use",
        "rounding_mode": "half_up",
        "required_usage_units": list(required_units),
        "rates": rates,
        "charge_base": charge_base,
        "markup_base": markup_base,
        "markup_bps": markup_bps,
        "markup_evidence_state": "calculated_from_measured_usage" if markup_bps is not None else "unknown",
        "test_only": True,
    }
    if late_usage:
        terms["late_usage_policy"] = {"accept_after_completion": True}
    return terms


class CommercialLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory(prefix="commercial-ledger-")
        self.database = Path(self.directory.name) / "commercial.sqlite3"
        self.ledger = CommercialLedger(self.database)
        self.offer = self.ledger.pin_offer(_fixture_terms())
        self.ledger.configure_budget("account-test", "TST", 6, 500, budget_id="budget-test-v1")
        self._authorize()

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _authorize(self, *, ceiling: int = 500, run_id: str = "run-test",
                   assignment_id: str = "assignment-test", attempt_id: str = "attempt-test",
                   authorization_id: str = "auth-test") -> None:
        self.ledger.record_authorization(
            authorization_id=authorization_id,
            principal_id="principal.synthetic-test",
            account_id="account-test",
            supplier_id="supplier.synthetic",
            offer_digest=self.offer["offer_digest"],
            run_id=run_id,
            assignment_id=assignment_id,
            attempt_id=attempt_id,
            currency="TST",
            atomic_scale=6,
            ceiling_units=ceiling,
            expires_at=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            evidence_ref="test-only://synthetic/authority",
        )

    def _purchase(self, purchase_id: str = "purchase-test", *, ceiling: int = 100,
                  authorization_id: str = "auth-test", run_id: str = "run-test",
                  assignment_id: str = "assignment-test", attempt_id: str = "attempt-test",
                  task_id: str | None = "task-test") -> dict:
        return self.ledger.reserve_purchase(
            purchase_id=purchase_id,
            idempotency_key=f"purchase-key-{purchase_id}",
            authorization_id=authorization_id,
            offer_digest=self.offer["offer_digest"],
            run_id=run_id,
            assignment_id=assignment_id,
            attempt_id=attempt_id,
            service_identity="service.synthetic-runtime",
            currency="TST",
            atomic_scale=6,
            ceiling_units=ceiling,
            task_id=task_id,
        )

    def _usage(self, purchase_id: str, unit: str, quantity: int | None, *,
               evidence: str = "measured", usage_id: str | None = None,
               late: bool = False, task_id: str | None = "task-test") -> dict:
        return self.ledger.record_usage(
            usage_id=usage_id or f"{purchase_id}-{unit}-{quantity}",
            purchase_id=purchase_id,
            unit=unit,
            quantity=quantity,
            evidence_state=evidence,
            source="synthetic-test-fixture",
            service_identity="service.synthetic-runtime",
            assignment_id="assignment-test",
            attempt_id="attempt-test",
            task_id=task_id,
            model_call_id=f"call-{usage_id or unit}",
            evidence_ref="test-only://synthetic/usage",
            late=late,
        )

    def _full_usage(self, purchase_id: str = "purchase-test") -> dict:
        self._usage(purchase_id, "input_units", 3)
        self._usage(purchase_id, "output_units", 4)
        result = self._usage(purchase_id, "host_seconds", 3)
        return self.ledger.mark_usage_complete(
            purchase_id, evidence_ref="test-only://synthetic/usage-complete",
            idempotency_key=f"complete-{purchase_id}")

    def _test_settlement(self, purchase_id: str, *, suffix: str = "one") -> dict:
        return self.ledger.begin_settlement(
            settlement_id=f"settlement-{purchase_id}-{suffix}",
            idempotency_key=f"settlement-key-{purchase_id}-{suffix}",
            purchase_id=purchase_id,
            adapter_profile="test-only-recorded",
        )

    def test_offer_requires_explicit_nonnegative_atomic_scale(self) -> None:
        missing = _fixture_terms(offer_id="missing-scale")
        del missing["atomic_scale"]
        with self.assertRaises(CommercialError):
            self.ledger.pin_offer(missing)
        negative = _fixture_terms(offer_id="negative-scale")
        negative["atomic_scale"] = -1
        with self.assertRaises(CommercialError):
            self.ledger.pin_offer(negative)
        zero = self.ledger.pin_offer(_fixture_terms(offer_id="zero-scale") | {"atomic_scale": 0})
        self.assertEqual(zero["atomic_scale"], 0)

    def test_synthetic_usage_reproduces_distinct_cost_and_price_layers(self) -> None:
        self._purchase()
        result = self._full_usage()
        costs = result["costs"]
        self.assertEqual(costs["inference_cost"]["amount_units"], 13)
        self.assertEqual(costs["hosting_cost"]["amount_units"], 3)
        self.assertEqual(costs["markup_base"]["amount_units"], 13)
        self.assertEqual(costs["markup"]["amount_units"], 3)
        self.assertEqual(costs["supplier_charge"]["amount_units"], 19)
        self.assertEqual(costs["supplier_charge"]["currency"], "TST")
        self.assertEqual(costs["supplier_charge"]["atomic_scale"], 6)
        self.assertEqual(costs["inference_cost"]["evidence"], "calculated_from_measured_usage")

        for kind, amount in (("payment_fee", 2), ("owner_overhead", 1),
                             ("customer_price", 40)):
            self.ledger.record_economic_fact(
                fact_id=f"fact-{kind}", idempotency_key=f"key-{kind}", kind=kind,
                account_id="account-test", run_id="run-test", currency="TST", atomic_scale=6,
                amount_units=amount, evidence_state="measured", source="synthetic-test-fixture",
                basis={"test_only": True}, purchase_id="purchase-test",
                assignment_id="assignment-test", attempt_id="attempt-test")
        economics = self.ledger.get_purchase("purchase-test")["economics"]
        self.assertEqual(economics["payment_fees"]["amount_units"], 2)
        self.assertEqual(economics["owner_overhead"]["amount_units"], 1)
        self.assertEqual(economics["production_cost"]["amount_units"], 22)
        self.assertEqual(economics["customer_price"]["amount_units"], 40)

    def test_purchase_idempotency_rejects_changed_reuse(self) -> None:
        request = dict(
            purchase_id="purchase-idempotent", idempotency_key="idempotency-one",
            authorization_id="auth-test", offer_digest=self.offer["offer_digest"],
            run_id="run-test", assignment_id="assignment-test", attempt_id="attempt-test",
            service_identity="service.synthetic-runtime", currency="TST", atomic_scale=6,
            ceiling_units=20,
            task_id="task-test")
        first = self.ledger.reserve_purchase(**request)
        reopened = CommercialLedger(self.database)
        replay = reopened.reserve_purchase(**request)
        self.assertEqual(first["purchase_id"], replay["purchase_id"])
        self.assertEqual(replay["atomic_scale"], 6)
        changed = dict(request, ceiling_units=21)
        with self.assertRaises(IdempotencyConflict):
            reopened.reserve_purchase(**changed)
        changed_scale = dict(request, atomic_scale=5)
        with self.assertRaises(IdempotencyConflict):
            reopened.reserve_purchase(**changed_scale)

    def test_authorization_and_purchase_require_the_pinned_scale(self) -> None:
        with self.assertRaises(AuthorizationRejected):
            self.ledger.record_authorization(
                authorization_id="auth-wrong-scale", principal_id="principal.synthetic-test",
                account_id="account-test", supplier_id="supplier.synthetic",
                offer_digest=self.offer["offer_digest"], run_id="run-test",
                assignment_id="assignment-test", attempt_id="attempt-test",
                currency="TST", atomic_scale=5, ceiling_units=20,
                expires_at=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())
        with self.assertRaises(AuthorizationRejected):
            self.ledger.reserve_purchase(
                purchase_id="purchase-wrong-scale", idempotency_key="wrong-scale-key",
                authorization_id="auth-test", offer_digest=self.offer["offer_digest"],
                run_id="run-test", assignment_id="assignment-test", attempt_id="attempt-test",
                service_identity="service.synthetic-runtime", currency="TST", atomic_scale=5,
                ceiling_units=20)

    def test_list_purchases_returns_filtered_safe_projections_in_order(self) -> None:
        self._purchase("purchase-z")
        self._purchase("purchase-a")
        self._authorize(authorization_id="auth-other-run", run_id="run-other",
                        assignment_id="assignment-other", attempt_id="attempt-other")
        self._purchase("purchase-other", authorization_id="auth-other-run", run_id="run-other",
                       assignment_id="assignment-other", attempt_id="attempt-other", task_id=None)
        reopened = CommercialLedger(self.database)
        all_purchases = reopened.list_purchases()
        self.assertEqual([item["purchase_id"] for item in all_purchases],
                         ["purchase-other", "purchase-a", "purchase-z"])
        selected = reopened.list_purchases(run_id="run-test")
        self.assertEqual([item["purchase_id"] for item in selected], ["purchase-a", "purchase-z"])
        self.assertEqual(selected[0], reopened.get_purchase("purchase-a"))
        self.assertEqual(reopened.list_purchases(run_id="missing-run"), [])
        self.assertNotIn("fingerprint", all_purchases[0])

    def test_recorded_timestamps_survive_restart_and_idempotent_replay(self) -> None:
        self._purchase()
        self._full_usage()
        initial_usage = self.ledger.get_purchase("purchase-test")["usage"]
        usage_timestamps = {row["usage_id"]: row["recorded_at"] for row in initial_usage}
        credit = self.ledger.record_credit(
            credit_id="credit-timestamp", idempotency_key="credit-timestamp-key",
            purchase_id="purchase-test", currency="TST", atomic_scale=6,
            amount_units=3, reason="synthetic timestamp fixture",
            evidence_state="provider_reported", evidence_ref="test-only://synthetic/credit")
        credit_recorded_at = credit["recorded_at"]
        settlement = self._test_settlement("purchase-test", suffix="timestamp")
        unresolved = self.ledger.mark_settlement_unresolved(
            settlement["settlement_id"], reason="synthetic pending",
            evidence_ref="test-only://synthetic/pending")
        settlement_recorded_at = unresolved["recorded_at"]

        reopened = CommercialLedger(self.database)
        self.ledger = reopened
        loaded = reopened.get_purchase("purchase-test")
        self.assertEqual({row["usage_id"]: row["recorded_at"] for row in loaded["usage"]},
                         usage_timestamps)
        self.assertEqual(loaded["credits"][0]["recorded_at"], credit_recorded_at)
        self.assertEqual(loaded["settlements"][0]["recorded_at"], settlement_recorded_at)
        replayed_credit = reopened.record_credit(
            credit_id="credit-timestamp", idempotency_key="credit-timestamp-key",
            purchase_id="purchase-test", currency="TST", atomic_scale=6,
            amount_units=3, reason="synthetic timestamp fixture",
            evidence_state="provider_reported", evidence_ref="test-only://synthetic/credit")
        self.assertEqual(replayed_credit["recorded_at"], credit_recorded_at)
        replayed_usage = self._usage("purchase-test", "input_units", 3)
        self.assertEqual({row["usage_id"]: row["recorded_at"] for row in replayed_usage["usage"]},
                         usage_timestamps)
        replayed_settlement = self._test_settlement("purchase-test", suffix="timestamp")
        self.assertEqual(replayed_settlement["recorded_at"], settlement_recorded_at)
        reconciled = reopened.reconcile_settlement(
            settlement["settlement_id"], result="settled", receipt_ref="test-only://receipt/16",
            amount_units=16, currency="TST", atomic_scale=6,
            evidence_state="provider_reported")
        replayed_reconciliation = reopened.reconcile_settlement(
            settlement["settlement_id"], result="settled", receipt_ref="test-only://receipt/16",
            amount_units=16, currency="TST", atomic_scale=6,
            evidence_state="provider_reported")
        self.assertEqual(replayed_reconciliation["recorded_at"], reconciled["recorded_at"])

    def test_callers_cannot_supply_recorded_at(self) -> None:
        self._purchase()
        with self.assertRaises(TypeError):
            self.ledger.record_usage(
                usage_id="backdated-usage", purchase_id="purchase-test", unit="input_units",
                quantity=1, evidence_state="measured", source="synthetic-test-fixture",
                service_identity="service.synthetic-runtime", assignment_id="assignment-test",
                attempt_id="attempt-test", task_id="task-test", recorded_at="2000-01-01T00:00:00Z")
        with self.assertRaises(TypeError):
            self.ledger.begin_settlement(
                settlement_id="backdated-settlement", idempotency_key="backdated-settlement-key",
                purchase_id="purchase-test", adapter_profile="test-only-recorded",
                recorded_at="2000-01-01T00:00:00Z")
        with self.assertRaises(TypeError):
            self.ledger.record_credit(
                credit_id="backdated-credit", idempotency_key="backdated-credit-key",
                purchase_id="purchase-test", currency="TST", atomic_scale=6, amount_units=1,
                reason="synthetic", evidence_state="measured", recorded_at="2000-01-01T00:00:00Z")

    def test_reservations_are_atomic_across_processes(self) -> None:
        # The authorization and its offer must match; configure the shared fixture
        # through a fresh ledger with a 10-unit budget and a single pinned offer.
        database = Path(self.directory.name) / "concurrent.sqlite3"
        ledger = CommercialLedger(database)
        offer = ledger.pin_offer(_fixture_terms(offer_id="concurrent-fixture"))
        ledger.configure_budget("account-test", "TST", 6, 10, budget_id="budget-concurrent")
        ledger.record_authorization(
            authorization_id="auth-shared", principal_id="principal.synthetic-test",
            account_id="account-test", supplier_id="supplier.synthetic",
            offer_digest=offer["offer_digest"], run_id="run-shared",
            assignment_id="assignment-shared", attempt_id="attempt-shared", currency="TST",
            atomic_scale=6, ceiling_units=10,
            expires_at=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())
        context = multiprocessing.get_context("spawn")
        barrier, results = context.Barrier(2), context.Queue()
        processes = [context.Process(target=_reserve_worker,
                                     args=(str(database), offer["offer_digest"], barrier, results,
                                           f"purchase-{index}"))
                     for index in range(2)]
        for process in processes:
            process.start()
        outcomes = [results.get(timeout=30) for _ in processes]
        for process in processes:
            process.join(timeout=15)
            self.assertEqual(process.exitcode, 0)
        self.assertCountEqual([outcome for _, outcome in outcomes],
                              ["reserved", "budget-exceeded"])
        self.assertEqual(ledger.account_status("account-test", "TST", 6)["committed_units"], 8)

    def test_process_race_restart_late_usage_credit_reconciliation_acceptance(self) -> None:
        """Separate-process admission followed by durable public-ledger settlement."""
        database = Path(self.directory.name) / "separate-process-acceptance.sqlite3"
        ledger = CommercialLedger(database)
        terms = _fixture_terms(
            offer_id="separate-process-acceptance",
            required_units=["input_units"],
            rates=[{"unit": "input_units", "component": "inference_cost",
                    "numerator": 2, "denominator": 1, "evidence_state": "provider_reported",
                    "source_ref": "test-only://synthetic/process-acceptance"}],
            charge_base=["inference_cost"], markup_base=["inference_cost"],
            markup_bps=0, late_usage=True)
        offer = ledger.pin_offer(terms)
        ledger.configure_budget("account-test", "TST", 6, 8, budget_id="acceptance-budget")
        ledger.record_authorization(
            authorization_id="auth-shared", principal_id="principal.synthetic-test",
            account_id="account-test", supplier_id="supplier.synthetic",
            offer_digest=offer["offer_digest"], run_id="run-shared",
            assignment_id="assignment-shared", attempt_id="attempt-shared",
            currency="TST", atomic_scale=6, ceiling_units=8,
            expires_at=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            evidence_ref="test-only://synthetic/authority")

        context = multiprocessing.get_context("spawn")
        barrier, results = context.Barrier(2), context.Queue()
        purchase_ids = ("acceptance-purchase-a", "acceptance-purchase-b")
        processes = [context.Process(target=_reserve_worker,
                                     args=(str(database), offer["offer_digest"], barrier,
                                           results, purchase_id))
                     for purchase_id in purchase_ids]
        for process in processes:
            process.start()
        outcomes = []
        try:
            outcomes = [results.get(timeout=30) for _ in processes]
        finally:
            for process in processes:
                process.join(timeout=15)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
                self.assertEqual(process.exitcode, 0)
        self.assertEqual(len(processes), 2)
        self.assertCountEqual([outcome for _, outcome in outcomes],
                              ["reserved", "budget-exceeded"])
        winners = [purchase_id for purchase_id, outcome in outcomes if outcome == "reserved"]
        self.assertEqual(len(winners), 1)
        winner_id = winners[0]
        self.assertEqual([item["purchase_id"] for item in ledger.list_purchases()], [winner_id])

        # Reopen before recording source usage; all following operations use only
        # the durable public API and the local test-only recorded-outcome profile.
        reopened = CommercialLedger(database)
        purchase = reopened.get_purchase(winner_id)
        self.assertEqual(purchase["reservation"]["amount_units"], 8)
        first_usage = reopened.record_usage(
            usage_id="acceptance-initial-usage", purchase_id=winner_id,
            unit="input_units", quantity=1, evidence_state="measured",
            source="synthetic-process-acceptance", service_identity="service.synthetic-runtime",
            assignment_id="assignment-shared", attempt_id="attempt-shared",
            evidence_ref="test-only://synthetic/initial-usage")
        initial_recorded_at = next(item["recorded_at"] for item in first_usage["usage"]
                                   if item["usage_id"] == "acceptance-initial-usage")
        completed = reopened.mark_usage_complete(
            winner_id, evidence_ref="test-only://synthetic/initial-complete",
            idempotency_key="acceptance-initial-complete")
        self.assertEqual(completed["supplier_charge"]["amount_units"], 2)
        credit = reopened.record_credit(
            credit_id="acceptance-credit", idempotency_key="acceptance-credit-key",
            purchase_id=winner_id, currency="TST", atomic_scale=6, amount_units=1,
            reason="synthetic test credit", evidence_state="provider_reported",
            evidence_ref="test-only://synthetic/credit")
        self.assertEqual(credit["amount_units"], 1)
        first_settlement = reopened.begin_settlement(
            settlement_id="acceptance-settlement-initial",
            idempotency_key="acceptance-settlement-initial-key", purchase_id=winner_id,
            adapter_profile="test-only-recorded")
        self.assertEqual(first_settlement["amount_units"], 1)
        unresolved = reopened.mark_settlement_unresolved(
            first_settlement["settlement_id"], reason="synthetic pending receipt",
            evidence_ref="test-only://synthetic/pending")

        after_restart = CommercialLedger(database)
        purchase_after_restart = after_restart.get_purchase(winner_id)
        self.assertEqual(purchase_after_restart["usage"][0]["recorded_at"], initial_recorded_at)
        self.assertEqual(purchase_after_restart["credits"][0]["recorded_at"], credit["recorded_at"])
        self.assertEqual(purchase_after_restart["settlements"][0]["recorded_at"],
                         unresolved["recorded_at"])
        replayed_credit = after_restart.record_credit(
            credit_id="acceptance-credit", idempotency_key="acceptance-credit-key",
            purchase_id=winner_id, currency="TST", atomic_scale=6, amount_units=1,
            reason="synthetic test credit", evidence_state="provider_reported",
            evidence_ref="test-only://synthetic/credit")
        self.assertEqual(replayed_credit["recorded_at"], credit["recorded_at"])
        reconciled_initial = after_restart.reconcile_settlement(
            first_settlement["settlement_id"], result="settled",
            receipt_ref="test-only://synthetic/receipt-initial", amount_units=1,
            currency="TST", atomic_scale=6, evidence_state="provider_reported")
        self.assertEqual(reconciled_initial["state"], "settled")

        late = after_restart.record_usage(
            usage_id="acceptance-late-usage", purchase_id=winner_id,
            unit="input_units", quantity=1, evidence_state="measured", late=True,
            source="synthetic-process-acceptance", service_identity="service.synthetic-runtime",
            assignment_id="assignment-shared", attempt_id="attempt-shared",
            evidence_ref="test-only://synthetic/late-usage")
        self.assertFalse(late["usage_complete"])
        self.assertIsNone(late["supplier_charge"]["amount_units"])
        after_late_completion = after_restart.mark_usage_complete(
            winner_id, evidence_ref="test-only://synthetic/late-complete",
            idempotency_key="acceptance-late-complete")
        self.assertEqual(after_late_completion["supplier_charge"]["amount_units"], 4)
        self.assertEqual(after_late_completion["paid_units"], 1)
        self.assertEqual(after_late_completion["credited_units"], 1)
        self.assertEqual(after_late_completion["outstanding_units"], 2)

        second_settlement = after_restart.begin_settlement(
            settlement_id="acceptance-settlement-late",
            idempotency_key="acceptance-settlement-late-key", purchase_id=winner_id,
            adapter_profile="test-only-recorded")
        self.assertEqual(second_settlement["amount_units"], 2)
        pending_late = after_restart.mark_settlement_unresolved(
            second_settlement["settlement_id"], reason="synthetic late receipt pending",
            evidence_ref="test-only://synthetic/late-pending")
        final_ledger = CommercialLedger(database)
        self.assertEqual(final_ledger.get_purchase(winner_id)["settlements"][1]["recorded_at"],
                         pending_late["recorded_at"])
        final_reconciliation = final_ledger.reconcile_settlement(
            second_settlement["settlement_id"], result="settled",
            receipt_ref="test-only://synthetic/receipt-late", amount_units=2,
            currency="TST", atomic_scale=6, evidence_state="provider_reported")
        self.assertEqual(final_reconciliation["state"], "settled")
        final_purchase = final_ledger.close_purchase(
            winner_id, evidence_ref="test-only://synthetic/closed")
        self.assertEqual(final_purchase["supplier_charge"]["amount_units"], 4)
        self.assertEqual(final_purchase["paid_units"] + final_purchase["credited_units"] +
                         final_purchase["outstanding_units"], 4)
        self.assertEqual(final_purchase["outstanding_units"], 0)
        self.assertEqual(final_purchase["reservation"]["amount_units"], 0)
        account = final_ledger.account_status("account-test", "TST", 6)
        self.assertEqual(account["committed_units"], 3)
        self.assertEqual(account["available_units"], 5)

        print("COMMERCE_PUBLIC_LEDGER_ACCEPTANCE " + json.dumps({
            "process_count": len(processes),
            "winning_admissions": len(winners),
            "winner_purchase_id": winner_id,
            "budget_limit_units": account["limit_units"],
            "committed_units": account["committed_units"],
            "available_units": account["available_units"],
            "late_gross_units": final_purchase["supplier_charge"]["amount_units"],
            "settled_units": final_purchase["paid_units"],
            "credited_units": final_purchase["credited_units"],
            "outstanding_units": final_purchase["outstanding_units"],
            "charge_balance_conserved": True,
            "external_payment_calls": 0,
        }, sort_keys=True))

    def test_restart_preserves_usage_reservation_and_obligation(self) -> None:
        self._purchase()
        saved = self._full_usage()
        reopened = CommercialLedger(self.database)
        loaded = reopened.get_purchase("purchase-test")
        self.assertEqual(loaded["reservation"], {"amount_units": 100, "currency": "TST",
                                                  "atomic_scale": 6, "state": "held"})
        self.assertEqual(loaded["atomic_scale"], 6)
        self.assertEqual(loaded["supplier_charge"]["atomic_scale"], 6)
        self.assertEqual(loaded["obligation"]["atomic_scale"], 6)
        self.assertEqual(loaded["supplier_charge"]["amount_units"], 19)
        self.assertEqual(loaded["obligation"]["obligation_id"], saved["obligation"]["obligation_id"])
        self.assertEqual(len(loaded["usage"]), 3)

    def test_unknown_obligation_stays_accrued_and_cannot_settle(self) -> None:
        self._purchase()
        self._usage("purchase-test", "input_units", None, evidence="unknown")
        result = self.ledger.mark_usage_complete(
            "purchase-test", evidence_ref="test-only://synthetic/incomplete",
            idempotency_key="complete-unknown")
        self.assertIsNone(result["supplier_charge"]["amount_units"])
        self.assertEqual(result["supplier_charge"]["evidence"], "unknown")
        with self.assertRaises(CommercialError):
            self._test_settlement("purchase-test")
        loaded = self.ledger.get_purchase("purchase-test")
        self.assertEqual(loaded["reservation"]["state"], "held")
        self.assertEqual(loaded["payment_state"], "unknown")
        reopened = CommercialLedger(self.database)
        restored = reopened.get_purchase("purchase-test")
        self.assertIsNone(restored["supplier_charge"]["amount_units"])
        self.assertEqual(restored["supplier_charge"]["evidence"], "unknown")
        self.assertEqual(restored["reservation"]["state"], "held")
        self.assertEqual(restored["obligation"]["obligation_id"], result["obligation"]["obligation_id"])

    def test_late_usage_creates_incremental_obligation_without_erasing_paid_amount(self) -> None:
        terms = _fixture_terms(offer_id="late-fixture", required_units=["input_units"],
                               rates=[{"unit": "input_units", "component": "inference_cost",
                                       "numerator": 2, "denominator": 1,
                                       "evidence_state": "provider_reported",
                                       "source_ref": "test-only://synthetic/late"}],
                               charge_base=["inference_cost"], markup_base=["inference_cost"],
                               markup_bps=0, late_usage=True)
        self.offer = self.ledger.pin_offer(terms)
        self.ledger.configure_budget("account-test", "TST", 6, 500, budget_id="late-budget")
        self._authorize(authorization_id="auth-late")
        self._purchase(authorization_id="auth-late", ceiling=100)
        self._usage("purchase-test", "input_units", 1)
        initial = self.ledger.mark_usage_complete(
            "purchase-test", evidence_ref="test-only://synthetic/initial-complete",
            idempotency_key="complete-initial")
        self.assertEqual(initial["supplier_charge"]["amount_units"], 2)
        settlement = self._test_settlement("purchase-test", suffix="initial")
        self.ledger.reconcile_settlement(
            settlement["settlement_id"], result="settled", receipt_ref="test-only://receipt/1",
            amount_units=2, currency="TST", atomic_scale=6, evidence_state="provider_reported")

        late = self._usage("purchase-test", "input_units", 1,
                           usage_id="usage-late-input", late=True)
        self.assertFalse(late["usage_complete"])
        self.assertIsNone(late["supplier_charge"]["amount_units"])
        completed = self.ledger.mark_usage_complete(
            "purchase-test", evidence_ref="test-only://synthetic/late-complete",
            idempotency_key="complete-late")
        self.assertEqual(completed["supplier_charge"]["amount_units"], 4)
        self.assertEqual(completed["paid_units"], 2)
        self.assertEqual(completed["outstanding_units"], 2)
        next_settlement = self._test_settlement("purchase-test", suffix="late")
        self.assertEqual(next_settlement["amount_units"], 2)

    def test_credit_reduces_reproducible_charge_without_rewriting_usage(self) -> None:
        self._purchase()
        original = self._full_usage()
        before = self.ledger.get_purchase("purchase-test")["usage"]
        credit = self.ledger.record_credit(
            credit_id="credit-test", idempotency_key="credit-key-test",
            purchase_id="purchase-test", currency="TST", atomic_scale=6,
            amount_units=3, reason="synthetic adjustment",
            evidence_state="provider_reported", evidence_ref="test-only://synthetic/credit")
        self.assertEqual(credit["amount_units"], 3)
        after = self.ledger.get_purchase("purchase-test")["usage"]
        self.assertEqual(after, before)
        settlement = self._test_settlement("purchase-test")
        self.assertEqual(settlement["amount_units"], 16)
        self.assertEqual(original["supplier_charge"]["amount_units"], 19)

    def test_reconciliation_keeps_unresolved_intent_until_exact_receipt(self) -> None:
        self._purchase()
        self._full_usage()
        pending = self._test_settlement("purchase-test")
        unresolved = self.ledger.mark_settlement_unresolved(
            pending["settlement_id"], reason="synthetic receipt pending",
            evidence_ref="test-only://synthetic/pending")
        self.assertEqual(unresolved["state"], "unresolved")
        with self.assertRaises(CommercialError):
            self._test_settlement("purchase-test", suffix="duplicate")
        with self.assertRaises(ReconciliationConflict):
            self.ledger.reconcile_settlement(
                pending["settlement_id"], result="settled", receipt_ref="test-only://receipt/19",
                amount_units=19, currency="TST", atomic_scale=5,
                evidence_state="provider_reported")
        reconciled = self.ledger.reconcile_settlement(
            pending["settlement_id"], result="settled", receipt_ref="test-only://receipt/19",
            amount_units=19, currency="TST", atomic_scale=6,
            evidence_state="provider_reported")
        self.assertEqual(reconciled["state"], "settled")
        self.assertEqual(self.ledger.get_purchase("purchase-test")["outstanding_units"], 0)
        with self.assertRaises(IdempotencyConflict):
            self.ledger.reconcile_settlement(
                pending["settlement_id"], result="settled", receipt_ref="test-only://receipt/changed",
                amount_units=19, currency="TST", atomic_scale=6,
                evidence_state="provider_reported")

    def test_unknown_and_explicit_zero_remain_distinct(self) -> None:
        self._purchase("purchase-unknown")
        self._usage("purchase-unknown", "input_units", None, evidence="undisclosed",
                    usage_id="unknown-input")
        self._usage("purchase-unknown", "output_units", 0, usage_id="zero-output")
        self._usage("purchase-unknown", "host_seconds", 0, usage_id="zero-host")
        unknown = self.ledger.mark_usage_complete(
            "purchase-unknown", evidence_ref="test-only://synthetic/unknown-complete",
            idempotency_key="complete-unknown")
        self.assertIsNone(unknown["supplier_charge"]["amount_units"])
        self.assertEqual(unknown["costs"]["inference_cost"]["evidence"], "undisclosed")

        self._purchase("purchase-zero")
        self._usage("purchase-zero", "input_units", 0, usage_id="zero-input")
        self._usage("purchase-zero", "output_units", 0, usage_id="zero-output-2")
        self._usage("purchase-zero", "host_seconds", 0, usage_id="zero-host-2")
        zero = self.ledger.mark_usage_complete(
            "purchase-zero", evidence_ref="test-only://synthetic/zero-complete",
            idempotency_key="complete-zero")
        self.assertEqual(zero["supplier_charge"]["amount_units"], 0)
        self.assertEqual(zero["supplier_charge"]["evidence"], "calculated_from_measured_usage")


class PaymentAdapterDeclarationTests(unittest.TestCase):
    def test_protocol_capabilities_are_canonical_copies_and_unqualified(self) -> None:
        catalog = PaymentAdapterCatalog()
        declarations = catalog.declarations()
        self.assertEqual({entry["adapter_id"] for entry in declarations},
                         {"mpp-session", "x402-upto", "ap2-v0.2", "test-only-recorded"})
        by_id = {entry["adapter_id"]: entry for entry in declarations}
        self.assertEqual(by_id["mpp-session"]["protocol"], "MPP")
        self.assertEqual(by_id["mpp-session"]["profile"], "session")
        self.assertEqual(by_id["x402-upto"]["protocol"], "x402")
        self.assertEqual(by_id["x402-upto"]["profile"], "upto")
        self.assertEqual(by_id["x402-upto"]["version"], "2")
        self.assertEqual(by_id["ap2-v0.2"]["protocol"], "AP2")
        self.assertEqual(by_id["ap2-v0.2"]["version"], "0.2")
        self.assertEqual(by_id["ap2-v0.2"]["prerequisites"]["nested_agent_delegation"],
                         "out_of_scope_in_v0.2")
        self.assertEqual(by_id["test-only-recorded"]["version_status"], "test_only")
        for profile in ("mpp-session", "x402-upto", "ap2-v0.2"):
            self.assertFalse(by_id[profile]["configured"])
            self.assertFalse(by_id[profile]["qualified"])
            self.assertFalse(by_id[profile]["advertisable"])
            self.assertFalse(by_id[profile]["invokable"])
            self.assertIn("operations", by_id[profile])
            self.assertIn("prerequisites", by_id[profile])
        self.assertEqual(catalog.advertised_profiles(), [])
        declarations[0]["prerequisites"]["network"] = "mutated-copy"
        self.assertIsNone(catalog.get("mpp-session")["prerequisites"]["network"])

    def test_unqualified_profiles_are_rejected_before_any_local_or_network_io(self) -> None:
        with tempfile.TemporaryDirectory(prefix="commercial-no-io-") as folder:
            ledger = CommercialLedger(Path(folder) / "commercial.sqlite3")
            with patch.object(ledger, "_transaction", side_effect=AssertionError("database I/O")) as tx:
                with patch.object(ledger, "_connect", side_effect=AssertionError("database I/O")) as connect:
                    for profile in ("mpp-session", "x402-upto", "ap2-v0.2"):
                        with self.subTest(profile=profile), self.assertRaises(UnsupportedAdapter):
                            ledger.begin_settlement(
                                settlement_id=f"s-{profile}", idempotency_key=f"k-{profile}",
                                purchase_id="missing-purchase", adapter_profile=profile)
                    tx.assert_not_called()
                    connect.assert_not_called()
            self.assertEqual(set(PAYMENT_ADAPTER_DECLARATIONS),
                             {entry["adapter_id"] for entry in ledger.adapters.declarations()})


if __name__ == "__main__":
    unittest.main()
