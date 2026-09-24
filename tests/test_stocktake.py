from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from oil_supply.api import JsonApplication
from oil_supply.clock import FrozenClock
from oil_supply.errors import Conflict, Forbidden, InvalidState
from oil_supply.planning import Decimal, allocate_lot_deltas, reconcile_inventory
from oil_supply.service import SupplyService
from oil_supply.storage import connect


class ReconcilePureFunctionTests(unittest.TestCase):
    def test_zero_book_with_measured_gain_always_escalates(self) -> None:
        result = reconcile_inventory(Decimal("0"), Decimal("5"), Decimal("100"))
        self.assertEqual(result["delta_barrels"], "5.000")
        self.assertIsNone(result["variance_percent"])
        self.assertFalse(result["within_tolerance"])

    def test_zero_book_zero_measured_is_clean(self) -> None:
        result = reconcile_inventory(Decimal("0"), Decimal("0"), Decimal("0"))
        self.assertTrue(result["within_tolerance"])
        self.assertEqual(result["delta_barrels"], "0.000")

    def test_lot_delta_allocation_preserves_total_and_lots(self) -> None:
        shares = allocate_lot_deltas(
            [("lot-a", Decimal("600")), ("lot-b", Decimal("300"))],
            Decimal("-10"),
        )
        self.assertEqual(
            [(row["lot_id"], row["delta_barrels"]) for row in shares],
            [("lot-a", "-6.667"), ("lot-b", "-3.333")],
        )

    def test_zero_balance_gain_is_attributed_to_last_snapshot_lot(self) -> None:
        shares = allocate_lot_deltas(
            [("lot-a", Decimal("0")), ("lot-b", Decimal("0"))],
            Decimal("8"),
        )
        self.assertEqual(
            [(row["lot_id"], row["delta_barrels"]) for row in shares],
            [("lot-a", "0"), ("lot-b", "8.000")],
        )

    def test_loss_cannot_exceed_book(self) -> None:
        with self.assertRaises(ValueError):
            allocate_lot_deltas([("lot-a", Decimal("4"))], Decimal("-5"))


class StockCountServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = SupplyService(self.connection, self.clock)
        for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)
        self.service.create_facility("plan", {"facility_id": "field-a", "name": "北部油田", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_barrels": "500000"})
        self.service.create_facility("plan", {"facility_id": "terminal-b", "name": "沿海终端", "kind": "terminal", "timezone": "Asia/Shanghai", "capacity_barrels": "800000"})
        self.service.create_route("plan", {"route_id": "pipe-a-b", "origin_id": "field-a", "destination_id": "terminal-b", "product": "crude", "daily_capacity": "100000", "loss_basis_points": 25, "transit_hours": 36})

    def tearDown(self) -> None:
        self.connection.close()

    def _lot(self, lot_id: str, facility: str, quantity: str) -> None:
        self.service.add_inventory_lot("dispatch", {"lot_id": lot_id, "facility_id": facility, "product": "crude", "grade": "BRENT", "quantity_barrels": quantity, "unit_cost_usd": "91", "received_at": "2026-09-24T06:00:00Z"})

    def _dispatch_in_transit(self, number: int, lot_id: str, loaded: str) -> None:
        self.service.submit_nomination("dispatch", {"nomination_id": f"nom-{number}", "route_id": "pipe-a-b", "shipper_id": f"shipper-{number}", "service_date": "2026-09-25", "requested_barrels": loaded, "priority": 10, "idempotency_key": f"key-{number}"})
        self.service.allocate("dispatch", "pipe-a-b", "2026-09-25")
        self.service.dispatch_transfer("dispatch", f"transfer-{number}", f"nom-{number}", lot_id, 2)

    def _ship_out_of_terminal(self, number: str, lot_id: str, loaded: str) -> None:
        self.service.create_route("plan", {"route_id": f"pipe-b-out-{number}", "origin_id": "terminal-b", "destination_id": "field-a", "product": "crude", "daily_capacity": "100000", "loss_basis_points": 0, "transit_hours": 12})
        self.service.submit_nomination("dispatch", {"nomination_id": f"nom-{number}", "route_id": f"pipe-b-out-{number}", "shipper_id": f"shipper-{number}", "service_date": "2026-09-25", "requested_barrels": loaded, "priority": 10, "idempotency_key": f"key-{number}"})
        self.service.allocate("dispatch", f"pipe-b-out-{number}", "2026-09-25")
        self.service.dispatch_transfer("dispatch", f"transfer-{number}", f"nom-{number}", lot_id, 2)

    def test_open_freezes_book_and_in_transit_snapshot(self) -> None:
        self._lot("lot-1", "field-a", "1000")
        self._dispatch_in_transit(1, "lot-1", "400")
        session = self.service.open_stock_count("dispatch", {"session_id": "count-1", "facility_id": "terminal-b", "product": "crude", "tolerance_percent": "1"})
        self.assertEqual(session["status"], "unmeasured")
        self.assertEqual(session["snapshots"]["book_barrels"], "0.000")
        self.assertEqual(len(session["snapshots"]["in_transit"]), 1)
        self.assertEqual(session["snapshots"]["in_transit"][0]["transfer_id"], "transfer-1")
        self.assertEqual(len(session["snapshots"]["book_snapshot_sha256"]), 64)

    def test_within_tolerance_settles_with_idempotent_adjustment_and_balances(self) -> None:
        self._lot("lot-1", "terminal-b", "1000")
        opened = self.service.open_stock_count("dispatch", {"session_id": "count-1", "facility_id": "terminal-b", "product": "crude", "tolerance_percent": "1"})
        token = opened["live_inventory"]["balance_token"]
        self.service.record_measurement("dispatch", "count-1", {"tank_id": "t-1", "measured_barrels": "995", "observed_at": "2026-09-24T08:05:00Z"})
        payload = {"idempotency_key": "settle-1", "reason_code": "tank-calibration", "note": "罐表偏差", "expected_revision": 2, "expected_balance_token": token}
        settled = self.service.settle_stock_count("dispatch", "count-1", payload)
        replayed = self.service.settle_stock_count("dispatch", "count-1", payload)
        self.assertEqual(settled, replayed)
        self.assertEqual(settled["status"], "adjusted")
        self.assertEqual(self.service.inventory_lot("lot-1")["available_barrels"], "995.000")
        adjustment = settled["adjustments"]["rows"][0]
        self.assertEqual(adjustment["balance_before_barrels"], "1000.000")
        self.assertEqual(adjustment["delta_barrels"], "-5.000")
        self.assertEqual(adjustment["balance_after_barrels"], "995.000")
        self.assertEqual(adjustment["signed_by"], "dispatch")
        self.assertEqual(adjustment["previous_signature"], "0" * 64)
        self.assertEqual(len(adjustment["signature_sha256"]), 64)
        self.assertEqual(settled["adjustments"]["chain_head"], adjustment["signature_sha256"])

    def test_zero_inventory_gain_requires_risk_adjust(self) -> None:
        self._lot("lot-1", "terminal-b", "0.001")
        # 全部发运使终端批次余额归零（出库在途不计入终端待卸货快照）。
        self._ship_out_of_terminal("zero", "lot-1", "0.001")
        # 另一条从油田驶向终端的在途卸货进入快照。
        self._lot("lot-2", "field-a", "100")
        self._dispatch_in_transit(2, "lot-2", "100")
        opened = self.service.open_stock_count("dispatch", {"session_id": "count-1", "facility_id": "terminal-b", "product": "crude", "tolerance_percent": "5"})
        self.assertEqual(opened["snapshots"]["book_barrels"], "0.000")
        self.assertEqual(len(opened["snapshots"]["in_transit"]), 1)
        token = opened["live_inventory"]["balance_token"]
        self.service.record_measurement("dispatch", "count-1", {"tank_id": "t-1", "measured_barrels": "8", "observed_at": "2026-09-24T08:05:00Z"})
        settle_payload = {"idempotency_key": "settle-1", "reason_code": "gain", "note": "盘盈", "expected_revision": 2, "expected_balance_token": token}
        escalated = self.service.settle_stock_count("dispatch", "count-1", settle_payload)
        self.assertEqual(escalated["status"], "pending_review")
        self.assertEqual(escalated["revision"], 3)
        self.assertFalse(escalated["reconciliation"]["within_tolerance"])
        # 当班人员不能自行复核。
        with self.assertRaises(Forbidden):
            self.service.review_stock_count("dispatch", "count-1", {"idempotency_key": "rev-1", "decision": "adjust", "reason_code": "gain-confirmed", "expected_revision": 3, "expected_balance_token": token})
        reviewed = self.service.review_stock_count("risk", "count-1", {"idempotency_key": "rev-1", "decision": "adjust", "reason_code": "gain-confirmed", "note": "确认罐底残留", "expected_revision": 3, "expected_balance_token": token})
        self.assertEqual(reviewed["status"], "adjusted")
        self.assertEqual(self.service.inventory_lot("lot-1")["available_barrels"], "8.000")
        self.assertEqual(reviewed["adjustments"]["rows"][0]["signed_by"], "risk")

    def test_zero_book_zero_measured_closes_without_adjustment(self) -> None:
        opened = self.service.open_stock_count("dispatch", {"session_id": "count-1", "facility_id": "terminal-b", "product": "crude", "tolerance_percent": "1"})
        self.service.record_measurement("dispatch", "count-1", {"tank_id": "t-1", "measured_barrels": "0", "observed_at": "2026-09-24T08:05:00Z"})
        settled = self.service.settle_stock_count("dispatch", "count-1", {"idempotency_key": "settle-1", "reason_code": "clean", "expected_revision": 2, "expected_balance_token": opened["live_inventory"]["balance_token"]})
        self.assertEqual(settled["status"], "adjusted")
        self.assertEqual(settled["adjustments"]["count"], 0)

    def test_late_measurement_within_tolerance_withdraws_escalation(self) -> None:
        self._lot("lot-1", "terminal-b", "1000")
        opened = self.service.open_stock_count("dispatch", {"session_id": "count-1", "facility_id": "terminal-b", "product": "crude", "tolerance_percent": "1"})
        token = opened["live_inventory"]["balance_token"]
        self.service.record_measurement("dispatch", "count-1", {"tank_id": "t-1", "measured_barrels": "900", "observed_at": "2026-09-24T08:05:00Z"})
        settle_payload = {"idempotency_key": "settle-1", "reason_code": "loss", "expected_revision": 2, "expected_balance_token": token}
        escalated = self.service.settle_stock_count("dispatch", "count-1", settle_payload)
        self.assertEqual(escalated["status"], "pending_review")
        self.assertEqual(escalated["revision"], 3)
        # 风险人员持有旧版本时，迟到的第二罐读数到达。
        self.service.record_measurement("dispatch", "count-1", {"tank_id": "t-2", "measured_barrels": "95", "observed_at": "2026-09-24T08:20:00Z"})
        current = self.service.stock_count("count-1", "audit")
        self.assertEqual(current["status"], "open")
        self.assertEqual(current["revision"], 4)
        with self.assertRaises(Conflict):
            self.service.review_stock_count("risk", "count-1", {"idempotency_key": "rev-old", "decision": "reject", "reason_code": "loss", "expected_revision": 3})
        settled = self.service.settle_stock_count("dispatch", "count-1", {"idempotency_key": "settle-2", "reason_code": "tank-calibration", "expected_revision": 4, "expected_balance_token": token})
        self.assertEqual(settled["status"], "adjusted")
        self.assertEqual(self.service.inventory_lot("lot-1")["available_barrels"], "995.000")

    def test_measurement_after_terminal_decision_is_rejected(self) -> None:
        self._lot("lot-1", "terminal-b", "1000")
        opened = self.service.open_stock_count("dispatch", {"session_id": "count-1", "facility_id": "terminal-b", "product": "crude", "tolerance_percent": "1"})
        token = opened["live_inventory"]["balance_token"]
        self.service.record_measurement("dispatch", "count-1", {"tank_id": "t-1", "measured_barrels": "900", "observed_at": "2026-09-24T08:05:00Z"})
        self.service.settle_stock_count("dispatch", "count-1", {"idempotency_key": "settle-1", "reason_code": "loss", "expected_revision": 2, "expected_balance_token": token})
        self.service.review_stock_count("risk", "count-1", {"idempotency_key": "rev-1", "decision": "reject", "reason_code": "suspected-mislot", "expected_revision": 3})
        with self.assertRaises(InvalidState):
            self.service.record_measurement("dispatch", "count-1", {"tank_id": "t-2", "measured_barrels": "5", "observed_at": "2026-09-24T09:00:00Z"})

    def test_risk_split_investigation_and_reject_statuses(self) -> None:
        self._lot("lot-1", "terminal-b", "1000")
        opened = self.service.open_stock_count("dispatch", {"session_id": "count-1", "facility_id": "terminal-b", "product": "crude", "tolerance_percent": "1"})
        token = opened["live_inventory"]["balance_token"]
        self.service.record_measurement("dispatch", "count-1", {"tank_id": "t-1", "measured_barrels": "800", "observed_at": "2026-09-24T08:05:00Z"})
        self.service.settle_stock_count("dispatch", "count-1", {"idempotency_key": "settle-1", "reason_code": "loss", "expected_revision": 2, "expected_balance_token": token})
        split = self.service.review_stock_count("risk", "count-1", {"idempotency_key": "rev-1", "decision": "split_investigation", "reason_code": "mislot-or-loss", "expected_revision": 3})
        self.assertEqual(split["status"], "split_investigation")
        self.assertEqual(split["reviews"][0]["decision"], "split_investigation")

    def test_shipment_during_count_moves_balance_and_versions_conflict(self) -> None:
        self._lot("lot-1", "terminal-b", "1000")
        opened = self.service.open_stock_count("dispatch", {"session_id": "count-1", "facility_id": "terminal-b", "product": "crude", "tolerance_percent": "1"})
        stale_token = opened["live_inventory"]["balance_token"]
        self.service.record_measurement("dispatch", "count-1", {"tank_id": "t-1", "measured_barrels": "999", "observed_at": "2026-09-24T08:05:00Z"})
        # 盘点期间相关批次仍可发运（从 terminal-b 运出 50 桶）。
        self._ship_out_of_terminal("3", "lot-1", "50")
        with self.assertRaises(Conflict):
            self.service.settle_stock_count("dispatch", "count-1", {"idempotency_key": "settle-bad", "reason_code": "calibration", "expected_revision": 2, "expected_balance_token": stale_token})
        refreshed = self.service.stock_count("count-1", "dispatch")
        self.assertTrue(refreshed["live_inventory"]["moved_since_snapshot"])
        settled = self.service.settle_stock_count("dispatch", "count-1", {"idempotency_key": "settle-ok", "reason_code": "calibration", "expected_revision": 2, "expected_balance_token": refreshed["live_inventory"]["balance_token"]})
        self.assertEqual(settled["status"], "adjusted")
        # 盘亏 1 桶落在发运后的 950 桶余额上。
        self.assertEqual(self.service.inventory_lot("lot-1")["available_barrels"], "949.000")
        adjustment = settled["adjustments"]["rows"][0]
        self.assertEqual(adjustment["balance_before_barrels"], "950.000")

    def test_adjustment_chain_verifies_and_detects_tampering(self) -> None:
        self._lot("lot-1", "terminal-b", "1000")
        opened = self.service.open_stock_count("dispatch", {"session_id": "count-1", "facility_id": "terminal-b", "product": "crude", "tolerance_percent": "1"})
        token = opened["live_inventory"]["balance_token"]
        self.service.record_measurement("dispatch", "count-1", {"tank_id": "t-1", "measured_barrels": "995", "observed_at": "2026-09-24T08:05:00Z"})
        settled = self.service.settle_stock_count("dispatch", "count-1", {"idempotency_key": "settle-1", "reason_code": "calibration", "expected_revision": 2, "expected_balance_token": token})
        chain = self.service.verify_adjustment_chain("audit", "count-1")
        self.assertTrue(chain["valid"])
        self.assertEqual(chain["adjustments"], 1)
        self.assertEqual(chain["chain_head"], settled["adjustments"]["chain_head"])
        self.connection.execute("UPDATE stock_count_adjustments SET reason_code='forged' WHERE session_id='count-1'")
        self.assertFalse(self.service.verify_adjustment_chain("audit", "count-1")["valid"])

    def test_list_filters_statuses_distinctly(self) -> None:
        self._lot("lot-1", "terminal-b", "1000")
        opened = self.service.open_stock_count("dispatch", {"session_id": "count-1", "facility_id": "terminal-b", "product": "crude", "tolerance_percent": "1"})
        token = opened["live_inventory"]["balance_token"]
        listing = self.service.list_stock_counts("audit", facility_id="terminal-b", status="unmeasured")
        self.assertEqual([item["session_id"] for item in listing["sessions"]], ["count-1"])
        self.service.record_measurement("dispatch", "count-1", {"tank_id": "t-1", "measured_barrels": "900", "observed_at": "2026-09-24T08:05:00Z"})
        self.service.settle_stock_count("dispatch", "count-1", {"idempotency_key": "settle-1", "reason_code": "loss", "expected_revision": 2, "expected_balance_token": token})
        pending = self.service.list_stock_counts("audit", status="pending_review")
        self.assertEqual([item["session_id"] for item in pending["sessions"]], ["count-1"])
        self.service.review_stock_count("risk", "count-1", {"idempotency_key": "rev-1", "decision": "reject", "reason_code": "mislot", "expected_revision": 3})
        rejected = self.service.list_stock_counts("audit", status="rejected")
        self.assertEqual([item["session_id"] for item in rejected["sessions"]], ["count-1"])
        self.assertEqual(self.service.list_stock_counts("audit", status="unmeasured")["count"], 0)


class StockCountApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.service = SupplyService(self.connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
        self.app = JsonApplication(self.service)
        for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)
        self.service.create_facility("plan", {"facility_id": "terminal-b", "name": "沿海终端", "kind": "terminal", "timezone": "Asia/Shanghai", "capacity_barrels": "800000"})

    def tearDown(self) -> None:
        self.connection.close()

    def test_api_routes_and_role_boundary(self) -> None:
        import json

        def call(method: str, path: str, actor: str | None, payload: dict | None = None):
            body = b"" if payload is None else json.dumps(payload).encode()
            headers = None if actor is None else {"X-Actor-Id": actor}
            return self.app.handle(method, path, headers, body)

        response = call("POST", "/stock-counts", "dispatch", {"session_id": "c1", "facility_id": "terminal-b", "product": "crude", "tolerance_percent": "1"})
        self.assertEqual(response.status, 201)
        self.assertEqual(response.body["status"], "unmeasured")
        response = call("GET", "/stock-counts?status=unmeasured", "audit", None)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["count"], 1)
        response = call("GET", "/stock-counts/c1", "audit", None)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["snapshots"]["book_barrels"], "0.000")
        response = call("GET", "/stock-counts/c1/adjustment-chain", "audit", None)
        self.assertEqual(response.status, 200)
        self.assertTrue(response.body["valid"])
        # 无测量直接结清返回状态冲突。
        response = call("POST", "/stock-counts/c1/settle", "dispatch", {"idempotency_key": "x", "reason_code": "x", "expected_revision": 1, "expected_balance_token": "t"})
        self.assertEqual(response.status, 409)
        # 风险角色不能结清，调度角色不能复核。
        self.service.add_inventory_lot("dispatch", {"lot_id": "lot-1", "facility_id": "terminal-b", "product": "crude", "grade": "BRENT", "quantity_barrels": "1000", "unit_cost_usd": "91", "received_at": "2026-09-24T06:00:00Z"})
        self.service.open_stock_count("dispatch", {"session_id": "c2", "facility_id": "terminal-b", "product": "crude", "tolerance_percent": "1"})
        response = call("POST", "/stock-counts/c2/settle", "risk", {"idempotency_key": "x", "reason_code": "x", "expected_revision": 1, "expected_balance_token": "t"})
        self.assertEqual(response.status, 403)


class ConcurrentSettleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "concurrent.sqlite3"
        connection = connect(self.db_path)
        service = SupplyService(connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
        for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk")):
            service.create_user(user_id, user_id, role)
        service.create_facility("plan", {"facility_id": "terminal-b", "name": "沿海终端", "kind": "terminal", "timezone": "Asia/Shanghai", "capacity_barrels": "800000"})
        service.add_inventory_lot("dispatch", {"lot_id": "lot-1", "facility_id": "terminal-b", "product": "crude", "grade": "BRENT", "quantity_barrels": "1000", "unit_cost_usd": "91", "received_at": "2026-09-24T06:00:00Z"})
        opened = service.open_stock_count("dispatch", {"session_id": "count-1", "facility_id": "terminal-b", "product": "crude", "tolerance_percent": "1"})
        service.record_measurement("dispatch", "count-1", {"tank_id": "t-1", "measured_barrels": "996", "observed_at": "2026-09-24T08:05:00Z"})
        self.token = opened["live_inventory"]["balance_token"]
        connection.close()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _worker(self, key: str, barrier: threading.Barrier, results: list, errors: list) -> None:
        connection = connect(self.db_path)
        try:
            service = SupplyService(connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
            payload = {"idempotency_key": key, "reason_code": "calibration", "expected_revision": 2, "expected_balance_token": self.token}
            barrier.wait()
            results.append(service.settle_stock_count("dispatch", "count-1", payload))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            connection.close()

    def test_same_idempotency_key_concurrent_settle_applies_once(self) -> None:
        barrier = threading.Barrier(2)
        results: list = []
        errors: list = []
        threads = [threading.Thread(target=self._worker, args=("same-key", barrier, results, errors)) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], results[1])
        connection = connect(self.db_path)
        try:
            row = connection.execute("SELECT available_barrels,revision FROM inventory_lots WHERE lot_id='lot-1'").fetchone()
            self.assertEqual(row["available_barrels"], "996.000")
            self.assertEqual(row["revision"], 2)
            count = connection.execute("SELECT COUNT(1) c FROM stock_count_adjustments WHERE session_id='count-1'").fetchone()
            self.assertEqual(count["c"], 1)
        finally:
            connection.close()

    def test_distinct_keys_concurrent_settle_only_one_wins(self) -> None:
        barrier = threading.Barrier(2)
        results: list = []
        errors: list = []
        threads = [
            threading.Thread(target=self._worker, args=(f"key-{index}", barrier, results, errors))
            for index in range(2)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(results), 1)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], Conflict)


if __name__ == "__main__":
    unittest.main()
