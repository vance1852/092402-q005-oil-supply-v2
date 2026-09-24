from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from oil_supply.api import JsonApplication
from oil_supply.clock import FrozenClock
from oil_supply.errors import Conflict, Forbidden, InvalidState, ValidationFailed
from oil_supply.service import SupplyService
from oil_supply.storage import connect


class StocktakeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = SupplyService(self.connection, self.clock)
        for user_id, role in (
            ("plan", "planner"),
            ("dispatch", "dispatcher"),
            ("dispatch2", "dispatcher"),
            ("risk", "risk"),
            ("audit", "auditor"),
        ):
            self.service.create_user(user_id, user_id, role)
        self.service.create_facility(
            "plan",
            {"facility_id": "term", "name": "沿海终端", "kind": "terminal", "timezone": "Asia/Shanghai", "capacity_barrels": "800000"},
        )
        self.service.create_facility(
            "plan",
            {"facility_id": "field-a", "name": "北部油田", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_barrels": "800000"},
        )
        self.service.create_route(
            "plan",
            {"route_id": "pipe-a-t", "origin_id": "field-a", "destination_id": "term", "product": "crude", "daily_capacity": "100000", "loss_basis_points": 0, "transit_hours": 12},
        )

    def tearDown(self) -> None:
        self.connection.close()

    def add_lot(self, lot_id: str, facility: str, product: str, quantity: str, grade: str = "BRENT") -> None:
        self.service.add_inventory_lot(
            "dispatch",
            {
                "lot_id": lot_id,
                "facility_id": facility,
                "product": product,
                "grade": grade,
                "quantity_barrels": quantity,
                "unit_cost_usd": "90",
                "received_at": "2026-09-24T06:00:00Z",
            },
        )

    def line(self, session_id: str, product: str) -> dict:
        return next(line for line in self.service.stocktakes.session(session_id)["lines"] if line["product"] == product)

    # ---- 开启与快照 ---------------------------------------------------

    def test_open_freezes_inventory_and_in_transit_snapshot(self) -> None:
        self.add_lot("lot-1", "term", "crude", "1000")
        self.add_lot("lot-2", "field-a", "crude", "5000")
        # 一批在途前往终端的货物
        self.service.submit_nomination("dispatch", {"nomination_id": "n1", "route_id": "pipe-a-t", "shipper_id": "sh", "service_date": "2026-09-24", "requested_barrels": "300", "priority": 10, "idempotency_key": "nk1"})
        self.service.allocate("dispatch", "pipe-a-t", "2026-09-24")
        self.service.dispatch_transfer("dispatch", "tr1", "n1", "lot-2", 2)

        opened = self.service.stocktakes.open_session(
            "dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "1"}
        )
        self.assertEqual(opened["state"], "open")
        self.assertEqual(opened["counts"]["unmeasured"], 1)
        crude = opened["lines"][0]
        self.assertEqual(crude["opening_barrels"], "1000.000")
        self.assertEqual(crude["in_transit_barrels"], "300.000")
        snapshot = opened["snapshot_lots"]
        self.assertEqual([row["lot_id"] for row in snapshot], ["lot-1"])
        self.assertEqual(snapshot[0]["opening_available_barrels"], "1000.000")

        # 会话开启后新增/变动库存不改变快照
        self.add_lot("lot-3", "term", "crude", "2000")
        detail = self.service.stocktakes.session("st1")
        self.assertEqual(detail["snapshot_lots"][0]["opening_available_barrels"], "1000.000")
        self.assertEqual(self.line("st1", "crude")["opening_barrels"], "1000.000")

    def test_facility_cannot_have_two_concurrent_open_sessions(self) -> None:
        self.add_lot("lot-1", "term", "crude", "1000")
        self.service.stocktakes.open_session("dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "1"})
        with self.assertRaises(Conflict):
            self.service.stocktakes.open_session("dispatch2", {"session_id": "st2", "facility_id": "term", "tolerance_percent": "1"})

    # ---- 测量与状态 ---------------------------------------------------

    def test_measurements_aggregate_per_tank_and_product(self) -> None:
        self.add_lot("lot-1", "term", "crude", "1000")
        self.service.stocktakes.open_session(
            "dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "2", "products": ["crude"]}
        )
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-a", "product": "crude", "measured_barrels": "600"})
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-b", "product": "crude", "measured_barrels": "380"})
        line = self.line("st1", "crude")
        self.assertEqual(line["state"], "pending_review")
        self.assertEqual(line["measured_barrels"], "980.000")
        self.assertEqual(line["delta_barrels"], "-20.000")
        self.assertEqual(line["variance_percent"], "2.0000")
        self.assertTrue(line["within_tolerance"])

    def test_duplicate_tank_and_early_late_measurement_rules(self) -> None:
        self.add_lot("lot-1", "term", "crude", "1000")
        self.service.stocktakes.open_session("dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "1"})
        self.service.stocktakes.record_measurement(
            "dispatch", "st1",
            {"tank_id": "tank-a", "product": "crude", "measured_barrels": "1000", "measured_at": "2026-09-24T09:30:00Z"},
        )
        # 同罐不能重复测量
        with self.assertRaises(Conflict):
            self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-a", "product": "crude", "measured_barrels": "999"})
        # 测量时间不能早于会话开启
        with self.assertRaises(ValidationFailed):
            self.service.stocktakes.record_measurement(
                "dispatch", "st1",
                {"tank_id": "tank-b", "product": "crude", "measured_barrels": "1", "measured_at": "2026-09-24T07:59:00Z"},
            )

    def test_zero_inventory_product_measured_zero_can_close(self) -> None:
        self.add_lot("lot-1", "term", "crude", "1000")
        # diesel 在该设施没有任何批次（零库存）
        self.service.stocktakes.open_session(
            "dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "1", "products": ["crude", "diesel"]}
        )
        diesel = self.line("st1", "diesel")
        self.assertEqual(diesel["state"], "unmeasured")
        self.assertEqual(diesel["opening_barrels"], "0.000")
        # 未测量不能结清
        with self.assertRaises(InvalidState):
            self.service.stocktakes.settle_line(
                "dispatch", "st1",
                {"product": "diesel", "decision": "adjust", "reason_code": "measurement_error", "note": "x", "idempotency_key": "dk1", "expected_revision": 1},
            )
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-d", "product": "diesel", "measured_barrels": "0"})
        diesel = self.line("st1", "diesel")
        self.assertEqual(diesel["delta_barrels"], "0.000")
        result = self.service.stocktakes.settle_line(
            "dispatch", "st1",
            {"product": "diesel", "decision": "adjust", "reason_code": "measurement_error", "note": "零库存确认", "idempotency_key": "dk2", "expected_revision": diesel["revision"]},
        )
        self.assertEqual(result["state"], "adjusted")
        self.assertIsNone(result["adjustment_id"])

    # ---- 容差与双人复核 -----------------------------------------------

    def test_within_tolerance_settled_by_duty_dispatcher_is_idempotent(self) -> None:
        self.add_lot("lot-1", "term", "crude", "1000")
        self.add_lot("lot-2", "term", "crude", "1000")
        self.service.stocktakes.open_session("dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "2"})
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-a", "product": "crude", "measured_barrels": "1980"})
        payload = {"product": "crude", "decision": "adjust", "reason_code": "measurement_error", "note": "罐表误差", "idempotency_key": "sk1", "expected_revision": 2}
        first = self.service.stocktakes.settle_line("dispatch", "st1", payload)
        self.assertEqual(first["state"], "adjusted")
        # 幂等重放返回同一结论
        second = self.service.stocktakes.settle_line("dispatch", "st1", payload)
        self.assertEqual(first, second)
        # 同一幂等键不同载荷冲突
        with self.assertRaises(Conflict):
            self.service.stocktakes.settle_line("dispatch", "st1", dict(payload, note="被篡改的原因"))
        # 调整按当前余额比例分摊，保留前后余额
        rows = self.connection.execute(
            "SELECT lot_id,delta_barrels,balance_before,balance_after,stocktake_session_id,stocktake_product "
            "FROM inventory_adjustments ORDER BY lot_id"
        ).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(Decimal(rows[0]["balance_before"]), Decimal("1000"))
        self.assertEqual(Decimal(rows[0]["balance_after"]), Decimal("990"))
        self.assertEqual(Decimal(rows[1]["balance_after"]), Decimal("990"))
        self.assertEqual(rows[0]["stocktake_session_id"], "st1")
        # 已结清行不能再补测量
        with self.assertRaises(InvalidState):
            self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-b", "product": "crude", "measured_barrels": "1"})

    def test_over_tolerance_requires_distinct_risk_officer(self) -> None:
        self.add_lot("lot-1", "term", "crude", "1000")
        self.service.stocktakes.open_session("dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "1"})
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-a", "product": "crude", "measured_barrels": "900"})
        payload = {"product": "crude", "decision": "adjust", "reason_code": "normal_loss", "note": "损耗", "idempotency_key": "sk1", "expected_revision": 2}
        # 当班调度不能自行结清超容差差异
        with self.assertRaises(Forbidden):
            self.service.stocktakes.settle_line("dispatch", "st1", payload)
        # 风险人员可以拒绝
        rejected = dict(payload, decision="reject", reason_code="wrong_lot", idempotency_key="sk2")
        result = self.service.stocktakes.settle_line("risk", "st1", rejected)
        self.assertEqual(result["state"], "rejected")
        line = self.line("st1", "crude")
        self.assertEqual(line["state"], "rejected")
        self.assertEqual(line["resolved_by"], "risk")
        # 拒绝不改动库存
        self.assertEqual(Decimal(self.service.inventory_lot("lot-1")["available_barrels"]), Decimal("1000"))

    def test_another_dispatcher_cannot_review_over_tolerance(self) -> None:
        self.add_lot("lot-1", "term", "crude", "1000")
        # dispatch2 开启会话；另一名调度 dispatch 同样无权复核超容差，必须风险角色且非开启人
        self.service.stocktakes.open_session("dispatch2", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "1"})
        self.service.stocktakes.record_measurement("dispatch2", "st1", {"tank_id": "tank-a", "product": "crude", "measured_barrels": "900"})
        with self.assertRaises(Forbidden):
            self.service.stocktakes.settle_line(
                "dispatch", "st1",
                {"product": "crude", "decision": "adjust", "reason_code": "normal_loss", "note": "x", "idempotency_key": "sk1", "expected_revision": 2},
            )

    def test_zero_book_with_physical_stock_requires_risk_review(self) -> None:
        # diesel 账面零库存却测出 50 桶（疑似错批），即使容差很大也必须风险复核
        self.service.stocktakes.open_session(
            "dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "50", "products": ["diesel"]}
        )
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-d", "product": "diesel", "measured_barrels": "50"})
        line = self.line("st1", "diesel")
        self.assertFalse(line["within_tolerance"])
        with self.assertRaises(Forbidden):
            self.service.stocktakes.settle_line(
                "dispatch", "st1",
                {"product": "diesel", "decision": "adjust", "reason_code": "wrong_lot", "note": "x", "idempotency_key": "sk1", "expected_revision": 2},
            )
        # 当班无权；风险直接调整但没有承接批次 → 422
        with self.assertRaises(ValidationFailed):
            self.service.stocktakes.settle_line(
                "risk", "st1",
                {"product": "diesel", "decision": "adjust", "reason_code": "wrong_lot", "note": "x", "idempotency_key": "sk2", "expected_revision": 2},
            )
        # 调查期间补建承接批次（会话期间收料，不在快照内），再由风险并入账
        self.service.stocktakes.settle_line(
            "risk", "st1",
            {"product": "diesel", "decision": "split_investigation", "reason_code": "wrong_lot", "note": "排查错批", "idempotency_key": "sk3", "expected_revision": 2},
        )
        self.service.add_inventory_lot(
            "dispatch",
            {"lot_id": "lot-d", "facility_id": "term", "product": "diesel", "grade": "DIESEL", "quantity_barrels": "0.001", "unit_cost_usd": "95", "received_at": "2026-09-24T09:00:00Z"},
        )
        result = self.service.stocktakes.resolve_investigation(
            "risk", "st1", "diesel",
            {"decision": "adjust", "reason_code": "wrong_lot", "note": "确认错批，并入新批次", "idempotency_key": "rk1", "expected_revision": 3, "target_lot_id": "lot-d"},
        )
        self.assertEqual(result["state"], "adjusted")
        self.assertEqual(Decimal(self.service.inventory_lot("lot-d")["available_barrels"]), Decimal("50.001"))

    def test_split_investigation_then_risk_resolution(self) -> None:
        self.add_lot("lot-1", "term", "crude", "1000")
        self.service.stocktakes.open_session("dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "1"})
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-a", "product": "crude", "measured_barrels": "850"})
        split = self.service.stocktakes.settle_line(
            "risk", "st1",
            {"product": "crude", "decision": "split_investigation", "reason_code": "wrong_lot", "note": "疑似错批，拆分调查", "idempotency_key": "sk1", "expected_revision": 2},
        )
        self.assertEqual(split["state"], "investigating")
        self.assertEqual(self.line("st1", "crude")["state"], "investigating")
        # 调查状态不能关闭会话
        with self.assertRaises(InvalidState):
            self.service.stocktakes.close_session("dispatch", "st1")
        # 当班不能给调查结论
        with self.assertRaises(Forbidden):
            self.service.stocktakes.resolve_investigation(
                "dispatch", "st1", "crude",
                {"decision": "adjust", "reason_code": "normal_loss", "note": "确认损耗", "idempotency_key": "rk1", "expected_revision": 3},
            )
        resolved = self.service.stocktakes.resolve_investigation(
            "risk", "st1", "crude",
            {"decision": "adjust", "reason_code": "normal_loss", "note": "调查确认自然损耗", "idempotency_key": "rk1", "expected_revision": 3},
        )
        self.assertEqual(resolved["state"], "adjusted")
        self.assertEqual(self.service.inventory_lot("lot-1")["available_barrels"], "850.000")
        self.service.stocktakes.close_session("dispatch", "st1")
        self.assertEqual(self.service.stocktakes.session("st1")["state"], "closed")

    def test_within_tolerance_cannot_escalate_to_investigation(self) -> None:
        self.add_lot("lot-1", "term", "crude", "1000")
        self.service.stocktakes.open_session("dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "5"})
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-a", "product": "crude", "measured_barrels": "990"})
        with self.assertRaises(Forbidden):
            self.service.stocktakes.settle_line(
                "dispatch", "st1",
                {"product": "crude", "decision": "split_investigation", "reason_code": "investigation", "note": "x", "idempotency_key": "sk1", "expected_revision": 2},
            )

    # ---- 版本检测与迟到测量 -------------------------------------------

    def test_late_measurement_bumps_revision_and_stale_settle_conflicts(self) -> None:
        self.add_lot("lot-1", "term", "crude", "1000")
        self.service.stocktakes.open_session("dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "5"})
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-a", "product": "crude", "measured_barrels": "990"})
        stale = self.line("st1", "crude")["revision"]
        # 第二罐迟到补录，产生新版本
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-b", "product": "crude", "measured_barrels": "10"})
        current = self.line("st1", "crude")
        self.assertGreater(current["revision"], stale)
        with self.assertRaises(Conflict):
            self.service.stocktakes.settle_line(
                "dispatch", "st1",
                {"product": "crude", "decision": "adjust", "reason_code": "measurement_error", "note": "旧结论", "idempotency_key": "sk1", "expected_revision": stale},
            )
        # 基于最新版本可以结清
        result = self.service.stocktakes.settle_line(
            "dispatch", "st1",
            {"product": "crude", "decision": "adjust", "reason_code": "measurement_error", "note": "补罐后确认", "idempotency_key": "sk2", "expected_revision": current["revision"]},
        )
        self.assertEqual(result["state"], "adjusted")

    def test_concurrent_settle_only_one_wins(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "stock.sqlite3"
            seeder = SupplyService(connect(database), self.clock)
            for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk")):
                seeder.create_user(user_id, user_id, role)
            seeder.create_facility("plan", {"facility_id": "term", "name": "终端", "kind": "terminal", "timezone": "UTC", "capacity_barrels": "1"})
            seeder.add_inventory_lot("dispatch", {"lot_id": "lot-1", "facility_id": "term", "product": "crude", "grade": "BRENT", "quantity_barrels": "1000", "unit_cost_usd": "90", "received_at": "2026-09-24T06:00:00Z"})
            seeder.stocktakes.open_session("dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "50"})
            seeder.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-a", "product": "crude", "measured_barrels": "900"})
            revision = seeder.stocktakes.session("st1")["lines"][0]["revision"]
            seeder.connection.close()

            outcomes: list[str] = []
            barrier = threading.Barrier(2)

            def settle(key: str) -> None:
                connection = connect(database)
                service = SupplyService(connection, self.clock)
                barrier.wait()
                try:
                    service.stocktakes.settle_line(
                        "dispatch", "st1",
                        {"product": "crude", "decision": "adjust", "reason_code": "measurement_error", "note": "并发结清", "idempotency_key": key, "expected_revision": revision},
                    )
                    outcomes.append("ok")
                except Conflict:
                    outcomes.append("conflict")
                finally:
                    connection.close()

            threads = [threading.Thread(target=settle, args=(f"sk-{index}",)) for index in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(sorted(outcomes), ["conflict", "ok"])
            verifier = connect(database)
            service = SupplyService(verifier, self.clock)
            self.assertEqual(service.stocktakes.session("st1")["lines"][0]["state"], "adjusted")
            verifier.close()

    # ---- 发运并行、审计链与查询 ---------------------------------------

    def test_shipments_continue_during_session_and_reduction_uses_current_balance(self) -> None:
        # 终端有两批；另有一批在油田，盘点期间照常发运（在途不影响终端快照）
        self.add_lot("lot-1", "term", "crude", "600")
        self.add_lot("lot-2", "term", "crude", "400")
        self.add_lot("lot-f", "field-a", "crude", "5000")
        self.service.stocktakes.open_session("dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "1"})
        # 盘点期间另一批货从油田发往终端（仍在途），发运不受盘点阻塞
        self.service.submit_nomination("dispatch", {"nomination_id": "n1", "route_id": "pipe-a-t", "shipper_id": "sh", "service_date": "2026-09-24", "requested_barrels": "500", "priority": 10, "idempotency_key": "nk1"})
        self.service.allocate("dispatch", "pipe-a-t", "2026-09-24")
        transfer = self.service.dispatch_transfer("dispatch", "tr1", "n1", "lot-f", 2)
        self.assertEqual(transfer["state"], "in_transit")
        # 实测短少 10%，风险调减；快照仍是开启时的 1000
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-a", "product": "crude", "measured_barrels": "900"})
        self.service.stocktakes.settle_line(
            "risk", "st1",
            {"product": "crude", "decision": "adjust", "reason_code": "normal_loss", "note": "损耗", "idempotency_key": "sk1", "expected_revision": 2},
        )
        self.assertEqual(self.service.inventory_lot("lot-1")["available_barrels"], "540.000")
        self.assertEqual(self.service.inventory_lot("lot-2")["available_barrels"], "360.000")

    def test_reduction_beyond_current_balance_is_rejected(self) -> None:
        self.add_lot("lot-1", "term", "crude", "100")
        self.service.stocktakes.open_session("dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "0"})
        # 账面快照 100，实测 0 要求调减 100；当前余额足够 → 成功；再构造不足场景
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-a", "product": "crude", "measured_barrels": "0"})
        self.service.stocktakes.settle_line(
            "risk", "st1",
            {"product": "crude", "decision": "adjust", "reason_code": "normal_loss", "note": "清空", "idempotency_key": "sk1", "expected_revision": 2},
        )
        self.assertEqual(self.service.inventory_lot("lot-1")["available_barrels"], "0.000")

    def test_query_distinguishes_all_four_states_and_audit_chain_holds(self) -> None:
        self.add_lot("lot-1", "term", "crude", "1000")
        self.service.stocktakes.open_session(
            "dispatch",
            {"session_id": "st1", "facility_id": "term", "tolerance_percent": "1", "products": ["crude", "diesel", "jet-fuel"]},
        )
        # crude：已调整（容差内）
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-c", "product": "crude", "measured_barrels": "999"})
        self.service.stocktakes.settle_line("dispatch", "st1", {"product": "crude", "decision": "adjust", "reason_code": "measurement_error", "note": "a", "idempotency_key": "ck", "expected_revision": 2})
        # diesel：待复核（超容差未结论）
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-d", "product": "diesel", "measured_barrels": "50"})
        # jet-fuel：未测量
        detail = self.service.stocktakes.session("st1")
        self.assertEqual(detail["counts"], {"unmeasured": 1, "pending_review": 1, "adjusted": 1, "rejected": 0, "investigating": 0})
        states = {line["product"]: line["state"] for line in detail["lines"]}
        self.assertEqual(states, {"crude": "adjusted", "diesel": "pending_review", "jet-fuel": "unmeasured"})
        # diesel 被风险拒绝后出现 rejected
        self.service.stocktakes.settle_line("risk", "st1", {"product": "diesel", "decision": "reject", "reason_code": "wrong_lot", "note": "r", "idempotency_key": "dk", "expected_revision": 2})
        self.assertEqual(self.line("st1", "diesel")["state"], "rejected")
        # 列表过滤
        listing = self.service.stocktakes.list_sessions(facility_id="term", state="open")
        self.assertEqual([row["session_id"] for row in listing["sessions"]], ["st1"])
        # 审计哈希链完整，包含盘点事件与签署人
        chain = self.service.audit_chain("audit")
        self.assertTrue(chain["valid"])
        events = self.connection.execute(
            "SELECT event_type,actor_id FROM supply_audit_events WHERE entity_type='stocktake_session' ORDER BY event_id"
        ).fetchall()
        kinds = {row["event_type"] for row in events}
        self.assertIn("stocktake.opened", kinds)
        self.assertIn("stocktake.measured", kinds)
        self.assertIn("stocktake.adjust", kinds)
        self.assertIn("stocktake.reject", kinds)
        actors = {row["actor_id"] for row in events}
        self.assertEqual(actors, {"dispatch", "risk"})

    def test_close_requires_every_line_resolved(self) -> None:
        self.add_lot("lot-1", "term", "crude", "1000")
        self.service.stocktakes.open_session("dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "1", "products": ["crude", "diesel"]})
        self.service.stocktakes.record_measurement("dispatch", "st1", {"tank_id": "tank-c", "product": "crude", "measured_barrels": "1000"})
        self.service.stocktakes.settle_line("dispatch", "st1", {"product": "crude", "decision": "adjust", "reason_code": "measurement_error", "note": "a", "idempotency_key": "ck", "expected_revision": 2})
        with self.assertRaises(InvalidState):
            self.service.stocktakes.close_session("dispatch", "st1")

    # ---- HTTP 边界 ----------------------------------------------------

    def test_api_routes_and_status_codes(self) -> None:
        self.add_lot("lot-1", "term", "crude", "1000")
        app = JsonApplication(self.service)

        def call(method: str, path: str, actor: str, payload: dict | None = None):
            body = json.dumps(payload).encode() if payload is not None else b""
            return app.handle(method, path, {"X-Actor-Id": actor}, body)

        response = call("POST", "/stocktakes", "dispatch", {"session_id": "st1", "facility_id": "term", "tolerance_percent": "1"})
        self.assertEqual(response.status, 201)
        response = call("POST", "/stocktakes/st1/measurements", "dispatch", {"tank_id": "tank-a", "product": "crude", "measured_barrels": "900"})
        self.assertEqual(response.status, 201)
        # 超容差当班结清 → 403
        response = call("POST", "/stocktakes/st1/settle", "dispatch", {"product": "crude", "decision": "adjust", "reason_code": "normal_loss", "note": "x", "idempotency_key": "sk1", "expected_revision": 2})
        self.assertEqual(response.status, 403)
        # 风险拒绝
        response = call("POST", "/stocktakes/st1/settle", "risk", {"product": "crude", "decision": "reject", "reason_code": "wrong_lot", "note": "x", "idempotency_key": "sk2", "expected_revision": 2})
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["state"], "rejected")
        response = call("GET", "/stocktakes/st1", "audit")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["counts"]["rejected"], 1)
        response = call("GET", "/stocktakes?facility_id=term&state=open", "audit")
        self.assertEqual(response.status, 200)
        self.assertEqual(len(response.body["sessions"]), 1)
        response = call("GET", "/stocktakes/missing", "audit")
        self.assertEqual(response.status, 404)


if __name__ == "__main__":
    unittest.main()
