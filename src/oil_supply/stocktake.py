"""交接班盘点会话：快照冻结、逐罐测量、容差结清与双人复核。"""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Mapping

from .clock import parse_utc, utc_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .models import STOCKTAKE_REASONS, StocktakeDecision, StocktakeOpen, TankMeasurement
from .planning import (
    ZERO,
    canonical_json,
    decimal_text,
    digest,
    quantize_volume,
    reconcile_inventory,
)
from .storage import transaction

if TYPE_CHECKING:
    from .service import SupplyService


class StocktakeService:
    """盘点会话用例，与 :class:`SupplyService` 共享同一个 SQLite 连接。"""

    def __init__(self, connection: sqlite3.Connection, supply: "SupplyService", clock=None) -> None:
        self.connection = connection
        self.supply = supply
        self.clock = clock or supply.clock

    def _now(self) -> str:
        return utc_text(self.clock.now())

    # ---- 内部读取辅助 -------------------------------------------------

    def _session_row(self, session_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM stocktake_sessions WHERE session_id=?", (session_id,)
        ).fetchone()
        if row is None:
            raise NotFound("盘点会话不存在")
        return row

    def _line_row(self, session_id: str, product: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM stocktake_product_lines WHERE session_id=? AND product=?",
            (session_id, product),
        ).fetchone()
        if row is None:
            raise NotFound("该油品不在盘点范围内")
        return row

    def _settle_permission(self, actor_id: str, session: sqlite3.Row, within_tolerance: bool) -> None:
        """结清鉴权：容差内当班调度即可；超容差必须另一名风险人员。"""
        if within_tolerance:
            self.supply._require(actor_id, "stocktake.settle")
            return
        user = self.supply._require(actor_id, "stocktake.review")
        if user["role"] != "risk":
            raise Forbidden("超出容差的差异必须由风险人员复核")
        if actor_id == session["opened_by"]:
            raise Forbidden("复核人不能是开启盘点会话的当班人员")

    def _in_transit_by_product(self, facility_id: str) -> dict[str, Decimal]:
        """在 facility 卸货途中的在途量（transfer 已发运未到货，目的地为本设施）。"""
        rows = self.connection.execute(
            "SELECT r.product, t.loaded_barrels barrels FROM transfers t "
            "JOIN nominations n ON n.nomination_id=t.nomination_id "
            "JOIN routes r ON r.route_id=n.route_id "
            "WHERE r.destination_id=? AND t.state='in_transit'",
            (facility_id,),
        ).fetchall()
        totals: dict[str, Decimal] = {}
        for row in rows:
            totals[row["product"]] = totals.get(row["product"], ZERO) + Decimal(row["barrels"])
        return totals

    # ---- 会话开启与快照 -----------------------------------------------

    def open_session(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        user = self.supply._require(actor_id, "stocktake.open")
        request = StocktakeOpen.from_dict(raw)
        facility = self.connection.execute(
            "SELECT facility_id FROM facilities WHERE facility_id=?", (request.facility_id,)
        ).fetchone()
        if facility is None:
            raise NotFound("设施不存在")
        duplicate = self.connection.execute(
            "SELECT session_id FROM stocktake_sessions WHERE facility_id=? AND state='open'",
            (request.facility_id,),
        ).fetchone()
        if duplicate is not None:
            raise Conflict("该设施已有未关闭的盘点会话")

        lot_rows = self.connection.execute(
            "SELECT lot_id,product,grade,available_barrels,revision FROM inventory_lots "
            "WHERE facility_id=? ORDER BY lot_id",
            (request.facility_id,),
        ).fetchall()
        products = request.products
        if not products:
            products = tuple(dict.fromkeys(row["product"] for row in lot_rows))
        if not products:
            raise ValidationFailed("设施没有任何可盘点油品")
        in_transit = self._in_transit_by_product(request.facility_id)
        snapshot_lots = [dict(row) for row in lot_rows]
        snapshot_payload = {
            "facility_id": request.facility_id,
            "opened_at": self._now(),
            "lots": snapshot_lots,
            "in_transit": {product: decimal_text(value) for product, value in sorted(in_transit.items())},
        }
        snapshot_sha = digest(snapshot_payload)

        opening_by_product: dict[str, Decimal] = {}
        for row in lot_rows:
            opening_by_product[row["product"]] = opening_by_product.get(row["product"], ZERO) + Decimal(
                row["available_barrels"]
            )

        with transaction(self.connection, immediate=True):
            try:
                self.connection.execute(
                    "INSERT INTO stocktake_sessions(session_id,facility_id,tolerance_percent,products_json,"
                    "snapshot_sha256,opened_by,opened_at) VALUES(?,?,?,?,?,?,?)",
                    (
                        request.session_id,
                        request.facility_id,
                        decimal_text(request.tolerance_percent),
                        canonical_json(list(products)),
                        snapshot_sha,
                        actor_id,
                        self._now(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                message = str(exc)
                if "facility_id" in message:
                    raise Conflict("该设施已有未关闭的盘点会话") from exc
                raise Conflict("盘点会话编号已经存在") from exc
            for row in lot_rows:
                self.connection.execute(
                    "INSERT INTO stocktake_snapshot_lots(session_id,lot_id,product,grade,"
                    "opening_available_barrels,lot_revision) VALUES(?,?,?,?,?,?)",
                    (
                        request.session_id,
                        row["lot_id"],
                        row["product"],
                        row["grade"],
                        decimal_text(quantize_volume(Decimal(row["available_barrels"]))),
                        row["revision"],
                    ),
                )
            for product in products:
                self.connection.execute(
                    "INSERT INTO stocktake_product_lines(session_id,product,state,opening_barrels,"
                    "in_transit_barrels) VALUES(?,?, 'unmeasured',?,?)",
                    (
                        request.session_id,
                        product,
                        decimal_text(quantize_volume(opening_by_product.get(product, ZERO))),
                        decimal_text(quantize_volume(in_transit.get(product, ZERO))),
                    ),
                )
            self.supply._audit(
                "stocktake_session",
                request.session_id,
                "stocktake.opened",
                actor_id,
                {"facility_id": request.facility_id, "products": list(products), "snapshot_sha256": snapshot_sha},
            )
        return self.session(request.session_id)

    # ---- 逐罐测量 -----------------------------------------------------

    def record_measurement(self, actor_id: str, session_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self.supply._require(actor_id, "stocktake.measure")
        session = self._session_row(session_id)
        if session["state"] != "open":
            raise InvalidState("盘点会话已关闭")
        measurement = TankMeasurement.from_dict(raw)
        line = self._line_row(session_id, measurement.product)
        measured_at = measurement.measured_at or self._now()
        if measurement.measured_at is not None:
            measured_time = parse_utc(measured_at)
            if measured_time < parse_utc(session["opened_at"]):
                raise ValidationFailed("测量时间不能早于会话开启时间（迟到测量请在开启后补录）")

        with transaction(self.connection, immediate=True):
            try:
                self.connection.execute(
                    "INSERT INTO stocktake_tank_measurements(session_id,tank_id,product,measured_barrels,"
                    "measured_at,recorded_by,recorded_at) VALUES(?,?,?,?,?,?,?)",
                    (
                        session_id,
                        measurement.tank_id,
                        measurement.product,
                        decimal_text(measurement.measured_barrels),
                        measured_at,
                        actor_id,
                        self._now(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise Conflict("该储罐在本会话中已经测量") from exc
            self._refresh_line(session, line, measurement.product)
            self.supply._audit(
                "stocktake_session",
                session_id,
                "stocktake.measured",
                actor_id,
                {"tank_id": measurement.tank_id, "product": measurement.product},
            )
        return self.session(session_id)

    def _refresh_line(self, session: sqlite3.Row, existing: sqlite3.Row, product: str) -> None:
        """根据当前全部测量重算油品行；已结清行不允许被新测量改写。"""
        if existing["state"] in {"adjusted", "rejected", "investigating"}:
            raise InvalidState("该油品已结清，新测量不能改写结论")
        rows = self.connection.execute(
            "SELECT measured_barrels,measured_at FROM stocktake_tank_measurements "
            "WHERE session_id=? AND product=? ORDER BY measurement_id",
            (session["session_id"], product),
        ).fetchall()
        measured = sum((Decimal(row["measured_barrels"]) for row in rows), ZERO)
        measured = quantize_volume(measured)
        opening = Decimal(existing["opening_barrels"])
        reconciliation = reconcile_inventory(opening, measured, Decimal(session["tolerance_percent"]))
        # 账面为零却测出实物属于盘盈/错批，无论容差百分比多少都必须风险复核。
        within = bool(reconciliation["within_tolerance"]) and not (opening == ZERO and measured != ZERO)
        input_sha = digest([dict(row) for row in rows])
        state = "pending_review" if rows else "unmeasured"
        self.connection.execute(
            "UPDATE stocktake_product_lines SET state=?,measured_barrels=?,delta_barrels=?,"
            "variance_percent=?,within_tolerance=?,measured_input_sha256=?,revision=revision+1 "
            "WHERE session_id=? AND product=?",
            (
                state,
                reconciliation["measured_quantity"],
                reconciliation["delta_barrels"],
                reconciliation["variance_percent"],
                1 if within else 0,
                input_sha,
                session["session_id"],
                product,
            ),
        )

    # ---- 结清：当班（容差内）或风险（超容差） -------------------------

    def settle_line(self, actor_id: str, session_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        decision = StocktakeDecision.from_dict(raw)
        session = self._session_row(session_id)
        if session["state"] != "open":
            raise InvalidState("盘点会话已关闭")
        line = self._line_row(session_id, decision.product)
        within_tolerance = bool(line["within_tolerance"])

        # 先鉴权（包括幂等重放）：容差内当班调度，超容差必须另一名风险人员。
        self._settle_permission(actor_id, session, within_tolerance)
        if within_tolerance and decision.decision == "split_investigation":
            raise Forbidden("容差内差异不需要拆分调查")

        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM supply_idempotency "
            "WHERE scope='stocktake_settle' AND idempotency_key=?",
            (decision.idempotency_key,),
        ).fetchone()
        request_digest = digest(raw)
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同的盘点结论")
            return json.loads(stored["response_json"])

        if line["state"] == "unmeasured":
            raise InvalidState("该油品尚未测量，无法结清")
        if line["state"] in {"adjusted", "rejected", "investigating"}:
            raise InvalidState("该油品已经结清")
        if line["revision"] != decision.expected_revision:
            raise Conflict("测量结果已经产生新版本，请刷新后基于最新版本结清")

        if decision.decision == "reject" and Decimal(line["delta_barrels"]) == ZERO and Decimal(line["measured_barrels"]) == ZERO:
            raise InvalidState("测量与账面完全一致，应选择调整确认而不是拒绝")

        response: dict[str, Any]
        with transaction(self.connection, immediate=True):
            # 版本检测：测量版本和快照版本都必须与客户端看到的一致。
            current = self.connection.execute(
                "SELECT * FROM stocktake_product_lines WHERE session_id=? AND product=?",
                (session_id, decision.product),
            ).fetchone()
            if current["revision"] != line["revision"] or current["state"] != "pending_review":
                raise Conflict("盘点结论存在并发冲突，请刷新后重试")

            adjustment_id: int | None = None
            next_state = {
                "adjust": "adjusted",
                "split_investigation": "investigating",
                "reject": "rejected",
            }[decision.decision]

            if decision.decision == "adjust":
                delta = Decimal(current["delta_barrels"])
                if delta != ZERO:
                    adjustment_id = self._apply_adjustment(
                        actor_id=actor_id,
                        session=session,
                        line=current,
                        delta=delta,
                        reason_code=decision.reason_code,
                        note=decision.note,
                        target_lot_id=decision.target_lot_id,
                    )

            self.connection.execute(
                "INSERT INTO stocktake_decisions(session_id,product,decision,reason_code,note,"
                "expected_line_revision,expected_snapshot_sha256,idempotency_key,actor_id,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    session_id,
                    decision.product,
                    decision.decision,
                    decision.reason_code,
                    decision.note,
                    current["revision"],
                    session["snapshot_sha256"],
                    decision.idempotency_key,
                    actor_id,
                    self._now(),
                ),
            )
            self.connection.execute(
                "UPDATE stocktake_product_lines SET state=?,adjustment_id=COALESCE(?,adjustment_id),"
                "resolved_by=?,resolved_at=?,reason_code=?,note=?,revision=revision+1 "
                "WHERE session_id=? AND product=? AND revision=?",
                (
                    next_state,
                    adjustment_id,
                    actor_id,
                    self._now(),
                    decision.reason_code,
                    decision.note,
                    session_id,
                    decision.product,
                    current["revision"],
                ),
            )
            self.supply._audit(
                "stocktake_session",
                session_id,
                f"stocktake.{decision.decision}",
                actor_id,
                {
                    "product": decision.product,
                    "reason_code": decision.reason_code,
                    "adjustment_id": adjustment_id,
                    "line_revision": current["revision"],
                    "snapshot_sha256": session["snapshot_sha256"],
                },
            )
            response = {
                "session_id": session_id,
                "product": decision.product,
                "state": next_state,
                "revision": current["revision"] + 1,
                "adjustment_id": adjustment_id,
            }
            self.connection.execute(
                "INSERT INTO supply_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
                "VALUES('stocktake_settle',?,?,?,?)",
                (decision.idempotency_key, request_digest, canonical_json(response), self._now()),
            )
        return response

    def _apply_adjustment(
        self,
        *,
        actor_id: str,
        session: sqlite3.Row,
        line: sqlite3.Row,
        delta: Decimal,
        reason_code: str,
        note: str,
        target_lot_id: str | None = None,
    ) -> int:
        """把差异分摊到批次并写入幂等调整，保留每批前后余额。

        正向差异记入最早的快照批次；账面没有任何批次时（零库存盘盈/错批），
        必须由风险结论显式指定同设施同油品的承接批次。负向差异按各批次当前
        余额比例分摊，最后一个批次承担舍入尾差。会话期间批次仍可发运，因此
        按当前余额扣减，并用批次版本号检测并发改写。
        """
        snapshot_rows = self.connection.execute(
            "SELECT s.lot_id lot_id,l.available_barrels current,l.revision current_revision "
            "FROM stocktake_snapshot_lots s JOIN inventory_lots l ON l.lot_id=s.lot_id "
            "WHERE s.session_id=? AND s.product=? ORDER BY s.lot_id",
            (session["session_id"], line["product"]),
        ).fetchall()

        process_rows: list[sqlite3.Row]
        shares: dict[str, Decimal]
        if delta < ZERO:
            if not snapshot_rows:
                raise InvalidState("快照中没有该油品的库存批次，无法记账调减")
            total_current = sum((Decimal(row["current"]) for row in snapshot_rows), ZERO)
            if total_current + delta < ZERO:
                raise InvalidState("当前库存不足以完成调减（会话期间发运已改变余额）")
            shares = {}
            remaining = delta
            for index, row in enumerate(snapshot_rows):
                if index < len(snapshot_rows) - 1:
                    lot_current = Decimal(row["current"])
                    share = quantize_volume(delta * lot_current / total_current) if total_current > ZERO else ZERO
                else:
                    share = remaining
                shares[row["lot_id"]] = share
                remaining -= share
            if remaining != ZERO:
                target = max(snapshot_rows, key=lambda r: Decimal(r["current"]))["lot_id"]
                shares[target] = quantize_volume(shares[target] + remaining)
            process_rows = snapshot_rows
        else:
            if snapshot_rows:
                first = snapshot_rows[0]
                shares = {row["lot_id"]: ZERO for row in snapshot_rows}
                shares[first["lot_id"]] = delta
                process_rows = snapshot_rows
            else:
                # 账面零库存却盘盈：必须显式指定承接批次
                if not target_lot_id:
                    raise ValidationFailed("账面无批次的盘盈必须提供 target_lot_id 承接批次")
                target_row = self.connection.execute(
                    "SELECT lot_id,available_barrels current,revision current_revision FROM inventory_lots "
                    "WHERE lot_id=? AND facility_id=? AND product=?",
                    (target_lot_id, session["facility_id"], line["product"]),
                ).fetchone()
                if target_row is None:
                    raise ValidationFailed("承接批次不存在或不属于本设施/油品")
                shares = {target_lot_id: delta}
                process_rows = [target_row]

        first_adjustment_id: int | None = None
        for row in process_rows:
            share = quantize_volume(shares[row["lot_id"]])
            if share == ZERO:
                continue
            before = quantize_volume(Decimal(row["current"]))
            after = quantize_volume(before + share)
            idem_key = f"{session['session_id']}:{row['lot_id']}:{line['measured_input_sha256'][:12]}"
            cursor = self.connection.execute(
                "INSERT INTO inventory_adjustments(lot_id,delta_barrels,reason_code,note,idempotency_key,"
                "actor_id,created_at,balance_before,balance_after,lot_revision_before,"
                "stocktake_session_id,stocktake_product) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    row["lot_id"],
                    decimal_text(share),
                    reason_code,
                    f"[{session['session_id']}] {note}",
                    idem_key,
                    actor_id,
                    self._now(),
                    decimal_text(before),
                    decimal_text(after),
                    row["current_revision"],
                    session["session_id"],
                    line["product"],
                ),
            )
            if first_adjustment_id is None:
                first_adjustment_id = int(cursor.lastrowid)
            updated = self.connection.execute(
                "UPDATE inventory_lots SET available_barrels=?,revision=revision+1 "
                "WHERE lot_id=? AND revision=?",
                (decimal_text(after), row["lot_id"], row["current_revision"]),
            )
            if updated.rowcount != 1:
                raise Conflict("库存批次在结清期间被其他操作改写，请重试")
            self.supply._audit(
                "inventory_lot",
                row["lot_id"],
                "inventory.adjusted",
                actor_id,
                {
                    "delta_barrels": decimal_text(share),
                    "balance_before": decimal_text(before),
                    "balance_after": decimal_text(after),
                    "reason_code": reason_code,
                    "stocktake_session_id": session["session_id"],
                },
            )
        if first_adjustment_id is None:
            raise InvalidState("差异为零，未产生库存调整")
        return first_adjustment_id

    # ---- 调查拆分结论 -------------------------------------------------

    def resolve_investigation(
        self,
        actor_id: str,
        session_id: str,
        product: str,
        raw: Mapping[str, Any],
    ) -> dict[str, Any]:
        """风险人员对拆分调查的油品给出最终结论：调整或拒绝。"""
        self.supply._require(actor_id, "stocktake.review")
        session = self._session_row(session_id)
        line = self._line_row(session_id, product)
        if line["state"] != "investigating":
            raise InvalidState("该油品不处于拆分调查状态")
        decision_value = str(raw.get("decision", "")).strip()
        if decision_value not in {"adjust", "reject"}:
            raise ValidationFailed("调查结论必须是 adjust 或 reject")
        reason_code = str(raw.get("reason_code", "investigation")).strip()
        if reason_code not in STOCKTAKE_REASONS:
            raise ValidationFailed("reason_code 不是受支持的原因代码")
        note = str(raw.get("note", "")).strip()
        if not note:
            raise ValidationFailed("调查结论必须填写说明")
        idem_key = str(raw.get("idempotency_key", "")).strip()
        if not idem_key:
            raise ValidationFailed("idempotency_key 不能为空")
        expected_revision = raw.get("expected_revision")
        if isinstance(expected_revision, bool) or not isinstance(expected_revision, int) or expected_revision <= 0:
            raise ValidationFailed("expected_revision 必须是正整数")
        if line["revision"] != expected_revision:
            raise Conflict("盘点行已经产生新版本，请刷新后重新提交结论")

        stored = self.connection.execute(
            "SELECT response_json FROM supply_idempotency WHERE scope='stocktake_investigation' AND idempotency_key=?",
            (idem_key,),
        ).fetchone()
        if stored is not None:
            return json.loads(stored["response_json"])

        with transaction(self.connection, immediate=True):
            current = self.connection.execute(
                "SELECT * FROM stocktake_product_lines WHERE session_id=? AND product=?",
                (session_id, product),
            ).fetchone()
            if current["state"] != "investigating":
                raise InvalidState("该油品调查已被结论")
            adjustment_id = None
            next_state = "rejected"
            if decision_value == "adjust":
                if Decimal(current["delta_barrels"]) == ZERO:
                    raise InvalidState("差异为零，无需调整")
                target_lot_id = raw.get("target_lot_id")
                if target_lot_id is not None:
                    target_lot_id = str(target_lot_id).strip()
                adjustment_id = self._apply_adjustment(
                    actor_id=actor_id,
                    session=session,
                    line=current,
                    delta=Decimal(current["delta_barrels"]),
                    reason_code=reason_code,
                    note=note,
                    target_lot_id=target_lot_id,
                )
                next_state = "adjusted"
            self.connection.execute(
                "INSERT INTO stocktake_decisions(session_id,product,decision,reason_code,note,"
                "expected_line_revision,expected_snapshot_sha256,idempotency_key,actor_id,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    session_id,
                    product,
                    "investigation_resolved",
                    reason_code,
                    note,
                    current["revision"],
                    session["snapshot_sha256"],
                    idem_key,
                    actor_id,
                    self._now(),
                ),
            )
            self.connection.execute(
                "UPDATE stocktake_product_lines SET state=?,adjustment_id=COALESCE(?,adjustment_id),"
                "resolved_by=?,resolved_at=?,reason_code=?,note=?,revision=revision+1 "
                "WHERE session_id=? AND product=?",
                (
                    next_state,
                    adjustment_id,
                    actor_id,
                    self._now(),
                    reason_code,
                    note,
                    session_id,
                    product,
                ),
            )
            self.supply._audit(
                "stocktake_session",
                session_id,
                "stocktake.investigation_resolved",
                actor_id,
                {"product": product, "decision": decision_value, "adjustment_id": adjustment_id},
            )
            response = {
                "session_id": session_id,
                "product": product,
                "state": next_state,
                "revision": current["revision"] + 1,
                "adjustment_id": adjustment_id,
            }
            self.connection.execute(
                "INSERT INTO supply_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
                "VALUES('stocktake_investigation',?,?,?,?)",
                (idem_key, digest(raw), canonical_json(response), self._now()),
            )
        return response

    # ---- 关闭与查询 ---------------------------------------------------

    def close_session(self, actor_id: str, session_id: str) -> dict[str, Any]:
        self.supply._require(actor_id, "stocktake.close")
        session = self._session_row(session_id)
        if session["state"] != "open":
            raise InvalidState("盘点会话已经关闭")
        lines = self.connection.execute(
            "SELECT state FROM stocktake_product_lines WHERE session_id=?", (session_id,)
        ).fetchall()
        unresolved = [row["state"] for row in lines if row["state"] in {"unmeasured", "pending_review", "investigating"}]
        if unresolved:
            raise InvalidState(f"仍有油品未结清：{', '.join(sorted(set(unresolved)))}")
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE stocktake_sessions SET state='closed',closed_at=?,revision=revision+1 "
                "WHERE session_id=? AND state='open'",
                (self._now(), session_id),
            )
            self.supply._audit("stocktake_session", session_id, "stocktake.closed", actor_id, {})
        return self.session(session_id)

    def session(self, session_id: str) -> dict[str, Any]:
        session = self._session_row(session_id)
        lines = self.connection.execute(
            "SELECT * FROM stocktake_product_lines WHERE session_id=? ORDER BY product",
            (session_id,),
        ).fetchall()
        measurements = self.connection.execute(
            "SELECT tank_id,product,measured_barrels,measured_at,recorded_by,recorded_at "
            "FROM stocktake_tank_measurements WHERE session_id=? ORDER BY product,measurement_id",
            (session_id,),
        ).fetchall()
        snapshot_lots = self.connection.execute(
            "SELECT lot_id,product,grade,opening_available_barrels,lot_revision "
            "FROM stocktake_snapshot_lots WHERE session_id=? ORDER BY lot_id",
            (session_id,),
        ).fetchall()
        counts = {"unmeasured": 0, "pending_review": 0, "adjusted": 0, "rejected": 0, "investigating": 0}
        line_payload = []
        for line in lines:
            counts[line["state"]] += 1
            line_payload.append(
                {
                    "product": line["product"],
                    "state": line["state"],
                    "opening_barrels": line["opening_barrels"],
                    "in_transit_barrels": line["in_transit_barrels"],
                    "measured_barrels": line["measured_barrels"],
                    "delta_barrels": line["delta_barrels"],
                    "variance_percent": line["variance_percent"],
                    "within_tolerance": bool(line["within_tolerance"]),
                    "revision": line["revision"],
                    "adjustment_id": line["adjustment_id"],
                    "reason_code": line["reason_code"],
                    "note": line["note"],
                    "resolved_by": line["resolved_by"],
                    "resolved_at": line["resolved_at"],
                }
            )
        return {
            "session_id": session["session_id"],
            "facility_id": session["facility_id"],
            "state": session["state"],
            "revision": session["revision"],
            "tolerance_percent": session["tolerance_percent"],
            "products": json.loads(session["products_json"]),
            "snapshot_sha256": session["snapshot_sha256"],
            "opened_by": session["opened_by"],
            "opened_at": session["opened_at"],
            "closed_at": session["closed_at"],
            "counts": counts,
            "lines": line_payload,
            "snapshot_lots": [dict(row) for row in snapshot_lots],
            "measurements": [dict(row) for row in measurements],
        }

    def list_sessions(self, facility_id: str | None = None, state: str | None = None) -> dict[str, Any]:
        sql = "SELECT session_id,facility_id,state,revision,opened_at,closed_at FROM stocktake_sessions"
        clauses: list[str] = []
        params: list[Any] = []
        if facility_id:
            clauses.append("facility_id=?")
            params.append(facility_id)
        if state:
            if state not in {"open", "closed"}:
                raise ValidationFailed("state 必须是 open 或 closed")
            clauses.append("state=?")
            params.append(state)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY opened_at DESC, session_id"
        rows = self.connection.execute(sql, params).fetchall()
        return {"sessions": [dict(row) for row in rows]}
