"""交接班盘点会话：快照冻结、逐罐测量、容差结清与风险复核。"""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from typing import Any, Mapping

from .errors import Conflict, Forbidden, InvalidState, NotFound
from .models import StockCountSessionRequest, TankMeasurement
from .planning import (
    ZERO,
    allocate_lot_deltas,
    canonical_json,
    decimal_text,
    digest,
    quantize_volume,
    reconcile_inventory,
)
from .storage import transaction

# API 暴露的会话状态：未测量、测量中、待复核、已调整、拆分调查、已驳回。
DISPLAY_STATES = (
    "unmeasured",
    "open",
    "pending_review",
    "adjusted",
    "split_investigation",
    "rejected",
)
REVIEW_DECISIONS = ("adjust", "split_investigation", "reject")
# 复核期间或之后不再接收测量的终态。
TERMINAL_STATES = ("adjusted", "rejected", "split_investigation")


class StockCountMixin:
    """依赖宿主类提供 connection、clock、_now、_require、_audit。"""

    connection: sqlite3.Connection

    def _lot_snapshot_rows(self, session_id: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM stock_count_lot_snapshots WHERE session_id=? ORDER BY lot_id",
            (session_id,),
        ).fetchall()

    def _measurement_rows(self, session_id: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM stock_count_measurements WHERE session_id=? ORDER BY measurement_id",
            (session_id,),
        ).fetchall()

    def _current_book_rows(self, facility_id: str, product: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT lot_id,available_barrels,revision FROM inventory_lots "
            "WHERE facility_id=? AND product=? ORDER BY received_at,lot_id",
            (facility_id, product),
        ).fetchall()

    def _balance_token(self, facility_id: str, product: str) -> str:
        rows = self._current_book_rows(facility_id, product)
        return digest([
            {"lot_id": row["lot_id"], "available_barrels": row["available_barrels"], "revision": row["revision"]}
            for row in rows
        ])

    def _measured_total(self, session_id: str) -> Decimal:
        rows = self._measurement_rows(session_id)
        return quantize_volume(sum((Decimal(row["measured_barrels"]) for row in rows), ZERO))

    def _recompute_session(self, session: sqlite3.Row) -> dict[str, Any]:
        measured = self._measured_total(session["session_id"])
        book = Decimal(session["book_barrels"])
        result = reconcile_inventory(book, measured, Decimal(session["tolerance_percent"]))
        return {
            "measured": measured,
            "book": book,
            "delta": Decimal(result["delta_barrels"]),
            "variance_percent": result["variance_percent"],
            "within_tolerance": bool(result["within_tolerance"]),
        }

    def open_stock_count(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "stock_count.open")
        request = StockCountSessionRequest.from_dict(raw)
        facility = self.connection.execute(
            "SELECT facility_id FROM facilities WHERE facility_id=?", (request.facility_id,)
        ).fetchone()
        if facility is None:
            raise NotFound("设施不存在")
        with transaction(self.connection, immediate=True):
            lot_rows = self.connection.execute(
                "SELECT lot_id,product,grade,available_barrels,revision FROM inventory_lots "
                "WHERE facility_id=? AND product=? ORDER BY received_at,lot_id",
                (request.facility_id, request.product),
            ).fetchall()
            transit_rows = self.connection.execute(
                "SELECT t.transfer_id,t.nomination_id,t.inventory_lot_id,t.expected_delivered_barrels,t.state "
                "FROM transfers t JOIN nominations n ON n.nomination_id=t.nomination_id "
                "JOIN routes r ON r.route_id=n.route_id "
                "WHERE t.state='in_transit' AND r.destination_id=? AND r.product=? "
                "ORDER BY t.transfer_id",
                (request.facility_id, request.product),
            ).fetchall()
            book_total = quantize_volume(
                sum((Decimal(row["available_barrels"]) for row in lot_rows), ZERO)
            )
            lot_snapshot = [dict(row) for row in lot_rows]
            transit_snapshot = [dict(row) for row in transit_rows]
            book_digest = digest(lot_snapshot)
            transit_digest = digest(transit_snapshot)
            try:
                self.connection.execute(
                    "INSERT INTO stock_count_sessions(session_id,facility_id,product,tolerance_percent,"
                    "book_snapshot_sha256,transit_snapshot_sha256,book_barrels,opened_by,opened_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        request.session_id,
                        request.facility_id,
                        request.product,
                        decimal_text(request.tolerance_percent),
                        book_digest,
                        transit_digest,
                        decimal_text(book_total),
                        actor_id,
                        self._now(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise Conflict("盘点会话编号已经存在") from exc
            self.connection.executemany(
                "INSERT INTO stock_count_lot_snapshots(session_id,lot_id,product,grade,"
                "available_barrels,lot_revision) VALUES(?,?,?,?,?,?)",
                [
                    (
                        request.session_id,
                        row["lot_id"],
                        row["product"],
                        row["grade"],
                        row["available_barrels"],
                        row["revision"],
                    )
                    for row in lot_rows
                ],
            )
            self.connection.executemany(
                "INSERT INTO stock_count_transit_snapshots(session_id,transfer_id,nomination_id,"
                "inventory_lot_id,expected_delivered_barrels,transfer_state) VALUES(?,?,?,?,?,?)",
                [
                    (
                        request.session_id,
                        row["transfer_id"],
                        row["nomination_id"],
                        row["inventory_lot_id"],
                        row["expected_delivered_barrels"],
                        row["state"],
                    )
                    for row in transit_rows
                ],
            )
            self._audit("stock_count", request.session_id, "stock_count.opened", actor_id, {
                "facility_id": request.facility_id,
                "product": request.product,
                "book_barrels": decimal_text(book_total),
                "lot_count": len(lot_rows),
                "in_transit_count": len(transit_rows),
                "book_snapshot_sha256": book_digest,
                "transit_snapshot_sha256": transit_digest,
            })
        return self.stock_count(request.session_id, actor_id)

    def record_measurement(self, actor_id: str, session_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "stock_count.measure")
        measurement = TankMeasurement.from_dict(raw)
        with transaction(self.connection, immediate=True):
            session = self.connection.execute(
                "SELECT * FROM stock_count_sessions WHERE session_id=?", (session_id,)
            ).fetchone()
            if session is None:
                raise NotFound("盘点会话不存在")
            if session["state"] in TERMINAL_STATES:
                raise InvalidState("盘点会话已结清，不能再登记测量")
            try:
                self.connection.execute(
                    "INSERT INTO stock_count_measurements(session_id,tank_id,measured_barrels,"
                    "observed_at,recorded_by,recorded_at) VALUES(?,?,?,?,?,?)",
                    (
                        session_id,
                        measurement.tank_id,
                        decimal_text(measurement.measured_barrels),
                        measurement.observed_at,
                        actor_id,
                        self._now(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise Conflict("该罐在本次盘点中已经测量") from exc
            summary = self._recompute_session(session)
            new_state = session["state"]
            if session["state"] == "pending_review":
                # 迟到测量：复核结论尚未签署，新读数使差异回到容差内时撤回待复核，
                # 仍超容差则保留待复核并推进版本，使原复核请求变成旧版本。
                new_state = "open" if summary["within_tolerance"] else "pending_review"
            self.connection.execute(
                "UPDATE stock_count_sessions SET measured_barrels=?,delta_barrels=?,"
                "variance_percent=?,within_tolerance=?,state=?,revision=revision+1 WHERE session_id=?",
                (
                    decimal_text(summary["measured"]),
                    decimal_text(summary["delta"]),
                    summary["variance_percent"],
                    1 if summary["within_tolerance"] else 0,
                    new_state,
                    session_id,
                ),
            )
            self._audit("stock_count", session_id, "measurement.recorded", actor_id, {
                "tank_id": measurement.tank_id,
                "measured_barrels": decimal_text(measurement.measured_barrels),
                "late": session["state"] == "pending_review",
                "state": new_state,
            })
        return self.stock_count(session_id, actor_id)

    def _persist_idempotent_response(
        self, scope: str, key: str, raw: Mapping[str, Any], response: Mapping[str, Any]
    ) -> None:
        self.connection.execute(
            "INSERT INTO supply_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
            "VALUES(?,?,?,?,?)",
            (scope, key, digest(raw), canonical_json(response), self._now()),
        )

    def _replay_idempotent(
        self, scope: str, key: str, raw: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM supply_idempotency WHERE scope=? AND idempotency_key=?",
            (scope, key),
        ).fetchone()
        if stored is None:
            return None
        if stored["request_sha256"] != digest(raw):
            raise Conflict("幂等键对应不同的请求内容")
        return json.loads(stored["response_json"])

    def settle_stock_count(
        self,
        actor_id: str,
        session_id: str,
        raw: Mapping[str, Any],
    ) -> dict[str, Any]:
        self._require(actor_id, "stock_count.settle")
        key = str(raw.get("idempotency_key", "")).strip()
        if not key:
            raise InvalidState("结清请求必须携带 idempotency_key")
        replay = self._replay_idempotent("stock_settle", key, raw)
        if replay is not None:
            return replay
        reason_code = str(raw.get("reason_code", "")).strip()
        if not reason_code or len(reason_code) > 48:
            raise InvalidState("reason_code 必填且不能超过 48 个字符")
        note = str(raw.get("note", "")).strip()
        expected_revision = raw.get("expected_revision")
        expected_token = str(raw.get("expected_balance_token", "")).strip()
        if not isinstance(expected_revision, int) or isinstance(expected_revision, bool):
            raise InvalidState("expected_revision 必须是整数")
        if not expected_token:
            raise InvalidState("expected_balance_token 不能为空")
        with transaction(self.connection, immediate=True):
            # 在写锁内复查，确保并发的重复结清请求拿到同一个响应。
            replayed = self._replay_idempotent("stock_settle", key, raw)
            if replayed is not None:
                return replayed
            session = self.connection.execute(
                "SELECT * FROM stock_count_sessions WHERE session_id=?", (session_id,)
            ).fetchone()
            if session is None:
                raise NotFound("盘点会话不存在")
            if session["revision"] != expected_revision:
                raise Conflict("盘点会话版本已变化，请刷新测量结果后重试")
            if session["state"] in TERMINAL_STATES:
                raise InvalidState("盘点会话已经结清")
            if not self._measurement_rows(session_id):
                raise InvalidState("尚未接收任何罐表测量，无法结清")
            live_token = self._balance_token(session["facility_id"], session["product"])
            if live_token != expected_token:
                raise Conflict("盘点期间批次余额因发运发生变化，请刷新余额版本后重试")
            summary = self._recompute_session(session)
            if not summary["within_tolerance"]:
                self.connection.execute(
                    "UPDATE stock_count_sessions SET measured_barrels=?,delta_barrels=?,"
                    "variance_percent=?,within_tolerance=0,state='pending_review',revision=revision+1 "
                    "WHERE session_id=?",
                    (
                        decimal_text(summary["measured"]),
                        decimal_text(summary["delta"]),
                        summary["variance_percent"],
                        session_id,
                    ),
                )
                self._audit("stock_count", session_id, "stock_count.escalated", actor_id, {
                    "delta_barrels": decimal_text(summary["delta"]),
                    "variance_percent": summary["variance_percent"],
                })
                response = self.stock_count(session_id, actor_id)
                self._persist_idempotent_response("stock_settle", key, raw, response)
                return response
            adjustment_ids = self._apply_adjustments(
                session=session,
                summary=summary,
                reason_code=reason_code,
                note=note,
                signer=actor_id,
                idempotency_key=key,
                expected_revision=expected_revision,
            )
            self._audit("stock_count", session_id, "stock_count.settled", actor_id, {
                "adjustment_ids": adjustment_ids,
                "delta_barrels": decimal_text(summary["delta"]),
                "reason_code": reason_code,
            })
            response = self.stock_count(session_id, actor_id)
            self._persist_idempotent_response("stock_settle", key, raw, response)
        return response

    def review_stock_count(
        self,
        actor_id: str,
        session_id: str,
        raw: Mapping[str, Any],
    ) -> dict[str, Any]:
        self._require(actor_id, "stock_count.review")
        key = str(raw.get("idempotency_key", "")).strip()
        if not key:
            raise InvalidState("复核请求必须携带 idempotency_key")
        replay = self._replay_idempotent("stock_review", key, raw)
        if replay is not None:
            return replay
        decision = str(raw.get("decision", "")).strip()
        if decision not in REVIEW_DECISIONS:
            raise InvalidState("decision 必须是 adjust、split_investigation 或 reject")
        reason_code = str(raw.get("reason_code", "")).strip()
        if not reason_code or len(reason_code) > 48:
            raise InvalidState("reason_code 必填且不能超过 48 个字符")
        note = str(raw.get("note", "")).strip()
        expected_revision = raw.get("expected_revision")
        if not isinstance(expected_revision, int) or isinstance(expected_revision, bool):
            raise InvalidState("expected_revision 必须是整数")
        expected_token = str(raw.get("expected_balance_token", "")).strip()
        if decision == "adjust" and not expected_token:
            raise InvalidState("调整决定必须携带 expected_balance_token")
        with transaction(self.connection, immediate=True):
            # 在写锁内复查，确保并发的重复复核请求拿到同一个响应。
            replayed = self._replay_idempotent("stock_review", key, raw)
            if replayed is not None:
                return replayed
            session = self.connection.execute(
                "SELECT * FROM stock_count_sessions WHERE session_id=?", (session_id,)
            ).fetchone()
            if session is None:
                raise NotFound("盘点会话不存在")
            if session["revision"] != expected_revision:
                raise Conflict("复核依据的测量版本已变化（存在迟到测量），请重新复核")
            if session["state"] in TERMINAL_STATES:
                raise InvalidState("盘点会话已经结清")
            if session["state"] != "pending_review":
                raise InvalidState("只有待复核会话可以复核")
            if actor_id == session["opened_by"]:
                raise Forbidden("复核必须由当班人员之外的另一名风险人员签署")
            if decision == "adjust":
                live_token = self._balance_token(session["facility_id"], session["product"])
                if live_token != expected_token:
                    raise Conflict("待复核期间批次余额因发运发生变化，请刷新余额版本后重试")
            self.connection.execute(
                "INSERT INTO stock_count_reviews(session_id,decision,reason_code,note,"
                "expected_session_revision,reviewer_id,created_at) VALUES(?,?,?,?,?,?,?)",
                (session_id, decision, reason_code, note, expected_revision, actor_id, self._now()),
            )
            if decision == "adjust":
                summary = self._recompute_session(session)
                adjustment_ids = self._apply_adjustments(
                    session=session,
                    summary=summary,
                    reason_code=reason_code,
                    note=note,
                    signer=actor_id,
                    idempotency_key=key,
                    expected_revision=expected_revision,
                )
                self._audit("stock_count", session_id, "stock_count.review_adjusted", actor_id, {
                    "adjustment_ids": adjustment_ids,
                    "reason_code": reason_code,
                })
            else:
                new_state = "split_investigation" if decision == "split_investigation" else "rejected"
                cursor = self.connection.execute(
                    f"UPDATE stock_count_sessions SET state='{new_state}',revision=revision+1,"
                    "closed_at=? WHERE session_id=? AND revision=?",
                    (self._now(), session_id, expected_revision),
                )
                if cursor.rowcount != 1:
                    raise Conflict("盘点会话版本冲突，复核未生效")
                self._audit("stock_count", session_id, f"stock_count.{new_state}", actor_id, {
                    "reason_code": reason_code,
                })
            response = self.stock_count(session_id, actor_id)
            self._persist_idempotent_response("stock_review", key, raw, response)
        return response

    def _apply_adjustments(
        self,
        *,
        session: sqlite3.Row,
        summary: Mapping[str, Any],
        reason_code: str,
        note: str,
        signer: str,
        idempotency_key: str,
        expected_revision: int,
    ) -> list[int]:
        session_id = session["session_id"]
        snapshots = self._lot_snapshot_rows(session_id)
        delta: Decimal = summary["delta"]
        # 摊派基数取当前余额而非快照：盘点期间批次可能已发运，
        # 按当前持有比例摊派盘亏才能避免把某个批次扣成负数。
        live_lots = {
            row["lot_id"]: Decimal(row["available_barrels"])
            for row in self._current_book_rows(session["facility_id"], session["product"])
        }
        basis: list[tuple[str, Decimal]] = []
        for snapshot in snapshots:
            basis.append((snapshot["lot_id"], live_lots.get(snapshot["lot_id"], ZERO)))
        live_total = sum((balance for _, balance in basis), ZERO)
        if delta < ZERO and -delta > live_total:
            raise Conflict("盘亏数量超过盘点期间发运后的当前库存，需重新测量后复核")
        if delta != ZERO and not snapshots:
            raise InvalidState("快照中没有批次可承接盘点差异，需先登记入库批次")
        shares = allocate_lot_deltas(basis, delta)
        created_at = self._now()
        previous = self.connection.execute(
            "SELECT signature_sha256 FROM stock_count_adjustments ORDER BY adjustment_id DESC LIMIT 1"
        ).fetchone()
        previous_signature = "0" * 64 if previous is None else previous["signature_sha256"]
        adjustment_ids: list[int] = []
        for index, share in enumerate(shares):
            lot_delta = Decimal(share["delta_barrels"])
            if lot_delta == ZERO:
                continue
            lot = self.connection.execute(
                "SELECT available_barrels,revision FROM inventory_lots WHERE lot_id=?",
                (share["lot_id"],),
            ).fetchone()
            if lot is None:
                raise InvalidState(f"快照批次 {share['lot_id']} 已不存在")
            before = Decimal(lot["available_barrels"])
            after = quantize_volume(before + lot_delta)
            before_text = decimal_text(quantize_volume(before))
            after_text = decimal_text(after)
            if after < ZERO:
                raise Conflict(
                    f"批次 {share['lot_id']} 在盘点期间已发运，当前余额不足以结清差异"
                )
            cursor = self.connection.execute(
                "UPDATE inventory_lots SET available_barrels=?,revision=revision+1 "
                "WHERE lot_id=? AND revision=?",
                (decimal_text(after), share["lot_id"], lot["revision"]),
            )
            if cursor.rowcount != 1:
                raise Conflict(f"批次 {share['lot_id']} 版本已变化，结清冲突")
            lot_key = f"{idempotency_key}:{index}:{share['lot_id']}"
            body = {
                "session_id": session_id,
                "lot_id": share["lot_id"],
                "balance_before_barrels": before_text,
                "delta_barrels": decimal_text(lot_delta),
                "balance_after_barrels": after_text,
                "lot_revision_before": lot["revision"],
                "lot_revision_after": lot["revision"] + 1,
                "reason_code": reason_code,
                "note": note,
                "signed_by": signer,
                "idempotency_key": lot_key,
                "created_at": created_at,
                "previous_signature": previous_signature,
            }
            signature = digest(body)
            try:
                cursor = self.connection.execute(
                    "INSERT INTO stock_count_adjustments(session_id,lot_id,lot_revision_before,"
                    "lot_revision_after,balance_before_barrels,delta_barrels,balance_after_barrels,"
                    "reason_code,note,idempotency_key,signed_by,previous_signature,signature_sha256,"
                    "created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        session_id,
                        share["lot_id"],
                        lot["revision"],
                        lot["revision"] + 1,
                        before_text,
                        decimal_text(lot_delta),
                        after_text,
                        reason_code,
                        note,
                        lot_key,
                        signer,
                        previous_signature,
                        signature,
                        created_at,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise Conflict("盘点调整幂等键冲突") from exc
            adjustment_ids.append(int(cursor.lastrowid))
            previous_signature = signature
        cursor = self.connection.execute(
            "UPDATE stock_count_sessions SET state='adjusted',revision=revision+1,closed_at=? "
            "WHERE session_id=? AND revision=?",
            (created_at, session_id, expected_revision),
        )
        if cursor.rowcount != 1:
            raise Conflict("盘点会话版本冲突，结清未生效")
        return adjustment_ids

    def stock_count(self, session_id: str, actor_id: str | None = None) -> dict[str, Any]:
        if actor_id is not None:
            self._require(actor_id, "stock_count.read")
        session = self.connection.execute(
            "SELECT * FROM stock_count_sessions WHERE session_id=?", (session_id,)
        ).fetchone()
        if session is None:
            raise NotFound("盘点会话不存在")
        measurements = self._measurement_rows(session_id)
        snapshots = self._lot_snapshot_rows(session_id)
        transit = self.connection.execute(
            "SELECT * FROM stock_count_transit_snapshots WHERE session_id=? ORDER BY transfer_id",
            (session_id,),
        ).fetchall()
        reviews = self.connection.execute(
            "SELECT * FROM stock_count_reviews WHERE session_id=? ORDER BY review_id",
            (session_id,),
        ).fetchall()
        adjustments = self.connection.execute(
            "SELECT * FROM stock_count_adjustments WHERE session_id=? ORDER BY adjustment_id",
            (session_id,),
        ).fetchall()
        live_rows = self._current_book_rows(session["facility_id"], session["product"])
        live_total = quantize_volume(
            sum((Decimal(row["available_barrels"]) for row in live_rows), ZERO)
        )
        measured = quantize_volume(
            sum((Decimal(row["measured_barrels"]) for row in measurements), ZERO)
        )
        book = Decimal(session["book_barrels"])
        reconciliation = reconcile_inventory(book, measured, Decimal(session["tolerance_percent"]))
        if session["state"] == "open" and not measurements:
            display_status = "unmeasured"
        else:
            display_status = session["state"]
        chain_head = self.connection.execute(
            "SELECT signature_sha256 FROM stock_count_adjustments WHERE session_id=? "
            "ORDER BY adjustment_id DESC LIMIT 1",
            (session_id,),
        ).fetchone()
        return {
            "session_id": session_id,
            "facility_id": session["facility_id"],
            "product": session["product"],
            "state": session["state"],
            "status": display_status,
            "revision": session["revision"],
            "tolerance_percent": session["tolerance_percent"],
            "opened_by": session["opened_by"],
            "opened_at": session["opened_at"],
            "closed_at": session["closed_at"],
            "snapshots": {
                "book_barrels": decimal_text(book),
                "book_snapshot_sha256": session["book_snapshot_sha256"],
                "transit_snapshot_sha256": session["transit_snapshot_sha256"],
                "lots": [dict(row) for row in snapshots],
                "in_transit": [dict(row) for row in transit],
            },
            "measurement": {
                "measured_barrels": decimal_text(measured),
                "tank_count": len(measurements),
                "tanks": [dict(row) for row in measurements],
            },
            "reconciliation": reconciliation,
            "live_inventory": {
                "available_barrels": decimal_text(live_total),
                "balance_token": self._balance_token(session["facility_id"], session["product"]),
                "moved_since_snapshot": decimal_text(live_total) != session["book_barrels"],
            },
            "reviews": [dict(row) for row in reviews],
            "adjustments": {
                "count": len(adjustments),
                "chain_head": None if chain_head is None else chain_head["signature_sha256"],
                "rows": [dict(row) for row in adjustments],
            },
        }

    def verify_adjustment_chain(self, actor_id: str, session_id: str) -> dict[str, Any]:
        self._require(actor_id, "stock_count.read")
        rows = self.connection.execute(
            "SELECT * FROM stock_count_adjustments WHERE session_id=? ORDER BY adjustment_id",
            (session_id,),
        ).fetchall()
        # 全局链：本会话首条记录的前驱必须指向全局顺序中紧邻它的上一条调整。
        first_id = rows[0]["adjustment_id"] if rows else None
        global_previous = None
        if first_id is not None:
            global_previous = self.connection.execute(
                "SELECT signature_sha256 FROM stock_count_adjustments "
                "WHERE adjustment_id<? ORDER BY adjustment_id DESC LIMIT 1",
                (first_id,),
            ).fetchone()
        expected_previous = "0" * 64 if global_previous is None else global_previous["signature_sha256"]
        valid = True
        for row in rows:
            body = {
                "session_id": row["session_id"],
                "lot_id": row["lot_id"],
                "balance_before_barrels": row["balance_before_barrels"],
                "delta_barrels": row["delta_barrels"],
                "balance_after_barrels": row["balance_after_barrels"],
                "lot_revision_before": row["lot_revision_before"],
                "lot_revision_after": row["lot_revision_after"],
                "reason_code": row["reason_code"],
                "note": row["note"],
                "signed_by": row["signed_by"],
                "idempotency_key": row["idempotency_key"],
                "created_at": row["created_at"],
                "previous_signature": row["previous_signature"],
            }
            calculated = digest(body)
            if row["previous_signature"] != expected_previous or row["signature_sha256"] != calculated:
                valid = False
                break
            expected_previous = row["signature_sha256"]
        return {
            "session_id": session_id,
            "valid": valid,
            "adjustments": len(rows),
            "chain_head": expected_previous if rows and valid else None,
        }

    def list_stock_counts(        self,
        actor_id: str,
        *,
        facility_id: str | None = None,
        product: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        self._require(actor_id, "stock_count.read")
        clauses: list[str] = []
        params: list[Any] = []
        if facility_id:
            clauses.append("facility_id=?")
            params.append(facility_id)
        if product:
            clauses.append("product=?")
            params.append(product)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self.connection.execute(
            f"SELECT * FROM stock_count_sessions{where} ORDER BY opened_at,session_id",
            params,
        ).fetchall()
        sessions: list[dict[str, Any]] = []
        for row in rows:
            measured = self.connection.execute(
                "SELECT COUNT(1) count,COALESCE(SUM(CAST(measured_barrels AS REAL)),0) total "
                "FROM stock_count_measurements WHERE session_id=?",
                (row["session_id"],),
            ).fetchone()
            display = "unmeasured" if row["state"] == "open" and measured["count"] == 0 else row["state"]
            if status is not None and display != status:
                continue
            sessions.append({
                "session_id": row["session_id"],
                "facility_id": row["facility_id"],
                "product": row["product"],
                "state": row["state"],
                "status": display,
                "revision": row["revision"],
                "book_barrels": row["book_barrels"],
                "measured_barrels": None if row["measured_barrels"] is None else row["measured_barrels"],
                "delta_barrels": None if row["delta_barrels"] is None else row["delta_barrels"],
                "within_tolerance": None if row["within_tolerance"] is None else bool(row["within_tolerance"]),
                "opened_at": row["opened_at"],
                "closed_at": row["closed_at"],
            })
        return {"sessions": sessions, "count": len(sessions)}
