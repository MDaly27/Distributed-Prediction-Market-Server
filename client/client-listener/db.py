import asyncio
import random
import uuid
from collections.abc import Sequence
from typing import Any

import asyncpg

from models import CancelOrderRequest, SubmitOrderRequest


class SubmissionError(Exception):
    pass


class RetryableOCCError(Exception):
    pass


OPEN_ORDER_STATUSES = ("ACCEPTED", "OPEN", "PARTIALLY_FILLED")


def _is_occ_error(exc: Exception) -> bool:
    if not isinstance(exc, asyncpg.PostgresError):
        return False
    return exc.sqlstate in {"40001", "40P01"}


def _require_uuid(value: Any, name: str) -> str:
    try:
        out = str(value)
        uuid.UUID(out)
        return out
    except Exception as exc:
        raise SubmissionError(f"{name} must be a valid UUID") from exc


def _safe_limit(value: Any, default: int, max_limit: int = 500) -> int:
    try:
        n = int(value) if value is not None else default
    except Exception as exc:
        raise SubmissionError("limit must be an integer") from exc
    if n < 1 or n > max_limit:
        raise SubmissionError(f"limit must be between 1 and {max_limit}")
    return n


class OrderRepository:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def _with_occ_retry(self, fn):
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            try:
                return await fn()
            except RetryableOCCError:
                if attempt == max_attempts:
                    raise SubmissionError("database conflict after retries")
                delay = (0.02 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.02)
                await asyncio.sleep(delay)

    async def ready(self) -> bool:
        async with self.pool.acquire() as conn:
            val = await conn.fetchval("SELECT 1")
            return int(val) == 1

    async def submit_order(self, req: SubmitOrderRequest) -> dict[str, Any]:
        return await self._with_occ_retry(lambda: self._submit_order_once(req))

    async def cancel_order(self, req: CancelOrderRequest) -> dict[str, Any]:
        return await self._with_occ_retry(lambda: self._cancel_order_once(req))

    async def get_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        order_id = _require_uuid(payload.get("order_id"), "order_id")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT request_id, global_seq, account_id, market_id, side, qty,
                       remaining_qty, price_cents, time_in_force, status,
                       reject_reason, created_at, updated_at
                FROM orders
                WHERE request_id = $1
                """,
                order_id,
            )
            if not row:
                raise SubmissionError("order not found")

            cancel_row = await conn.fetchrow(
                """
                SELECT cancel_id, reason, created_at
                FROM order_cancels
                WHERE order_id = $1
                ORDER BY cancel_seq DESC
                LIMIT 1
                """,
                order_id,
            )
            result = {
                "request_id": str(row["request_id"]),
                "global_seq": int(row["global_seq"]),
                "account_id": str(row["account_id"]),
                "market_id": str(row["market_id"]),
                "side": str(row["side"]),
                "qty": int(row["qty"]),
                "remaining_qty": int(row["remaining_qty"]),
                "price_cents": int(row["price_cents"]),
                "time_in_force": str(row["time_in_force"]),
                "status": str(row["status"]),
                "reject_reason": row["reject_reason"],
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat(),
            }
            if cancel_row:
                result["cancel"] = {
                    "cancel_id": str(cancel_row["cancel_id"]),
                    "reason": cancel_row["reason"],
                    "created_at": cancel_row["created_at"].isoformat(),
                }
            return result

    async def list_orders(self, payload: dict[str, Any]) -> dict[str, Any]:
        limit = _safe_limit(payload.get("limit"), 100)
        status_filter = payload.get("statuses")

        clauses = []
        args: list[Any] = []
        idx = 1

        if payload.get("account_id") is not None:
            clauses.append(f"account_id = ${idx}")
            args.append(_require_uuid(payload.get("account_id"), "account_id"))
            idx += 1
        if payload.get("market_id") is not None:
            clauses.append(f"market_id = ${idx}")
            args.append(_require_uuid(payload.get("market_id"), "market_id"))
            idx += 1
        if status_filter is not None:
            if not isinstance(status_filter, Sequence) or isinstance(status_filter, (str, bytes)):
                raise SubmissionError("statuses must be an array of strings")
            statuses = [str(s).upper() for s in status_filter]
            clauses.append(f"status = ANY(${idx}::text[])")
            args.append(statuses)
            idx += 1

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        args.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT request_id, account_id, market_id, side, qty, remaining_qty,
                       price_cents, status, global_seq, created_at, updated_at
                FROM orders
                {where_sql}
                ORDER BY global_seq DESC
                LIMIT ${idx}
                """,
                *args,
            )
        return {
            "orders": [
                {
                    "request_id": str(r["request_id"]),
                    "account_id": str(r["account_id"]),
                    "market_id": str(r["market_id"]),
                    "side": str(r["side"]),
                    "qty": int(r["qty"]),
                    "remaining_qty": int(r["remaining_qty"]),
                    "price_cents": int(r["price_cents"]),
                    "status": str(r["status"]),
                    "global_seq": int(r["global_seq"]),
                    "created_at": r["created_at"].isoformat(),
                    "updated_at": r["updated_at"].isoformat(),
                }
                for r in rows
            ]
        }

    async def list_open_orders(self, payload: dict[str, Any]) -> dict[str, Any]:
        account_id = _require_uuid(payload.get("account_id"), "account_id")
        market_id = payload.get("market_id")
        limit = _safe_limit(payload.get("limit"), 100)

        args: list[Any] = [account_id]
        clause = ""
        if market_id is not None:
            clause = "AND market_id = $2"
            args.append(_require_uuid(market_id, "market_id"))
            limit_idx = 3
        else:
            limit_idx = 2
        args.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT request_id, market_id, side, qty, remaining_qty, price_cents, status, global_seq
                FROM orders
                WHERE account_id = $1
                  {clause}
                  AND status IN ('ACCEPTED','OPEN','PARTIALLY_FILLED')
                  AND remaining_qty > 0
                ORDER BY global_seq DESC
                LIMIT ${limit_idx}
                """,
                *args,
            )
        return {
            "orders": [
                {
                    "request_id": str(r["request_id"]),
                    "market_id": str(r["market_id"]),
                    "side": str(r["side"]),
                    "qty": int(r["qty"]),
                    "remaining_qty": int(r["remaining_qty"]),
                    "price_cents": int(r["price_cents"]),
                    "status": str(r["status"]),
                    "global_seq": int(r["global_seq"]),
                }
                for r in rows
            ]
        }

    async def cancel_all_orders(self, payload: dict[str, Any]) -> dict[str, Any]:
        account_id = _require_uuid(payload.get("account_id"), "account_id")
        market_id = payload.get("market_id")
        if market_id is not None:
            market_id = _require_uuid(market_id, "market_id")
        reason = payload.get("reason")
        if reason is not None:
            reason = str(reason)

        async def _op():
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    args: list[Any] = [account_id]
                    clause = ""
                    if market_id is not None:
                        clause = "AND market_id = $2"
                        args.append(market_id)
                    rows = await conn.fetch(
                        f"""
                        SELECT request_id
                        FROM orders
                        WHERE account_id = $1
                          {clause}
                          AND status IN ('ACCEPTED','OPEN','PARTIALLY_FILLED')
                          AND remaining_qty > 0
                        ORDER BY global_seq
                        """,
                        *args,
                    )
                    cancelled = []
                    for row in rows:
                        result = await self._cancel_order_tx(
                            conn,
                            cancel_id=str(uuid.uuid4()),
                            order_id=str(row["request_id"]),
                            account_id=account_id,
                            reason=reason or "cancel_all_orders",
                        )
                        cancelled.append(result)
                    return {
                        "cancelled_count": len(cancelled),
                        "orders": cancelled,
                    }

        return await self._with_occ_retry(_op)

    async def replace_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        cancel_id = _require_uuid(payload.get("cancel_id"), "cancel_id")
        old_order_id = _require_uuid(payload.get("old_order_id"), "old_order_id")
        account_id = _require_uuid(payload.get("account_id"), "account_id")

        request_payload = payload.get("new_order")
        if not isinstance(request_payload, dict):
            raise SubmissionError("new_order payload must be an object")
        req = SubmitOrderRequest.from_dict(request_payload)

        if req.account_id != account_id:
            raise SubmissionError("new_order account_id must match account_id")

        async def _op():
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    old_order = await conn.fetchrow(
                        """
                        SELECT market_id
                        FROM orders
                        WHERE request_id = $1
                        """,
                        old_order_id,
                    )
                    if not old_order:
                        raise SubmissionError("old_order_id not found")
                    if str(old_order["market_id"]) != req.market_id:
                        raise SubmissionError("new_order market_id must match old order market")

                    cancel_result = await self._cancel_order_tx(
                        conn,
                        cancel_id=cancel_id,
                        order_id=old_order_id,
                        account_id=account_id,
                        reason="replace_order",
                    )
                    submit_result = await self._submit_order_tx(conn, req)
                    return {
                        "cancel": cancel_result,
                        "new_order": submit_result,
                    }

        return await self._with_occ_retry(_op)

    async def get_trades(self, payload: dict[str, Any]) -> dict[str, Any]:
        limit = _safe_limit(payload.get("limit"), 100)
        market_id = payload.get("market_id")
        account_id = payload.get("account_id")
        order_id = payload.get("order_id")

        clauses = []
        args: list[Any] = []
        idx = 1

        if market_id is not None:
            clauses.append(f"t.market_id = ${idx}")
            args.append(_require_uuid(market_id, "market_id"))
            idx += 1
        if order_id is not None:
            oid = _require_uuid(order_id, "order_id")
            clauses.append(f"(t.resting_order_id = ${idx} OR t.aggressing_order_id = ${idx})")
            args.append(oid)
            idx += 1
        if account_id is not None:
            clauses.append(
                f"EXISTS (SELECT 1 FROM trade_parties tp WHERE tp.trade_id = t.trade_id AND tp.account_id = ${idx})"
            )
            args.append(_require_uuid(account_id, "account_id"))
            idx += 1

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        args.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT t.trade_id, t.market_id, t.resting_order_id, t.aggressing_order_id,
                       t.qty, t.yes_price_cents, t.match_seq, t.created_at
                FROM trades t
                {where_sql}
                ORDER BY t.match_seq DESC
                LIMIT ${idx}
                """,
                *args,
            )

        return {
            "trades": [
                {
                    "trade_id": str(r["trade_id"]),
                    "market_id": str(r["market_id"]),
                    "resting_order_id": str(r["resting_order_id"]),
                    "aggressing_order_id": str(r["aggressing_order_id"]),
                    "qty": int(r["qty"]),
                    "yes_price_cents": int(r["yes_price_cents"]),
                    "match_seq": int(r["match_seq"]),
                    "created_at": r["created_at"].isoformat(),
                }
                for r in rows
            ]
        }

    async def get_positions(self, payload: dict[str, Any]) -> dict[str, Any]:
        account_id = _require_uuid(payload.get("account_id"), "account_id")
        market_id = payload.get("market_id")

        args: list[Any] = [account_id]
        clause = ""
        if market_id is not None:
            clause = "AND market_id = $2"
            args.append(_require_uuid(market_id, "market_id"))

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT market_id, yes_shares, no_shares, locked_yes_shares, locked_no_shares, updated_at
                FROM positions
                WHERE account_id = $1
                  {clause}
                ORDER BY market_id
                """,
                *args,
            )

        return {
            "positions": [
                {
                    "market_id": str(r["market_id"]),
                    "yes_shares": int(r["yes_shares"]),
                    "no_shares": int(r["no_shares"]),
                    "locked_yes_shares": int(r["locked_yes_shares"]),
                    "locked_no_shares": int(r["locked_no_shares"]),
                    "updated_at": r["updated_at"].isoformat(),
                }
                for r in rows
            ]
        }

    async def get_account_balances(self, payload: dict[str, Any]) -> dict[str, Any]:
        account_id = _require_uuid(payload.get("account_id"), "account_id")
        async with self.pool.acquire() as conn:
            account = await conn.fetchrow(
                """
                SELECT account_id, status, available_cash_cents, locked_cash_cents, updated_at
                FROM accounts
                WHERE account_id = $1
                """,
                account_id,
            )
            if not account:
                raise SubmissionError("account not found")

            dep_sum = await conn.fetchval(
                """
                SELECT COALESCE(SUM(amount_cents), 0)
                FROM cash_transactions
                WHERE account_id = $1 AND type = 'DEPOSIT' AND status = 'COMPLETED'
                """,
                account_id,
            )
            wd_sum = await conn.fetchval(
                """
                SELECT COALESCE(SUM(amount_cents), 0)
                FROM cash_transactions
                WHERE account_id = $1 AND type = 'WITHDRAWAL' AND status = 'COMPLETED'
                """,
                account_id,
            )

        return {
            "account_id": str(account["account_id"]),
            "status": str(account["status"]),
            "available_cash_cents": int(account["available_cash_cents"]),
            "locked_cash_cents": int(account["locked_cash_cents"]),
            "total_deposits_cents": int(dep_sum),
            "total_withdrawals_cents": int(wd_sum),
            "updated_at": account["updated_at"].isoformat(),
        }

    async def deposit_cash(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._with_occ_retry(lambda: self._cash_tx(payload, tx_type="DEPOSIT"))

    async def withdraw_cash(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._with_occ_retry(lambda: self._cash_tx(payload, tx_type="WITHDRAWAL"))

    async def _cash_tx(self, payload: dict[str, Any], tx_type: str) -> dict[str, Any]:
        cash_txn_id = _require_uuid(payload.get("cash_txn_id"), "cash_txn_id")
        account_id = _require_uuid(payload.get("account_id"), "account_id")
        try:
            amount = int(payload.get("amount_cents"))
        except Exception as exc:
            raise SubmissionError("amount_cents must be an integer") from exc
        if amount <= 0:
            raise SubmissionError("amount_cents must be > 0")
        external_ref = payload.get("external_ref")
        notes = payload.get("notes")

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                existing = await conn.fetchrow(
                    """
                    SELECT cash_txn_id, type, amount_cents, status
                    FROM cash_transactions
                    WHERE cash_txn_id = $1
                    """,
                    cash_txn_id,
                )
                if existing:
                    if str(existing["type"]) != tx_type or int(existing["amount_cents"]) != amount:
                        raise SubmissionError("cash_txn_id already used with different payload")
                    return {
                        "idempotent": True,
                        "cash_txn_id": str(existing["cash_txn_id"]),
                        "type": str(existing["type"]),
                        "amount_cents": int(existing["amount_cents"]),
                        "status": str(existing["status"]),
                    }

                if tx_type == "DEPOSIT":
                    account_update = await conn.execute(
                        """
                        UPDATE accounts
                        SET available_cash_cents = available_cash_cents + $2,
                            updated_at = now()
                        WHERE account_id = $1
                          AND status = 'ACTIVE'
                        """,
                        account_id,
                        amount,
                    )
                    cash_delta = amount
                else:
                    account_update = await conn.execute(
                        """
                        UPDATE accounts
                        SET available_cash_cents = available_cash_cents - $2,
                            updated_at = now()
                        WHERE account_id = $1
                          AND status = 'ACTIVE'
                          AND available_cash_cents >= $2
                        """,
                        account_id,
                        amount,
                    )
                    cash_delta = -amount

                if account_update != "UPDATE 1":
                    raise SubmissionError("account update failed (inactive or insufficient funds)")

                await conn.execute(
                    """
                    INSERT INTO cash_transactions (
                        cash_txn_id, account_id, type, amount_cents, status,
                        external_ref, notes, completed_at
                    )
                    VALUES ($1, $2, $3, $4, 'COMPLETED', $5, $6, now())
                    """,
                    cash_txn_id,
                    account_id,
                    tx_type,
                    amount,
                    external_ref,
                    notes,
                )

                await conn.execute(
                    """
                    INSERT INTO ledger_entries (
                        entry_id, account_id, cash_txn_id,
                        cash_delta_cents, locked_cash_delta_cents,
                        yes_share_delta, no_share_delta, locked_yes_delta, locked_no_delta,
                        reason, notes
                    )
                    VALUES ($1, $2, $3, $4, 0, 0, 0, 0, 0, $5, $6)
                    """,
                    str(uuid.uuid4()),
                    account_id,
                    cash_txn_id,
                    cash_delta,
                    tx_type,
                    notes or f"{tx_type} via rpc",
                )

                return {
                    "idempotent": False,
                    "cash_txn_id": cash_txn_id,
                    "type": tx_type,
                    "amount_cents": amount,
                    "status": "COMPLETED",
                }

    async def create_market(self, payload: dict[str, Any]) -> dict[str, Any]:
        market_id = _require_uuid(payload.get("market_id"), "market_id")
        slug = str(payload.get("slug", "")).strip()
        title = str(payload.get("title", "")).strip()
        if not slug:
            raise SubmissionError("slug is required")
        if not title:
            raise SubmissionError("title is required")

        status = str(payload.get("status", "DRAFT")).upper()
        if status not in {"DRAFT", "ACTIVE", "HALTED", "CLOSED", "RESOLVED", "CANCELLED"}:
            raise SubmissionError("invalid market status")

        tick_size = int(payload.get("tick_size_cents", 1))
        min_price = int(payload.get("min_price_cents", 1))
        max_price = int(payload.get("max_price_cents", 99))
        created_by = payload.get("created_by")
        if created_by is not None:
            created_by = _require_uuid(created_by, "created_by")

        if tick_size <= 0:
            raise SubmissionError("tick_size_cents must be > 0")
        if min_price < 0 or max_price > 100 or min_price > max_price:
            raise SubmissionError("invalid price bounds")

        description = payload.get("description")
        close_time = payload.get("close_time")
        resolve_time = payload.get("resolve_time")

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                existing = await conn.fetchrow(
                    "SELECT market_id, slug, status FROM markets WHERE market_id = $1",
                    market_id,
                )
                if existing:
                    return {
                        "idempotent": True,
                        "market_id": str(existing["market_id"]),
                        "slug": str(existing["slug"]),
                        "status": str(existing["status"]),
                    }

                row = await conn.fetchrow(
                    """
                    INSERT INTO markets (
                        market_id, slug, title, description, status,
                        tick_size_cents, min_price_cents, max_price_cents,
                        close_time, resolve_time, created_by
                    )
                    VALUES (
                        $1, $2, $3, $4, $5,
                        $6, $7, $8,
                        $9::timestamptz, $10::timestamptz, $11
                    )
                    RETURNING market_id, slug, status
                    """,
                    market_id,
                    slug,
                    title,
                    description,
                    status,
                    tick_size,
                    min_price,
                    max_price,
                    close_time,
                    resolve_time,
                    created_by,
                )
                return {
                    "idempotent": False,
                    "market_id": str(row["market_id"]),
                    "slug": str(row["slug"]),
                    "status": str(row["status"]),
                }

    async def update_market_status(self, payload: dict[str, Any]) -> dict[str, Any]:
        market_id = _require_uuid(payload.get("market_id"), "market_id")
        new_status = str(payload.get("new_status", "")).upper()
        if new_status not in {"DRAFT", "ACTIVE", "HALTED", "CLOSED", "RESOLVED", "CANCELLED"}:
            raise SubmissionError("invalid new_status")

        allowed = {
            "DRAFT": {"ACTIVE", "CANCELLED"},
            "ACTIVE": {"HALTED", "CLOSED", "RESOLVED", "CANCELLED"},
            "HALTED": {"ACTIVE", "CLOSED", "CANCELLED"},
            "CLOSED": {"RESOLVED", "CANCELLED"},
            "RESOLVED": set(),
            "CANCELLED": set(),
        }

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                market = await conn.fetchrow(
                    "SELECT market_id, status FROM markets WHERE market_id = $1",
                    market_id,
                )
                if not market:
                    raise SubmissionError("market not found")
                current = str(market["status"])

                if current == new_status:
                    return {
                        "idempotent": True,
                        "market_id": market_id,
                        "old_status": current,
                        "new_status": new_status,
                    }

                if new_status not in allowed.get(current, set()):
                    raise SubmissionError(f"invalid transition: {current} -> {new_status}")

                updated = await conn.execute(
                    "UPDATE markets SET status = $2 WHERE market_id = $1",
                    market_id,
                    new_status,
                )
                if updated != "UPDATE 1":
                    raise RetryableOCCError("market changed concurrently")

                return {
                    "idempotent": False,
                    "market_id": market_id,
                    "old_status": current,
                    "new_status": new_status,
                }

    async def resolve_market(self, payload: dict[str, Any]) -> dict[str, Any]:
        resolution_id = _require_uuid(payload.get("resolution_id"), "resolution_id")
        market_id = _require_uuid(payload.get("market_id"), "market_id")
        outcome = str(payload.get("outcome", "")).upper()
        if outcome not in {"YES", "NO", "CANCELLED"}:
            raise SubmissionError("outcome must be YES, NO, or CANCELLED")
        resolved_by = payload.get("resolved_by")
        if resolved_by is not None:
            resolved_by = _require_uuid(resolved_by, "resolved_by")
        notes = payload.get("notes")

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                market = await conn.fetchrow(
                    "SELECT market_id, status FROM markets WHERE market_id = $1",
                    market_id,
                )
                if not market:
                    raise SubmissionError("market not found")

                existing = await conn.fetchrow(
                    "SELECT resolution_id, outcome FROM market_resolutions WHERE market_id = $1",
                    market_id,
                )
                if existing:
                    if str(existing["outcome"]) != outcome:
                        raise SubmissionError("market already resolved with a different outcome")
                    return {
                        "idempotent": True,
                        "resolution_id": str(existing["resolution_id"]),
                        "market_id": market_id,
                        "outcome": str(existing["outcome"]),
                    }

                await conn.execute(
                    """
                    INSERT INTO market_resolutions (resolution_id, market_id, outcome, resolved_by, notes)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    resolution_id,
                    market_id,
                    outcome,
                    resolved_by,
                    notes,
                )
                market_status = "CANCELLED" if outcome == "CANCELLED" else "RESOLVED"
                await conn.execute(
                    "UPDATE markets SET status = $2, resolve_time = COALESCE(resolve_time, now()) WHERE market_id = $1",
                    market_id,
                    market_status,
                )
                return {
                    "idempotent": False,
                    "resolution_id": resolution_id,
                    "market_id": market_id,
                    "outcome": outcome,
                    "market_status": market_status,
                }

    async def get_order_book(self, payload: dict[str, Any]) -> dict[str, Any]:
        market_id = _require_uuid(payload.get("market_id"), "market_id")
        depth = _safe_limit(payload.get("depth"), 10, max_limit=100)

        async with self.pool.acquire() as conn:
            yes_rows = await conn.fetch(
                """
                SELECT price_cents, SUM(remaining_qty) AS qty
                FROM orders
                WHERE market_id = $1
                  AND side = 'YES'
                  AND status IN ('ACCEPTED','OPEN','PARTIALLY_FILLED')
                  AND remaining_qty > 0
                GROUP BY price_cents
                ORDER BY price_cents DESC
                LIMIT $2
                """,
                market_id,
                depth,
            )
            no_rows = await conn.fetch(
                """
                SELECT price_cents, SUM(remaining_qty) AS qty
                FROM orders
                WHERE market_id = $1
                  AND side = 'NO'
                  AND status IN ('ACCEPTED','OPEN','PARTIALLY_FILLED')
                  AND remaining_qty > 0
                GROUP BY price_cents
                ORDER BY price_cents DESC
                LIMIT $2
                """,
                market_id,
                depth,
            )

        yes_levels = [{"price_cents": int(r["price_cents"]), "qty": int(r["qty"])} for r in yes_rows]
        no_levels = [{"price_cents": int(r["price_cents"]), "qty": int(r["qty"])} for r in no_rows]

        return {
            "market_id": market_id,
            "yes": yes_levels,
            "no": no_levels,
            "best_yes_bid": yes_levels[0]["price_cents"] if yes_levels else None,
            "best_no_bid": no_levels[0]["price_cents"] if no_levels else None,
        }

    async def _submit_order_once(self, req: SubmitOrderRequest) -> dict[str, Any]:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                return await self._submit_order_tx(conn, req)

    async def _submit_order_tx(self, conn: asyncpg.Connection, req: SubmitOrderRequest) -> dict[str, Any]:
        needed_cash = req.qty * req.price_cents
        try:
            existing = await conn.fetchrow(
                """
                SELECT request_id, global_seq, status, remaining_qty
                FROM orders
                WHERE request_id = $1
                """,
                req.request_id,
            )
            if existing:
                return {
                    "idempotent": True,
                    "request_id": str(existing["request_id"]),
                    "global_seq": int(existing["global_seq"]),
                    "status": str(existing["status"]),
                    "remaining_qty": int(existing["remaining_qty"]),
                }

            market = await conn.fetchrow(
                """
                SELECT market_id, status, min_price_cents, max_price_cents
                FROM markets
                WHERE market_id = $1
                """,
                req.market_id,
            )
            if not market:
                raise SubmissionError("market not found")
            if market["status"] != "ACTIVE":
                raise SubmissionError(f"market status is {market['status']}, not ACTIVE")
            if req.price_cents < market["min_price_cents"] or req.price_cents > market["max_price_cents"]:
                raise SubmissionError("price outside market bounds")

            account_update = await conn.execute(
                """
                UPDATE accounts
                SET
                    available_cash_cents = available_cash_cents - $2,
                    locked_cash_cents = locked_cash_cents + $2,
                    updated_at = now()
                WHERE account_id = $1
                  AND status = 'ACTIVE'
                  AND available_cash_cents >= $2
                """,
                req.account_id,
                needed_cash,
            )
            if account_update != "UPDATE 1":
                raise SubmissionError("insufficient funds or inactive account")

            order_row = await conn.fetchrow(
                """
                INSERT INTO orders (
                    request_id, account_id, market_id, side, order_type, time_in_force,
                    qty, remaining_qty, price_cents, ingress_ts_ns, status
                )
                VALUES ($1, $2, $3, $4, 'LIMIT', $5, $6, $6, $7, $8, 'ACCEPTED')
                RETURNING request_id, global_seq, status, remaining_qty
                """,
                req.request_id,
                req.account_id,
                req.market_id,
                req.side,
                req.time_in_force,
                req.qty,
                req.price_cents,
                req.ingress_ts_ns,
            )

            await conn.execute(
                """
                INSERT INTO ledger_entries (
                    entry_id, account_id, market_id, order_id,
                    cash_delta_cents, locked_cash_delta_cents,
                    yes_share_delta, no_share_delta, locked_yes_delta, locked_no_delta,
                    reason, notes
                )
                VALUES ($1, $2, $3, $4, 0, $5, 0, 0, 0, 0, 'ORDER_LOCK', $6)
                """,
                str(uuid.uuid4()),
                req.account_id,
                req.market_id,
                req.request_id,
                needed_cash,
                f"TCP order lock for {req.request_id}",
            )

            return {
                "idempotent": False,
                "request_id": str(order_row["request_id"]),
                "global_seq": int(order_row["global_seq"]),
                "status": str(order_row["status"]),
                "remaining_qty": int(order_row["remaining_qty"]),
            }
        except asyncpg.PostgresError as exc:
            if _is_occ_error(exc):
                raise RetryableOCCError from exc
            raise SubmissionError(f"database error: {exc}") from exc

    async def _cancel_order_once(self, req: CancelOrderRequest) -> dict[str, Any]:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                return await self._cancel_order_tx(
                    conn,
                    cancel_id=req.cancel_id,
                    order_id=req.order_id,
                    account_id=req.account_id,
                    reason=req.reason,
                )

    async def _cancel_order_tx(
        self,
        conn: asyncpg.Connection,
        *,
        cancel_id: str,
        order_id: str,
        account_id: str,
        reason: str | None,
    ) -> dict[str, Any]:
        try:
            existing_cancel = await conn.fetchrow(
                """
                SELECT cancel_id, order_id
                FROM order_cancels
                WHERE cancel_id = $1
                """,
                cancel_id,
            )
            if existing_cancel:
                if str(existing_cancel["order_id"]) != order_id:
                    raise SubmissionError("cancel_id already used for a different order")
                existing_order = await conn.fetchrow(
                    """
                    SELECT request_id, status, remaining_qty
                    FROM orders
                    WHERE request_id = $1
                    """,
                    existing_cancel["order_id"],
                )
                if not existing_order:
                    raise SubmissionError("idempotent cancel references missing order")
                return {
                    "idempotent": True,
                    "cancel_id": str(existing_cancel["cancel_id"]),
                    "order_id": str(existing_order["request_id"]),
                    "status": str(existing_order["status"]),
                    "remaining_qty": int(existing_order["remaining_qty"]),
                    "unlocked_cash_cents": 0,
                }

            order_row = await conn.fetchrow(
                """
                SELECT request_id, account_id, market_id, price_cents, remaining_qty, status
                FROM orders
                WHERE request_id = $1
                """,
                order_id,
            )
            if not order_row:
                raise SubmissionError("order not found")
            if str(order_row["account_id"]) != account_id:
                raise SubmissionError("order does not belong to account")
            if order_row["status"] not in OPEN_ORDER_STATUSES:
                raise SubmissionError(f"order status is {order_row[status]}, not cancellable")
            if int(order_row["remaining_qty"]) <= 0:
                raise SubmissionError("order has no remaining quantity to cancel")

            unlock_cash = int(order_row["remaining_qty"]) * int(order_row["price_cents"])

            order_update = await conn.execute(
                """
                UPDATE orders
                SET
                    remaining_qty = 0,
                    status = 'CANCELLED',
                    updated_at = now()
                WHERE request_id = $1
                  AND account_id = $2
                  AND status IN ('ACCEPTED','OPEN','PARTIALLY_FILLED')
                  AND remaining_qty > 0
                """,
                order_id,
                account_id,
            )
            if order_update != "UPDATE 1":
                raise RetryableOCCError("order changed concurrently during cancel")

            account_update = await conn.execute(
                """
                UPDATE accounts
                SET
                    available_cash_cents = available_cash_cents + $2,
                    locked_cash_cents = locked_cash_cents - $2,
                    updated_at = now()
                WHERE account_id = $1
                  AND status = 'ACTIVE'
                  AND locked_cash_cents >= $2
                """,
                account_id,
                unlock_cash,
            )
            if account_update != "UPDATE 1":
                raise RetryableOCCError("account changed concurrently during cancel")

            await conn.execute(
                """
                INSERT INTO order_cancels (
                    cancel_id, order_id, account_id, reason
                )
                VALUES ($1, $2, $3, $4)
                """,
                cancel_id,
                order_id,
                account_id,
                reason,
            )

            await conn.execute(
                """
                INSERT INTO ledger_entries (
                    entry_id, account_id, market_id, order_id,
                    cash_delta_cents, locked_cash_delta_cents,
                    yes_share_delta, no_share_delta, locked_yes_delta, locked_no_delta,
                    reason, notes
                )
                VALUES ($1, $2, $3, $4, 0, $5, 0, 0, 0, 0, 'ORDER_UNLOCK', $6)
                """,
                str(uuid.uuid4()),
                account_id,
                str(order_row["market_id"]),
                order_id,
                -unlock_cash,
                f"Cancel order unlock for {order_id}",
            )

            return {
                "idempotent": False,
                "cancel_id": cancel_id,
                "order_id": order_id,
                "status": "CANCELLED",
                "remaining_qty": 0,
                "unlocked_cash_cents": unlock_cash,
            }
        except asyncpg.PostgresError as exc:
            if _is_occ_error(exc):
                raise RetryableOCCError from exc
            raise SubmissionError(f"database error: {exc}") from exc


async def create_pool(dsn: str, min_size: int, max_size: int) -> asyncpg.Pool:
    async def _dsql_safe_reset(_conn: asyncpg.Connection) -> None:
        return None

    return await asyncpg.create_pool(
        dsn=dsn,
        min_size=min_size,
        max_size=max_size,
        reset=_dsql_safe_reset,
    )
