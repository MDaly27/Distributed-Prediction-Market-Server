import asyncio
import random
import uuid
from typing import Any

import asyncpg

from models import SubmitOrderRequest


class SubmissionError(Exception):
    pass


class RetryableOCCError(Exception):
    pass


def _is_occ_error(exc: Exception) -> bool:
    if not isinstance(exc, asyncpg.PostgresError):
        return False
    return exc.sqlstate in {"40001", "40P01"}


class OrderRepository:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def submit_order(self, req: SubmitOrderRequest) -> dict[str, Any]:
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            try:
                return await self._submit_order_once(req)
            except RetryableOCCError:
                if attempt == max_attempts:
                    raise SubmissionError("database conflict after retries")
                delay = (0.02 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.02)
                await asyncio.sleep(delay)

    async def _submit_order_once(self, req: SubmitOrderRequest) -> dict[str, Any]:
        needed_cash = req.qty * req.price_cents
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
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
                            "global_seq": existing["global_seq"],
                            "status": existing["status"],
                            "remaining_qty": existing["remaining_qty"],
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
                        "global_seq": order_row["global_seq"],
                        "status": order_row["status"],
                        "remaining_qty": order_row["remaining_qty"],
                    }
        except asyncpg.PostgresError as exc:
            if _is_occ_error(exc):
                raise RetryableOCCError from exc
            raise SubmissionError(f"database error: {exc}") from exc


async def create_pool(dsn: str, min_size: int, max_size: int) -> asyncpg.Pool:
    async def _dsql_safe_reset(_conn: asyncpg.Connection) -> None:
        # Aurora DSQL does not support pg_advisory_unlock_all(), which is part
        # of asyncpg's default pool reset routine. Use a no-op reset callback.
        return None

    return await asyncpg.create_pool(
        dsn=dsn,
        min_size=min_size,
        max_size=max_size,
        reset=_dsql_safe_reset,
    )
