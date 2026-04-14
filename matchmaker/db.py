import asyncio
import random
import uuid

import asyncpg

from models import Order


class MatchError(Exception):
    pass


class RetryableOCCError(Exception):
    pass


def _is_occ_error(exc: Exception) -> bool:
    return isinstance(exc, asyncpg.PostgresError) and exc.sqlstate in {"40001", "40P01"}


async def create_pool(dsn: str, min_size: int, max_size: int) -> asyncpg.Pool:
    async def _dsql_safe_reset(_conn: asyncpg.Connection) -> None:
        # Aurora DSQL does not support pg_advisory_unlock_all().
        return None

    return await asyncpg.create_pool(
        dsn=dsn,
        min_size=min_size,
        max_size=max_size,
        reset=_dsql_safe_reset,
    )


class MatchRepository:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def ensure_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS matchmaker_market_leases (
                    market_id UUID PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    lease_expires_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            await conn.execute(
                """
                CREATE INDEX ASYNC IF NOT EXISTS matchmaker_market_leases_exp_idx
                ON matchmaker_market_leases(lease_expires_at)
                """
            )

    async def list_candidate_markets(self, limit: int) -> list[str]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT market_id
                FROM orders
                WHERE status IN ('ACCEPTED', 'OPEN', 'PARTIALLY_FILLED')
                  AND remaining_qty > 0
                GROUP BY market_id
                ORDER BY min(global_seq)
                LIMIT $1
                """,
                limit,
            )
            return [str(r["market_id"]) for r in rows]

    async def try_acquire_market_lease(
        self,
        market_id: str,
        owner_id: str,
        lease_seconds: int,
    ) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO matchmaker_market_leases (
                    market_id,
                    owner_id,
                    lease_expires_at,
                    updated_at
                )
                VALUES ($1, $2, now() + ($3::text || ' seconds')::interval, now())
                ON CONFLICT (market_id)
                DO UPDATE
                SET owner_id = EXCLUDED.owner_id,
                    lease_expires_at = EXCLUDED.lease_expires_at,
                    updated_at = now()
                WHERE matchmaker_market_leases.lease_expires_at < now()
                   OR matchmaker_market_leases.owner_id = EXCLUDED.owner_id
                RETURNING owner_id
                """,
                market_id,
                owner_id,
                f"{lease_seconds} seconds",
            )
            return bool(row and row["owner_id"] == owner_id)

    async def load_market_orders(self, market_id: str, limit: int) -> list[Order]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT request_id, global_seq, account_id, market_id, side,
                       remaining_qty, price_cents, status
                FROM orders
                WHERE market_id = $1
                  AND status IN ('ACCEPTED', 'OPEN', 'PARTIALLY_FILLED')
                  AND remaining_qty > 0
                ORDER BY global_seq
                LIMIT $2
                """,
                market_id,
                limit,
            )
        return [
            Order(
                request_id=str(r["request_id"]),
                global_seq=r["global_seq"],
                account_id=str(r["account_id"]),
                market_id=str(r["market_id"]),
                side=str(r["side"]),
                remaining_qty=r["remaining_qty"],
                price_cents=r["price_cents"],
                status=str(r["status"]),
            )
            for r in rows
        ]

    async def execute_match(
        self,
        market_id: str,
        yes_order_id: str,
        no_order_id: str,
        qty: int,
    ) -> bool:
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            try:
                return await self._execute_match_once(market_id, yes_order_id, no_order_id, qty)
            except RetryableOCCError:
                if attempt == max_attempts:
                    raise MatchError("database conflict after retries")
                delay = (0.02 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.02)
                await asyncio.sleep(delay)

    async def _execute_match_once(
        self,
        market_id: str,
        yes_order_id: str,
        no_order_id: str,
        qty: int,
    ) -> bool:
        if qty <= 0:
            return False

        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    yes_row = await conn.fetchrow(
                        """
                        SELECT request_id, account_id, market_id, side, remaining_qty,
                               price_cents, status, global_seq
                        FROM orders
                        WHERE request_id = $1
                        """,
                        yes_order_id,
                    )
                    no_row = await conn.fetchrow(
                        """
                        SELECT request_id, account_id, market_id, side, remaining_qty,
                               price_cents, status, global_seq
                        FROM orders
                        WHERE request_id = $1
                        """,
                        no_order_id,
                    )

                    if not yes_row or not no_row:
                        return False
                    if str(yes_row["market_id"]) != market_id or str(no_row["market_id"]) != market_id:
                        return False
                    if yes_row["side"] != "YES" or no_row["side"] != "NO":
                        return False
                    if yes_row["status"] not in {"ACCEPTED", "OPEN", "PARTIALLY_FILLED"}:
                        return False
                    if no_row["status"] not in {"ACCEPTED", "OPEN", "PARTIALLY_FILLED"}:
                        return False
                    if yes_row["remaining_qty"] < qty or no_row["remaining_qty"] < qty:
                        return False
                    if yes_row["price_cents"] + no_row["price_cents"] < 100:
                        return False

                    yes_fill_cost = qty * yes_row["price_cents"]
                    no_fill_cost = qty * no_row["price_cents"]

                    yes_account_update = await conn.execute(
                        """
                        UPDATE accounts
                        SET locked_cash_cents = locked_cash_cents - $2,
                            updated_at = now()
                        WHERE account_id = $1
                          AND locked_cash_cents >= $2
                        """,
                        yes_row["account_id"],
                        yes_fill_cost,
                    )
                    no_account_update = await conn.execute(
                        """
                        UPDATE accounts
                        SET locked_cash_cents = locked_cash_cents - $2,
                            updated_at = now()
                        WHERE account_id = $1
                          AND locked_cash_cents >= $2
                        """,
                        no_row["account_id"],
                        no_fill_cost,
                    )
                    if yes_account_update != "UPDATE 1" or no_account_update != "UPDATE 1":
                        return False

                    yes_order_update = await conn.execute(
                        """
                        UPDATE orders
                        SET remaining_qty = remaining_qty - $2,
                            status = CASE
                                WHEN remaining_qty - $2 = 0 THEN 'FILLED'
                                ELSE 'PARTIALLY_FILLED'
                            END,
                            updated_at = now()
                        WHERE request_id = $1
                          AND status IN ('ACCEPTED', 'OPEN', 'PARTIALLY_FILLED')
                          AND remaining_qty >= $2
                        """,
                        yes_order_id,
                        qty,
                    )
                    no_order_update = await conn.execute(
                        """
                        UPDATE orders
                        SET remaining_qty = remaining_qty - $2,
                            status = CASE
                                WHEN remaining_qty - $2 = 0 THEN 'FILLED'
                                ELSE 'PARTIALLY_FILLED'
                            END,
                            updated_at = now()
                        WHERE request_id = $1
                          AND status IN ('ACCEPTED', 'OPEN', 'PARTIALLY_FILLED')
                          AND remaining_qty >= $2
                        """,
                        no_order_id,
                        qty,
                    )
                    if yes_order_update != "UPDATE 1" or no_order_update != "UPDATE 1":
                        raise RetryableOCCError("order changed concurrently")

                    trade_id = str(uuid.uuid4())
                    resting_order_id = yes_order_id
                    aggressing_order_id = no_order_id
                    if yes_row["global_seq"] > no_row["global_seq"]:
                        resting_order_id = no_order_id
                        aggressing_order_id = yes_order_id

                    await conn.execute(
                        """
                        INSERT INTO trades (
                            trade_id, market_id, resting_order_id, aggressing_order_id,
                            qty, yes_price_cents
                        )
                        VALUES ($1, $2, $3, $4, $5, $6)
                        """,
                        trade_id,
                        market_id,
                        resting_order_id,
                        aggressing_order_id,
                        qty,
                        yes_row["price_cents"],
                    )

                    await conn.execute(
                        """
                        INSERT INTO positions (account_id, market_id, yes_shares, no_shares, updated_at)
                        VALUES ($1, $2, $3, 0, now())
                        ON CONFLICT (account_id, market_id)
                        DO UPDATE SET yes_shares = positions.yes_shares + EXCLUDED.yes_shares,
                                      updated_at = now()
                        """,
                        yes_row["account_id"],
                        market_id,
                        qty,
                    )
                    await conn.execute(
                        """
                        INSERT INTO positions (account_id, market_id, yes_shares, no_shares, updated_at)
                        VALUES ($1, $2, 0, $3, now())
                        ON CONFLICT (account_id, market_id)
                        DO UPDATE SET no_shares = positions.no_shares + EXCLUDED.no_shares,
                                      updated_at = now()
                        """,
                        no_row["account_id"],
                        market_id,
                        qty,
                    )

                    await conn.execute(
                        """
                        INSERT INTO ledger_entries (
                            entry_id, account_id, market_id, order_id,
                            cash_delta_cents, locked_cash_delta_cents,
                            yes_share_delta, no_share_delta,
                            locked_yes_delta, locked_no_delta,
                            reason, notes
                        )
                        VALUES
                        ($1, $2, $3, $4, 0, $5, $6, 0, 0, 0, 'TRADE_EXECUTION', $7),
                        ($8, $9, $3, $10, 0, $11, 0, $12, 0, 0, 'TRADE_EXECUTION', $13)
                        """,
                        str(uuid.uuid4()),
                        yes_row["account_id"],
                        market_id,
                        yes_order_id,
                        -yes_fill_cost,
                        qty,
                        f"Match execution for YES order {yes_order_id}",
                        str(uuid.uuid4()),
                        no_row["account_id"],                        
                        no_order_id,                      
                        -no_fill_cost,
                        qty,
                        f"Match execution for NO order {no_order_id}",
                    )

                    return True
        except asyncpg.PostgresError as exc:
            if _is_occ_error(exc):
                raise RetryableOCCError from exc
            raise MatchError(f"database error: {exc}") from exc
