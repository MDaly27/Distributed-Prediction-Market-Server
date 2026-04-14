import asyncio
from datetime import datetime, timezone

from config import load_settings
from db import MatchRepository, create_pool
from models import Order


def _books(orders: list[Order]) -> tuple[list[Order], list[Order]]:
    yes = [o for o in orders if o.side == "YES"]
    no = [o for o in orders if o.side == "NO"]

    # Price-time priority: higher price first, then older sequence.
    yes.sort(key=lambda o: (-o.price_cents, o.global_seq))
    no.sort(key=lambda o: (-o.price_cents, o.global_seq))
    return yes, no


async def match_market(repo: MatchRepository, market_id: str, order_scan_limit: int) -> int:
    orders = await repo.load_market_orders(market_id, order_scan_limit)
    if not orders:
        return 0

    yes_book, no_book = _books(orders)
    matched = 0

    i = 0
    j = 0
    while i < len(yes_book) and j < len(no_book):
        yes_order = yes_book[i]
        no_order = no_book[j]

        if yes_order.remaining_qty <= 0:
            i += 1
            continue
        if no_order.remaining_qty <= 0:
            j += 1
            continue

        # Skip self-cross candidates; keep scanning for valid counterparties.
        if yes_order.account_id == no_order.account_id:
            if yes_order.global_seq <= no_order.global_seq:
                i += 1
            else:
                j += 1
            continue

        # Binary market crossing condition.
        if yes_order.price_cents + no_order.price_cents < 100:
            break

        fill_qty = min(yes_order.remaining_qty, no_order.remaining_qty)
        ok = await repo.execute_match(
            market_id=market_id,
            yes_order_id=yes_order.request_id,
            no_order_id=no_order.request_id,
            qty=fill_qty,
        )

        # Another matcher likely won this race. Reload next loop.
        if not ok:
            return matched

        yes_order.remaining_qty -= fill_qty
        no_order.remaining_qty -= fill_qty
        matched += 1

        if yes_order.remaining_qty == 0:
            i += 1
        if no_order.remaining_qty == 0:
            j += 1

    return matched


async def main() -> None:
    settings = load_settings()
    pool = await create_pool(
        dsn=settings.db_dsn,
        min_size=settings.db_min_pool_size,
        max_size=settings.db_max_pool_size,
    )
    repo = MatchRepository(pool)
    await repo.ensure_schema()

    print(
        f"[{datetime.now(timezone.utc).isoformat()}] matchmaker started "
        f"instance_id={settings.instance_id}"
    )

    try:
        while True:
            markets = await repo.list_candidate_markets(settings.market_scan_limit)
            if not markets:
                await asyncio.sleep(settings.poll_interval_ms / 1000)
                continue

            total_matches = 0
            for market_id in markets:
                acquired = await repo.try_acquire_market_lease(
                    market_id=market_id,
                    owner_id=settings.instance_id,
                    lease_seconds=settings.lease_seconds,
                )
                if not acquired:
                    continue

                matched = await match_market(repo, market_id, settings.order_scan_limit)
                total_matches += matched

            if total_matches > 0:
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] "
                    f"matched={total_matches} markets_scanned={len(markets)}"
                )

            await asyncio.sleep(settings.poll_interval_ms / 1000)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
