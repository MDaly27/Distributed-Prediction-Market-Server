import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    db_dsn: str
    db_min_pool_size: int
    db_max_pool_size: int
    poll_interval_ms: int
    market_scan_limit: int
    order_scan_limit: int
    lease_seconds: int
    instance_id: str


def load_settings() -> Settings:
    return Settings(
        db_dsn=os.getenv(
            "MATCHMAKER_DB_DSN",
            "postgresql://postgres:postgres@127.0.0.1:5432/postgres?sslmode=require",
        ),
        db_min_pool_size=int(os.getenv("MATCHMAKER_DB_MIN_POOL", "1")),
        db_max_pool_size=int(os.getenv("MATCHMAKER_DB_MAX_POOL", "10")),
        poll_interval_ms=int(os.getenv("MATCHMAKER_POLL_INTERVAL_MS", "500")),
        market_scan_limit=int(os.getenv("MATCHMAKER_MARKET_SCAN_LIMIT", "50")),
        order_scan_limit=int(os.getenv("MATCHMAKER_ORDER_SCAN_LIMIT", "2000")),
        lease_seconds=int(os.getenv("MATCHMAKER_LEASE_SECONDS", "5")),
        instance_id=os.getenv("MATCHMAKER_INSTANCE_ID", "matchmaker-local"),
    )
