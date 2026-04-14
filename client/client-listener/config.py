import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    db_dsn: str
    db_min_pool_size: int
    db_max_pool_size: int
    auth_mode: str
    auth_token: str
    auth_secret_arn: str
    aws_region: str


def load_settings() -> Settings:
    return Settings(
        host=os.getenv("CLIENT_LISTENER_HOST", "0.0.0.0"),
        port=int(os.getenv("CLIENT_LISTENER_PORT", "9001")),
        db_dsn=os.getenv(
            "CLIENT_LISTENER_DB_DSN",
            "postgresql://postgres:postgres@127.0.0.1:5432/postgres?sslmode=require",
        ),
        db_min_pool_size=int(os.getenv("CLIENT_LISTENER_DB_MIN_POOL", "1")),
        db_max_pool_size=int(os.getenv("CLIENT_LISTENER_DB_MAX_POOL", "10")),
        auth_mode=os.getenv("CLIENT_LISTENER_AUTH_MODE", "static-token"),
        auth_token=os.getenv("CLIENT_LISTENER_AUTH_TOKEN", ""),
        auth_secret_arn=os.getenv("CLIENT_LISTENER_AUTH_SECRET_ARN", ""),
        aws_region=os.getenv("AWS_REGION", "us-east-2"),
    )
