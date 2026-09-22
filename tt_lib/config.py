"""Configuration read from the environment."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ServiceConfig:
    service_name: str
    port: int
    database_url: str | None
    redis_url: str | None


def load_config() -> ServiceConfig:
    """Read the configuration from the environment, with development defaults."""
    return ServiceConfig(
        service_name=os.getenv("SERVICE_NAME") or "unnamed-service",
        port=int(os.getenv("PORT") or 8080),
        database_url=os.getenv("DATABASE_URL") or None,
        redis_url=os.getenv("REDIS_URL") or None,
    )
