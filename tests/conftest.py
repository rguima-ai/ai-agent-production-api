"""Fixtures compartidas con Redis completamente simulado en memoria."""

from collections.abc import AsyncIterator

import fakeredis.aioredis
import pytest
from redis.asyncio import Redis

from app.config import Settings
from app.repository import RedisJobRepository


@pytest.fixture
def settings() -> Settings:
    return Settings(
        redis_url="redis://unused:6379",
        queue_name="test:queue",
        job_key_prefix="test:job:",
        agent_mode="simulated",
        observability_enabled=False,
        job_ttl_seconds=3_600,
        worker_poll_timeout_seconds=1,
    )


@pytest.fixture
async def redis_client() -> AsyncIterator[Redis]:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def repository(redis_client: Redis, settings: Settings) -> RedisJobRepository:
    return RedisJobRepository(
        redis_client,
        queue_name=settings.queue_name,
        job_key_prefix=settings.job_key_prefix,
        ttl_seconds=settings.job_ttl_seconds,
    )
