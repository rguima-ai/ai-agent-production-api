"""Contrato HTTP: respuesta 202 inmediata, polling y aprobación segura."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast

import httpx
import pytest
from fastapi import FastAPI
from redis.asyncio import Redis

from app.config import Settings
from app.main import RedisFactory, create_app
from app.schemas import JobStatus


@asynccontextmanager
async def api_client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client


@pytest.mark.asyncio
async def test_create_and_poll_are_non_blocking_contracts(
    redis_client: Redis,
    settings: Settings,
) -> None:
    factory = cast(RedisFactory, lambda url: redis_client)
    app = create_app(settings, redis_factory=factory)

    async with api_client(app) as client:
        response = await client.post(
            "/tasks",
            json={"query": "Investigá la arquitectura asíncrona con Redis"},
        )
        assert response.status_code == 202
        payload = response.json()
        assert payload["status"] == "PENDING"

        status_response = await client.get(payload["status_url"])
        assert status_response.status_code == 200
        assert status_response.json()["job_id"] == payload["job_id"]


@pytest.mark.asyncio
async def test_unknown_job_returns_404(redis_client: Redis, settings: Settings) -> None:
    app = create_app(settings, redis_factory=cast(RedisFactory, lambda url: redis_client))

    async with api_client(app) as client:
        response = await client.get("/tasks/not-found")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_approve_only_accepts_waiting_job(
    redis_client: Redis,
    settings: Settings,
) -> None:
    app = create_app(settings, redis_factory=cast(RedisFactory, lambda url: redis_client))

    async with api_client(app) as client:
        created = await client.post(
            "/tasks",
            json={"query": "Analizá una operación que necesita aprobación"},
        )
        job_id = created.json()["job_id"]
        response = await client.post(
            f"/tasks/{job_id}/approve",
            json={"approved": True, "reviewer": "Rocco", "comment": "OK"},
        )

    assert response.status_code == 409
    assert JobStatus.PENDING.value in response.json()["detail"]


@pytest.mark.asyncio
async def test_api_does_not_export_polling_traces_to_phoenix(
    redis_client: Redis,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registered: list[object] = []
    monkeypatch.setattr("phoenix.otel.register", lambda **kwargs: registered.append(kwargs))
    monkeypatch.setattr("app.observability._configured", False)
    enabled = settings.model_copy(update={"observability_enabled": True})
    app = create_app(enabled, redis_factory=cast(RedisFactory, lambda url: redis_client))

    async with api_client(app) as client:
        assert (await client.get("/health")).status_code == 200

    # La API no ejecuta LLMs: miles de spans de polling taparían las 5 trazas del agente.
    assert registered == []
