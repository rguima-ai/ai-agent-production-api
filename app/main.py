"""FastAPI: valida, persiste y encola sin ejecutar el agente en el request."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Annotated, cast

from fastapi import Depends, FastAPI, HTTPException, Request, status
from redis.asyncio import Redis

from app.config import Settings, get_settings
from app.repository import (
    InvalidJobTransitionError,
    JobNotFoundError,
    RedisJobRepository,
)
from app.schemas import (
    HealthResponse,
    HumanDecision,
    JobRecord,
    JobView,
    TaskCreated,
    TaskRequest,
)

RedisFactory = Callable[[str], Redis]


def default_redis_factory(url: str) -> Redis:
    return Redis.from_url(url, decode_responses=True)


def get_repository(request: Request) -> RedisJobRepository:
    """Recupera la dependencia inicializada durante el lifespan."""

    return cast(RedisJobRepository, request.app.state.repository)


RepositoryDependency = Annotated[
    RedisJobRepository,
    Depends(get_repository),
]


def create_app(
    settings: Settings | None = None,
    redis_factory: RedisFactory = default_redis_factory,
) -> FastAPI:
    """Application factory inyectable para tests sin Redis real."""

    resolved = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        client = redis_factory(resolved.redis_url)
        repository = RedisJobRepository(
            client,
            queue_name=resolved.queue_name,
            job_key_prefix=resolved.job_key_prefix,
            ttl_seconds=resolved.job_ttl_seconds,
        )
        try:
            await repository.ping()
            app.state.repository = repository
            # Sin Phoenix en la API: no ejecuta LLMs y el polling taparía las trazas del agente.
            yield
        finally:
            await client.aclose()

    application = FastAPI(
        title="Pre-entrega 7 · API multi-agente",
        version="1.0.0",
        description=(
            "API asíncrona: registra el trabajo en Redis, lo encola y permite consultar o "
            "aprobar su ejecución sin bloquear el request HTTP."
        ),
        lifespan=lifespan,
    )

    @application.get("/health", response_model=HealthResponse, tags=["operación"])
    async def health(
        repository: RepositoryDependency,
    ) -> HealthResponse:
        redis_ok = await repository.ping()
        return HealthResponse(status="ok", redis="connected" if redis_ok else "unavailable")

    @application.post(
        "/tasks",
        response_model=TaskCreated,
        status_code=status.HTTP_202_ACCEPTED,
        tags=["tareas"],
    )
    async def create_task(
        payload: TaskRequest,
        repository: RepositoryDependency,
    ) -> TaskCreated:
        job_id = str(uuid.uuid4())
        record = JobRecord(
            job_id=job_id,
            query=payload.query,
            force_critical=payload.force_critical,
        )
        await repository.create_and_enqueue(record)
        return TaskCreated(
            job_id=job_id,
            status=record.status,
            status_url=f"/tasks/{job_id}",
        )

    @application.get("/tasks/{job_id}", response_model=JobView, tags=["tareas"])
    async def get_task(
        job_id: str,
        repository: RepositoryDependency,
    ) -> JobView:
        record = await repository.get(job_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Job no encontrado.")
        return JobView.model_validate(record.model_dump())

    @application.post(
        "/tasks/{job_id}/approve",
        response_model=JobView,
        status_code=status.HTTP_202_ACCEPTED,
        tags=["human-in-the-loop"],
    )
    async def approve_task(
        job_id: str,
        decision: HumanDecision,
        repository: RepositoryDependency,
    ) -> JobView:
        try:
            record = await repository.submit_approval(job_id, decision)
        except JobNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Job no encontrado.") from exc
        except InvalidJobTransitionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return JobView.model_validate(record.model_dump())

    return application


app = create_app()


def run() -> None:
    """Punto de entrada para desarrollo local."""

    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)


if __name__ == "__main__":
    run()
