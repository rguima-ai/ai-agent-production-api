"""Persistencia asíncrona de trabajos y cola simple sobre Redis."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import WatchError

from app.schemas import HumanDecision, JobRecord, JobStatus


class JobNotFoundError(LookupError):
    """El identificador solicitado no existe."""


class InvalidJobTransitionError(RuntimeError):
    """El estado actual no permite la transición solicitada."""


class RedisJobRepository:
    """Guarda jobs como JSON y coordina su cola usando operaciones atómicas."""

    def __init__(
        self,
        redis: Redis,
        *,
        queue_name: str,
        job_key_prefix: str,
        ttl_seconds: int,
    ) -> None:
        self.redis = redis
        self.queue_name = queue_name
        self.job_key_prefix = job_key_prefix
        self.ttl_seconds = ttl_seconds

    def _key(self, job_id: str) -> str:
        return f"{self.job_key_prefix}{job_id}"

    async def create_and_enqueue(self, record: JobRecord) -> None:
        """Persiste PENDING antes de publicar el ID en la cola."""

        key = self._key(record.job_id)
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.watch(key)
            if await pipe.exists(key):
                raise InvalidJobTransitionError(f"El job {record.job_id} ya existe.")
            pipe.multi()  # type: ignore[no-untyped-call]
            pipe.set(key, record.model_dump_json(), ex=self.ttl_seconds)
            pipe.rpush(self.queue_name, record.job_id)
            await pipe.execute()

    async def get(self, job_id: str) -> JobRecord | None:
        raw = await self.redis.get(self._key(job_id))
        if raw is None:
            return None
        return JobRecord.model_validate_json(raw)

    async def require(self, job_id: str) -> JobRecord:
        record = await self.get(job_id)
        if record is None:
            raise JobNotFoundError(job_id)
        return record

    async def update(self, job_id: str, **changes: Any) -> JobRecord:
        """Actualiza un job preservando su vencimiento."""

        record = await self.require(job_id)
        changes["updated_at"] = datetime.now(UTC)
        updated = record.model_copy(update=changes)
        await self.redis.set(
            self._key(job_id),
            updated.model_dump_json(),
            ex=self.ttl_seconds,
        )
        return updated

    async def dequeue(self, timeout_seconds: int) -> str | None:
        """Espera sin bloquear el event loop hasta que llegue un trabajo."""

        item = await self.redis.blpop(self.queue_name, timeout=timeout_seconds)
        if item is None:
            return None
        _, job_id = item
        if isinstance(job_id, bytes):
            return job_id.decode("utf-8")
        return str(job_id)

    async def submit_approval(
        self,
        job_id: str,
        decision: HumanDecision,
        *,
        retries: int = 3,
    ) -> JobRecord:
        """Transiciona WAITING_APPROVAL -> PENDING y reencola de forma atómica."""

        key = self._key(job_id)
        for _ in range(retries):
            async with self.redis.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    if raw is None:
                        raise JobNotFoundError(job_id)
                    current = JobRecord.model_validate_json(raw)
                    if current.status is not JobStatus.WAITING_APPROVAL:
                        raise InvalidJobTransitionError(
                            f"El job está en {current.status}; no espera aprobación."
                        )
                    updated = current.model_copy(
                        update={
                            "status": JobStatus.PENDING,
                            "approval_decision": decision,
                            "updated_at": datetime.now(UTC),
                        }
                    )
                    pipe.multi()  # type: ignore[no-untyped-call]
                    pipe.set(key, updated.model_dump_json(), ex=self.ttl_seconds)
                    pipe.rpush(self.queue_name, job_id)
                    await pipe.execute()
                    return updated
                except WatchError:
                    continue
        raise InvalidJobTransitionError(
            "No fue posible guardar la aprobación por actualizaciones concurrentes."
        )

    async def ping(self) -> bool:
        return bool(await self.redis.ping())
