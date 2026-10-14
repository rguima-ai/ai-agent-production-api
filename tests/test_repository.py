"""Pruebas de persistencia, orden de encolado y transición HITL."""

import asyncio

import pytest

from app.repository import InvalidJobTransitionError, RedisJobRepository
from app.schemas import HumanDecision, JobRecord, JobStatus


@pytest.mark.asyncio
async def test_create_persists_before_worker_can_dequeue(
    repository: RedisJobRepository,
) -> None:
    record = JobRecord(job_id="job-1", query="Consulta suficientemente extensa")

    await repository.create_and_enqueue(record)
    dequeued = await repository.dequeue(timeout_seconds=1)
    stored = await repository.require("job-1")

    assert dequeued == "job-1"
    assert stored.status is JobStatus.PENDING


@pytest.mark.asyncio
async def test_approval_requeues_waiting_job(repository: RedisJobRepository) -> None:
    await repository.create_and_enqueue(
        JobRecord(job_id="job-2", query="Ejecutar un reembolso importante")
    )
    await repository.dequeue(timeout_seconds=1)
    await repository.update("job-2", status=JobStatus.WAITING_APPROVAL)

    updated = await repository.submit_approval(
        "job-2",
        HumanDecision(approved=True, reviewer="Rocco"),
    )

    assert updated.status is JobStatus.PENDING
    assert updated.approval_decision is not None
    assert await repository.dequeue(timeout_seconds=1) == "job-2"


@pytest.mark.asyncio
async def test_approval_rejects_invalid_transition(
    repository: RedisJobRepository,
) -> None:
    await repository.create_and_enqueue(
        JobRecord(job_id="job-3", query="Consulta informativa suficientemente larga")
    )

    with pytest.raises(InvalidJobTransitionError):
        await repository.submit_approval(
            "job-3",
            HumanDecision(approved=True, reviewer="Rocco"),
        )


@pytest.mark.asyncio
async def test_claim_is_atomic_and_only_accepts_pending(
    repository: RedisJobRepository,
) -> None:
    await repository.create_and_enqueue(
        JobRecord(job_id="job-4", query="Consulta para reclamar una sola vez")
    )

    first, second = await asyncio.gather(repository.claim("job-4"), repository.claim("job-4"))

    assert [first is not None, second is not None].count(True) == 1
    assert (await repository.require("job-4")).status is JobStatus.RUNNING
    assert await repository.claim("missing-job") is None


@pytest.mark.asyncio
async def test_concurrent_approvals_produce_a_single_transition(
    repository: RedisJobRepository,
) -> None:
    await repository.create_and_enqueue(
        JobRecord(job_id="job-5", query="Ejecutar un reembolso importante")
    )
    await repository.dequeue(timeout_seconds=1)
    await repository.update("job-5", status=JobStatus.WAITING_APPROVAL)

    results = await asyncio.gather(
        repository.submit_approval("job-5", HumanDecision(approved=True, reviewer="Ana")),
        repository.submit_approval("job-5", HumanDecision(approved=False, reviewer="Beto")),
        return_exceptions=True,
    )

    assert sum(isinstance(item, InvalidJobTransitionError) for item in results) == 1
    assert await repository.redis.llen(repository.queue_name) == 1
