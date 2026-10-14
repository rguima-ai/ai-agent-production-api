"""Pruebas de persistencia, orden de encolado y transición HITL."""

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
