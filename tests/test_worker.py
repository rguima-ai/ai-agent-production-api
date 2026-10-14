"""El worker siempre deja un estado terminal u observable en Redis."""

from typing import Any

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from opentelemetry import trace

from app.config import Settings
from app.graph import SimulatedBackend, build_graph
from app.repository import RedisJobRepository
from app.schemas import JobRecord, JobStatus
from app.worker import TaskWorker


@pytest.mark.asyncio
async def test_worker_completes_non_critical_job(
    repository: RedisJobRepository,
    settings: Settings,
) -> None:
    await repository.create_and_enqueue(
        JobRecord(
            job_id="worker-ok",
            query="Explicá la separación entre la API y un worker asíncrono",
            force_critical=False,
        )
    )
    await repository.dequeue(timeout_seconds=1)
    graph = build_graph(SimulatedBackend(), checkpointer=InMemorySaver())
    worker = TaskWorker(repository, graph, trace.get_tracer("test"), settings)

    await worker.process_job("worker-ok")
    stored = await repository.require("worker-ok")

    assert stored.status is JobStatus.DONE
    assert stored.result is not None
    assert stored.error is None


@pytest.mark.asyncio
async def test_worker_turns_background_exception_into_failed_state(
    repository: RedisJobRepository,
    settings: Settings,
) -> None:
    class ExplodingGraph:
        async def ainvoke(self, graph_input: Any, config: Any) -> None:
            del graph_input, config
            raise TimeoutError("simulated provider timeout")

    await repository.create_and_enqueue(
        JobRecord(job_id="worker-fail", query="Consulta que provoca un fallo controlado")
    )
    await repository.dequeue(timeout_seconds=1)
    worker = TaskWorker(
        repository,
        ExplodingGraph(),
        trace.get_tracer("test"),
        settings,
    )

    await worker.process_job("worker-fail")
    stored = await repository.require("worker-fail")

    assert stored.status is JobStatus.FAILED
    assert stored.error is not None
    assert "TimeoutError" in stored.error
