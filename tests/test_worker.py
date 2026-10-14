"""El worker siempre deja un estado terminal u observable en Redis."""

import asyncio
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


@pytest.mark.asyncio
async def test_worker_survives_expired_or_unknown_job(
    repository: RedisJobRepository,
    settings: Settings,
) -> None:
    graph = build_graph(SimulatedBackend(), checkpointer=InMemorySaver())
    worker = TaskWorker(repository, graph, trace.get_tracer("test"), settings)

    await worker.process_job("expired-job")

    assert await repository.get("expired-job") is None


@pytest.mark.asyncio
async def test_duplicate_queue_entries_execute_the_graph_once(
    repository: RedisJobRepository,
    settings: Settings,
) -> None:
    class CountingGraph:
        def __init__(self) -> None:
            self.inner = build_graph(SimulatedBackend(), checkpointer=InMemorySaver())
            self.calls = 0

        async def ainvoke(self, graph_input: Any, config: Any) -> Any:
            self.calls += 1
            return await self.inner.ainvoke(graph_input, config=config)

        async def aget_state(self, config: Any) -> Any:
            return await self.inner.aget_state(config)

    await repository.create_and_enqueue(
        JobRecord(
            job_id="worker-dup",
            query="Explicá cómo evitar ejecuciones duplicadas en un worker",
            force_critical=False,
        )
    )
    graph = CountingGraph()
    worker = TaskWorker(repository, graph, trace.get_tracer("test"), settings)

    await asyncio.gather(worker.process_job("worker-dup"), worker.process_job("worker-dup"))

    assert graph.calls == 1
    assert (await repository.require("worker-dup")).status is JobStatus.DONE
