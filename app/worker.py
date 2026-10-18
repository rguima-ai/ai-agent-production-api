"""Worker separado: consume Redis, ejecuta LangGraph y persiste el resultado."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, cast

from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.redis.aio import AsyncRedisSaver
from langgraph.types import Command
from opentelemetry.trace import Status, StatusCode, Tracer
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.config import Settings, get_settings
from app.graph import ProductionState, build_graph, create_backend
from app.observability import configure_observability, set_span_attributes
from app.repository import RedisJobRepository
from app.schemas import JobStatus

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class TaskWorker:
    """Procesa jobs idempotentemente y convierte excepciones en estado FAILED."""

    def __init__(
        self,
        repository: RedisJobRepository,
        graph: Any,
        tracer: Tracer,
        settings: Settings,
    ) -> None:
        self.repository = repository
        self.graph = graph
        self.tracer = tracer
        self.settings = settings

    async def process_job(self, job_id: str) -> None:
        record = await self.repository.claim(job_id)
        if record is None:
            logger.warning("Job %s ignorado: no existe, venció o no está PENDING", job_id)
            return

        with self.tracer.start_as_current_span("agent.job") as span:
            set_span_attributes(
                span,
                {
                    "job.id": job_id,
                    "job.agent_mode": self.settings.agent_mode,
                    "job.resume": record.approval_decision is not None,
                },
            )
            try:
                config: RunnableConfig = {
                    "configurable": {"thread_id": job_id},
                    "recursion_limit": self.settings.max_graph_steps,
                    "metadata": {"job_id": job_id, "agent_mode": self.settings.agent_mode},
                    "tags": ["pre-entrega-7", self.settings.agent_mode],
                }
                graph_input: ProductionState | Command[Any]
                if record.approval_decision is not None:
                    graph_input = Command(
                        resume=record.approval_decision.model_dump(mode="json")
                    )
                else:
                    graph_input = {
                        "messages": [HumanMessage(content=record.query)],
                        "query": record.query,
                        "force_critical": record.force_critical,
                    }

                await self.graph.ainvoke(graph_input, config=config)
                snapshot = await self.graph.aget_state(config)
                state = cast(ProductionState, snapshot.values)

                if snapshot.next:
                    analysis = state.get("analysis_output", {})
                    await self.repository.update(
                        job_id,
                        status=JobStatus.WAITING_APPROVAL,
                        approval_request={
                            "question": "¿Aprobás que el agente continúe?",
                            "risk_reason": analysis.get("risk_reason", "Riesgo crítico"),
                            "next_nodes": list(snapshot.next),
                        },
                    )
                    span.set_attribute("job.outcome", "waiting_approval")
                    return

                report = state.get("final_report")
                if report is None:
                    raise RuntimeError("El grafo terminó sin producir final_report.")
                await self.repository.update(
                    job_id,
                    status=JobStatus.DONE,
                    result=report,
                    approval_request=None,
                )
                span.set_attribute("job.outcome", "done")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Falló el job %s", job_id)
                await self.repository.update(
                    job_id,
                    status=JobStatus.FAILED,
                    error=f"{type(exc).__name__}: {exc}",
                )
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))

    async def run_forever(self) -> None:
        logger.info("Worker iniciado; esperando jobs en %s", self.repository.queue_name)
        while True:
            try:
                job_id = await self.repository.dequeue(
                    self.settings.worker_poll_timeout_seconds
                )
            except (RedisConnectionError, RedisTimeoutError):
                # Redis reiniciando o red inestable: reintentar en vez de morir.
                logger.warning("Redis no disponible al esperar jobs; reintento en 1 s")
                await asyncio.sleep(1)
                continue
            if job_id is None:
                continue
            try:
                await self.process_job(job_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Ej.: Redis cae mientras se persiste FAILED. El loop sigue vivo.
                logger.exception("Error no recuperado procesando el job %s", job_id)


def create_worker_redis(settings: Settings) -> Redis:
    """Cliente cuyo socket_timeout supera la espera de BLPOP.

    redis-py 8 usa socket_timeout=5 s por defecto; un BLPOP de la misma duración
    se cortaría del lado del cliente antes de que Redis responda.
    """

    return Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=settings.worker_poll_timeout_seconds + 5,
    )


async def main() -> None:
    settings = get_settings()
    settings.validate_runtime()
    tracer = configure_observability(settings)
    redis = create_worker_redis(settings)
    repository = RedisJobRepository(
        redis,
        queue_name=settings.queue_name,
        job_key_prefix=settings.job_key_prefix,
        ttl_seconds=settings.job_ttl_seconds,
    )
    await repository.ping()

    try:
        async with AsyncRedisSaver.from_conn_string(settings.redis_url) as checkpointer:
            await checkpointer.asetup()
            graph = build_graph(create_backend(settings), checkpointer=checkpointer)
            worker = TaskWorker(repository, graph, tracer, settings)
            await worker.run_forever()
    finally:
        await redis.aclose()


def run() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    run()
