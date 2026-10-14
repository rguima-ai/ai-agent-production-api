"""Pruebas del grafo, la pausa crítica y la reanudación con Command."""

from typing import Any

import pytest
from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from app.config import Settings
from app.graph import ProductionState, SimulatedBackend, build_graph, create_backend


def initial_state(*, critical: bool) -> ProductionState:
    query = (
        "Ejecutá un reembolso de alto valor para un cliente"
        if critical
        else "Explicá cómo escalar una API con una cola Redis"
    )
    return {
        "messages": [HumanMessage(content=query)],
        "query": query,
        "force_critical": critical,
    }


@pytest.mark.asyncio
async def test_non_critical_flow_finishes_without_pause() -> None:
    graph = build_graph(SimulatedBackend(), checkpointer=InMemorySaver())
    config: RunnableConfig = {"configurable": {"thread_id": "safe-job"}}

    result = await graph.ainvoke(initial_state(critical=False), config=config)
    snapshot = await graph.aget_state(config)

    assert not snapshot.next
    assert result["completed"] is True
    assert result["final_report"]["approved"] is True


@pytest.mark.asyncio
async def test_critical_flow_pauses_and_resumes_after_approval() -> None:
    graph = build_graph(SimulatedBackend(), checkpointer=InMemorySaver())
    config: RunnableConfig = {"configurable": {"thread_id": "critical-job"}}

    await graph.ainvoke(initial_state(critical=True), config=config)
    paused = await graph.aget_state(config)

    assert paused.next == ("approval",)
    assert paused.tasks[0].interrupts

    result = await graph.ainvoke(
        Command[Any](
            resume={"approved": True, "reviewer": "Rocco", "comment": "Revisado"}
        ),
        config=config,
    )

    assert result["completed"] is True
    assert result["approved"] is True
    assert result["final_report"]["approved"] is True


@pytest.mark.asyncio
async def test_rejected_critical_flow_finishes_without_authorizing_action() -> None:
    graph = build_graph(SimulatedBackend(), checkpointer=InMemorySaver())
    config: RunnableConfig = {"configurable": {"thread_id": "rejected-job"}}
    await graph.ainvoke(initial_state(critical=True), config=config)

    result = await graph.ainvoke(
        Command[Any](
            resume={"approved": False, "reviewer": "Rocco", "comment": "Riesgo alto"}
        ),
        config=config,
    )

    assert result["completed"] is True
    assert result["approved"] is False
    assert "no continuará" in result["final_report"]["answer"]


def test_openai_backend_and_graph_compile_without_network_call() -> None:
    settings = Settings(
        agent_mode="openai",
        openai_api_key="sk-test-only",
        observability_enabled=False,
    )

    graph = build_graph(create_backend(settings), checkpointer=InMemorySaver())
    mermaid = graph.get_graph().draw_mermaid()

    assert "supervisor" in mermaid
    assert "approval" in mermaid
    assert "synthesizer" in mermaid
