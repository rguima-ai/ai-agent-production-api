"""Orquestador multi-agente con Supervisor, especialistas y pausa HITL."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Literal, Protocol, cast

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.graph.state import CompiledStateGraph
from pydantic import SecretStr

from app.config import Settings
from app.hitl import human_approval_node
from app.schemas import AnalysisArtifact, FinalReport, ResearchArtifact

Route = Literal["researcher", "analyst", "approval", "synthesizer", "FINISH"]


class ProductionState(MessagesState, total=False):
    """Memoria compartida; cada especialista escribe su propio artefacto."""

    query: str
    force_critical: bool | None
    research_output: dict[str, Any]
    analysis_output: dict[str, Any]
    approval_decision: dict[str, Any]
    approved: bool
    final_report: dict[str, Any]
    next_agent: Route
    completed: bool


class AgentBackend(Protocol):
    """Interfaz común para backend real y simulación sin costo."""

    async def research(self, query: str) -> ResearchArtifact: ...

    async def analyze(
        self,
        query: str,
        research: ResearchArtifact,
        force_critical: bool | None,
    ) -> AnalysisArtifact: ...

    async def synthesize(
        self,
        query: str,
        research: ResearchArtifact,
        analysis: AnalysisArtifact,
        approved: bool,
    ) -> FinalReport: ...


class SimulatedBackend:
    """Backend determinístico para tests y demo de infraestructura sin API key."""

    _critical_words = {
        "borrar",
        "eliminar",
        "enviar correo",
        "pagar",
        "pago",
        "reembolso",
        "transferir",
    }

    async def research(self, query: str) -> ResearchArtifact:
        await asyncio.sleep(0.02)
        return ResearchArtifact(
            summary=f"Investigación simulada y reproducible sobre: {query}",
            findings=[
                "La arquitectura desacoplada evita mantener requests HTTP largos.",
                "Redis permite coordinar la cola y persistir el estado observable.",
            ],
            sources=["kb://arquitectura-async", "kb://redis-workers"],
            confidence=0.92,
        )

    async def analyze(
        self,
        query: str,
        research: ResearchArtifact,
        force_critical: bool | None,
    ) -> AnalysisArtifact:
        await asyncio.sleep(0.02)
        normalized = query.casefold()
        detected = any(word in normalized for word in self._critical_words)
        critical = force_critical if force_critical is not None else detected
        return AnalysisArtifact(
            analysis=(
                f"Se analizaron {len(research.findings)} hallazgos. "
                "La tarea requiere desacoplar recepción, ejecución y consulta de estado."
            ),
            key_metric="2 componentes de evidencia validados",
            critical=critical,
            risk_reason=(
                "La solicitud podría producir un efecto externo irreversible o costoso."
                if critical
                else "La solicitud es informativa y no produce efectos externos."
            ),
            confidence=0.95,
        )

    async def synthesize(
        self,
        query: str,
        research: ResearchArtifact,
        analysis: AnalysisArtifact,
        approved: bool,
    ) -> FinalReport:
        await asyncio.sleep(0.02)
        answer = (
            f"La tarea '{query}' fue analizada y aprobada para continuar. {analysis.analysis}"
            if approved
            else f"La tarea '{query}' fue analizada, pero no continuará porque fue rechazada."
        )
        return FinalReport(
            title="Resultado del orquestador multi-agente",
            answer=answer,
            recommendation=(
                "Ejecutar la acción bajo monitoreo y conservar la traza de auditoría."
                if approved
                else "Revisar el alcance con una persona responsable antes de reintentar."
            ),
            approved=approved,
            sources=research.sources,
        )


class OpenAIBackend:
    """Especialistas reales con salidas estructuradas y trazado automático."""

    def __init__(self, settings: Settings) -> None:
        model = ChatOpenAI(
            model=settings.openai_model,
            temperature=0,
            api_key=SecretStr(settings.openai_api_key),
        )
        self._researcher = model.with_structured_output(ResearchArtifact)
        self._analyst = model.with_structured_output(AnalysisArtifact)
        self._synthesizer = model.with_structured_output(FinalReport)

    async def research(self, query: str) -> ResearchArtifact:
        result = await self._researcher.ainvoke(
            [
                SystemMessage(
                    content=(
                        "Sos Investigador. Reuní evidencia concreta, indicá fuentes y no tomes "
                        "decisiones finales. Si no tenés acceso externo, declaralo en los "
                        "hallazgos."
                    )
                ),
                HumanMessage(content=query),
            ]
        )
        return ResearchArtifact.model_validate(result)

    async def analyze(
        self,
        query: str,
        research: ResearchArtifact,
        force_critical: bool | None,
    ) -> AnalysisArtifact:
        payload = {
            "query": query,
            "research": research.model_dump(mode="json"),
            "forced_criticality": force_critical,
            "critical_policy": (
                "Es crítica toda acción con dinero, borrado, comunicaciones externas o efectos "
                "irreversibles. Si forced_criticality no es null, respetá ese valor."
            ),
        }
        result = await self._analyst.ainvoke(
            [
                SystemMessage(
                    content="Sos Analista. Evaluá evidencia, métrica central y riesgo operativo."
                ),
                HumanMessage(content=json.dumps(payload, ensure_ascii=False)),
            ]
        )
        return AnalysisArtifact.model_validate(result)

    async def synthesize(
        self,
        query: str,
        research: ResearchArtifact,
        analysis: AnalysisArtifact,
        approved: bool,
    ) -> FinalReport:
        payload = {
            "query": query,
            "research": research.model_dump(mode="json"),
            "analysis": analysis.model_dump(mode="json"),
            "approved": approved,
        }
        result = await self._synthesizer.ainvoke(
            [
                SystemMessage(
                    content=(
                        "Sos el agente de Síntesis. Usá solo la evidencia recibida. Si approved es "
                        "false, explicá que la acción no continuará. Conservá las fuentes."
                    )
                ),
                HumanMessage(content=json.dumps(payload, ensure_ascii=False)),
            ]
        )
        return FinalReport.model_validate(result)


def create_backend(settings: Settings) -> AgentBackend:
    settings.validate_runtime()
    if settings.agent_mode == "openai":
        return OpenAIBackend(settings)
    return SimulatedBackend()


def supervisor_node(state: ProductionState) -> dict[str, Any]:
    """Decide dinámicamente el siguiente especialista según el estado validado."""

    if not state.get("research_output"):
        route: Route = "researcher"
    elif not state.get("analysis_output"):
        route = "analyst"
    elif "approved" not in state:
        route = "approval"
    elif not state.get("final_report"):
        route = "synthesizer"
    else:
        route = "FINISH"
    return {
        "next_agent": route,
        "completed": route == "FINISH",
        "messages": [AIMessage(content=f"Supervisor delega en: {route}")],
    }


def route_supervisor(state: ProductionState) -> Route:
    return state.get("next_agent", "FINISH")


def build_graph(
    backend: AgentBackend,
    checkpointer: BaseCheckpointSaver[Any] | None = None,
) -> CompiledStateGraph[ProductionState, None, Any, Any]:
    """Compila Supervisor → especialistas → Supervisor con checkpoints."""

    async def researcher_node(state: ProductionState) -> dict[str, Any]:
        artifact = await backend.research(state["query"])
        return {
            "research_output": artifact.model_dump(mode="json"),
            "messages": [AIMessage(content="Investigación completada.", name="researcher")],
        }

    async def analyst_node(state: ProductionState) -> dict[str, Any]:
        research = ResearchArtifact.model_validate(state["research_output"])
        artifact = await backend.analyze(
            state["query"], research, state.get("force_critical")
        )
        return {
            "analysis_output": artifact.model_dump(mode="json"),
            "messages": [AIMessage(content="Análisis y riesgo validados.", name="analyst")],
        }

    async def synthesizer_node(state: ProductionState) -> dict[str, Any]:
        research = ResearchArtifact.model_validate(state["research_output"])
        analysis = AnalysisArtifact.model_validate(state["analysis_output"])
        artifact = await backend.synthesize(
            state["query"], research, analysis, state.get("approved", False)
        )
        return {
            "final_report": artifact.model_dump(mode="json"),
            "messages": [AIMessage(content=artifact.answer, name="synthesizer")],
        }

    def approval_node(state: ProductionState) -> dict[str, Any]:
        return human_approval_node(cast(dict[str, Any], state))

    builder = StateGraph(ProductionState)
    builder.add_node("supervisor", supervisor_node)
    builder.add_node("researcher", researcher_node)
    builder.add_node("analyst", analyst_node)
    builder.add_node("approval", approval_node)
    builder.add_node("synthesizer", synthesizer_node)
    builder.add_edge(START, "supervisor")
    builder.add_conditional_edges(
        "supervisor",
        route_supervisor,
        {
            "researcher": "researcher",
            "analyst": "analyst",
            "approval": "approval",
            "synthesizer": "synthesizer",
            "FINISH": END,
        },
    )
    builder.add_edge("researcher", "supervisor")
    builder.add_edge("analyst", "supervisor")
    builder.add_edge("approval", "supervisor")
    builder.add_edge("synthesizer", "supervisor")
    return builder.compile(checkpointer=checkpointer)
