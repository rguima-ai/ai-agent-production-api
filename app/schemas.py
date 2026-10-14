"""Contratos Pydantic de la API, Redis y los agentes."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class JobStatus(StrEnum):
    """Estados observables de un trabajo desacoplado."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    DONE = "DONE"
    FAILED = "FAILED"


class TaskRequest(BaseModel):
    """Solicitud validada que recibe la API."""

    query: str = Field(min_length=8, max_length=2_000)
    force_critical: bool | None = Field(
        default=None,
        description="Solo para demos: fuerza o evita la pausa de aprobación humana.",
    )


class TaskCreated(BaseModel):
    """Ticket que la API devuelve sin esperar la ejecución del agente."""

    job_id: str
    status: JobStatus
    status_url: str


class HumanDecision(BaseModel):
    """Decisión externa utilizada para reanudar el grafo."""

    approved: bool
    reviewer: str = Field(min_length=2, max_length=120)
    comment: str = Field(default="", max_length=500)


class ResearchArtifact(BaseModel):
    """Salida estructurada del agente Investigador."""

    summary: str = Field(min_length=20)
    findings: list[str] = Field(min_length=2)
    sources: list[str] = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)


class AnalysisArtifact(BaseModel):
    """Salida estructurada del agente Analista y clasificación de riesgo."""

    analysis: str = Field(min_length=20)
    key_metric: str = Field(min_length=5)
    critical: bool
    risk_reason: str = Field(min_length=10)
    confidence: float = Field(ge=0.0, le=1.0)


class FinalReport(BaseModel):
    """Respuesta final producida después de la revisión humana cuando corresponde."""

    title: str = Field(min_length=5)
    answer: str = Field(min_length=30)
    recommendation: str = Field(min_length=15)
    approved: bool
    sources: list[str] = Field(min_length=1)


class JobRecord(BaseModel):
    """Representación persistida del trabajo en Redis."""

    job_id: str
    query: str
    force_critical: bool | None = None
    status: JobStatus = JobStatus.PENDING
    result: dict[str, Any] | None = None
    error: str | None = None
    approval_request: dict[str, Any] | None = None
    approval_decision: HumanDecision | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class JobView(JobRecord):
    """Respuesta completa del endpoint de polling."""


class HealthResponse(BaseModel):
    """Estado mínimo para health checks."""

    status: str
    redis: str
