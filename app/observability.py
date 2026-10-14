"""Inicialización de Phoenix/OpenTelemetry y helpers de trazado manual."""

from __future__ import annotations

import logging
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import Tracer

from app.config import Settings

logger = logging.getLogger(__name__)
_configured = False


def configure_observability(settings: Settings) -> Tracer:
    """Registra Phoenix una sola vez por proceso y devuelve un tracer."""

    global _configured
    if not settings.observability_enabled:
        return trace.get_tracer("pre-entrega-7.disabled")

    if not _configured:
        try:
            from phoenix.otel import register

            register(
                project_name=settings.phoenix_project_name,
                endpoint=settings.phoenix_collector_endpoint,
                protocol="http/protobuf",
                auto_instrument=True,
                batch=True,
            )
            _configured = True
            logger.info(
                "Observabilidad Phoenix activa en %s",
                settings.phoenix_collector_endpoint,
            )
        except Exception:  # Phoenix no debe impedir que la API atienda health checks.
            logger.exception("No se pudo inicializar Phoenix; la aplicación continúa sin exportar.")

    return trace.get_tracer("pre-entrega-7")


def set_span_attributes(span: Any, attributes: dict[str, Any]) -> None:
    """Agrega solamente valores compatibles con OpenTelemetry."""

    for key, value in attributes.items():
        if value is not None and isinstance(value, (str, bool, int, float)):
            span.set_attribute(key, value)
