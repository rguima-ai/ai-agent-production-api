"""Phoenix debe recibir una sola copia de cada llamada LLM y solo trazas del agente."""

from typing import Any

import pytest
from openinference.instrumentation.langchain import LangChainInstrumentor
from openinference.instrumentation.openai import OpenAIInstrumentor
from opentelemetry.sdk.trace import TracerProvider

import app.observability as observability
from app.config import Settings


def test_only_langchain_is_instrumented_to_avoid_double_cost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_register(**kwargs: Any) -> TracerProvider:
        calls.append(kwargs)
        return TracerProvider()

    monkeypatch.setattr("phoenix.otel.register", fake_register)
    monkeypatch.setattr(observability, "_configured", False)

    try:
        observability.configure_observability(Settings(observability_enabled=True))

        assert calls and calls[0]["auto_instrument"] is False
        assert calls[0]["project_name"] == "pre-entrega-7"
        assert LangChainInstrumentor().is_instrumented_by_opentelemetry
        # ChatOpenAI ya produce un span LLM con tokens; instrumentar también el SDK
        # de OpenAI duplicaba cada llamada y Phoenix sumaba el costo dos veces.
        assert not OpenAIInstrumentor().is_instrumented_by_opentelemetry
    finally:
        LangChainInstrumentor().uninstrument()
