"""Configuración validada y centralizada de la aplicación."""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Variables de entorno requeridas por API, worker y observabilidad."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    redis_url: str = "redis://localhost:6379"
    queue_name: str = "agent:tasks"
    job_key_prefix: str = "agent:job:"
    agent_mode: Literal["simulated", "openai"] = "simulated"
    openai_api_key: str = ""
    openai_model: str = "gpt-4.1-mini"
    observability_enabled: bool = True
    phoenix_collector_endpoint: str = "http://localhost:6006/v1/traces"
    phoenix_project_name: str = "pre-entrega-7"
    worker_poll_timeout_seconds: int = Field(default=5, ge=1, le=60)
    job_ttl_seconds: int = Field(default=86_400, ge=300)
    max_graph_steps: int = Field(default=16, ge=6, le=64)

    def validate_runtime(self) -> None:
        """Evita iniciar el backend real sin credenciales."""

        if self.agent_mode == "openai" and not self.openai_api_key:
            raise RuntimeError(
                "AGENT_MODE=openai requiere OPENAI_API_KEY. "
                "Usá AGENT_MODE=simulated para la demostración sin costo."
            )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Devuelve una única configuración validada por proceso."""

    return Settings()
