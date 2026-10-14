"""Envía cinco tareas concurrentes y calcula latencia HTTP p95 local."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
from pathlib import Path
from time import perf_counter
from typing import Any

import httpx

QUERIES = [
    "Investigá cómo una cola Redis evita bloquear una API de inteligencia artificial.",
    "Analizá las ventajas de separar FastAPI del worker que ejecuta LangGraph.",
    "Explicá por qué los checkpoints son necesarios para Human-in-the-loop.",
    "Compará trazas, spans y métricas de latencia para un sistema multi-agente.",
    "Proponé controles de seguridad para un worker asíncrono de producción.",
]
TERMINAL = {"DONE", "FAILED", "WAITING_APPROVAL"}


def percentile_95(values: list[float]) -> float:
    """Percentil nearest-rank, fácil de auditar con solo cinco muestras."""

    ordered = sorted(values)
    rank = max(1, math.ceil(0.95 * len(ordered)))
    return ordered[rank - 1]


async def submit_and_wait(client: httpx.AsyncClient, query: str) -> dict[str, Any]:
    started = perf_counter()
    response = await client.post("/tasks", json={"query": query, "force_critical": False})
    response.raise_for_status()
    job_id = response.json()["job_id"]

    while True:
        status_response = await client.get(f"/tasks/{job_id}")
        status_response.raise_for_status()
        payload = status_response.json()
        if payload["status"] in TERMINAL:
            return {
                "job_id": job_id,
                "status": payload["status"],
                "latency_seconds": round(perf_counter() - started, 4),
            }
        await asyncio.sleep(0.25)


async def run(base_url: str, output: Path) -> None:
    async with httpx.AsyncClient(base_url=base_url, timeout=120) as client:
        results = await asyncio.gather(
            *(submit_and_wait(client, query) for query in QUERIES)
        )
    latencies = [float(item["latency_seconds"]) for item in results]
    report = {
        "requests": results,
        "local_http_p95_seconds": percentile_95(latencies),
        "note": (
            "La métrica oficial a entregar es la p95 del dashboard Phoenix. "
            "Este valor local sirve para contrastarla."
        ),
    }
    await asyncio.to_thread(output.parent.mkdir, parents=True, exist_ok=True)
    await asyncio.to_thread(
        output.write_text,
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument(
        "--output", type=Path, default=Path("reports/load_test_results.json")
    )
    arguments = parser.parse_args()
    asyncio.run(run(arguments.base_url, arguments.output))
