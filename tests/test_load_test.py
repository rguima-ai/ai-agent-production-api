"""Verifica el cálculo auditable del percentil 95."""

import httpx

from scripts.load_test import percentile_95, submit_and_wait


def test_percentile_95_uses_nearest_rank() -> None:
    assert percentile_95([1.0, 5.0, 2.0, 4.0, 3.0]) == 5.0


async def test_polling_stops_after_timeout_instead_of_hanging() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"job_id": "stuck", "status": "PENDING"})
        return httpx.Response(200, json={"job_id": "stuck", "status": "RUNNING"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        result = await submit_and_wait(
            client, "Consulta que nunca termina", timeout_seconds=0.05, poll_interval=0.01
        )

    assert result["status"] == "TIMEOUT(RUNNING)"
