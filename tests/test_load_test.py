"""Verifica el cálculo auditable del percentil 95."""

from scripts.load_test import percentile_95


def test_percentile_95_uses_nearest_rank() -> None:
    assert percentile_95([1.0, 5.0, 2.0, 4.0, 3.0]) == 5.0
