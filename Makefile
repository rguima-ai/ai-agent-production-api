.PHONY: install test lint typecheck check up down load

install:
	python -m pip install -e ".[dev]"

test:
	python -m pytest -q

lint:
	python -m ruff check .

typecheck:
	python -m mypy app tests

check: test lint typecheck

up:
	docker compose up --build

down:
	docker compose down

load:
	python scripts/load_test.py
