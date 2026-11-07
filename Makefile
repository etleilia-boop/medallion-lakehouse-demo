.PHONY: install test lint run clean

install:
	pip install -e ".[dev]"

test:
	pytest -q

lint:
	ruff check src tests
	ruff format --check src tests

run:
	python -m lakehouse.pipeline --root data

clean:
	rm -rf data/lake .pytest_cache .ruff_cache
