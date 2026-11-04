.PHONY: help install data run test lint cov clean up down logs

help:
	@echo "install  install package and dev dependencies"
	@echo "data     generate synthetic source data"
	@echo "run      run the full pipeline locally"
	@echo "test     run unit tests"
	@echo "lint     run ruff"
	@echo "up       start the docker stack"
	@echo "down     stop the docker stack"

install:
	pip install -e ".[dev]"

data:
	python scripts/generate_sample_data.py

run: data
	python -m pipeline.run all

test:
	pytest -q

cov:
	pytest --cov=pipeline --cov-report=term-missing

lint:
	ruff check .

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f airflow-scheduler

clean:
	rm -rf data/lake .pytest_cache .ruff_cache
