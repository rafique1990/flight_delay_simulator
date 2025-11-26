.PHONY: help install install-dev build up down stop logs clean-docker test lint format simulate-det simulate-mc clean clean-results dev api

help:
	@echo "Available targets:"
	@echo "  install         - Install core dependencies with UV"
	@echo "  install-dev     - Install development dependencies with UV"
	@echo "  build           - Build Docker image (tagged flight-simulator:latest)"
	@echo "  up              - Build and start Docker services"
	@echo "  down            - Stop and remove containers and networks"
	@echo "  stop            - Stop running containers (preserves state)"
	@echo "  logs            - Stream logs from the API service"
	@echo "  clean-docker    - Stop, remove containers, networks, volumes, and images"
	@echo "  test            - Run pytest with coverage"
	@echo "  lint            - Run linting checks (black, isort, flake8)"
	@echo "  format          - Format code with black and isort"
	@echo "  simulate-det    - Run deterministic simulation via CLI"
	@echo "  simulate-mc     - Run Monte Carlo simulation via CLI"
	@echo "  clean           - Clean pycache, coverage files, and artifacts"
	@echo "  clean-results   - Remove generated simulation results"
	@echo "  dev             - Run FastAPI locally with hot reload"
	@echo "  api             - Run FastAPI in production mode"

install:
	uv pip install -e .

install-dev:
	uv pip install -e ".[dev]"

build:
	docker build -t flight-simulator:latest .

up:
	docker compose up --build -d

down:
	docker compose down

stop:
	docker compose stop

logs:
	docker compose logs -f api

clean-docker:
	@echo "Stopping and removing all project-related containers, networks, volumes, and images..."
	docker compose down -v --rmi all

test:
	pytest -v --cov=src/flightrobustness --cov-report=html --cov-report=term-missing

lint:
	black --check src tests
	isort --check-only src tests
	flake8 src tests

format:
	black src tests
	isort src tests

simulate-det:
	python -m flightrobustness.interfaces.cli --config config.yaml --mode deterministic

simulate-mc:
	python -m flightrobustness.interfaces.cli --config config.yaml --mode monte_carlo --runs 5

clean:
	rm -rf .pytest_cache .coverage htmlcov
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".ipynb_checkpoints" -exec rm -rf {} + 2>/dev/null || true

clean-results:
	rm -rf data/results/*.csv
	rm -rf data/results/*.png

dev:
	uvicorn flightrobustness.interfaces.api:app --reload --host 0.0.0.0 --port 8000

api:
	uvicorn flightrobustness.interfaces.api:app --host 0.0.0.0 --port 8000
