PYTHON := .venv/bin/python
PIP := .venv/bin/pip
PYTEST := .venv/bin/pytest

.PHONY: install install-dev lint format test test-all typecheck coverage

install:
	$(PIP) install -e .

install-dev:
	$(PIP) install -e ".[dev]"

lint:
	@echo "Running ruff..."
	$(PYTHON) -m ruff check src/ tests/
	@echo "Running mypy..."
	$(PYTHON) -m mypy src/
	@echo "Checking formatting with black..."
	$(PYTHON) -m black --check src/ tests/

format:
	@echo "Formatting with black..."
	$(PYTHON) -m black src/ tests/
	@echo "Sorting imports with ruff..."
	$(PYTHON) -m ruff check --fix --select I src/ tests/

test:
	$(PYTHON) -m pytest tests/ -n auto -m "not slow"

test-all:
	$(PYTHON) -m pytest tests/ -n auto

typecheck:
	@echo "Running mypy type checker..."
	$(PYTHON) -m mypy src/

coverage:
	@echo "Running tests with coverage..."
	$(PYTHON) -m pytest --cov=src tests/
