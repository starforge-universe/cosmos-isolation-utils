.PHONY: help install install-dev test test-cov lint clean build install-cli

help:  ## Show this help message
	@echo "CosmosDB Isolation Utilities - Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install the package in development mode
	pip install -e .

install-dev:  ## Install the package with development dependencies
	pip install -e ".[dev]"

install-cli: install  ## Install the CLI tool
	@echo "CLI tool 'cosmos-isolation-utils' installed successfully!"
	@echo "Usage: cosmos-isolation-utils --help"

test:  ## Run the test suite
	python -m unittest discover tests/ -v

test-cov:  ## Run tests with coverage
	python -m coverage run -m unittest discover tests/ -v
	python -m coverage report --include="cosmos_isolation_utils/*"
	python -m coverage html --include="cosmos_isolation_utils/*"

lint:  ## Run linting checks
	pylint cosmos_isolation_utils/ --rcfile=pyproject.toml

clean:  ## Clean up build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete

build:  ## Build the package
	python -m build

demo:  ## Run a demo of the CLI tool
	@echo "=== CosmosDB Isolation Utilities CLI Demo ==="
	@echo ""
	@echo "1. Show help:"
	@echo "   python -m cosmos_isolation_utils --help"
	@echo ""
	@echo "2. Show subcommand help:"
	@echo "   python -m cosmos_isolation_utils -e dummy -k dummy -d dummy test --help"
	@echo ""
	@echo "3. List available subcommands:"
	@echo "   python -m cosmos_isolation_utils --help"
	@echo ""
	@echo "Note: Replace 'dummy' with actual values for real usage"

