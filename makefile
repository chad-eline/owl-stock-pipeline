SHELL = /bin/bash

## help: Show available commands
help:
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'

## setup: Install dependencies from uv.lock
setup:
	uv sync

## reset-db: Resets the sqlite db by deleting it
reset-db:
	rm -rf owl.db

## lint: Format all python files with black (uses pyproject.toml config)
lint:
	uv run black . --fast

## run-v1: Load the v1 source XLSX
run-v1:
	uv run pipeline.py --file data/stock-data-se-owl.xlsx --db owl.db

## run-v2: Load v2 (schema migration + backfill + in-place updates)
run-v2:
	uv run pipeline.py --file data/stock-data-se-owl-part2.xlsx --db owl.db

## query: Run the example analytics query
query:
	uv run queries.py

## test: Run the test suite
test:
	uv run pytest -q

all: setup reset-db lint test run-v1 query run-v2 

.PHONY: help setup run-v1 run-v2 query test