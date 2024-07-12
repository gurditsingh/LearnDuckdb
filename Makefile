ingest:
	poetry run python -m ingestion.pipeline

test:
	pytest tests/

DBT_FOLDER = transform/t_duckdb
DBT_TARGET = dev

transform:
	cd ./transform/t_duckdb
