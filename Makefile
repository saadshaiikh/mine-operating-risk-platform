.PHONY: db-up db-down db-reset db-migrate db-psql backtest bi-views

DB_URL ?= postgresql://postgres:postgres@localhost:5432/mine_risk

export DATABASE_URL = $(DB_URL)


db-up:
	docker compose up -d postgres

db-down:
	docker compose down

db-reset:
	docker compose down -v
	docker compose up -d postgres
	python -m src.orchestration.run_migrations

db-migrate:
	python -m src.orchestration.run_migrations

db-psql:
	psql $$DATABASE_URL

backtest:
	python -m src.backtesting.run_rolling_backtest

bi-views:
	python -m src.transforms.marts.build_bi_views
