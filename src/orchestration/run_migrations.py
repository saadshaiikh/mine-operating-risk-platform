from __future__ import annotations

import os
from pathlib import Path
import sys

import psycopg2


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _migration_dir() -> Path:
    return _repo_root() / "db" / "migrations"


def _get_db_url() -> str:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return db_url


def _load_sql_files(migrations_dir: Path) -> list[Path]:
    return sorted(p for p in migrations_dir.iterdir() if p.suffix == ".sql")


def run() -> None:
    migrations_dir = _migration_dir()
    if not migrations_dir.exists():
        raise RuntimeError(f"Migrations directory not found: {migrations_dir}")

    sql_files = _load_sql_files(migrations_dir)
    if not sql_files:
        print("No migration files found.")
        return

    db_url = _get_db_url()

    with psycopg2.connect(db_url) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for path in sql_files:
                sql = path.read_text()
                cur.execute(sql)
                print(f"Applied {path.name}")


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        print(f"Migration failed: {exc}")
        sys.exit(1)
