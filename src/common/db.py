from __future__ import annotations

import os
from typing import Any, Iterable

import psycopg2
from psycopg2.extras import execute_values


def get_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(db_url)


def delete_run_rows(conn, table: str, run_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table} WHERE run_id = %s", (run_id,))


def insert_rows(
    conn,
    table: str,
    columns: list[str],
    rows: Iterable[Iterable[Any]],
    page_size: int = 5000,
) -> None:
    if not rows:
        return
    values_template = "(" + ",".join(["%s"] * len(columns)) + ")"
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, template=values_template, page_size=page_size)
