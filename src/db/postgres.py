from __future__ import annotations

from typing import Sequence

import psycopg
from psycopg import sql
from psycopg.rows import tuple_row

from src.config import Settings


def connect(
    settings: Settings,
    autocommit: bool = False,
    row_factory=tuple_row,
):
    conn = psycopg.connect(
        settings.pg_dsn,
        autocommit=autocommit,
        row_factory=row_factory,
    )
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(settings.pg_schema)
            )
        )
    if not autocommit:
        conn.commit()
    return conn


def create_schema(settings: Settings) -> None:
    conn = connect(settings, autocommit=True)
    schema = sql.Identifier(settings.pg_schema)

    conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema))

    conn.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {schema}.events (
                id BIGINT PRIMARY KEY,
                day DATE NOT NULL,
                ts TIMESTAMP NOT NULL,
                user_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                payload TEXT,
                amount DOUBLE PRECISION NOT NULL
            )
            """
        ).format(schema=schema)
    )

    conn.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {schema}.sales (
                id BIGINT PRIMARY KEY,
                region TEXT NOT NULL,
                amount DOUBLE PRECISION NOT NULL,
                ts TIMESTAMP NOT NULL,
                day DATE NOT NULL
            )
            """
        ).format(schema=schema)
    )

    conn.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {schema}.flight_upload_demo (
                id BIGINT,
                day DATE,
                ts TIMESTAMP,
                user_id INTEGER,
                kind TEXT,
                payload TEXT,
                amount DOUBLE PRECISION
            )
            """
        ).format(schema=schema)
    )

    idx_events_day = sql.Identifier(f"{settings.pg_schema}_events_day_idx")
    idx_events_day_kind = sql.Identifier(f"{settings.pg_schema}_events_day_kind_idx")
    idx_sales_day = sql.Identifier(f"{settings.pg_schema}_sales_day_idx")

    conn.execute(
        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.events (day)").format(
            idx_events_day, schema
        )
    )
    conn.execute(
        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.events (day, kind)").format(
            idx_events_day_kind, schema
        )
    )
    conn.execute(
        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.sales (day)").format(
            idx_sales_day, schema
        )
    )

    conn.close()


def truncate_tables(settings: Settings) -> None:
    conn = connect(settings, autocommit=True)
    schema = sql.Identifier(settings.pg_schema)
    for table in ("events", "sales", "flight_upload_demo"):
        conn.execute(
            sql.SQL("TRUNCATE TABLE {}.{} RESTART IDENTITY CASCADE").format(
                schema, sql.Identifier(table)
            )
        )
    conn.close()


def copy_csv_to_table(
    settings: Settings,
    table: str,
    csv_path: str,
    columns: Sequence[str],
) -> None:
    conn = connect(settings)
    schema = sql.Identifier(settings.pg_schema)
    table_id = sql.Identifier(table)
    cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in columns)

    query = sql.SQL("COPY {schema}.{table} ({cols}) FROM STDIN WITH (FORMAT csv)").format(
        schema=schema,
        table=table_id,
        cols=cols_sql,
    )

    with conn.cursor() as cur:
        with cur.copy(query) as copy:
            with open(csv_path, "rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    copy.write(chunk)

    conn.commit()
    conn.close()


def insert_record_batches(settings: Settings, table: str, batches) -> int:
    if not batches:
        return 0

    conn = connect(settings)
    schema = sql.Identifier(settings.pg_schema)
    table_id = sql.Identifier(table)

    first = batches[0]
    cols = first.schema.names
    col_ids = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
    placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(cols))

    stmt = sql.SQL("INSERT INTO {schema}.{table} ({cols}) VALUES ({vals})").format(
        schema=schema,
        table=table_id,
        cols=col_ids,
        vals=placeholders,
    )

    inserted = 0
    with conn.cursor() as cur:
        for batch in batches:
            rows = batch.to_pylist()
            values = [tuple(row[c] for c in cols) for row in rows]
            if values:
                cur.executemany(stmt, values)
                inserted += len(values)

    conn.commit()
    conn.close()
    return inserted


def explain_analyze_time_ms(settings: Settings, query: str) -> float:
    conn = connect(settings)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("EXPLAIN (ANALYZE, FORMAT JSON) {query}").format(
                query=sql.SQL(query)
            )
        )
        result = cur.fetchone()[0]
    conn.commit()
    conn.close()
    return float(result[0]["Execution Time"])