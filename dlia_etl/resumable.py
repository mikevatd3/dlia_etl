"""Resumable chunked processing for crash-resilient ETL tasks.

Uses a LEFT ANTI-JOIN against the target table to find unprocessed rows,
streamed via a server-side cursor. On crash and restart, already-written
rows are automatically skipped.
"""

from typing import Iterator

import pandas as pd
from sqlalchemy import Engine, text


def count_remaining(
    source_engine: Engine,
    source_table: str,
    target_engine: Engine,
    target_table: str,
    join_keys: list[str],
    source_schema: str = "dlia",
    target_schema: str = "dlia",
) -> int:
    """Count how many source rows have not yet been written to the target."""
    join_clause = " AND ".join(f"s.{k} = t.{k}" for k in join_keys)
    null_check = f"t.{join_keys[0]} IS NULL"

    query = f"""
        SELECT COUNT(*)
        FROM {source_schema}.{source_table} s
        LEFT JOIN {target_schema}.{target_table} t
          ON {join_clause}
        WHERE {null_check}
    """
    with source_engine.connect() as conn:
        return conn.execute(text(query)).scalar()


def _ensure_index(engine: Engine, table: str, schema: str, keys: list[str]):
    """Create an index on the target table's join keys if it doesn't exist."""
    index_name = f"idx_{table}_{'_'.join(keys)}"
    cols = ", ".join(keys)
    ddl = f"""
        CREATE INDEX IF NOT EXISTS {index_name}
        ON {schema}.{table} ({cols})
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def resumable_chunks(
    source_engine: Engine,
    source_table: str,
    target_engine: Engine,
    target_table: str,
    join_keys: list[str],
    chunksize: int = 5000,
    source_schema: str = "dlia",
    target_schema: str = "dlia",
) -> Iterator[pd.DataFrame]:
    """Yield chunks of unprocessed rows from the source table.

    Uses a LEFT ANTI-JOIN against the target table with a server-side
    cursor for memory-efficient streaming. Safe to restart after crashes:
    already-written rows are excluded by the join.

    Requires that ``join_keys`` form a unique key in both tables.
    """
    # Ensure target table exists before querying it
    with target_engine.connect() as conn:
        exists = conn.execute(text(
            "SELECT to_regclass(:tbl)",
        ), {"tbl": f"{target_schema}.{target_table}"}).scalar()

    if exists:
        _ensure_index(target_engine, target_table, target_schema, join_keys)

        join_clause = " AND ".join(f"s.{k} = t.{k}" for k in join_keys)
        null_check = f"t.{join_keys[0]} IS NULL"

        query = f"""
            SELECT s.*
            FROM {source_schema}.{source_table} s
            LEFT JOIN {target_schema}.{target_table} t
              ON {join_clause}
            WHERE {null_check}
        """
    else:
        # Target doesn't exist yet — return all source rows
        query = f"SELECT * FROM {source_schema}.{source_table}"

    with source_engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql(text(query), conn, chunksize=chunksize):
            yield chunk
