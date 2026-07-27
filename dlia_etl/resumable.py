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
    distinct: bool = False,
    estimate: bool = False,
) -> int:
    """Count how many source rows have not yet been written to the target.

    When ``estimate=True``, uses pg_class approximate row counts for the
    source table (instant) and an exact count for the smaller target table.
    Good enough for tqdm totals on large, stable tables.
    """
    if estimate:
        with source_engine.connect() as conn:
            source_count = conn.execute(text(
                "SELECT reltuples::bigint FROM pg_class "
                "WHERE oid = CAST(:tbl AS regclass)"
            ), {"tbl": f"{source_schema}.{source_table}"}).scalar() or 0

        with target_engine.connect() as conn:
            target_exists = conn.execute(text(
                "SELECT to_regclass(:tbl)"
            ), {"tbl": f"{target_schema}.{target_table}"}).scalar()
            if target_exists:
                target_count = conn.execute(text(
                    f"SELECT COUNT(*) FROM {target_schema}.{target_table}"
                )).scalar()
            else:
                target_count = 0

        return max(0, source_count - target_count)

    join_clause = " AND ".join(f"s.{k} = t.{k}" for k in join_keys)
    null_check = f"t.{join_keys[0]} IS NULL"

    if distinct:
        count_expr = f"COUNT(DISTINCT s.{join_keys[0]})"
    else:
        count_expr = "COUNT(*)"

    query = f"""
        SELECT {count_expr}
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
    distinct: bool = False,
    stream: bool = True,
) -> Iterator[pd.DataFrame]:
    """Yield chunks of unprocessed rows from the source table.

    Uses a LEFT ANTI-JOIN against the target table. Safe to restart after
    crashes: already-written rows are excluded by the join.

    When ``stream=True`` (default), uses a server-side cursor for
    memory-efficient streaming. When ``stream=False``, uses repeated
    LIMIT queries — each query is independent and releases resources
    between chunks, which avoids memory pressure on the server.

    When ``distinct=True``, deduplicates source rows by ``join_keys``
    using ``DISTINCT ON``, so each key is yielded only once even if it
    appears in multiple source rows.

    Requires that ``join_keys`` form a unique key in the target table.
    """
    distinct_clause = ""
    if distinct:
        distinct_cols = ", ".join(join_keys)
        distinct_clause = f"DISTINCT ON ({distinct_cols}) "

    # Ensure target table exists before querying it
    with target_engine.connect() as conn:
        exists = conn.execute(text(
            "SELECT to_regclass(:tbl)",
        ), {"tbl": f"{target_schema}.{target_table}"}).scalar()

    # Index source join keys for DISTINCT ON performance
    _ensure_index(source_engine, source_table, source_schema, join_keys)

    if exists:
        _ensure_index(target_engine, target_table, target_schema, join_keys)

    if stream:
        query = _build_query(
            source_schema, source_table, target_schema, target_table,
            join_keys, distinct_clause, exists,
        )
        with source_engine.connect().execution_options(stream_results=True) as conn:
            for chunk in pd.read_sql(text(query), conn, chunksize=chunksize):
                yield chunk
    else:
        while True:
            query = _build_query(
                source_schema, source_table, target_schema, target_table,
                join_keys, distinct_clause, exists,
                limit=chunksize,
            )
            with source_engine.connect() as conn:
                chunk = pd.read_sql(text(query), conn)
            if chunk.empty:
                break
            yield chunk


def _build_query(
    source_schema: str,
    source_table: str,
    target_schema: str,
    target_table: str,
    join_keys: list[str],
    distinct_clause: str,
    target_exists: bool,
    limit: int | None = None,
) -> str:
    """Build the anti-join SELECT query."""
    limit_clause = f" LIMIT {limit}" if limit else ""

    if target_exists:
        join_clause = " AND ".join(f"s.{k} = t.{k}" for k in join_keys)
        null_check = f"t.{join_keys[0]} IS NULL"
        return f"""
            SELECT {distinct_clause}s.*
            FROM {source_schema}.{source_table} s
            LEFT JOIN {target_schema}.{target_table} t
              ON {join_clause}
            WHERE {null_check}{limit_clause}
        """
    else:
        return f"SELECT {distinct_clause}* FROM {source_schema}.{source_table}{limit_clause}"
