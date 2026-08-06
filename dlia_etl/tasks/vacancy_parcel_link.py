import logging

from tqdm import tqdm
from sqlalchemy import Engine, text
import pandas as pd

from dlia_etl.registry import task, TaskResult
from dlia_etl.config import OUT_SCHEMA, PARCEL_TABLE

from dressy.standardize import standardize

logger = logging.getLogger(__name__)

WRITE_TABLE = "vacancy_parcel_link"
VERICAST_NORM = "_vericast_normalized"
PARCELS_NORM = "_parcels_normalized"


def _normalize_df(df: pd.DataFrame, address_col: str, id_col: str) -> pd.DataFrame:
    """Run addresses through libpostal standardize and return normalized components."""
    rows = []
    for _, row in df.iterrows():
        raw = str(row[address_col]).strip()
        if not raw:
            continue
        try:
            parsed, key = standardize(raw)
            rows.append({
                "id": row[id_col],
                "normalized_house_number": parsed.house_number,
                "normalized_street_name": parsed.street_name,
                "normalized_street_type": parsed.street_type,
            })
        except Exception:
            continue

    return pd.DataFrame(rows)


def _normalize_df_batch(df: pd.DataFrame, address_col: str, id_col: str) -> pd.DataFrame:
    """Batch-normalize addresses through libpostal standardize."""
    ids = []
    raw_addresses = []
    for _, row in df.iterrows():
        raw = str(row[address_col]).strip()
        if raw:
            ids.append(row[id_col])
            raw_addresses.append(raw)

    if not raw_addresses:
        return pd.DataFrame(columns=["id", "normalized_house_number", "normalized_street_name", "normalized_street_type"])

    rows = []
    for i, raw in enumerate(raw_addresses):
        try:
            parsed, _ = standardize(raw)
            rows.append({
                "id": ids[i],
                "normalized_house_number": parsed.house_number,
                "normalized_street_name": parsed.street_name,
                "normalized_street_type": parsed.street_type,
            })
        except Exception:
            continue

    return pd.DataFrame(rows)


@task("vacancy_parcel_link", phase=2,
      description="Link vericast addresses to parcels via libpostal-normalized matching")
def run(source: Engine, target: Engine) -> TaskResult:
    chunksize = 10_000

    # Step 1: Normalize vericast addresses
    logger.info("Step 1: Normalizing vericast addresses...")

    with target.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {OUT_SCHEMA}.{VERICAST_NORM}"))

    vericast_q = f"""
        SELECT valassis_key,
            COALESCE(street_num, '') || ' ' ||
            COALESCE(street_pre_directional, '') || ' ' ||
            COALESCE(street_name, '') || ' ' ||
            COALESCE(street_suffix, '') AS raw_address
        FROM {OUT_SCHEMA}.vericast_unique
        WHERE TRIM(street_name) != 'PO BOX'
    """

    with source.connect().execution_options(stream_results=True) as conn:
        for chunk in tqdm(
            pd.read_sql(text(vericast_q), conn, chunksize=chunksize),
            desc="Normalizing vericast",
        ):
            normalized = _normalize_df_batch(chunk, "raw_address", "valassis_key")
            if not normalized.empty:
                normalized.to_sql(
                    VERICAST_NORM, target, schema=OUT_SCHEMA,
                    index=False, if_exists="append",
                )

    with target.begin() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{VERICAST_NORM}_house_street "
            f"ON {OUT_SCHEMA}.{VERICAST_NORM} (normalized_house_number, normalized_street_name)"
        ))

    # Step 2: Normalize parcel addresses
    logger.info("Step 2: Normalizing parcel addresses...")

    with target.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {OUT_SCHEMA}.{PARCELS_NORM}"))

    parcels_q = f"""
        SELECT parcel_id,
            COALESCE(street_number, '') || ' ' ||
            COALESCE(street_prefix, '') || ' ' ||
            COALESCE(street_name, '') AS raw_address
        FROM {OUT_SCHEMA}.{PARCEL_TABLE}
    """

    with source.connect().execution_options(stream_results=True) as conn:
        for chunk in tqdm(
            pd.read_sql(text(parcels_q), conn, chunksize=chunksize),
            desc="Normalizing parcels",
        ):
            normalized = _normalize_df_batch(chunk, "raw_address", "parcel_id")
            if not normalized.empty:
                normalized.to_sql(
                    PARCELS_NORM, target, schema=OUT_SCHEMA,
                    index=False, if_exists="append",
                )

    with target.begin() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{PARCELS_NORM}_house_street "
            f"ON {OUT_SCHEMA}.{PARCELS_NORM} (normalized_house_number, normalized_street_name)"
        ))

    # Step 3: Exact match on normalized components
    logger.info("Step 3: Exact matching on normalized addresses...")

    with target.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {OUT_SCHEMA}.{WRITE_TABLE}"))
        conn.execute(text(f"""
            CREATE TABLE {OUT_SCHEMA}.{WRITE_TABLE} AS
            SELECT DISTINCT ON (v.id)
                v.id AS valassis_key,
                p.id AS parcel_id,
                1.0::float AS match_score
            FROM {OUT_SCHEMA}.{VERICAST_NORM} v
            JOIN {OUT_SCHEMA}.{PARCELS_NORM} p
              ON v.normalized_house_number = p.normalized_house_number
              AND v.normalized_street_name = p.normalized_street_name
            ORDER BY v.id
        """))

        exact_count = conn.execute(text(
            f"SELECT COUNT(*) FROM {OUT_SCHEMA}.{WRITE_TABLE}"
        )).scalar()
    logger.info("Exact matches: %d", exact_count)

    # Step 4: Fuzzy match remainder
    logger.info("Step 4: Fuzzy matching remaining addresses...")

    with target.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))

        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{PARCELS_NORM}_street_trgm "
            f"ON {OUT_SCHEMA}.{PARCELS_NORM} USING GIN (normalized_street_name gin_trgm_ops)"
        ))

        result = conn.execute(text(f"""
            INSERT INTO {OUT_SCHEMA}.{WRITE_TABLE} (valassis_key, parcel_id, match_score)
            SELECT DISTINCT ON (v.id)
                v.id AS valassis_key,
                p.id AS parcel_id,
                similarity(v.normalized_street_name, p.normalized_street_name)::float AS match_score
            FROM {OUT_SCHEMA}.{VERICAST_NORM} v
            JOIN {OUT_SCHEMA}.{PARCELS_NORM} p
              ON v.normalized_house_number = p.normalized_house_number
              AND similarity(v.normalized_street_name, p.normalized_street_name) >= 0.5
            WHERE NOT EXISTS (
                SELECT 1 FROM {OUT_SCHEMA}.{WRITE_TABLE} l
                WHERE l.valassis_key = v.id
            )
            ORDER BY v.id, similarity(v.normalized_street_name, p.normalized_street_name) DESC
        """))
        fuzzy_count = result.rowcount
    logger.info("Fuzzy matches: %d", fuzzy_count)

    # Step 5: Index output, cleanup temp tables
    with target.begin() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{WRITE_TABLE}_valassis_key "
            f"ON {OUT_SCHEMA}.{WRITE_TABLE} (valassis_key)"
        ))
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{WRITE_TABLE}_parcel_id "
            f"ON {OUT_SCHEMA}.{WRITE_TABLE} (parcel_id)"
        ))
        conn.execute(text(f"DROP TABLE IF EXISTS {OUT_SCHEMA}.{VERICAST_NORM}"))
        conn.execute(text(f"DROP TABLE IF EXISTS {OUT_SCHEMA}.{PARCELS_NORM}"))

    total = exact_count + fuzzy_count
    return TaskResult(task_name="vacancy_parcel_link", rows_inserted=total, success=True)
