import logging

from tqdm import tqdm
from sqlalchemy import Engine, text
import pandas as pd

from dlia_etl.registry import task, TaskResult
from dlia_etl.config import IN_SCHEMA, OUT_SCHEMA, PARCEL_TABLE
from dressy.standardize import standardize_batch

logger = logging.getLogger(__name__)

WRITE_TABLE = "vacancy_parcel_link"
VERICAST_NORM = "tmp_vericast_normalized"
PARCELS_NORM = "tmp_parcels_normalized"


def _normalize_chunk(df: pd.DataFrame, address_col: str, id_col: str) -> pd.DataFrame:
    """Normalize a chunk of addresses through Dressy's batch standardize."""
    raw_addresses = df[address_col].astype(str).str.strip().tolist()
    ids = df[id_col].tolist()

    # Filter blanks but keep index alignment
    valid = [(i, raw) for i, raw in zip(ids, raw_addresses) if raw]
    if not valid:
        return pd.DataFrame(columns=["id", "normalized_house_number", "normalized_street_name", "normalized_street_type"])

    valid_ids, valid_raws = zip(*valid)

    try:
        results = standardize_batch(list(valid_raws))
    except Exception as e:
        logger.error("Batch standardize failed: %s", e)
        return pd.DataFrame(columns=["id", "normalized_house_number", "normalized_street_name", "normalized_street_type"])

    rows = []
    for id_val, (_, parsed, _) in zip(valid_ids, results):
        rows.append({
            "id": id_val,
            "normalized_house_number": parsed.house_number,
            "normalized_street_name": parsed.street_name,
            "normalized_street_type": parsed.street_type,
        })

    if not rows:
        return pd.DataFrame(columns=["id", "normalized_house_number", "normalized_street_name", "normalized_street_type"])
    return pd.DataFrame(rows)


@task("vacancy_parcel_link", phase=2,
      description="Link vericast addresses to parcels via libpostal-normalized matching")
def run(source: Engine, target: Engine) -> TaskResult:
    chunksize = 10_000

    # Step 1: Normalize vericast addresses
    logger.info("Step 1: Normalizing vericast addresses...")

    vericast_q = f"""
        SELECT valassis_key,
            COALESCE(street_num, '') || ' ' ||
            COALESCE(street_pre_directional, '') || ' ' ||
            COALESCE(street_name, '') || ' ' ||
            COALESCE(street_suffix, '') AS raw_address
        FROM {OUT_SCHEMA}.vericast_unique
        WHERE TRIM(street_name) != 'PO BOX'
          AND UPPER(TRIM(city_name)) = 'DETROIT'
    """

    vericast_if_exists = "replace"
    with source.connect().execution_options(stream_results=True) as conn:
        for chunk in tqdm(
            pd.read_sql(text(vericast_q), conn, chunksize=chunksize),
            desc="Normalizing vericast",
        ):
            normalized = _normalize_chunk(chunk, "raw_address", "valassis_key")
            if not normalized.empty:
                normalized.to_sql(
                    VERICAST_NORM, target, schema=OUT_SCHEMA,
                    index=False, if_exists=vericast_if_exists,
                )
                vericast_if_exists = "append"

    with target.begin() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{VERICAST_NORM}_house_street "
            f"ON {OUT_SCHEMA}.{VERICAST_NORM} (normalized_house_number, normalized_street_name)"
        ))

    # Step 2: Normalize parcel addresses
    logger.info("Step 2: Normalizing parcel addresses...")

    parcels_q = f"""
        SELECT parcel_id,
            COALESCE(street_number, '') || ' ' ||
            COALESCE(street_prefix, '') || ' ' ||
            COALESCE(street_name, '') AS raw_address
        FROM {IN_SCHEMA}.{PARCEL_TABLE}
    """

    parcels_if_exists = "replace"
    with source.connect().execution_options(stream_results=True) as conn:
        for chunk in tqdm(
            pd.read_sql(text(parcels_q), conn, chunksize=chunksize),
            desc="Normalizing parcels",
        ):
            normalized = _normalize_chunk(chunk, "raw_address", "parcel_id")
            if not normalized.empty:
                normalized.to_sql(
                    PARCELS_NORM, target, schema=OUT_SCHEMA,
                    index=False, if_exists=parcels_if_exists,
                )
                parcels_if_exists = "append"

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
