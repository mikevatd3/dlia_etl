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

POSTAL_PORT = 8400
POSTAL_URL = f"http://localhost:{POSTAL_PORT}"


def _start_postal_service():
    """Start the postal service if it isn't already running."""
    try:
        requests.get(f"{POSTAL_URL}/docs", timeout=2)
        logger.info("Postal service already running on port %d", POSTAL_PORT)
        return None
    except requests.ConnectionError:
        pass

    logger.info("Starting postal service on port %d...", POSTAL_PORT)
    proc = subprocess.Popen(
        ["uv", "run", "uvicorn", "dressy.postal_service:app",
         "--host", "0.0.0.0", "--port", str(POSTAL_PORT)],
        cwd=os.path.expanduser("~/2_responsibilities/dressy"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Wait for it to be ready
    for _ in range(30):
        try:
            requests.get(f"{POSTAL_URL}/docs", timeout=1)
            logger.info("Postal service started (pid %d)", proc.pid)
            return proc
        except requests.ConnectionError:
            time.sleep(1)

    proc.kill()
    raise RuntimeError("Postal service failed to start within 30 seconds")


def _stop_postal_service(proc):
    """Stop the postal service if we started it."""
    if proc is not None:
        proc.terminate()
        proc.wait(timeout=5)
        logger.info("Postal service stopped")


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
            COALESCE(street_number::text, '') || ' ' ||
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

    # Step 3: Full outer join with normalized values preserved
    logger.info("Step 3: Matching normalized addresses (full outer join)...")

    with target.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))

        conn.execute(text(f"DROP TABLE IF EXISTS {OUT_SCHEMA}.{WRITE_TABLE}"))
        conn.execute(text(f"""
            CREATE TABLE {OUT_SCHEMA}.{WRITE_TABLE} AS
            WITH best_matches AS (
                SELECT DISTINCT ON (v.id)
                    v.id AS valassis_key,
                    v.normalized_house_number AS vericast_house_number,
                    v.normalized_street_name AS vericast_street_name,
                    v.normalized_street_type AS vericast_street_type,
                    p.id AS parcel_id,
                    p.normalized_house_number AS parcel_house_number,
                    p.normalized_street_name AS parcel_street_name,
                    p.normalized_street_type AS parcel_street_type,
                    CASE
                        WHEN v.normalized_street_name = p.normalized_street_name THEN 1.0
                        ELSE similarity(v.normalized_street_name, p.normalized_street_name)
                    END::float AS match_score
                FROM {OUT_SCHEMA}.{VERICAST_NORM} v
                JOIN {OUT_SCHEMA}.{PARCELS_NORM} p
                  ON v.normalized_house_number = p.normalized_house_number
                  AND similarity(v.normalized_street_name, p.normalized_street_name) >= 0.5
                ORDER BY v.id, similarity(v.normalized_street_name, p.normalized_street_name) DESC
            )
            -- Matched vericast rows
            SELECT * FROM best_matches

            UNION ALL

            -- Unmatched vericast rows
            SELECT
                v.id AS valassis_key,
                v.normalized_house_number AS vericast_house_number,
                v.normalized_street_name AS vericast_street_name,
                v.normalized_street_type AS vericast_street_type,
                NULL AS parcel_id,
                NULL AS parcel_house_number,
                NULL AS parcel_street_name,
                NULL AS parcel_street_type,
                NULL::float AS match_score
            FROM {OUT_SCHEMA}.{VERICAST_NORM} v
            WHERE NOT EXISTS (
                SELECT 1 FROM best_matches b WHERE b.valassis_key = v.id
            )

            UNION ALL

            -- Unmatched parcel rows
            SELECT
                NULL AS valassis_key,
                NULL AS vericast_house_number,
                NULL AS vericast_street_name,
                NULL AS vericast_street_type,
                p.id AS parcel_id,
                p.normalized_house_number AS parcel_house_number,
                p.normalized_street_name AS parcel_street_name,
                p.normalized_street_type AS parcel_street_type,
                NULL::float AS match_score
            FROM {OUT_SCHEMA}.{PARCELS_NORM} p
            WHERE NOT EXISTS (
                SELECT 1 FROM best_matches b WHERE b.parcel_id = p.id
            )
        """))

        counts = conn.execute(text(f"""
            SELECT
                COUNT(*) FILTER (WHERE match_score IS NOT NULL) AS matched,
                COUNT(*) FILTER (WHERE parcel_id IS NULL) AS unmatched_vericast,
                COUNT(*) FILTER (WHERE valassis_key IS NULL) AS unmatched_parcels
            FROM {OUT_SCHEMA}.{WRITE_TABLE}
        """)).first()

    logger.info(
        "Matched: %d, Unmatched vericast: %d, Unmatched parcels: %d",
        counts[0], counts[1], counts[2],
    )

    # Step 4: Index output, cleanup temp tables
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

    return TaskResult(task_name="vacancy_parcel_link", rows_inserted=counts[0], success=True)
