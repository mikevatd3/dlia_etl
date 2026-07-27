from tqdm import tqdm
from sqlalchemy import Engine, text
import pandas as pd

from dlia_etl.registry import task, TaskResult
from dlia_etl.config import OUT_SCHEMA


TABLE_NAME = "vericast_unique"


@task("vacancy_unique", phase=1, description="Collapse vericast to unique addresses with date ranges")
def run(source: Engine, target: Engine) -> TaskResult:
    q = """
        SELECT
            valassis_key,
            MIN(start_date) AS start_date,
            MAX(end_date) AS end_date,
            -- keep address fields from any row (stable per valassis_key)
            MIN(street_num) AS street_num,
            MIN(street_pre_directional) AS street_pre_directional,
            MIN(street_name) AS street_name,
            MIN(street_post_directional) AS street_post_directional,
            MIN(street_suffix) AS street_suffix,
            MIN(city_name) AS city_name,
            MIN(state_code) AS state_code,
            MIN(zip_code) AS zip_code
        FROM dlia.vericast
        GROUP BY valassis_key
    """

    chunksize = 50_000
    rows_inserted = 0
    if_exists = "replace"

    with source.connect().execution_options(stream_results=True) as conn:
        for chunk in tqdm(pd.read_sql(text(q), conn, chunksize=chunksize)):
            chunk.to_sql(
                TABLE_NAME, target, schema=OUT_SCHEMA,
                index=False, if_exists=if_exists,
            )
            rows_inserted += len(chunk)
            if_exists = "append"

    # Index for downstream geocode joins
    with target.begin() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_valassis_key "
            f"ON {OUT_SCHEMA}.{TABLE_NAME} (valassis_key)"
        ))

    return TaskResult(task_name="vacancy_unique", rows_inserted=rows_inserted, success=True)
