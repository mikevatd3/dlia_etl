from tqdm import tqdm
from sqlalchemy import Engine, text
import pandas as pd

from dlia_etl.registry import task, TaskResult
from dlia_etl.config import OUT_SCHEMA


TABLE_NAME = "vericast_states"

# Columns that can change across quarters for the same valassis_key.
STATE_COLS = [
    "address_vacancy_indicator",
    "throw_back_indicator",
    "seasonal_delivery_indicator",
    "drop_indicator",
    "delivery_point_usage_code",
    "record_type_code",
    "no_stat__new_growth",
    "no_stat__vacant",
    "no_stat__throwback",
    "no_stat__drop_apartments",
    "general_nostat_indicator",
    "route_num",
    "walk_sequence",
    "congressional_district_number",
]


@task("vacancy_states", phase=1, description="Gap-and-island collapse of vericast address states with date ranges")
def run(source: Engine, target: Engine) -> TaskResult:
    # Build the change-detection expression: 1 when any state column
    # differs from the previous row for the same valassis_key
    lag_checks = " OR ".join(
        f"{col} IS DISTINCT FROM LAG({col}) OVER (PARTITION BY valassis_key ORDER BY start_date)"
        for col in STATE_COLS
    )

    state_cols_select = ", ".join(STATE_COLS)

    q = f"""
        WITH ordered AS (
            SELECT
                valassis_key,
                start_date,
                end_date,
                street_num,
                street_pre_directional,
                street_name,
                street_post_directional,
                street_suffix,
                city_name,
                state_code,
                zip_code,
                {state_cols_select},
                dnd_indicator,
                college_indicator,
                address_style_flag,
                simplify_address_count,
                CASE WHEN {lag_checks} THEN 1 ELSE 0 END AS state_changed
            FROM dlia.vericast
        ),
        grouped AS (
            SELECT *,
                SUM(state_changed) OVER (
                    PARTITION BY valassis_key ORDER BY start_date
                ) AS state_group
            FROM ordered
        )
        SELECT
            valassis_key,
            MIN(start_date) AS start_date,
            MAX(end_date) AS end_date,
            -- address fields (stable per key)
            MIN(street_num) AS street_num,
            MIN(street_pre_directional) AS street_pre_directional,
            MIN(street_name) AS street_name,
            MIN(street_post_directional) AS street_post_directional,
            MIN(street_suffix) AS street_suffix,
            MIN(city_name) AS city_name,
            MIN(state_code) AS state_code,
            MIN(zip_code) AS zip_code,
            -- state columns (identical within group)
            {", ".join(f"MIN({col}) AS {col}" for col in STATE_COLS)},
            -- stable columns
            MIN(dnd_indicator) AS dnd_indicator,
            MIN(college_indicator) AS college_indicator,
            MIN(address_style_flag) AS address_style_flag,
            MIN(simplify_address_count) AS simplify_address_count
        FROM grouped
        GROUP BY valassis_key, state_group
    """

    chunksize = 50_000
    rows_inserted = 0
    if_exists = "replace"

    # Upper bound estimate for tqdm
    with source.connect() as conn:
        total_estimate = conn.execute(text(
            "SELECT reltuples::bigint FROM pg_class "
            "WHERE oid = CAST(:tbl AS regclass)"
        ), {"tbl": f"{OUT_SCHEMA}.vericast"}).scalar() or 0
    total_chunks = (total_estimate + chunksize - 1) // chunksize or None

    with source.connect().execution_options(stream_results=True) as conn:
        for chunk in tqdm(pd.read_sql(text(q), conn, chunksize=chunksize), total=total_chunks):
            chunk.to_sql(
                TABLE_NAME, target, schema=OUT_SCHEMA,
                index=False, if_exists=if_exists,
            )
            rows_inserted += len(chunk)
            if_exists = "append"

    with target.begin() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_valassis_key "
            f"ON {OUT_SCHEMA}.{TABLE_NAME} (valassis_key)"
        ))

    return TaskResult(task_name="vacancy_states", rows_inserted=rows_inserted, success=True)
