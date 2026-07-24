import logging

from tqdm import tqdm
from sqlalchemy import Engine, text
import pandas as pd

from dlia_etl.registry import task, TaskResult
from dlia_etl.resumable import resumable_chunks, count_remaining

from dressy import Dressy


logger = logging.getLogger(__name__)

WRITE_TABLE = "vericast_geocode"
WRITE_SCHEMA = "dlia"
JOIN_KEYS = ["valassis_key", "start_date", "end_date"]


@task("vacancy_geocode_recovery", phase=2,
      description="Re-geocode unresolved vacancy rows using Dressy cache only (no external API calls)")
def run(source: Engine, target: Engine) -> TaskResult:
    chunksize = 5000

    # Delete unresolved rows so they show up as "unprocessed" in the anti-join
    with target.begin() as conn:
        deleted = conn.execute(text(
            f"DELETE FROM {WRITE_SCHEMA}.{WRITE_TABLE} WHERE geocode_method = 'unresolved'"
        )).rowcount
    logger.info("Deleted %d unresolved rows from %s.%s", deleted, WRITE_SCHEMA, WRITE_TABLE)

    # Now use resumable_chunks to re-read those source rows
    remaining = count_remaining(
        source, "vericast", target, WRITE_TABLE, JOIN_KEYS,
        source_schema=WRITE_SCHEMA, target_schema=WRITE_SCHEMA,
    )
    total_chunks = (remaining + chunksize - 1) // chunksize

    with Dressy() as d:
        rows_inserted = 0
        chunks = resumable_chunks(
            source_engine=source,
            source_table="vericast",
            target_engine=target,
            target_table=WRITE_TABLE,
            join_keys=JOIN_KEYS,
            chunksize=chunksize,
            source_schema=WRITE_SCHEMA,
            target_schema=WRITE_SCHEMA,
        )
        for chunk in tqdm(chunks, total=total_chunks):
            chunk = chunk[chunk["street_name"].str.strip() != "PO BOX"].copy()
            chunk["full_street"] = (
                chunk["street_pre_directional"]
                + chunk["street_name"]
                + chunk["street_post_directional"]
            )

            gced = d.geocode_df(
                chunk,
                column=None,
                columns={
                    "house_number": "street_num",
                    "street_name":  "full_street",
                    "street_type":  "street_suffix",
                    "city":         "city_name",
                    "state":        "state_code",
                    "zip_code":     "zip_code",
                },
                cache_only=True,
            )

            gced.to_sql(
                WRITE_TABLE, target, schema=WRITE_SCHEMA, index=False,
                if_exists="append"
            )
            rows_inserted += len(gced)

    return TaskResult(task_name="vacancy_geocode_recovery", rows_inserted=rows_inserted, success=True)
