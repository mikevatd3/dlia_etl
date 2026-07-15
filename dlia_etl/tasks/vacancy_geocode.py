from tqdm import tqdm
from sqlalchemy import Engine, text
import pandas as pd

from dlia_etl.registry import task, TaskResult
from dlia_etl.resumable import resumable_chunks, count_remaining

from dressy import Dressy


WRITE_TABLE = "vericast_geocode"
WRITE_SCHEMA = "dlia"
JOIN_KEYS = ["valassis_key", "start_date", "end_date"]


@task("vacancy_geocode", phase=2, description="Geocode all the vacancy rows (using dressy for caching)")
def run(source: Engine, target: Engine) -> TaskResult:
    chunksize = 5000

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
            # Remove po boxes
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
                }
            )

            gced.to_sql(
                WRITE_TABLE, target, schema=WRITE_SCHEMA, index=False,
                if_exists="append"
            )
            rows_inserted += len(gced)

    return TaskResult(task_name="vacancy_geocode", rows_inserted=rows_inserted, success=True)
