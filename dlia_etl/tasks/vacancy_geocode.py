from tqdm import tqdm
from sqlalchemy import Engine, text
import pandas as pd

from dlia_etl.registry import task, TaskResult
from dlia_etl.resumable import resumable_chunks, count_remaining
from dlia_etl.schemas.vacancy import VacancyGeocodeModel

from dressy import Dressy


SOURCE_TABLE = "vericast_unique"
WRITE_TABLE = "vericast_geocode"
WRITE_SCHEMA = "dlia"
JOIN_KEYS = ["valassis_key"]
PO_BOX_FILTER = "TRIM(s.street_name) != 'PO BOX'"


@task("vacancy_geocode", phase=2, description="Geocode all the vacancy rows (using dressy for caching)")
def run(source: Engine, target: Engine) -> TaskResult:
    chunksize = 5000

    remaining = count_remaining(
        source, SOURCE_TABLE, target, WRITE_TABLE, JOIN_KEYS,
        source_schema=WRITE_SCHEMA, target_schema=WRITE_SCHEMA,
    )
    total_chunks = (remaining + chunksize - 1) // chunksize

    with Dressy() as d:
        rows_inserted = 0
        chunks = resumable_chunks(
            source_engine=source,
            source_table=SOURCE_TABLE,
            target_engine=target,
            target_table=WRITE_TABLE,
            join_keys=JOIN_KEYS,
            chunksize=chunksize,
            source_schema=WRITE_SCHEMA,
            target_schema=WRITE_SCHEMA,
            where=PO_BOX_FILTER,
        )
        for chunk in tqdm(chunks, total=total_chunks):
            chunk["full_street"] = (
                chunk["street_pre_directional"].fillna("")
                + chunk["street_name"].fillna("")
                + chunk["street_post_directional"].fillna("")
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

            gced = gced[[
                "valassis_key",
                "latitude",
                "longitude",
                "geocode_method",
                "confidence",
            ]]

            validated = VacancyGeocodeModel.validate(gced)

            validated.to_sql(
                WRITE_TABLE, target, schema=WRITE_SCHEMA, index=False,
                if_exists="append"
            )
            rows_inserted += len(gced)

    _build_geom(target)

    return TaskResult(task_name="vacancy_geocode", rows_inserted=rows_inserted, success=True)


def _build_geom(engine: Engine):
    """Add and populate a PostGIS geometry column from lat/lon."""
    with engine.begin() as conn:
        conn.execute(text(f"""
            ALTER TABLE {WRITE_SCHEMA}.{WRITE_TABLE}
            ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326)
        """))
        conn.execute(text(f"""
            UPDATE {WRITE_SCHEMA}.{WRITE_TABLE}
            SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
            WHERE latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND geom IS NULL
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{WRITE_TABLE}_geom
            ON {WRITE_SCHEMA}.{WRITE_TABLE} USING GIST (geom)
        """))
