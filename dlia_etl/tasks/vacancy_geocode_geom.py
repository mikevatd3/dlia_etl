from sqlalchemy import Engine, text

from dlia_etl.registry import task, TaskResult
from dlia_etl.config import OUT_SCHEMA


TABLE_NAME = "vericast_geocode"


@task("vacancy_geocode_geom", phase=3, description="Add PostGIS geometry column to vericast_geocode from lat/lon")
def run(source: Engine, target: Engine) -> TaskResult:
    with target.begin() as conn:
        # Add geom column if it doesn't exist
        conn.execute(text(f"""
            ALTER TABLE {OUT_SCHEMA}.{TABLE_NAME}
            ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326)
        """))

        # Populate from lat/lon where coordinates exist
        result = conn.execute(text(f"""
            UPDATE {OUT_SCHEMA}.{TABLE_NAME}
            SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
            WHERE latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND geom IS NULL
        """))

        # Spatial index
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_geom
            ON {OUT_SCHEMA}.{TABLE_NAME} USING GIST (geom)
        """))

    return TaskResult(
        task_name="vacancy_geocode_geom",
        rows_inserted=result.rowcount,
        success=True,
    )
