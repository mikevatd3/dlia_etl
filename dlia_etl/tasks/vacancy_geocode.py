from tqdm import tqdm
from sqlalchemy import Engine

from dlia_etl.registry import task, TaskResult

from dressy import Dressy


WRITE_TABLE = "vericast_geocode"
WRITE_SCHEMA = "dlia"


@task("vacancy_geocode", phase=2, description="Geocode all the vacancy rows (using dressy for caching)")
def run(source: Engine, target: Engine) -> TaskResult:
    q = """SELECT * FROM dlia.vericast;"""


    with Dressy() as d: # Automatically connects to the db in .env
        print("Dressy was initialized")
        # zip_code,
        # street_num,
        # street_pre_directional,
        # street_name,
        # street_post_directional,
        # street_suffix,
        # city_name,
        # state_code,

        # Chunked iteration
        if_exists="replace"
        rows_inserted = 0
        for chunk in tqdm(d.geocode_sql(
            q,
            con=source,
            chunksize=5,
            column=None,
            columns={
                "house_number": "HOUSE_NO",
                "street_name":  "STREET",
                "street_type":  "ST_TYPE",
                "city":         "CITY",
                "state":        "STATE",
                "zip_code":     "ZIP",
            }
        )):

            print("A chunk was processed, pushing to db.")
            chunk.to_sql(
                WRITE_TABLE, target, schema=WRITE_SCHEMA, index=False,
                if_exists=if_exists
            )
            rows_inserted += len(chunk)
            if_exists="append"

        return TaskResult(task_name="vacancy_geocode", rows_inserted=rows_inserted, success=True)
