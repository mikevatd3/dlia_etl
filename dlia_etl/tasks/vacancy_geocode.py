from tqdm import tqdm
from sqlalchemy import Engine
import pandas as pd

from dlia_etl.registry import task, TaskResult

from dressy import Dressy


WRITE_TABLE = "vericast_geocode"
WRITE_SCHEMA = "dlia"


@task("vacancy_geocode", phase=2, description="Geocode all the vacancy rows (using dressy for caching)")
def run(source: Engine, target: Engine) -> TaskResult:
    q = """SELECT * FROM dlia.vericast;"""


    with Dressy() as d: # Automatically connects to the db in .env
        print("Dressy was initialized")

        # Chunked iteration
        if_exists="replace"
        rows_inserted = 0
        for chunk in tqdm(pd.read_sql(q, source, chunksize=5000)):
            
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
                if_exists=if_exists
            )
            rows_inserted += len(gced)
            if_exists="append"

        return TaskResult(task_name="vacancy_geocode", rows_inserted=rows_inserted, success=True)
