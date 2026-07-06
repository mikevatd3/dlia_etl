import json
import pandas as pd
from sqlalchemy import Engine, text
from sqlalchemy.exc import ProgrammingError

from dlia_etl.registry import task, TaskResult
from dlia_etl.config import (
    SOURCE_DIR,
    FIELD_REFERENCE_DIR,
    VAULT_PATH,
    OUT_SCHEMA,
)


TABLE_NAME = "assessors_sales"


@task("assessors_sales", phase=1, description="Property sales records from Detroit Assessors Office")
def run(_: Engine, target: Engine) -> TaskResult:
    datasets = pd.read_csv(SOURCE_DIR / "datasets_assessors_sales.csv")
    
    rows_inserted = 0
    for _, ds in datasets.iterrows():
        field_reference = json.loads(
            (
                FIELD_REFERENCE_DIR / "property_sales" / ds["field_reference"]
            ).read_text()
        )
        
        exists_q = text(f"""
            SELECT COUNT(*)
            FROM {TABLE_NAME}
            WHERE
                start_date = {ds["start_date"]}
                AND end_date = {ds["end_date"]}
        """)

        with target.begin() as conn:
            try:
                row_count = conn.execute(exists_q).scalar()

                if row_count > 0: # type: ignore
                    print(f"Rows for {ds['start_date']} to {ds['end_date']} are already in the table.")
                    print("Delete these rows if you want to re-insert them.")
                    continue

            except ProgrammingError:
                print(f"This is the push to '{TABLE_NAME}'.")
        
        print("Opening document and cleaning.")
        result = (
            pd.read_csv(VAULT_PATH / ds["path"]) # type: ignore
            .rename(columns=field_reference["renames"])
            .assign(
                start_date=ds["start_date"],
                end_date=ds["end_date"],
            )
        )[field_reference["out_cols"]]
        
        print("Pushing to the database.")
        result.to_sql(
            TABLE_NAME,
            target,
            schema=OUT_SCHEMA,
            index=False,
            if_exists="append"
        )

        rows_inserted += len(result)


    return TaskResult(
        task_name="assessors_sales",
        rows_inserted=rows_inserted,
        success=True
    )

