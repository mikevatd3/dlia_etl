import json
import pandas as pd
from sqlalchemy import Engine
from tqdm import tqdm

from dlia_etl.registry import task, TaskResult
from dlia_etl.config import (
    SOURCE_DIR,
    FIELD_REFERENCE_DIR,
    PROPRIETARY_PATH,
    OUT_SCHEMA,
)
from dlia_etl.schemas.vacancy import VacancyModel

TABLE_NAME = "vericast"


def read_frames_slowly(datasets, chunksize=10_000):
    for _, row in datasets.iterrows():
        path = row["path"]

        field_reference = json.loads(
            (
                FIELD_REFERENCE_DIR / "vacancy" / row["field_reference"]
            ).read_text()
        )
        widths = [w for w, _ in field_reference["widths"]]
        names = [n for _, n in field_reference["widths"]]

        start_date = pd.to_datetime(row["start_date"])
        end_date = pd.to_datetime(row["end_date"])

        for chunk in pd.read_fwf(
            PROPRIETARY_PATH / path,
            chunksize=chunksize,
            widths=widths,
            names=names
        ):

            for col in [
                "seasonal_start_suppression_date",
                "seasonal_end_suppression_date",
                "college_start_suppression_date",
                "college_end_suppression_date",
                "update_date",
                "file_release_date",
                "override_file_release_date",
            ]:
                chunk[col] = pd.to_datetime(chunk[col], errors="coerce")

            chunk = chunk.assign(start_date=start_date, end_date=end_date)
            yield chunk


@task("vacancy", phase=1, description="Property sales records from Detroit Assessors Office")
def run(_: Engine, target: Engine) -> TaskResult:
    datasets = pd.read_csv(SOURCE_DIR / "datasets_vacancy.csv")

    rows_inserted = 0
    if_exists = "replace"
    for chunk in tqdm(read_frames_slowly(datasets)):

        validated = VacancyModel.validate(chunk)

        validated.to_sql(
            TABLE_NAME,
            target,
            schema=OUT_SCHEMA,
            index=False,
            if_exists=if_exists
        )
        rows_inserted += len(chunk)
        if_exists = "append"

    return TaskResult(task_name="vacancy", rows_inserted=rows_inserted, success=True)
