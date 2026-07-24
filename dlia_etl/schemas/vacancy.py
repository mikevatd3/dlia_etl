import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series


class VacancyModel(pa.DataFrameModel):
    zip_code: str = pa.Field(nullable=True)
    route_num: str = pa.Field(nullable=True)
    zip4: str = pa.Field(nullable=True)
    walk_sequence: int = pa.Field(nullable=True)
    street_num: str = pa.Field(nullable=True)
    street_pre_directional: str  = pa.Field(nullable=True)
    street_name: str = pa.Field(nullable=True)
    street_post_directional: str = pa.Field(nullable=True)
    street_suffix: str = pa.Field(nullable=True)
    secondary_unit_designator: str = pa.Field(nullable=True)
    secondary_unit_number: str = pa.Field(nullable=True)
    address_vacancy_indicator: str = pa.Field(nullable=True)
    throw_back_indicator: str = pa.Field(nullable=True)
    seasonal_delivery_indicator: str = pa.Field(nullable=True)
    # seasonal_start_suppression_date: Series[pd.Timestamp] = pa.Field(nullable=True)
    # seasonal_end_suppression_date: Series[pd.Timestamp] = pa.Field(nullable=True)
    dnd_indicator: str = pa.Field(nullable=True)
    college_indicator: str = pa.Field(nullable=True)
    college_start_suppression_date: Series[pd.Timestamp] = pa.Field(nullable=True)
    college_end_suppression_date: Series[pd.Timestamp] = pa.Field(nullable=True)
    address_style_flag: str = pa.Field(nullable=True)
    simplify_address_count: int = pa.Field(nullable=True)
    drop_indicator: str = pa.Field(nullable=True)
    delivery_point_usage_code: str = pa.Field(nullable=True)
    dpbc_digit: int = pa.Field(nullable=True)
    dpbc_check_digit: int = pa.Field(nullable=True)
    update_date: Series[pd.Timestamp] = pa.Field(nullable=True)
    file_release_date: Series[pd.Timestamp] = pa.Field(nullable=True)
    override_file_release_date: Series[pd.Timestamp] = pa.Field(nullable=True)
    county_num: int = pa.Field(nullable=True)
    county_name: str = pa.Field(nullable=True)
    city_name: str = pa.Field(nullable=True)
    state_code: str = pa.Field(nullable=True)
    state_num: int = pa.Field(nullable=True)
    congressional_district_number: str = pa.Field(nullable=True)
    owgm_indicator: str = pa.Field(nullable=True)
    record_type_code: str = pa.Field(nullable=True)
    valassis_key: int = pa.Field(nullable=True)
    address_type: int = pa.Field(nullable=True)
    delivery_point_type_code: str = pa.Field(nullable=True)
    no_stat__new_growth : str = pa.Field(nullable=True)
    no_stat__vacant: str = pa.Field(nullable=True)
    no_stat__throwback: str = pa.Field(nullable=True)
    no_stat__drop_apartments: str = pa.Field(nullable=True)
    general_nostat_indicator: str = pa.Field(nullable=True)
    filler: str = pa.Field(nullable=True)
    start_date: Series[pd.Timestamp] = pa.Field(nullable=True)
    end_date: Series[pd.Timestamp] = pa.Field(nullable=True)

    class Config:
        coerce = True


class VacancyGeocodeModel(pa.DataFrameModel):
    valassis_key: int
    latitude: float = pa.Field(nullable=True)
    longitude: float = pa.Field(nullable=True)
    geocode_method: str = pa.Field(nullable=True)
    confidence: float = pa.Field(nullable=True)

    class Config:
        coerce = True
