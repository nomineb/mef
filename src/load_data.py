import pandas as pd
import numpy as np
import sqlalchemy as sa
import warnings
from pathlib import Path
from dask import dataframe as dd

from filepaths import downloads_folder, outputs_folder
from logging_util import get_logger

from pudl.metadata.fields import apply_pudl_dtypes

logger = get_logger(__name__)

# initialize the pudl_engine
PUDL_ENGINE = sa.create_engine("sqlite:///" + downloads_folder("pudl.sqlite"))

KG_IN_LB = 0.453592
KG_IN_TON = 907.185

OLD_NERC =["WSCC", "ERCOT", "UNK", "MAAC", "ECAR", "MAIN", "MAPP", "ASCC", "HICC"]

def load_cems_data(year):
    """
    Loads CEMS data for the specified year from the PUDL database
    Inputs:
        year: the year for which data should be retrieved (YYYY)
    Returns:
        cems: pandas dataframe with hourly CEMS data
    """

    # specify the columns to use from the CEMS database
    cems_columns = [
        "plant_id_eia",
        "plant_id_epa", 
        "operating_datetime_utc",
        "gross_load_mw",
        "co2_mass_tons",
        "nox_mass_lbs",
        "so2_mass_lbs",
        "state"
    ]

    # load the CEMS data
    cems = pd.read_parquet(
        downloads_folder("hourly_emissions_epacems.parquet"),
        filters=[ ("year", "in", [year-1, year])],
        columns=cems_columns
    )
    cems['operating_datetime_utc'] = pd.to_datetime(cems['operating_datetime_utc'])
    cems = cems[cems['operating_datetime_utc'].dt.year == year]


    # convert co2 mass in tons to kg, so2 and nox mass in lb to kg
    cems = convert_to_kg(cems, 'lbs', KG_IN_LB)
    cems = convert_to_kg(cems, 'tons', KG_IN_TON)

    # merge eia table for spatial aggregation purposes (NERC, ISO/RTO)
    eia_plant = load_eia_plants()
    cems = pd.merge(eia_plant, cems, on=["plant_id_eia"], how="right")

    # merge egrid data for eGRID subregion association 
    egrid_data = load_egrid()
    cems = pd.merge(egrid_data, cems, on=["plant_id_epa"], how="right")

    return cems

def load_eia_plants():
    plants_eia = pd.read_sql("out_eia__yearly_plants", PUDL_ENGINE).convert_dtypes(convert_floating=False)
    plants_eia_filtered = plants_eia[[
        "plant_id_eia",
        "nerc_region", 
        "iso_rto_code"
        ]].drop_duplicates()
    
    plants_eia_filtered.dropna(inplace=True)
    return plants_eia_filtered

def load_egrid():
    egrid_cols = [
        "ORISPL", "SUBRGN"
    ]

    egrid = pd.read_excel(
        downloads_folder(f"egrid/egrid2022_data.xlsx"),
        sheet_name=f"PLNT22",
        header=1,
        usecols=egrid_cols,
    )

    egrid_data = egrid.rename(columns={egrid_cols[0]: 'plant_id_epa', egrid_cols[1]:'egrid_subregions'})

    return egrid_data


def load_pudl_table(
    table_name: str, year: int = None, columns: list[str] = None, end_year: int = None
):
    """
    Loads a table from the pudl database.

    `table_name` must be one of the options specified in the data dictionary:
    https://catalystcoop-pudl.readthedocs.io/en/latest/data_dictionaries/pudl_db.html

    There are multiple options for date filtering:
        - if `year` is not specified, all years will be loaded
        - if `year` is specified, but not `end_year`, only a single year will be loaded
        - if both `year` and `end_year` are specified, all years in that range inclusive
          will be loaded. `end_year` must be >= `year`.

    If a list of `columns` is passed, only those columns will be returned. Otherwise,
    all columns will be returned.
    """

    if columns is None:
        columns_to_select = "*"
    else:
        columns_to_select = ", ".join(columns)

    if year is None:
        # load the table without filtering dates
        table = pd.read_sql(
            f"SELECT {columns_to_select} FROM {table_name}",
            PUDL_ENGINE,
        )
    elif year is not None and end_year is None:
        # load the table for a single year
        table = pd.read_sql(
            f"SELECT {columns_to_select} FROM {table_name} WHERE \
                report_date >= '{year}-01-01' AND report_date < '{year + 1}-01-01'",
            PUDL_ENGINE,
        )
    else:
        # load the data for the specified years
        table = pd.read_sql(
            f"SELECT {columns_to_select} FROM {table_name} WHERE \
                report_date >= '{year}-01-01' AND report_date < '{end_year + 1}-01-01'",
            PUDL_ENGINE,
        )

    table = apply_pudl_dtypes(table)

    return table


    
def convert_to_kg(df, unit_label, conversion_factor):
    old_unit_cols = [x for x in df.columns if unit_label in x]
    df[old_unit_cols] = df[old_unit_cols] * conversion_factor
    df.columns = [x.replace(unit_label, 'kg') for x in df.columns]
    return df