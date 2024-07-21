import math
import pandas as pd
import numpy as np
import shutil
import os
from pathlib import Path

import load_data as load_data
import column_checks as column_checks
from filepaths import outputs_folder, results_folder, data_folder
from logging_util import get_logger

logger = get_logger(__name__)

def output_intermediate_data(df, file_name, path_prefix, year, skip_outputs):
    path = outputs_folder(f"{path_prefix}{file_name}_{year}.csv")
    if Path(path).exists():
        logger.info("File already exists, skipping")
    else:
        if not skip_outputs:
            logger.info(f"Exporting {file_name} to data/outputs")
            df.to_csv(outputs_folder(f"{path_prefix}{file_name}_{year}.csv"), index=False)

def output_aggregated_data(
        df,
        path_prefix,
        year,
        skip_outputs):
    """Loads in cleaned CEMS data and outputs different csv files each aggregated by
    different regions. The regions are ISO/RTO, NERC, eGRID regions and state.

    Args:
        df (DataFrame): cleaned CEMS df
    """

    agg_region = ['iso_rto_code', 'nerc_region', 'egrid_subregions', 'state']
    for region in agg_region:
        logger.info(f"Exporting dfs to data/outputs, aggregated by {region}")
        agg_df = get_agg_df(df, region)
        diffs = get_diffs(agg_df, region)
        agg_df.reset_index(inplace=True)
        diffs.reset_index(inplace=True)
        output_intermediate_data(agg_df, f"cems_{region}", path_prefix, year, skip_outputs)
        output_intermediate_data(diffs, f"cems_diffs_{region}", path_prefix, year, skip_outputs)


def get_agg_df(df, agg_region):
    """Aggregates the df by agg_region

    Args:
        df (DataFrame): Cleaned CEMS data
        agg_region (str): The region to aggregate by

    Returns:
        DataFrame: Aggregated df
    """
    columns = ['operating_datetime_utc','so2_mass_kg', 'nox_mass_kg', 'co2_mass_kg', 'gross_load_mw']
    agg_df = df[[agg_region] + columns].copy()
    agg_df[agg_region] = agg_df[agg_region].replace('<NA>', np.NaN)
    agg_df.dropna(subset=[agg_region], inplace=True)
    agg_df['operating_datetime_utc'] = pd.to_datetime(agg_df['operating_datetime_utc'])
    agg_df = agg_df.groupby(['operating_datetime_utc', agg_region]).sum()
    #agg_df.reset_index(inplace=True)
    return agg_df

def get_diffs(df, agg_region):
    """Returns a df of the differences between each row in df

    Args:
        df (DataFrame): Aggregated df
        agg_region (str): The region to aggregated by
    """

    # Debug mode, make sure at each region what it's accessing (maybe pivot)

    df = df.reset_index().set_index([agg_region, 'operating_datetime_utc']).sort_index()
    
    # Take diffs and correct "spillover" between boundaries of regions
    diffs = df.diff().reset_index()
    mask = diffs[agg_region] != diffs[agg_region].shift(1)
    diffs[mask] = np.nan

    # Rearrange back to being sorted by date, then region
    diffs = diffs.set_index(['operating_datetime_utc', agg_region]).sort_index()

    # Drop any null diffs
    diffs = diffs.dropna(how='all')

    return diffs