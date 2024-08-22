import argparse
import os
import pandas as pd
import numpy as np
from scipy.stats import linregress as lm

import ipdb
import sys
from IPython.core import ultratb


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--save', default='../data/results/')
    parser.add_argument('--factorType', choices=['average', 'marginal'], required=True,
        help='type of factor to compute')
    parser.add_argument('--year', type=int, default=2017)
    args = parser.parse_args()

    # Read and process data
    print('Getting data')
    rto_df, nerc_df, state_df, egrid_df = read_process_data(args.factorType, args.year)

    # # Calculate factors and save results
    print('Calculating factors')
    save = args.save
    year = args.year
    calc_factors = calculate_mefs if args.factorType == 'marginal' else calculate_aefs
    grouping_names = ['SeasonalTOD', 'MonthTOD', 'TOD', 'YearOnly', 'Month', 'TimeSeries', 'TimeSeriesHour']
    grouping_cols = [['year', 'season', 'hour'], ['year', 'month', 'hour'], 
        ['year', 'hour'], ['year'], ['year', 'month'], ['month', 'weektype', 'hour'], ['month', 'hour']]

    for grouping_name, grouping in zip(grouping_names, grouping_cols):
        print('{}:'.format(grouping_name))
        print('ISO/RTO...')
        calc_factors(rto_df, 'iso_rto_code', grouping + ['iso_rto_code'], grouping_name, save, year)
        print('NERC...')
        calc_factors(nerc_df, 'nerc_region', grouping + ['nerc_region'], grouping_name, save, year)
        print('State...')
        calc_factors(state_df, 'state', grouping + ['state'], grouping_name, save, year)
        print('eGRID...')  
        calc_factors(egrid_df, 'egrid_subregions', grouping + ['egrid_subregions'], grouping_name, save, year)

    if args.factorType == 'average':
        grouping = ['operating_datetime_utc']
        print('Hour:')
        print('ISO/RTO...')
        calculate_aefs_hourly(rto_df, 'iso_rto_code', grouping + ['iso_rto_code'], save, year)
        print('NERC...')
        calculate_aefs_hourly(nerc_df, 'nerc_region', grouping + ['nerc_region'], save, year)
        print('State...')
        calculate_aefs_hourly(state_df, 'state', grouping + ['state'], save, year)
        print('eGRID...')
        calculate_aefs_hourly(egrid_df, 'egrid_subregions', grouping + ['egrid_subregions'], save, year)



# Global variables for regression labels and x-column name
LABELS = ['so2_mass_kg', 'nox_mass_kg', 'co2_mass_kg']
XCOL = 'gross_load_mw'

# Global variables for AP2 vs. EASIUR columns
DAM_COLS_AP2 = ['co2_dam', 'so2_dam_ap2', 'nox_dam_ap2', 'pm25_dam_ap2']
DAM_COLS_EAS = ['co2_dam', 'so2_dam_eas', 'nox_dam_eas', 'pm25_dam_eas']

def read_process_data(factor_type, year):
    """
    Read and process data for a given factor type and year.

    Args:
        factor_type (str): The type of factor to be processed. It can be either 'average' or 'diffs'.
        year (int): The year for which the data needs to be processed.

    Returns:
        Tuple[pd.DataFrame]: A tuple containing the following dataframes:
            - rto_df: Generation and emissions aggregated by ISO/RTO.
            - nerc_df: Generation and emissions aggregated by NERC region.
            - state_df: Generation and emissions aggregated by state.
            - egrid_df: Generation and emissions aggregated by eGRID region.
    """
    filename_add = '' if factor_type == 'average' else '_diffs'

    # Generation, and emissions aggregated by ISO/RTO
    rto_df = pd.read_csv(
        os.path.join(os.pardir, 'data', 'outputs', str(year), 
            f'cems{filename_add}_iso_rto_code_{year}.csv'),
        index_col=0, parse_dates=[0])

    # Generation, and emissions aggregated by NERC region
    nerc_df = pd.read_csv(
        os.path.join(os.pardir, 'data', 'outputs', str(year), 
            f'cems{filename_add}_nerc_region_{year}.csv'), 
        index_col=0, parse_dates=[0])
    
    # Generation, and emissions aggregated by eGRID region
    egrid_df = pd.read_csv(
        os.path.join(os.pardir, 'data', 'outputs', str(year), 
            f'cems{filename_add}_egrid_subregions_{year}.csv'), 
        index_col=0, parse_dates=[0])
    
    # Generation, and emissions aggregated by state
    state_df = pd.read_csv(
        os.path.join(os.pardir, 'data', 'outputs', str(year), 
            f'cems{filename_add}_state_{year}.csv'), 
        index_col=0, parse_dates=[0])
    
    rto_df = label_temporal_groupings(rto_df)
    nerc_df = label_temporal_groupings(nerc_df) 
    state_df = label_temporal_groupings(state_df)
    egrid_df = label_temporal_groupings(egrid_df)
    
    return rto_df, nerc_df, state_df, egrid_df


def label_temporal_groupings(df):
    """
    Label temporal groupings in the given dataframe.

    Args:
        df (pd.DataFrame): The dataframe to label temporal groupings for.

    Returns:
        pd.DataFrame: The dataframe with labeled temporal groupings.
    """
    # Copy df to not change in place
    df = df.copy()

    # Get year, month, hour
    df['year'] = df.index.year
    df['month'] = df.index.month
    df['hour'] = df.index.hour
    df['weektype'] = df.index.weekday.map(lambda x: 'weekday' if x < 5 else 'weekend')

    # Get season
    #  Summer = May-Sept
    #  Winter = Dec-Mar
    #  Transition = Apr, Oct
    month_to_season = ['winter'] * 3 + ['trans'] + ['summer'] * 5 + ['trans'] + ['winter'] * 2
    df['season'] = df.index.map(lambda x: month_to_season[x.month - 1])

    return df


def calculate_mefs(df, df_name, grouping, grouping_name, save, year):

    def calc_mefs_helper(data):
        x = data[XCOL].values
        y = data[LABELS]
        # Run regression for each column and store results
        results = {label: lm(x, y[label].values) for label in LABELS}
        return results

    # Get regression results
    results_df = factor_calculation_helper(df, grouping, calc_mefs_helper)
    results_df = format_regression_results(results_df)
    #results_df = sum_damages(results_df, 'MEF')
    
    # Save factors
    dirname = os.path.join(save, str(year),'mefs', grouping_name)
    if not os.path.exists(dirname): os.makedirs(dirname)
    results_df.to_csv(os.path.join(dirname, '{}_mefs.csv'.format(df_name)))


def calculate_aefs(df, df_name, grouping, grouping_name, save, year):

    def calc_aefs_helper(data):
        sums = data[[XCOL]+LABELS].dropna().sum()
        results = sums[LABELS] / sums[XCOL]

        return results

    # Get calculated AEFs
    results_df = factor_calculation_helper(df, grouping, calc_aefs_helper)
    results_df = sum_damages(results_df, 'AEF')
    
    # Save factors
    dirname = os.path.join(save, str(year), 'aefs', grouping_name)
    if not os.path.exists(dirname): os.makedirs(dirname)
    results_df.to_csv(os.path.join(dirname, '{}_aefs.csv'.format(df_name)))

def calculate_aefs_hourly(df, df_name, grouping, save, year):

    # Divide emissions/damages by generation, preserving index information 
    df = df.reset_index().set_index(grouping)
    # results_df = df[LABELS] / df[XCOL]
    results_df = df[LABELS].apply(lambda x: x / df[XCOL])
    results_df = sum_damages(results_df, 'AEF')

    # Save factors
    dirname = os.path.join(save, str(year), 'aefs', 'Hour')
    if not os.path.exists(dirname): os.makedirs(dirname)
    results_df.to_csv(os.path.join(dirname, '{}_aefs.csv'.format(df_name)))


def factor_calculation_helper(df, grouping, calc_fn):
    df = df.dropna()
    groups = df.groupby(grouping)

    # Calculate factor within each group
    results_dict = {}
    for name, data in groups:
        if np.all(data[XCOL].values == data[XCOL].values[0]):
            continue
        results_dict[name] = calc_fn(data)

    # Format results into one data frame
    results_df = pd.DataFrame.from_dict(results_dict, orient='index')
    results_df.index.names = grouping
    return results_df


def format_regression_results(results_df):
    # Extract slopes, standard error, r-value, and intercept
    stats_list = []
    stats_fns = [lambda x: x.slope, lambda x: x.stderr, lambda x: x.rvalue, lambda x: x.intercept, lambda x: x.pvalue]
    stats_labels = ['est', 'se', 'r', 'int', "p"]
    for fn, label in zip(stats_fns, stats_labels):
        df = results_df.map(fn).add_suffix('-{}'.format(label))
        stats_list.append(df)

    # Concatenate extracted values and sort columns in order
    sep_results_df = pd.concat(stats_list, axis=1)
    col_order = np.array(
        ['{0}-est,{0}-se,{0}-r,{0}-int,{0}-p'.format(x).split(',') for x in LABELS]).flatten()
    return sep_results_df.reindex(col_order, axis=1)


def sum_damages(df, factor_type):
    # For each of AP2 and EASIUR, get total damage factor and add to df
    for cols, col_type in zip([DAM_COLS_AP2, DAM_COLS_EAS], ['ap2', 'eas']):
        df = sum_damages_helper(df, cols, col_type, factor_type)
    return df

def sum_damages_helper(df, dam_cols, dam_type, factor_type):
    # For MEFs, total factor is sum of ests, and SE is sqrt of sum of squares of SEs
    #   For AEFs, simply sum ests to get total factor
    if factor_type == 'MEF':
        total_dam_est = df[['{}-est'.format(x) for x in dam_cols]].sum(axis=1)
        total_dam_se = np.sqrt((df[['{}-se'.format(x) for x in dam_cols]] ** 2).sum(axis=1))
        total_dam_cols = pd.concat([total_dam_est, total_dam_se], axis=1)
        total_dam_cols.columns = ['dam_{}-est'.format(dam_type), 'dam_{}-se'.format(dam_type)]
    else:
        total_dam_cols = pd.DataFrame(df[dam_cols].sum(axis=1))
        total_dam_cols.columns = ['dam_{}'.format(dam_type)]
    df = pd.concat([df, total_dam_cols], axis=1)
    return df


if __name__=='__main__':
    main()
