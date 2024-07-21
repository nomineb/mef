"""
Adapted from Open Grid Emissions

Entry point for creating final dataset and intermediate cleaned data products.

Run from `src` as `python data_pipeline.py` after installing conda environment

Optional arguments are --year (default 2022), --shape_individual_plants (default True)
"""

import argparse
import os
import shutil

# import local modules
import download_data 
from data_cleaning import clean_cems
import output_data
from filepaths import downloads_folder, outputs_folder, results_folder
from logging_util import get_logger, configure_root_logger



def get_args() -> argparse.Namespace:
    """Specify arguments here.

    Returns dictionary of {arg_name: arg_value}
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="Year for analysis", default=2022, type=int)

    parser.add_argument(
        "--skip_outputs",
        help="Skip outputting data to csv files for quicker testing.",
        default=False,
        action=argparse.BooleanOptionalAction,
    )

    args = parser.parse_args()

    return args


def print_args(args: argparse.Namespace, logger):
    """Print out the command line arguments."""
    argstring = "\n".join([f"  * {k} = {v}" for k, v in vars(args).items()])
    logger.info(f"\n\nRunning with the following options:\n{argstring}\n")


def main(args):
    """Runs the data pipeline."""

    year = args.year

    # 0. Set up directory structure
    path_prefix = f"{year}/"
    os.makedirs(downloads_folder(), exist_ok=True)
    os.makedirs(outputs_folder(f"{path_prefix}"), exist_ok=True)

    # If we are outputing, wipe results dir so we can be confident there are no old result files (eg because of a file name change)
    if os.path.exists(results_folder(f"{path_prefix}")):
        shutil.rmtree(results_folder(f"{path_prefix}"))
    os.makedirs(results_folder(f"{path_prefix}"), exist_ok=False)

    # configure the logger
    # Log the print statements to a file for debugging.
    configure_root_logger(
        logfile=results_folder(f"{year}/data_quality_metrics/data_pipeline.log")
    )
    logger = get_logger("data_pipeline")
    print_args(args, logger)

    logger.info(f"Running data pipeline for year {year}")

    # 1. Download data
    ####################################################################################
    logger.info("1. Downloading data")
    # PUDL
    download_data.download_pudl_data(source="aws")
    # eGRID 2022
    download_data.download_egrid_files()
    

    # 2. Clean Hourly Data from CEMS
    ####################################################################################
    logger.info("2. Cleaning CEMS data")
    cems = clean_cems(year)

    # 3. Output cleaned cems data
    ####################################################################################
    logger.info("3. Outputting CEMS data")
    output_data.output_intermediate_data(
        cems,
        "cems_cleaned",
        path_prefix,
        year,
        args.skip_outputs,
    )

    # 3. Aggregate the cleaned CEMS data by different options and save the results in the same folder
    #################################################################################################
    logger.info("4. Outputting CEMS data, aggregated by different regions")
    output_data.output_aggregated_data(
        cems,
        path_prefix,
        year,
        args.skip_outputs,
    )


if __name__ == "__main__":
    import sys
    sys.path.append('/Users/nomio/Documents/Research/MEF/mef/src')
    args = get_args()
    main(args)