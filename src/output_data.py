import math
import pandas as pd
import numpy as np
import shutil
import os

import load_data as load_data
import column_checks as column_checks
from filepaths import outputs_folder, results_folder, data_folder
from logging_util import get_logger

logger = get_logger(__name__)

def output_intermediate_data(df, file_name, path_prefix, year, skip_outputs):
    column_checks.check_columns(df, file_name)
    if not skip_outputs:
        logger.info(f"Exporting {file_name} to data/outputs")
        df.to_csv(outputs_folder(f"{path_prefix}{file_name}_{year}.csv"), index=False)