import marimo

__generated_with = "0.8.0"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import os
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import datetime
    import seaborn as sns
    from datetime import timedelta
    import pytz
    return datetime, mo, np, os, pd, plt, pytz, sns, timedelta


@app.cell
def __(os):
    available_results = os.listdir("data/results/")
    return available_results,


@app.cell
def __(available_results, mo):
    year_selector = mo.ui.dropdown(
        options=available_results,
        value=available_results[0],
        label="Select the year"
    )
    year_selector
    return year_selector,


@app.cell
def __(year_selector):
    year = year_selector.value
    return year,


@app.cell
def __(mo, os, year):
    try:
        available_groups = os.listdir(f"data/results/{year}/mefs")
        out = mo.md("Reading mefs!")
    except FileNotFoundError as e:
        out = mo.md(f"!!!!ERROR!!!! Run Marginal emissions calculations for the year : {year}")
    out
    return available_groups, out


@app.cell
def __(available_groups, mo):
    group_selector = mo.ui.dropdown(
        options=available_groups,
        value=available_groups[0],
        label="Select the groups"
    )
    group_selector
    return group_selector,


@app.cell
def __(mo):
    region_range_selector = mo.ui.dropdown(
        options={"ISO/RTO": "iso_rto_code",
                 "NERC": "nerc_region",
                 "State": "state",
                 "eGrid": "egrid_subregions"},
        value="ISO/RTO",
        label="Select the region groups"
    )
    region_range_selector
    return region_range_selector,


@app.cell
def __(group_selector, pd, region_range_selector, year):
    region = region_range_selector.value
    group = group_selector.value
    mef_data = pd.read_csv(f"data/results/{year}/mefs/{group}/{region}_mefs.csv")
    return group, mef_data, region


@app.cell
def __(mef_data, mo):
    mo.ui.data_explorer(mef_data)
    return


@app.cell
def __(mef_data, mo, region, region_range_selector):
    mo.md(
        f"""
        Unique values for the selected region groups: {region_range_selector.selected_key}

        {mef_data[region].unique()}
        """
    )
    return


@app.cell
def __(mef_data, mo, region):
    subregion = mo.ui.dropdown(
        options=mef_data[region].unique(),
        value=mef_data[region].unique()[0],
        label="Select Sub Region"
    )
    subregion
    return subregion,


@app.cell
def __(
    convert_utc_to_timezone,
    mef_data,
    region,
    subregion,
    timezone_dict,
):
    grouped_mef = mef_data[(mef_data[region] == subregion.value)].reset_index()
    if "hour" in grouped_mef.columns:
        grouped_mef["hour"] = grouped_mef["hour"].astype(int)
        # apply the timezone conversion
        grouped_mef["hour"] = grouped_mef.apply(lambda x: convert_utc_to_timezone(x["hour"], timezone_dict[region][subregion.value]), axis=1)
        # sort the data by hour
        grouped_mef = grouped_mef.sort_values(["month", "hour"])
    return grouped_mef,


@app.cell
def __(grouped_mef):
    grouped_mef
    return


@app.cell
def __(mo):
    ### plot the average marginal emissions for each month over the hour
    mo.md(r"""### Average Marginal Emissions Intensity (kg/MWh) by Month""")
    return


@app.cell
def __():
    timezone_dict = {
        "iso_rto_code": {
        'CAISO': 'America/Los_Angeles',  # Pacific Time (PT)
        'ERCOT': 'America/Chicago',      # Central Time (CT)
        'ISONE': 'America/New_York',     # Eastern Time (ET)
        'MISO': 'America/Chicago',       # Central Time (CT)
        'NYISO': 'America/New_York',     # Eastern Time (ET)
        'OTHER': 'America/New_York',     # Default to Eastern Time (ET)
        'PJM': 'America/New_York',       # Eastern Time (ET)
        'SPP': 'America/Chicago'         # Central Time (CT)
        },
        "nerc_region": {
        'MRO': 'America/Chicago',   # Central Time (CT)
        'NPCC': 'America/New_York', # Eastern Time (ET)
        'RFC': 'America/New_York',  # Eastern Time (ET)
        'SERC': 'America/New_York', # Eastern Time (ET)
        'SPP': 'America/Chicago',   # Central Time (CT)
        'TRE': 'America/Chicago',   # Central Time (CT)
        'WECC': 'America/Denver'    # Mountain Time (MT) / Pacific Time (PT)
        },
        "state": {
        'AK': 'America/Anchorage',    # Alaska Time (AKT)
        'AL': 'America/Chicago',      # Central Time (CT)
        'AR': 'America/Chicago',      # Central Time (CT)
        'AZ': 'America/Phoenix',      # Mountain Standard Time (MST, no DST)
        'CA': 'America/Los_Angeles',  # Pacific Time (PT)
        'CO': 'America/Denver',       # Mountain Time (MT)
        'CT': 'America/New_York',     # Eastern Time (ET)
        'DE': 'America/New_York',     # Eastern Time (ET)
        'FL': 'America/New_York',     # Eastern Time (ET)
        'GA': 'America/New_York',     # Eastern Time (ET)
        'IA': 'America/Chicago',      # Central Time (CT)
        'ID': 'America/Boise',        # Mountain Time (MT)
        'IL': 'America/Chicago',      # Central Time (CT)
        'IN': 'America/Indiana/Indianapolis', # Mostly Eastern Time (ET)
        'KS': 'America/Chicago',      # Central Time (CT)
        'KY': 'America/New_York',     # Eastern Time (ET)
        'LA': 'America/Chicago',      # Central Time (CT)
        'MA': 'America/New_York',     # Eastern Time (ET)
        'MD': 'America/New_York',     # Eastern Time (ET)
        'ME': 'America/New_York',     # Eastern Time (ET)
        'MI': 'America/Detroit',      # Mostly Eastern Time (ET)
        'MN': 'America/Chicago',      # Central Time (CT)
        'MO': 'America/Chicago',      # Central Time (CT)
        'MS': 'America/Chicago',      # Central Time (CT)
        'MT': 'America/Denver',       # Mountain Time (MT)
        'NC': 'America/New_York',     # Eastern Time (ET)
        'ND': 'America/Denver',       # Central Time (CT)
        'NE': 'America/Chicago',      # Mountain Time (MT)
        'NH': 'America/New_York',     # Eastern Time (ET)
        'NJ': 'America/New_York',     # Eastern Time (ET)
        'NM': 'America/Denver',       # Mountain Time (MT)
        'NV': 'America/Denver',       # Mountain Time (MT)
        'NY': 'America/New_York',     # Eastern Time (ET)
        'OH': 'America/New_York',     # Eastern Time (ET)
        'OK': 'America/Chicago',      # Central Time (CT)
        'OR': 'America/Los_Angeles',  # Pacific Time (PT)
        'PA': 'America/New_York',     # Eastern Time (ET)
        'RI': 'America/New_York',     # Eastern Time (ET)
        'SC': 'America/New_York',     # Eastern Time (ET)
        'SD': 'America/Denver',       # Mountain Time (MT)
        'TN': 'America/Chicago',      # Central Time (CT)
        'TX': 'America/Chicago',      # Central Time (CT)
        'UT': 'America/Denver',       # Mountain Time (MT)
        'VA': 'America/New_York',     # Eastern Time (ET)
        'VT': 'America/New_York',     # Eastern Time (ET)
        'WA': 'America/Los_Angeles',  # Pacific Time (PT)
        'WI': 'America/Chicago',      # Central Time (CT)
        'WV': 'America/New_York',     # Eastern Time (ET)
        'WY': 'America/Denver'        # Mountain Time (MT)
        },
        "egrid_subregions": {
        'AKGD': 'America/Anchorage',      # Alaska Time (AKT)
        'AZNM': 'America/Phoenix',        # Mountain Standard Time (MST, no DST)
        'CAMX': 'America/Los_Angeles',    # Pacific Time (PT)
        'ERCT': 'America/Chicago',        # Central Time (CT)
        'FRCC': 'America/New_York',       # Eastern Time (ET)
        'MROE': 'America/Chicago',        # Central Time (CT)
        'MROW': 'America/Chicago',        # Central Time (CT)
        'NEWE': 'America/New_York',       # Eastern Time (ET)
        'NWPP': 'America/Los_Angeles',    # Pacific Time (PT)
        'NYCW': 'America/New_York',       # Eastern Time (ET)
        'NYLI': 'America/New_York',       # Eastern Time (ET)
        'NYUP': 'America/New_York',       # Eastern Time (ET)
        'RFCE': 'America/New_York',       # Eastern Time (ET)
        'RFCM': 'America/Chicago',        # Central Time (CT)
        'RFCW': 'America/Chicago',        # Central Time (CT)
        'RMPA': 'America/Denver',         # Mountain Time (MT)
        'SPNO': 'America/Chicago',        # Central Time (CT)
        'SPSO': 'America/Chicago',        # Central Time (CT)
        'SRMV': 'America/New_York',       # Eastern Time (ET)
        'SRMW': 'America/Chicago',        # Central Time (CT)
        'SRSO': 'America/New_York',       # Eastern Time (ET)
        'SRTV': 'America/New_York',       # Eastern Time (ET)
        'SRVC': 'America/New_York'        # Eastern Time (ET)
        }
    }
    return timezone_dict,


@app.cell
def __(datetime, pytz):
    def convert_utc_to_timezone(utc_hour, timezone_str):
        """
        Convert UTC hour (0-23) to the corresponding hour in a specified timezone.

        Parameters:
        utc_hour (int): Hour in UTC (0-23).
        timezone_str (str): Timezone string, e.g., 'America/New_York'.

        Returns:
        int: Corresponding hour in the specified timezone.
        """
        # Ensure the UTC hour is within the valid range
        if not (0 <= utc_hour <= 23):
            raise ValueError("UTC hour must be between 0 and 23.")

        # Create a UTC datetime object with the specified hour
        utc_time = datetime.datetime.utcnow().replace(hour=utc_hour, minute=0, second=0, microsecond=0, tzinfo=pytz.utc)


        # Convert to the specified timezone
        target_timezone = pytz.timezone(timezone_str)
        local_time = utc_time.astimezone(target_timezone)

        return local_time.hour
    return convert_utc_to_timezone,


if __name__ == "__main__":
    app.run()
