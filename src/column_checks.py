"""
Check columns for standard data files output by data_pipeline.

Since file names and column names are hardcoded across several files, calling these checks
during file creation (data_pipeline.py) ensures that changes to file names and
column names are not made accidentally.

To make an intentional change in a file or column name, search the project for all
uses of that column/file, update all of them to the new column name, and then change
the name here.

To add a column, add the name here.

To remove a column, search the project for all uses of that column and remove
those files or uses, then remove it here.

After any change, re-run data_pipeline to regenerate all files and re-run these
checks.
"""

from logging_util import get_logger

logger = get_logger(__name__)


COLUMNS = {
    "cems_cleaned": {
        "plant_id_eia",
        "plant_id_epa",
        "nerc_region",
        "egrid_subregions",
        "iso_rto_code",
        "state",
        "operating_datetime_utc",
        "gross_load_mw",
        "co2_mass_kg",
        "nox_mass_kg",
        "so2_mass_kg"
    },
}


def check_columns(df, file_name):
    """
    Given a file name and a dataframe to export, check that its columns are as expected.
    """

    cols = set(list(df.columns))
    # Get expected columns
    if file_name not in COLUMNS:
        raise ValueError(
            f"Could not find file {file_name} in expected file names {COLUMNS.keys()}"
        )
    expected_cols = COLUMNS[file_name]

    # Check for extra columns. Warning not exception
    extras = cols - expected_cols
    if len(extras) > 0:
        logger.warning(
            f"columns {extras} in {file_name} are not guaranteed by column_checks.py"
        )

    # Raise exception for missing columns
    missing = expected_cols - cols
    if len(missing) > 0:
        raise ValueError(f"Columns {missing} missing from {file_name}")

    return


def get_dtypes():
    """Returns a dictionary of dtypes that should be used for each column name."""
    dtypes_to_use = {
        "acid_gas_removal_efficiency": "float64",
        "annual_nox_emission_rate_lb_per_mmbtu": "float64",
        "ba_code": "str",
        "ba_code_physical": "str",
        "boiler_id": "str",
        "capacity_mw": "float64",
        "cems_profile": "float64",
        "ch4_mass_lb": "float64",
        "ch4_mass_lb_adjusted": "float64",
        "ch4_mass_lb_for_electricity": "float64",
        "ch4_mass_lb_for_electricity_adjusted": "float64",
        "co2_mass_kg": "float64",
        "co2_mass_lb_adjusted": "float64",
        "co2_mass_lb_for_electricity": "float64",
        "co2_mass_lb_for_electricity_adjusted": "float64",
        "co2_mass_measurement_code": "category",
        "co2e_mass_lb": "float64",
        "co2e_mass_lb_adjusted": "float64",
        "co2e_mass_lb_for_electricity": "float64",
        "co2e_mass_lb_for_electricity_adjusted": "float64",
        "data_availability": "category",
        "distribution_flag": "bool",
        "egrid_subregions": "str",
        "eia930_profile": "float64",
        "emissions_unit_id_epa": "str",
        "energy_source_code": "str",
        "energy_source_code_1": "str",
        "equipment_tech_description": "str",
        "fgd_electricity_consumption_mwh": "float64",
        "fgd_sorbent_consumption_1000_tons": "float64",
        "firing_type_1": "str",
        "firing_type_2": "str",
        "firing_type_3": "str",
        "flat_profile": "float32",
        "fuel_category": "str",
        "fuel_category_eia930": "str",
        "fuel_consumed_for_electricity_mmbtu": "float64",
        "fuel_consumed_mmbtu": "float64",
        "fuel_mmbtu_per_unit": "float64",
        "generator_id": "str",
        "gross_generation_mwh": "float64",
        "gross_load_mw": "float64",
        "gtn_method": "category",
        "hourly_data_source": "category",
        "hours_in_service": "float64",
        "imputed_profile": "float64",
        "iso_rto_code":"str",
        "mercury_control_id_eia": "str",
        "mercury_emission_rate_lb_per_trillion_btu": "float64",
        "mercury_removal_efficiency": "float64",
        "n2o_mass_lb": "float64",
        "n2o_mass_lb_adjusted": "float64",
        "n2o_mass_lb_for_electricity": "float64",
        "n2o_mass_lb_for_electricity_adjusted": "float64",
        "nerc_region": "str",
        "net_generation_mwh": "float64",
        "nox_control_id_eia": "str",
        "nox_mass_kg": "float64",
        "nox_mass_lb_adjusted": "float64",
        "nox_mass_lb_for_electricity": "float64",
        "nox_mass_lb_for_electricity_adjusted": "float64",
        "nox_mass_measurement_code": "category",
        "operating_time_hours": "float16",
        "operational_status": "str",
        "ozone_season_nox_emission_rate_lb_per_mmbtu": "float64",
        "particulate_control_id_eia": "str",
        "particulate_emission_rate_lb_per_mmbtu": "float64",
        "particulate_removal_efficiency_annual": "float64",
        "particulate_removal_efficiency_at_full_load": "float64",
        "plant_id_eia": "Int32",
        "plant_id_epa": "Int32",
        "plant_primary_fuel": "str",
        "plant_primary_fuel_from_capacity_mw": "str",
        "plant_primary_fuel_from_fuel_consumed_for_electricity_mmbtu": "str",
        "plant_primary_fuel_from_mode": "str",
        "plant_primary_fuel_from_net_generation_mwh": "str",
        "prime_mover_code": "str",
        "profile": "float64",
        "profile_method": "str",
        "residual_profile": "float64",
        "scaled_residual_profile": "float64",
        "shifted_residual_profile": "float64",
        "so2_control_id_eia": "str",
        "so2_mass_kg": "float64",
        "so2_mass_lb_adjusted": "float64",
        "so2_mass_lb_for_electricity": "float64",
        "so2_mass_lb_for_electricity_adjusted": "float64",
        "so2_mass_measurement_code": "category",
        "so2_removal_efficiency_annual": "float64",
        "so2_removal_efficiency_at_full_load": "float64",
        "state": "str",
        "steam_load_1000_lb": "float64",
        "subplant_id": "Int16",
        "subplant_primary_fuel": "str",
        "subplant_primary_fuel_from_capacity_mw": "str",
        "subplant_primary_fuel_from_fuel_consumed_for_electricity_mmbtu": "str",
        "subplant_primary_fuel_from_mode": "str",
        "subplant_primary_fuel_from_net_generation_mwh": "str",
        "timezone": "str",
        "wet_dry_bottom": "str",
    }

    return dtypes_to_use


def apply_dtypes(df):
    """Applies specified dtypes to a dataframe and identifies if a dtype is not specified for a column."""
    dtypes = get_dtypes()
    datetime_columns = ["operating_datetime_utc", "datetime_local", "report_date"]
    cols_missing_dtypes = [
        col
        for col in df.columns
        if (col not in dtypes) and (col not in datetime_columns)
    ]
    if len(cols_missing_dtypes) > 0:
        logger.warning(
            "The following columns do not have dtypes assigned in `column_checks.get_dtypes()`"
        )
        logger.warning(cols_missing_dtypes)
    
    # for col in df.columns:
    #     if col in dtypes and col not in datetime_columns:
    #         try:
    #             df[col] = df[col].astype(dtypes[col])
    #         except TypeError as e:
    #             logger.error(f"Error converting column '{col}' to {dtypes[col]}: {e}")
    return df.astype({col: dtypes[col] for col in df.columns if col in dtypes})