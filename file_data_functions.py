import os
import pandas as pd
import numpy as np
from datetime import timedelta
import re
import json

# Opt into future behavior for pandas. Encouraged by FutureWarning message
# for pd.replace(): "Downcasting behavior in 'replace' is deprecated and
# will be removed in a future version."
pd.set_option('future.no_silent_downcasting', True)


# Default configuration dictionary.
config = {
    'primary_data_column_names': {
        'datetime': '',
        'water_level': ''
    },
    'reference_data_column_names': {
        'datetime': '',
        'water_level': ''
    },
    'filter_offsets_by_duration': {
        'threshold': '0 days',
        'type': 'min',
        'is_strict': False
    },
    'filter_offsets_by_value': {
        'threshold': 0.0,
        'use_abs': True,
        'type': 'min',
        'is_strict': False
    },
    'filter_gaps_by_duration': {
        'threshold': '0 days',
        'type': 'min',
        'is_strict': False
    },
    'offset_correction_parameters': {
        'number_of_intervals': 0
    }
}


""" ******************* Function implementation below ******************* """


# ***************************************************************************
# *********************** FUNCTION LOAD_CONFIGS *****************************
# ***************************************************************************

# Loads configurations, optional to customize by the user, from a JSON file.
# In case of missing keys in config.json, fall back on default global config
# dictionary.

def load_configs(file_path):
    global config

    try:
        with open(file_path, 'r') as file:
            user_config = json.load(file)
            for section, settings in user_config.items():
                if section in config:
                    config[section].update(settings)
    except FileNotFoundError:
        print(f"Error: Config file '{file_path}' not found. Using default configuration.")
# End load_configs.


# ***************************************************************************
# **************************** FUNCTION READ_FILE ***************************
# ***************************************************************************

# Reads csv file into a dataframe.
# Pass an index limit to only read file until a certain index.
# Uses end_file_index to stop reading file when valid data stops.
# If index_limit is None and assigned None by end_file_index(), then
# pd.read_csv will read the entire csv file.
# Option to pass a flag and error message to be retrieved from main() if
# user wants to verify that read was successful.

def read_file(file, index_limit=None, flag=[False], error=[""]):

    # Initialize df to None.
    df = None

    # If index_limit is None, find where valid data stops using
    # end_file_index() so that the nrows parameter can be assigned in
    # pd.read_csv().
    if index_limit is None:
        index_limit = end_file_index(file)

    # Read the file into a dataframe. If error reading or finding file,
    # append the error.
    if os.path.isfile(file):
        try:
            df = pd.read_csv(file, nrows=index_limit)
            flag[0] = True
        except Exception as e:
            error[0] = str(e)
            flag[0] = False
    else:
        error[0] = "File not found."
        flag[0] = False
    # End for.

    return df
# End read_file.


# ***************************************************************************
# ******************* FUNCTION END_FILE_INDEX VERSION 3 *********************
# ***************************************************************************

# Simply count the lines in the file (Python's iteration over file does
# not load the entire file into memory). Then, subtract the number of lines
# that begin with '# '. This assumes all of them are at the end.

def end_file_index(filename):

    line_count = 0
    trailing_lines = 0

    with open(filename, 'r', encoding='utf-8') as file:
        for line in file:
            line_count += 1
            if line.startswith("# "):
                trailing_lines += 1
    # Close file.

    valid_data_lines = line_count - trailing_lines - 1 # -1 for last new line.

    return valid_data_lines
# End end_file_index().


# ***************************************************************************
# ******************* FUNCTION GET_YEAR_FROM_USER ***************************
# ***************************************************************************

# Prompts the user to enter a year value. This allows the main program
# to get the range of years that the data has, and write whether data for
# a given year was not available to the results file.

def get_year_from_user(prompt):
    while True:
        try:
            # Asking for the user's input with a custom prompt.
            year = int(input(prompt))

            # Check if the year is a valid positive number.
            if year <= 0:
                print("Please enter a positive number for the year.")
            else:
                return year  # Return the year if the input is valid.
        except ValueError:
            print("Please enter a valid number for the year.")
# End get_year_from_user.


# ***************************************************************************
# ******************* FUNCTION SPLIT_BY_YEAR ********************************
# ***************************************************************************

# Splits a dataframe into yearly dataframes. Returns a list of
# yearly dataframes.
# Lighthouse data files are provided in multi-year csv files but need
# to be processed year by year so splitting them is necessary.

def split_by_year(df, datetime_col_name):
    # Case if column name is not assigned.
    if datetime_col_name is None:
        return df

    df[datetime_col_name] = pd.to_datetime(df[datetime_col_name])

    # Get an array of unique years from dt.year which extracts the year
    # components from the datetime objects in the datetime column of
    # the dataframe.
    years = df[datetime_col_name].dt.year.unique()

    # Split dataframe by year. For each year, filter df using boolean
    # mask [df[datetime_col_name].dt.year == year] to create a new
    # dataframe containing only the rows where the year (.dt.year) matches
    # the current 'year'. Append these dataframes to a list.
    data_by_year = []
    for year in years:
        data_by_year.append(df[df[datetime_col_name].dt.year == year].reset_index(drop=True))
    # End for.

    # Or use dictionary comprehension and return a dictionary where the
    # keys are the years and the items are the corresponding dataframes.
    # data_by_year = {year: df[df[date_col_name].dt.year == year] for year in years}

    # Return the list.
    return data_by_year
# End split_by_year.


# ***************************************************************************
# ******************* FUNCTION CLEAN_DATAFRAME ******************************
# ***************************************************************************

# Prepare dataframe for analysis by standardizing datetimes and numerical values.
# Option to clean backup water level (bwl) and harmonic water level (harmwl) columns.
# Option to pass flag pointer so that status of if clean_dataframe succeeded or failed
# can be checked by user in main program.

def clean_dataframe(df, datetime_col_name, pwl_col_name, harmwl_col_name=None,
                    bwl_col_name=None, error=[""]):

    # Replace missing values with NaN.
    df.replace([-999, -99, 99, 'NA', 'RM'], np.nan, inplace=True)

    # Convert to datetime and numeric. coerce changes invalid values
    # to NaT/NaN.
    try:
        df[datetime_col_name] = pd.to_datetime(df[datetime_col_name], errors='coerce')
        df[pwl_col_name] = pd.to_numeric(df[pwl_col_name], errors='coerce')
    except Exception as e:
        error[0] += str(e)

    if bwl_col_name is not None:
        df[bwl_col_name] = pd.to_numeric(df[bwl_col_name], errors='coerce')
    if harmwl_col_name is not None:
        df[harmwl_col_name] = pd.to_numeric(df[harmwl_col_name], errors='coerce')

    if df[datetime_col_name].isna().all() or df[pwl_col_name].isna().all():
        error[0] += "dataframe has column with only NaT or NaN. "
# End clean_dataframe.


# ***************************************************************************
# ******************* FUNCTION GET_DF_DICTIONARY ****************************
# ***************************************************************************

# Convert list of dataframes to a dictionary with the year as the keys and
# corresponding dataframes as values. Then, comparing the NOAA and Lighthouse
# dictionaries ensures data from the same year will be compared.

def get_df_dictionary(df_list, dt_col_name):

    dfs_dict = {}
    for df in df_list:

        year = df[dt_col_name].dt.year

        if not pd.isna(year[0]):
            dfs_dict[int(year[0])] = df
    # End for.

    return dfs_dict
# End get_df_dictionary.


# ***************************************************************************
# ******************* FUNCTION TEMPORAL_DESHIFTER ***************************
# ***************************************************************************

# Automated process for correcting a changing temporal shift.
# Writes notes about execution into text file.
# Returns a time-aligned/corrected copy of the dataframe.
# Note that vertical offsets are not corrected. This is the purpose of
# process_offsets.

def temporal_deshifter(merged_df, primary_col_name, ref_col_name, size):

    """ Loop over 7 temporal shifts: from -3 to 3
            Create copy of merged dataframe.
            Shift lighthouse by temporal shift loop variable value.
            Create dataframe to hold the de-shifted data.
            Call identify_offset.
                If returns nan, then continue to trying next temporal offset value.
                If offset,
                    find index where offset stops.
                    Save this index, only loop for index < size.
            Save corrections to de-shifted dataframe.
            Restart loop, beginning at last index, and first temporal offset value.
    """

    # Initialize dataframes. df_copy will be shifted to find offsets.
    # deshifted_df will hold the temporally corrected values.
    df_copy = merged_df.copy()
    deshifted_df = merged_df.copy()

    index = 0
    temporal_shifts = range(-3, 4)
    shift_val_index = 0

    # While indices of dataframe are valid, correct temporal shifts if possible.
    while index < size:

        # If all temporal shifts have been tried, consider this segment
        # of data impossible to automatically correct, possibly because of a
        # changing vertical offset or skipped values, etc.
        # Restart process at +10 indices from current.
        if shift_val_index > 6:
            shift_val_index = 0
            while index < index + 10:
                if index >= size:
                    break
                deshifted_df[primary_col_name].iloc[index] = df_copy[primary_col_name].iloc[index]
                index += 1

        # Current shift value.
        try_shift = temporal_shifts[shift_val_index]

        # Temporally shift the dataframe.
        df_copy[primary_col_name] = merged_df[primary_col_name].shift(try_shift)

        # Get the vertical offset. Note that identify_offset does not let missing
        # values contribute to the detection of an offset, but does include them in the
        # duration count.
        vert_offset = identify_offset(df_copy[primary_col_name], df_copy[ref_col_name],
                                      index, size, duration=10)

        # If there is no consistent vertical offset, try again for next temporal shift value.
        if pd.isna(vert_offset):
            shift_val_index += 1
            continue

        # If an offset is found, record df_copy values into deshifted_df while the vertical
        # offset is valid. Record the index where the offset stops.
        # When offset stops, undo the shift.
        while index < size:
            if (round(df_copy[primary_col_name].iloc[index] + vert_offset, 4) ==
                    round(df_copy[ref_col_name].iloc[index], 4)):
                index += 1
                deshifted_df[primary_col_name].iloc[index] = df_copy[primary_col_name].iloc[index]
            else:
                df_copy[primary_col_name] = merged_df[primary_col_name]
                # df_copy.drop(index, inplace=True)
                break
        # End inner while.

        shift_val_index = 0
    # End outer while.

    return deshifted_df
# End temporal_deshifter.


# ***************************************************************************
# ******************* FUNCTION PROCESS_OFFSETS ******************************
# ***************************************************************************

# Correct offsets in a data set, considering that the offset value may change throughout,
# and that some discrepancies cannot be considered offsets.
# Includes option to append an array with offset values including NaNs.
# Modifies passed columns. Returns nothing.

def process_offsets(offset_column, reference_column, size, index=0, offset_arr=None):

    # index = 0
    while index < size:
        # Skip NaNs.
        if pd.isna(offset_column.iloc[index]) or pd.isna(reference_column.iloc[index]):
            index += 1
            offset_arr.append(np.nan)
            continue

        # Get offset.
        offset = identify_offset(offset_column, reference_column, index, size)

        # Skip if no verified offset.
        if pd.isna(offset):
            index += 1
            offset_arr.append(np.nan)
            continue

        # Correct offset until the correction stops working.
        while index < size:
            if round(offset_column.iloc[index] + offset, 4) == round(reference_column.iloc[index], 4):
                offset_column.iloc[index] = offset_column.iloc[index] + offset
                offset_arr.append(offset)
                index += 1
                # print("fixing offset at index: ", index, " with difference: ", offset)

            else:
                break
        # End inner while.
    # End outer while.
# End process_offsets.


# ***************************************************************************
# ******************* FUNCTION IDENTIFY_OFFSET ******************************
# ***************************************************************************

# If there is an offset identified as a constant difference between the
# reference and suspected offset data, for [duration] number of intervals
# then return the offset value, else return NaN.
# Called by process_offsets and temporal_deshifter.

def identify_offset(offset_column, reference_column, index, size,
                    duration=config['offset_correction_parameters']['number_of_intervals']):
    offset_value = offset_column.iloc[index]
    ref_value = reference_column.iloc[index]
    difference = round(ref_value - offset_value, 4)

    f_loop = index
    # for loop in range(index, index + duration):
    while f_loop < (index + duration):
        if f_loop + 1 >= size:
            return np.nan

        # Skips over NaNs. Considers that the offset remains valid even if
        # some values are missing due to sensor failure or whatever.
        # This prevents invalidating an offset that is consistent otherwise.
        if pd.isna(offset_column.iloc[f_loop]) or pd.isna(reference_column.iloc[f_loop]):
            continue

        current_diff = round(reference_column.iloc[f_loop + 1] - offset_column.iloc[f_loop + 1], 4)

        if current_diff != difference:
            return np.nan

        # Increment index.
        f_loop += 1
    # End while.

    return difference
# End identify_offset.


def write_table_from_nested_dict(nested_dict, row_header, write_path, filename):
    # Extract all unique column names and determine maximum column width
    column_names = set()
    max_column_widths = {}

    # Determine maximum column width based on header and rows
    for row_name, row_data in nested_dict.items():
        for column_name, value in row_data.items():
            column_names.add(column_name)
            max_width = max(len(str(column_name)), len(str(value)))
            current_max = max_column_widths.get(column_name, 0)
            max_column_widths[column_name] = max(current_max, max_width)

    # Sort column names alphabetically
    column_names = sorted(column_names)

    # Prepare table header
    header = [row_header.ljust(len(row_header))] + [column_name.ljust(max_column_widths[column_name])
                                                    for column_name in column_names]
    header_line = " | ".join(header)
    divider_line = "-" * len(header_line)

    # Prepare table rows
    rows = []
    for row_name, row_data in nested_dict.items():
        row_values = [str(row_name).ljust(len(row_header))]
        for column_name in column_names:
            if column_name in row_data:
                row_values.append(str(row_data[column_name]).ljust(max_column_widths[column_name]))
            else:
                row_values.append("".ljust(max_column_widths[column_name]))  # Handle missing values
        rows.append(" | ".join(row_values))

    # Write to file
    with open(f'{write_path}/{filename}.txt', 'a') as file:
        file.write(header_line + "\n")
        file.write(divider_line + "\n")
        for row in rows:
            file.write(row + "\n")
