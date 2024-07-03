import os
import pandas as pd
import numpy as np
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


def read_file_to_df(file, index_limit=None, flag=[False], error=[""]):

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


def get_df_dictionary(df_list, dt_col_name):

    dfs_dict = {}
    for df in df_list:

        year = df[dt_col_name].dt.year

        if not pd.isna(year[0]):
            dfs_dict[int(year[0])] = df
    # End for.

    return dfs_dict
# End get_df_dictionary.


def write_table_from_nested_dict(nested_dict, row_header, write_path):
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
    with open(write_path, 'a') as file:
        file.write(header_line + "\n")
        file.write(divider_line + "\n")
        for row in rows:
            file.write(row + "\n")
# End write_table_from_nested_dict.
