import os
import pandas as pd
import numpy as np
import json
import datetime
import argparse

# Opt into future behavior for pandas. Encouraged by FutureWarning message
# for pd.replace(): "Downcasting behavior in 'replace' is deprecated and
# will be removed in a future version."
pd.set_option('future.no_silent_downcasting', True)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Parse arguments from user.")
    parser.add_argument('--config', type=str,
                        help='Path to configuration file', default='config.json')
    parser.add_argument('--filename', type=str,
                        help='Name of the file to write results to', default=None)
    parser.add_argument('--refdir', type=str,
                        help='Path to directory of reference data', default=None)
    parser.add_argument('--primarydir', type=str,
                        help='Path to directory of primary data', default=None)
    parser.add_argument('--output', type=str,
                        help='Path to write results text file(s) in', default='generated_files')
    parser.add_argument('--years', type=int, nargs='+',
                        help='Years to include in the analysis')
    parser.add_argument('--logging-off', dest='logging', action='store_false',
                        help="Opt out of logging")
    parser.set_defaults(logging=True)
    parser.add_argument('--mode', type=str, choices=['raw', 'corrected'],
                        help='Type of analysis')
    return parser.parse_args()


def get_filename(user_args, configs_filename):
    if user_args.filename:  # Let cmdl args overwrite config.
        return user_args.filename
    elif configs_filename:
        return configs_filename
    else:  # Default case if both cmdl arg and config are empty.
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        return f"output_{timestamp}"


def get_data_paths(user_args, configs_refdir, configs_primarydir, flag=[False]):
    # Allow cmdl args to overwrite config.
    refdir = user_args.refdir if user_args.refdir \
        else configs_refdir
    primarydir = user_args.primarydir if user_args.primarydir \
        else configs_primarydir

    # Set flag for if paths exist.
    if os.path.exists(refdir) and os.path.exists(primarydir):
        flag[0] = True
    else:
        flag[0] = False

    return refdir, primarydir


def get_output_path(user_args, configs_path):
    if user_args.output != 'generated_files':
        return user_args.output
    elif configs_path:
        return configs_path
    else:
        return user_args.output


def load_configs(file_path):
    try:
        with open(file_path, 'r') as file:
            user_config = json.load(file)
    except FileNotFoundError:
        print(f"Error: Config file '{file_path}' not found.")
    return user_config


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

    valid_data_lines = line_count - trailing_lines - 1  # -1 for last new line.

    return valid_data_lines
# End end_file_index().


def split_by_year(df, datetime_col_name):
    # Case if column name is not assigned.
    if datetime_col_name is None:
        return df

    df[datetime_col_name] = pd.to_datetime(df[datetime_col_name])

    # Get an array of unique years from dt.year.
    years = df[datetime_col_name].dt.year.unique()

    # Split dataframe by year: for each year, filter df using boolean
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


def get_df_dictionary(df_list, dt_col_pos):

    dfs_dict = {}
    for df in df_list:

        year = df.iloc[:, dt_col_pos].dt.year

        if not pd.isna(year[0]):
            dfs_dict[int(year[0])] = df
    # End for.

    return dfs_dict
# End get_df_dictionary.


def write_table_from_nested_dict(nested_dict, row_header, write_path):
    # Collect column names in the order of the first appearance
    column_names = []
    max_column_widths = {}

    for row_name, row_data in nested_dict.items():
        for column_name, value in row_data.items():
            if column_name not in column_names:
                column_names.append(column_name)
            max_width = max(len(str(column_name)), len(str(value)))
            current_max = max_column_widths.get(column_name, 0)
            max_column_widths[column_name] = max(current_max, max_width)

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

