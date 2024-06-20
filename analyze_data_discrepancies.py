# Import file_data_functions.py as da (discrepancy analysis).
import file_data_functions as da

# Imports continued...
import argparse
import os
import sys
import datetime
import glob
from datetime import timedelta
import pandas as pd


# parse_arguments will get command line arguments needed for program execution.
def parse_arguments():
    parser = argparse.ArgumentParser(description="Parse arguments from user.")
    parser.add_argument('--filename', type=str,
                        help='Name of the file to write to', default=None)
    parser.add_argument('--refdir', type=str,
                        help='Path to directory of reference data', default=None)
    parser.add_argument('--primarydir', type=str,
                        help='Path to directory of primary data', default=None)
    parser.add_argument('--writepath', type=str,
                        help='Path to write results text file in', default='generated_files')
    parser.add_argument('--include-msgs', action='store_true',
                        help="Enable writing program execution messages to results text file")
    parser.add_argument('--no-msgs', dest='include_msgs', action='store_false',
                        help="Opt out of writing execution messages to results text file")
    parser.set_defaults(include_msgs=True)
    return parser.parse_args()
# End parse_arguments.


# get_filename will return the filename input from the user's command line
# argument, or return a default unique filename using timestamp.
def get_filename(user_args):
    if user_args.filename:
        return user_args.filename
    else:
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        return f"output_{timestamp}.txt"
# End get_filename.


# get_directories returns the specified directories to pull data from
# given by command line arguments.
def get_data_paths(user_args, flag=[False]):
    if user_args.refdir and user_args.primarydir:
        if os.path.exists(user_args.refdir) and os.path.exists(user_args.primarydir):
            flag[0] = True
    else:
        flag[0] = False
    return user_args.refdir, user_args.primarydir
# End get_directories.


# ***************************************************************************
# *************************** PROGRAM START *********************************
# ***************************************************************************
def main(args):

    # Determine the filename results will be written to.
    filename = get_filename(args)

    # Get path that program will write results to.
    write_path = args.writepath

    # Assign paths to station data for Lighthouse and NOAA.
    args_flag_ptr = [False]
    paths = get_data_paths(args, flag=args_flag_ptr)

    # Check that get_data_paths succeeded.
    if args_flag_ptr[0] is True:
        noaa_path = paths[0]
        lighthouse_path = paths[1]
    if args_flag_ptr[0] is False:
        print("args_flag_ptr is False. Data paths not entered, "
              "or paths do not exist. Exiting program.")
        sys.exit()

    # Get all csv files from Lighthouse path.
    lighthouse_csv_files = glob.glob(f"{lighthouse_path}/*.csv")

    # Ignore csv files for harmonic water level (harmwl) from NOAA path.
    pattern = f"{noaa_path}/*_*_water_level.csv"
    noaa_csv_files = glob.glob(pattern)

    # Check that files matching the pattern were found.
    if not noaa_csv_files:
        print("Failed to match files to NOAA filename pattern. Exiting program.")
        sys.exit()

    # Prompt user for the start and end year of their data.
    if args.include_msgs:
        prompt = "Enter the starting year <yyyy> of your data: "
        start_year = da.get_year_from_user(prompt)
        prompt = "Enter the last year <yyyy> of your data: "
        end_year = da.get_year_from_user(prompt)

        # Get range of years.
        year_range = range(start_year, end_year + 1)

    # Initialize a summary of error messages. These will be written to the result file.
    error_summary = ["Messages about the program execution are below: \n"]

    # Initialize dataframe arrays to hold the yearly Lighthouse and NOAA data.
    lh_df_arr = []
    noaa_df_arr = []

    # Initialize a flag pointer to check if da.read_file() is successful.
    flag_ptr = [False]

    # Read and split up the lighthouse files.
    for lh_file in lighthouse_csv_files:

        # Read the file into a dataframe.
        df = da.read_file(lh_file, flag=flag_ptr)

        # If read was successful, split into yearly data and append to lh_df_arr.
        if flag_ptr[0]:
            # split_df is a list of dataframes. Use extend() to add each item in the
            # list to lh_df_arr.
            split_df = da.split_by_year(df, df.columns[0])
            lh_df_arr.extend(split_df)
        else:
            print(f"failed to read file: {lh_file}\n")
            error_summary.append(f"failed to read file: {lh_file}\n")
    # End for.

    # Send NOAA data into dataframes. The files are already split by year.
    for noaa_file in noaa_csv_files:

        df = da.read_file(noaa_file, flag=flag_ptr)

        if flag_ptr[0] is True:
            noaa_df_arr.append(df)
        else:
            msg = f"failed to read file: {noaa_file}\n"
            print(msg)
            error_summary.append(msg)

    # End for.

    # Get column names. Assumes all dataframes in the list have same column names.
    # Avoids repeated assignment in the loop which would not make this assumption.
    lh_dt_col_name = lh_df_arr[0].columns[0]
    lh_pwl_col_name = lh_df_arr[0].columns[1]
    noaa_dt_col_name = noaa_df_arr[0].columns[0]
    noaa_pwl_col_name = noaa_df_arr[0].columns[1]

    # Clean lighthouse dataframes. Print error messages.
    for lh_df in lh_df_arr:

        # Initialize error message.
        error_msg = [""]

        da.clean_dataframe(lh_df, lh_dt_col_name, lh_pwl_col_name, error=error_msg)
        year = lh_df[lh_dt_col_name].dt.year

        if not all(e == "" for e in error_msg):
            msg = (f"clean_dataframe returned message for lh file - year {year[0]}. "
                   f"error message: {error_msg}\n")
            print(msg)
            error_summary.append(msg)
    # End for.

    # Clean NOAA dataframes. Print error messages.
    for noaa_df in noaa_df_arr:

        # Initialize error message.
        error_msg = [""]

        da.clean_dataframe(noaa_df, noaa_dt_col_name, noaa_pwl_col_name, error=error_msg)
        year = noaa_df[noaa_dt_col_name].dt.year

        if not all(e == "" for e in error_msg):
            msg = (f"clean_dataframe returned message for noaa file - year {year[0]}. "
                   f"error message: {error_msg}\n")
            print(msg)
            error_summary.append(msg)
    # End for.

    # Make sure only common years are compared.
    # .keys() returns a view object that displays the list of dictionary keys.
    # set() converts the view object into a set of years.
    # Bitwise & is used to find the intersection of two sets, returning a new set that contains
    # the common years.
    lh_dfs_dict = da.get_df_dictionary(lh_df_arr, lh_dt_col_name)
    noaa_dfs_dict = da.get_df_dictionary(noaa_df_arr, noaa_dt_col_name)
    common_years = set(lh_dfs_dict.keys()) & set(noaa_dfs_dict.keys())

    # Record which years have no data for analysis.
    header = ["Analysis could not be done for year(s): \n"]
    bad_years = []
    if args.include_msgs: # Skip if user has opted out of program messages.
        for year in year_range:
            if year not in common_years:
                bad_years.append(year)
        # End for.

    # Process the dataframes of common years to get statistics.
    for year in common_years:

        # Assign dataframes from dictionaries.
        lh_df = lh_dfs_dict[year]
        noaa_df = noaa_dfs_dict[year]

        # Merge the dataframes on the datetimes. Any missing datetimes in one of the dfs
        # will result in the addition of a NaN in the other.
        merged_df = pd.merge(lh_df, noaa_df, how='outer', left_on=lh_dt_col_name,
                             right_on=noaa_dt_col_name, suffixes=('_primary', '_reference'))

        # Get size of merged dataframe.
        size = len(merged_df)

        # Reassign column names in case suffixes were added for repeated column names
        # found during merge. Column indices chosen because merged df still includes other
        # columns like bwl, harmwl, etc.
        lh_dt_col_name = merged_df.columns[0]
        lh_pwl_col_name = merged_df.columns[1]
        noaa_dt_col_name = merged_df.columns[4]
        noaa_pwl_col_name = merged_df.columns[5]

        # Get comparison table.
        stats_df = da.get_comparison_stats(merged_df[lh_pwl_col_name],
                                           merged_df[noaa_pwl_col_name], size)

        # Do get_run_data() to get a dataframe that can be filtered for
        # duration and value.
        runs_df = da.get_run_data(merged_df[lh_pwl_col_name], merged_df[noaa_pwl_col_name],
                                  merged_df[noaa_dt_col_name], size)

        # Filter for offsets (runs) >= 1 day.
        long_offsets_df = da.filter_duration(runs_df, timedelta(days=1))
        long_offsets_count = len(long_offsets_df)
        max_duration = runs_df['durations'].max()
        bool_mask = runs_df['durations'] == max_duration
        max_duration_offsets = runs_df.loc[bool_mask, 'offset (ref - primary, unit)'].to_list()

        # Filter by value >= 5 cm.
        large_offsets_df = da.filter_value(runs_df, threshold=0.05)
        large_offsets_count = len(large_offsets_df)
        max_offset = runs_df['offset (ref - primary, unit)'].max()
        min_offset = runs_df['offset (ref - primary, unit)'].min()
        bool_mask = runs_df['offset (ref - primary, unit)'] == max_offset
        max_offset_durations = runs_df.loc[bool_mask, 'durations'].to_list()
        bool_mask = runs_df['offset (ref - primary, unit)'] == min_offset
        min_offset_durations = runs_df.loc[bool_mask, 'durations'].to_list()

        # Hold these metrics in metric_data.
        metric_data = [
            ("Number of offsets with duration >= one day", long_offsets_count),
            ("Maximum duration of an offset", max_duration),
            ("Offset value(s) with <" + str(max_duration) + "> duration", max_duration_offsets),
            ("Number of offsets with abs value >= 5 cm", large_offsets_count),
            ("Maximum/minimum offset value (m)", str(max_offset) + "/" + str(min_offset)),
            ("Duration(s) of offset with value <" + str(max_offset) + "> cm", max_offset_durations),
            ("Duration(s) of offset with value <" + str(min_offset) + "> cm", min_offset_durations)
        ]

        # Write all stats to a .txt file (in append mode).
        with open(f'{write_path}/{filename}.txt', 'a') as file:

            file.write(f"Comparison Table for year {year}:\n {stats_df.to_string(index=True)}\n\n")

            # Find the longest key length for key alignment.
            max_key_length = max(len(key) for key, value in metric_data)

            # Write each key-value pair aligned.
            for key, value in metric_data:
                file.write(f"{key:{max_key_length}}: {value}\n")
            # End for.
            file.write("\n\n\n")
        # File closed.
    # End for.

    # Prepend error_summary and header to the text file if include_msgs is True.
    if args.include_msgs:
        bad_years.sort()
        header.extend([str(y) + " " for y in bad_years])
        results_title = ["***************************************************************************\n"
                         "******************************* RESULTS ***********************************\n"
                         "***************************************************************************\n\n"]
        prepend_text = header + ["\n\n"] + error_summary + ["\n\n"] + results_title
        da.prepend_to_file(f'{write_path}/{filename}.txt', prepend_text)
# End main.


if __name__ == "__main__":
    main_args = parse_arguments()
    main(main_args)

# ***************************************************************************
# *************************** PROGRAM END ***********************************
# ***************************************************************************
