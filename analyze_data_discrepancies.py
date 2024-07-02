# Import file_data_functions.py as da (file processing).
import file_data_functions as fp

# Import MetricsCalculator.py as mc
from MetricsCalculator import MetricsCalculator
from TransformData import TransformData

# Imports continued...
import argparse
import os
import sys
import datetime
import glob
import pandas as pd
import json


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
        return f"output_{timestamp}"
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

    # Store loaded configs.
    config = MetricsCalculator.load_configs('config.json')

    # Determine the filename results will be written to.
    filename = get_filename(args)

    # Get path that program will write results to. Default generated_files.
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
        start_year = fp.get_year_from_user(prompt)
        prompt = "Enter the last year <yyyy> of your data: "
        end_year = fp.get_year_from_user(prompt)

        # Get range of years.
        year_range = range(start_year, end_year + 1)

    # Initialize a summary of error messages to be written to the results file.
    error_summary = ["Messages about the program execution are below: \n"]

    # Initialize a flag pointer to check if da.read_file() is successful.
    flag_ptr = [False]

    # Read and split up the lighthouse files.
    lh_df_arr = []
    for lh_file in lighthouse_csv_files:

        # Read the file into a dataframe.
        df = fp.read_file_to_df(lh_file, flag=flag_ptr)

        # If read was successful, split into yearly data and append to lh_df_arr.
        if flag_ptr[0]:
            # split_df is a list of dataframes. Use extend() to add each item in the
            # list to lh_df_arr.
            split_df = fp.split_by_year(df, df.columns[0])
            lh_df_arr.extend(split_df)
        else:
            msg = f"failed to read file: {lh_file}\n"
            print(msg)
            error_summary.append(msg)
    # End for.

    # Send NOAA data into dataframes. The files are already split by year.
    noaa_df_arr = []
    for noaa_file in noaa_csv_files:

        df = fp.read_file_to_df(noaa_file, flag=flag_ptr)

        if flag_ptr[0] is True:
            noaa_df_arr.append(df)
        else:
            msg = f"failed to read file: {noaa_file}\n"
            print(msg)
            error_summary.append(msg)
    # End for.

    # Use da.config to get the necessary columns for assigning column names.
    lh_data_cols = fp.config['primary_data_column_names']
    noaa_data_cols = fp.config['reference_data_column_names']

    # Assign column names. If not configured, assume their positions in the dataframe,
    # and that all dfs in the array have same column names.
    lh_dt_col_name = lh_df_arr[0].columns[0] if not lh_data_cols['datetime'] \
        else lh_data_cols['datetime']
    lh_pwl_col_name = lh_df_arr[0].columns[1] if not lh_data_cols['water_level'] \
        else lh_data_cols['water_level']
    noaa_dt_col_name = noaa_df_arr[0].columns[0] if not noaa_data_cols['datetime'] \
        else noaa_data_cols['datetime']
    noaa_pwl_col_name = noaa_df_arr[0].columns[1] if not noaa_data_cols['water_level'] \
        else noaa_data_cols['water_level']

    # Clean lighthouse dataframes. Print error messages.
    for lh_df in lh_df_arr:

        # Initialize error message.
        error_msg = [""]

        fp.clean_dataframe(lh_df, lh_dt_col_name, lh_pwl_col_name, error=error_msg)
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

        fp.clean_dataframe(noaa_df, noaa_dt_col_name, noaa_pwl_col_name, error=error_msg)
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
    lh_dfs_dict = fp.get_df_dictionary(lh_df_arr, lh_dt_col_name)
    noaa_dfs_dict = fp.get_df_dictionary(noaa_df_arr, noaa_dt_col_name)
    common_years = set(lh_dfs_dict.keys()) & set(noaa_dfs_dict.keys())

    # Record which years have no data for analysis.
    header = ["Analysis could not be done for year(s): \n"]
    bad_years = []
    if args.include_msgs:  # Skip if user has opted out of program messages.
        for year in year_range:
            if year not in common_years:
                bad_years.append(year)
        # End for.

    # Process the dataframes of common years to get statistics. Initialize summary.
    summary = {}
    for year in common_years:

        # Assign dataframes from dictionaries.
        lh_df = lh_dfs_dict[year]
        noaa_df = noaa_dfs_dict[year]

        # Drop unrelated columns.
        for col in lh_df.columns:
            if col not in (lh_dt_col_name, lh_pwl_col_name):
                lh_df.drop(columns=col, inplace=True)
        for col in noaa_df.columns:
            if col not in (noaa_dt_col_name, noaa_pwl_col_name):
                noaa_df.drop(columns=col, inplace=True)

        # print(lh_df, "\n", noaa_df)

        # Merge the dataframes on the datetimes. Any missing datetimes in one of the dfs
        # will result in the addition of a NaN in the other.
        merged_df = pd.merge(lh_df, noaa_df, how='outer', left_on=lh_dt_col_name,
                             right_on=noaa_dt_col_name, suffixes=('_primary', '_reference'))

        # Reassign column names.
        lh_pwl_col_name = merged_df.columns[1]
        noaa_dt_col_name = merged_df.columns[2]
        noaa_pwl_col_name = merged_df.columns[3]

        # Get size of merged dataframe.
        size = len(merged_df)

        # If doing analysis on corrected data, correct data here.
        if year == 2012:
            corrected_df = merged_df.copy()
            corrector = TransformData()
            corrected_df = corrector.temporal_deshifter(corrected_df, lh_pwl_col_name, noaa_pwl_col_name, size, year)
            with open('correction_reports/dataframe_comparison_5.txt', 'a') as file:
                file.write(f"{corrected_df.to_string()}")


'''
        # Get comparison table.
        stats_df = MetricsCalculator.get_comparison_stats(merged_df[lh_pwl_col_name],
                                                          merged_df[noaa_pwl_col_name], size)

        # Instantiate an object to get metrics. Set configs.
        calculator = MetricsCalculator()
        calculator.set_configs(config)

        # Get offset runs dataframe.
        run_data_df = calculator.generate_runs_df(merged_df[lh_pwl_col_name],
                                                  merged_df[noaa_pwl_col_name], merged_df[noaa_dt_col_name], size)

        # Set the dataframe.
        calculator.set_runs_dataframe(run_data_df)

        # Calculate metrics.
        metrics = calculator.calculate_metrics()
        
        # Set metrics.
        calculator.set_metrics(metrics)

        # Format metrics.
        metrics_list = calculator.format_metrics()
        # for item in metrics_list:
        #     print(item, "\n")

        # Get table of long offsets.
        offsets_dict = calculator.generate_long_offsets_info()
        # print(offsets_dict, "\n")

        # Append year info to summary.
        summary[year] = {
            "% agree": stats_df.loc['total agreements', 'percent'],
            "# long offsets": metrics['long_offsets_count'],
            "# long gaps": metrics['long_gaps_count'],
            "unique long offsets": list(offsets_dict.keys()),
            "# large offsets": metrics['large_offsets_count'],
            "minimum value": metrics['min_max_offsets'][1],
            "maximum value": metrics['min_max_offsets'][0]
        }

        # Write report.
        MetricsCalculator.write_stats(stats_df, write_path, filename, year)
        MetricsCalculator.write_metrics_to_file(metrics_list, write_path, filename)
        MetricsCalculator.write_offsets_to_file(offsets_dict, write_path, filename)
    # End for.

    # Write summary file.
    with open(f'{write_path}/{filename}_summary.txt', 'a') as file:
        file.write(f"Configurations: {json.dumps(config, indent=4)}\n\n")
        file.write(f"% agree: Percentage of values that agree between datasets.\n"
                   f"# long offsets: The number of offsets that meet the duration threshold.\n"
                   f"# long gaps: The number of gaps that meet the duration threshold.\n"
                   f"unique long offsets: List of unique offset values that meet the duration threshold.\n"
                   f"# large offsets: The number of offsets that meet the value threshold.\n"
                   f"minimum value: The minimum discrepancy value.\n"
                   f"maximum value: The maximum discrepancy value.\n\n")
    fp.write_table_from_nested_dict(summary, 'Year', write_path, f"{filename}_summary")

    # Write error_summary and header to the text file if include_msgs is True.
    if args.include_msgs:
        bad_years.sort()
        header.extend([str(y) + " " for y in bad_years])
        results_title = ["***************************************************************************\n"
                         "******************************* RESULTS ***********************************\n"
                         "***************************************************************************\n\n"]
        text_list = header + ["\n\n"] + error_summary + ["\n\n"] + results_title
        with open(f'{write_path}/{filename}_execution_messages.txt', 'a') as file:
            file.write(''.join(text_list))
'''
# End main.


if __name__ == "__main__":
    main_args = parse_arguments()
    fp.load_configs('config.json')
    main(main_args)

# ***************************************************************************
# *************************** PROGRAM END ***********************************
# ***************************************************************************
