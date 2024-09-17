# Import file_data_functions.py as da (file processing).
import file_data_functions as fp

# Import classes.
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


def parse_arguments():
    parser = argparse.ArgumentParser(description="Parse arguments from user.")
    parser.add_argument('--config', type=str,
                        help='Path to configuration file', default='config.json')
    parser.add_argument('--filename', type=str,
                        help='Name of the file to write to', default=None)
    parser.add_argument('--refdir', type=str,
                        help='Path to directory of reference data', default=None)
    parser.add_argument('--primarydir', type=str,
                        help='Path to directory of primary data', default=None)
    parser.add_argument('--output', type=str,
                        help='Path to write results text file(s) in', default='generated_files')
    parser.add_argument('--years', type=int, nargs='+',
                        help='Years to include in the analysis')
    parser.add_argument('--include-msgs', dest='include_msgs', action='store_true',
                        help="Opt out of writing execution messages to results text file")
    parser.set_defaults(include_msgs=False)
    parser.add_argument('--mode', type=str, choices=['raw', 'corrected'],
                        help='Type of analysis')
    return parser.parse_args()


def get_filename(user_args, configs_filename):
    if user_args.filename:
        return user_args.filename
    elif configs_filename:
        return configs_filename
    else:
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        return f"output_{timestamp}"


def get_data_paths(user_args, configs_refdir, configs_primarydir, flag=[False]):
    refdir = user_args.refdir if user_args.refdir \
        else configs_refdir
    primarydir = user_args.primarydir if user_args.primarydir \
        else configs_primarydir

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


def get_write_msgs(user_args, configs_msgs):
    if user_args.include_msgs:
        return user_args.include_msgs
    else:
        return configs_msgs


def load_configs(file_path):
    try:
        with open(file_path, 'r') as file:
            user_config = json.load(file)
    except FileNotFoundError:
        print(f"Error: Config file '{file_path}' not found.")
    return user_config


# ***************************************************************************
# *************************** PROGRAM START *********************************
# ***************************************************************************
def main(args):

    # Store loaded configs.
    config = load_configs(args.config)

    # Get all program configurations. Config can be overridden by command line
    # arguments.

    # Get data paths.
    config_refdir = config['data']['paths']['refdir']
    config_primarydir = config['data']['paths']['primarydir']
    args_flag_ptr = [False]
    paths = get_data_paths(args, config_refdir, config_primarydir, flag=args_flag_ptr)

    # Get output configurations.
    config_filename = config['output']['base_filename']
    filename = get_filename(args, config_filename)
    config_path = config['output']['path']
    write_path = get_output_path(args, config_path)
    config_msgs = config['output']['execution_msgs']
    write_msgs = get_write_msgs(args, config_msgs)

    # Get mode of analysis.
    analysis = args.mode if args.mode else config['analysis']['mode']
    do_correction = True if analysis == "corrected" else False

    # Check that get_data_paths succeeded.
    if args_flag_ptr[0] is True:
        ref_path = paths[0]
        primary_path = paths[1]
    else:
        print("args_flag_ptr is False. Data paths not entered, "
              "or paths do not exist. Exiting program.")
        sys.exit()

    # Get all csv files from primary path.
    primary_csv_files = glob.glob(f"{primary_path}/*.csv")

    # Ignore csv files for harmonic water level (harmwl) from ref path.
    pattern = f"{ref_path}/*_*_water_level.csv"
    ref_csv_files = glob.glob(pattern)

    # Check that files matching the pattern were found.
    if not ref_csv_files:
        print("Failed to match files to ref filename pattern. Exiting program.")
        sys.exit()

    # Prompt user for the start and end year of their data.
    if write_msgs:
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

    # Read and split up the primary files.
    primary_df_arr = []
    for primary_file in primary_csv_files:

        # Read the file into a dataframe.
        df = fp.read_file_to_df(primary_file, flag=flag_ptr)

        # If read was successful, split into yearly data and append to primary_df_arr.
        if flag_ptr[0]:
            split_df = fp.split_by_year(df, df.columns[0])
            primary_df_arr.extend(split_df)
        else:
            msg = f"failed to read file: {primary_file}\n"
            print(msg)
            error_summary.append(msg)
    # End for.

    # Send ref data into dataframes. The files are already split by year.
    ref_df_arr = []
    for ref_file in ref_csv_files:

        df = fp.read_file_to_df(ref_file, flag=flag_ptr)

        if flag_ptr[0] is True:
            ref_df_arr.append(df)
        else:
            msg = f"failed to read file: {ref_file}\n"
            print(msg)
            error_summary.append(msg)
    # End for.

    # Use da.config to get the necessary columns for assigning column names.
    primary_data_cols = config['data']['primary_data_column_names']
    ref_data_cols = config['data']['reference_data_column_names']

    # Assign column names. If not configured, assume their positions in the dataframe,
    # and that all dfs in the array have same column names.
    primary_dt_col_name = primary_df_arr[0].columns[0] if not primary_data_cols['datetime'] \
        else primary_data_cols['datetime']
    primary_pwl_col_name = primary_df_arr[0].columns[1] if not primary_data_cols['water_level'] \
        else primary_data_cols['water_level']
    ref_dt_col_name = ref_df_arr[0].columns[0] if not ref_data_cols['datetime'] \
        else ref_data_cols['datetime']
    ref_pwl_col_name = ref_df_arr[0].columns[1] if not ref_data_cols['water_level'] \
        else ref_data_cols['water_level']

    # Clean primary dataframes. Print error messages.
    for primary_df in primary_df_arr:

        # Initialize error message.
        error_msg = [""]

        fp.clean_dataframe(primary_df, primary_dt_col_name, primary_pwl_col_name, error=error_msg)
        year = primary_df[primary_dt_col_name].dt.year

        if not all(e == "" for e in error_msg):
            msg = (f"clean_dataframe returned message for primary file - year {year[0]}. "
                   f"error message: {error_msg}\n")
            print(msg)
            error_summary.append(msg)
    # End for.

    # Clean ref dataframes. Print error messages.
    for ref_df in ref_df_arr:

        # Initialize error message.
        error_msg = [""]

        fp.clean_dataframe(ref_df, ref_dt_col_name, ref_pwl_col_name, error=error_msg)
        year = ref_df[ref_dt_col_name].dt.year

        if not all(e == "" for e in error_msg):
            msg = (f"clean_dataframe returned message for ref file - year {year[0]}. "
                   f"error message: {error_msg}\n")
            print(msg)
            error_summary.append(msg)
    # End for.

    # Make sure only common years are compared in the analysis.
    primary_dfs_dict = fp.get_df_dictionary(primary_df_arr, primary_dt_col_name)
    ref_dfs_dict = fp.get_df_dictionary(ref_df_arr, ref_dt_col_name)
    common_years = set(primary_dfs_dict.keys()) & set(ref_dfs_dict.keys())

    # Modify common_years to only include years from configurations.
    config_years = set(args.years) if args.years else set(config['analysis']['years'])

    if "all_years" in config_years:
        config_years.remove("all_years")
        config_years = common_years

    common_years = common_years & config_years
    common_years = sorted(common_years)

    # Get years for reports.
    config_report_years = config['output']['generate_reports_for_years']
    metrics_summary_years = common_years if config_report_years['metrics_summary'] == ['all_years'] \
        else config_report_years['metrics_summary']
    metrics_detailed_years = common_years if config_report_years['metrics_detailed'] == ['all_years'] \
        else config_report_years['metrics_detailed']
    temp_corr_summary_years = common_years if config_report_years['temporal_corrections_summary'] == ['all_years'] \
        else config_report_years['temporal_corrections_summary']
    temp_corr_proc_years = common_years if config_report_years['temporal_correction_processing'] == ['all_years'] \
        else config_report_years['temporal_correction_processing']

    # Record which years have no data for analysis.
    header = ["Analysis could not be done for year(s): \n"]
    bad_years = []
    if write_msgs:  # Skip if user has opted out of program messages.
        for year in year_range:
            if year not in common_years:
                bad_years.append(year)
        # End for.

    # Process the dataframes of common years to get statistics.
    # Initialize summary and temporal offsets summary dataframe.
    summary = {}
    all_processed_years_df = pd.DataFrame(columns=['year', 'temporal_offsets', 'vertical_offsets',
                                                   'initial_nan_percent', 'final_nan_percent',
                                                   'increased_nan_percent'])

    for year in common_years:

        # Instantiate an objects to get metrics and process offsets. Set configs.
        calculator = MetricsCalculator(user_config=config)
        corrector = TransformData(user_config=config)

        # Assign dataframes from dictionaries.
        primary_df = primary_dfs_dict[year]
        ref_df = ref_dfs_dict[year]

        # Drop unrelated columns.
        for col in primary_df.columns:
            if col not in (primary_dt_col_name, primary_pwl_col_name):
                primary_df.drop(columns=col, inplace=True)
        for col in ref_df.columns:
            if col not in (ref_dt_col_name, ref_pwl_col_name):
                ref_df.drop(columns=col, inplace=True)

        # Merge the dataframes on the datetimes. Any missing datetimes in one of the dfs
        # will result in the addition of a NaN in the other.
        merged_df = pd.merge(primary_df, ref_df, how='outer', left_on=primary_dt_col_name,
                             right_on=ref_dt_col_name, suffixes=('_primary', '_reference'))

        # Reassign column names.
        primary_pwl_col_name = merged_df.columns[1]
        ref_dt_col_name = merged_df.columns[2]
        ref_pwl_col_name = merged_df.columns[3]

        # Get size of merged dataframe.
        size = len(merged_df)

        # If doing analysis on corrected data, correct data here.
        if do_correction:

            # Initialize dataframe pointer to hold info about temporal offsets, and assign temporal offset stats.
            # shifts_summary_df = [TransformData.get_temporal_processing_summary_dataframe()]

            # Determine if temporal processing report should be written for the current year.
            write_processing_report = True if year in temp_corr_proc_years else False
            output_path = (f"{write_path}/correction_reports/{filename}_{year}"
                           f"_temporal_correction_processing.txt")\
                if write_processing_report else ""

            initial_nan_percentage = round((len(
                merged_df[merged_df[primary_pwl_col_name].isna()]) / size) * 100, 4)

            corrected_df = merged_df.copy()
            corrected_df = corrector.temporal_shift_corrector(corrected_df,
                                                              primary_data_column_name=primary_pwl_col_name,
                                                              reference_data_column_name=ref_pwl_col_name,
                                                              datetime_column_name=ref_dt_col_name,
                                                              enable_write=write_processing_report,
                                                              write_path=output_path)

            final_nan_percentage = round((len(
                corrected_df[corrected_df[primary_pwl_col_name].isna()]) / size) * 100, 4)

            # shifts_summary_df = shifts_summary_df[0]
            shifts_summary_df = corrector.get_shifts_summary_df()

            # Add to all-years summary dataframe.
            if year in temp_corr_summary_years:
                processed_year_row = pd.DataFrame({
                    'year': [year],
                    'temporal_offsets': [shifts_summary_df['temporal_shift'].unique().tolist()],
                    'vertical_offsets': [shifts_summary_df['vertical_offset'].unique().tolist()],
                    'initial_nan_percent': [initial_nan_percentage],
                    'final_nan_percent': [final_nan_percentage],
                    'increased_nan_percent': [final_nan_percentage - initial_nan_percentage]
                })
                all_processed_years_df = pd.concat([all_processed_years_df, processed_year_row],
                                                   ignore_index=True)

            # Update merged_df.
            merged_df = corrected_df.copy()
        # End temporal correction.

        # Get comparison table.
        stats_df = MetricsCalculator.get_comparison_stats(merged_df[primary_pwl_col_name],
                                                          merged_df[ref_pwl_col_name], size)

        # Get offset runs dataframe.
        run_data_df = calculator.generate_runs_df(merged_df[primary_pwl_col_name],
                                                  merged_df[ref_pwl_col_name],
                                                  merged_df[ref_dt_col_name], size)

        # Set the dataframe.
        calculator.set_runs_dataframe(run_data_df)

        # Calculate metrics.
        metrics = calculator.calculate_metrics()
        
        # Set metrics.
        calculator.set_metrics(metrics)

        # Format metrics.
        metrics_list = calculator.format_metrics()

        # Get table of long offsets.
        offsets_dict = calculator.generate_long_offsets_info()

        # Append year info to metrics summary.
        if year in metrics_summary_years:
            summary[year] = {
                "% agree": stats_df.loc['total agreements', 'percent'],
                "% values disagree": stats_df.loc['value disagreements', 'percent'],
                "% total disagree": stats_df.loc['total disagreements', 'percent'],
                "% missing": stats_df.loc['missing (primary)', 'percent'],
                "# long offsets": metrics['long_offsets_count'],
                "# long gaps": metrics['long_gaps_count'],
                "unique long offsets": list(offsets_dict.keys()),
                "# large discrepancies": metrics['large_offsets_count'],
                "minimum value": metrics['min_max_offsets'][1],
                "maximum value": metrics['min_max_offsets'][0]
            }

        # Write detailed metrics report.
        if year in metrics_detailed_years:
            MetricsCalculator.write_stats(stats_df, write_path, f"{filename}_metrics_detailed", year)
            MetricsCalculator.write_metrics_to_file(metrics_list, write_path, f"{filename}_metrics_detailed")
            MetricsCalculator.write_offsets_to_file(offsets_dict, write_path, f"{filename}_metrics_detailed")
    # End for.

    # Write summary file.
    if metrics_summary_years:
        with open(f'{write_path}/{filename}_metrics_summary.txt', 'a') as file:
            file.write(f"Configurations: {json.dumps(config, indent=4)}\n\n")
            file.write(f"% agree: Percentage of values that agree between datasets.\n"
                       f"% values disagree: Percentage of values that disagree (excluding NaNs).\n"
                       f"% total disagree: Percentage of data that disagrees (including NaNs).\n" 
                       f"% missing: Percentage of the primary data that is missing (NaN).\n"
                       f"# long offsets: The number of offsets that meet the duration threshold.\n"
                       f"# long gaps: The number of gaps that meet the duration threshold.\n"
                       f"unique long offsets: List of unique offset values that meet the duration threshold.\n"
                       f"# large discrepancies: The number of discrepancies that meet the value threshold.\n"
                       f"minimum value: The minimum discrepancy value.\n"
                       f"maximum value: The maximum discrepancy value.\n\n")
        fp.write_table_from_nested_dict(summary, 'Year',
                                        f'{write_path}/{filename}_metrics_summary.txt')

    # Write error_summary and header to the text file if include_msgs is True.
    if write_msgs:
        bad_years.sort()
        header.extend([str(y) + " " for y in bad_years])
        results_title = ["***************************************************************************\n"
                         "******************************* RESULTS ***********************************\n"
                         "***************************************************************************\n\n"]
        text_list = header + ["\n\n"] + error_summary + ["\n\n"] + results_title
        with open(f'{write_path}/{filename}_execution_messages.txt', 'a') as file:
            file.write(''.join(text_list))

    # Write temporal offset correction summary for all years.
    if temp_corr_summary_years:
        with open(f"{write_path}/correction_reports/{filename}"
                  f"_temporal_correction_summary.txt", 'a') as file:
            file.write(all_processed_years_df.to_string())
# End main.


if __name__ == "__main__":
    main_args = parse_arguments()
    main(main_args)

# ***************************************************************************
# *************************** PROGRAM END ***********************************
# ***************************************************************************
