# Import helpers.
import helpers

# Import classes.
from MetricsCalculator import MetricsCalculator
from TransformData import TransformData

# Imports continued...
import os
import sys
import glob
import pandas as pd
import numpy as np
import json
import logging
import datetime
import time


def custom_logger(user_level, file):
    level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    level = level_map.get(user_level.upper(), logging.INFO)  # Default level is INFO if user_level is invalid.

    # Create a custom logger.
    logger = logging.getLogger(__name__)  # Name of logger is module it is created in ('analyze_data').
    logger.setLevel(logging.DEBUG)  # Log all levels DEBUG and above.

    # Create handlers.
    console_handler = logging.StreamHandler()  # For logging to the console.
    file_handler = logging.FileHandler(file)  # For logging to a file.

    # Set levels for handlers.
    console_handler.setLevel(level)
    file_handler.setLevel(level)

    # Create formatters.
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to the logger.
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


''' ***********************************************************************************************************
    ********************************************** PROGRAM START **********************************************
    *********************************************************************************************************** '''
def main(args):
    program_start_time = time.perf_counter()

    # Store loaded configs.
    config = helpers.load_configs(args.config)

    # Configure logger.
    logging_enabled = args.logging and config['logging']['enabled']
    logging_level = config['logging']['level']
    logging_file = config['logging']['file']
    logger = custom_logger(logging_level, logging_file)
    if not logging_enabled:
        logging.disable(logging.CRITICAL)  # Disables all logging.

    logger.info(f"Program started with logging level: {logging_level}")

    # Get program configurations: allow config to be overridden by cmdl args.
    # Get data paths.
    config_refdir = config['data']['paths']['refdir']
    config_primarydir = config['data']['paths']['primarydir']
    args_flag_ptr = [True]
    paths = helpers.get_data_paths(args, config_refdir, config_primarydir, flag=args_flag_ptr)

    # Get output configurations.
    config_filename = config['output']['base_filename']
    filename = helpers.get_filename(args, config_filename)
    config_path = config['output']['path']
    write_path = helpers.get_output_path(args, config_path)
    if not os.path.exists(write_path):
        os.makedirs(write_path)

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

    logger.info("Program configured")

    # Get all csv files from primary path.
    primary_csv_files = glob.glob(f"{primary_path}/*.csv")

    # Get all csv files from ref path.
    ref_csv_files = glob.glob(f"{ref_path}/*.csv")

    # Read primary files into dataframes.
    primary_df_arr = []
    for primary_file in primary_csv_files:
        column_names = pd.read_csv(primary_file, nrows=0).columns.tolist()
        df = pd.read_csv(primary_file, parse_dates=[column_names[0]])
        primary_df_arr.append(df)
    # End for.

    # Read ref files into dataframes.
    ref_df_arr = []
    for ref_file in ref_csv_files:
        column_names = pd.read_csv(ref_file, nrows=0).columns.tolist()
        df = pd.read_csv(ref_file, parse_dates=[column_names[0]])
        ref_df_arr.append(df)
    # End for.

    logger.info("Data files read into dataframes")

    # Assume position of datetime columns.
    primary_dt_col_pos = 0
    ref_dt_col_pos = 0

    # Make sure only common years are compared in the analysis.
    primary_dfs_dict = helpers.get_df_dictionary(primary_df_arr, primary_dt_col_pos)
    ref_dfs_dict = helpers.get_df_dictionary(ref_df_arr, ref_dt_col_pos)
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
    vert_offset_info_duration_filtered_years = common_years if (
            config_report_years['vertical_offset_info_duration_filtered'] == ['all_years']) \
        else config_report_years['vertical_offset_info_duration_filtered']
    vert_offset_info_value_filtered_years = common_years if config_report_years['vertical_offset_info_value_filtered'] == [
        'all_years'] else config_report_years['vertical_offset_info_value_filtered']
    temp_corr_summary_years = common_years if config_report_years['temporal_shifts_summary'] == ['all_years'] \
        else config_report_years['temporal_shifts_summary']
    annotated_raw_data_years = common_years if config_report_years['annotated_raw_data'] == ['all_years'] \
        else config_report_years['annotated_raw_data']

    # Process the dataframes of common years to get statistics.
    # Initialize summary and temporal offsets summary dataframe.
    summary = {}
    all_processed_years_df = pd.DataFrame(columns=['Year', 'Temporal shifts', 'Vertical offsets', 'Time-shifted data %',
                                                   'Positive error %', 'Initial NaN %', 'Final NaN %',
                                                   'Increased NaN %'])

    # Initialize series data dictionary. This will be concat-ed for every year in for loop.
    getXformDataDict = TransformData()
    series_data_concat_dict = getXformDataDict.get_time_shift_table()

    ''' ***********************************************************************************************************
        ********************************************* PROCESSING LOOP *********************************************
        *********************************************************************************************************** '''
    logger.info("Processing loop start")
    processing_loop_start_time = time.perf_counter()
    for year in common_years:
        logger.info(f"Processing year {common_years.index(year) + 1}/{len(common_years)}")
        process_year_start_time = time.perf_counter()

        # Instantiate an objects to get metrics and process offsets. Set configs.
        calculator = MetricsCalculator(user_config=config)
        corrector = TransformData(user_config=config)

        # Set documenting the corrected data entries to False so the raw data is reflected.
        corrector.set_document_corrected_time_shift_series_data(False)

        # Assign dataframes from dictionaries.
        primary_df = primary_dfs_dict[year]
        ref_df = ref_dfs_dict[year]

        # Drop unrelated columns.
        primary_col_names = primary_df.columns[0], primary_df.columns[1]
        ref_col_names = ref_df.columns[0], ref_df.columns[1]
        for col in primary_df.columns:
            if col not in primary_col_names:
                primary_df.drop(columns=col, inplace=True)
        for col in ref_df.columns:
            if col not in ref_col_names:
                ref_df.drop(columns=col, inplace=True)

        # To merge the ref and primary dfs, do two left joins on the expected datetimes.
        # Create the datetime column manually.
        datetimes_list = pd.date_range(
            start=datetime.datetime(year=year, month=1, day=1),
            end=datetime.datetime(year=year, month=12, day=31, hour=23, minute=54),
            freq=datetime.timedelta(minutes=6)  # Frequency of 6 minutes
        ).tolist()
        datetimes_col = pd.DataFrame({'datetime': datetimes_list})

        # Do the left joins.
        datetimes_col.set_index('datetime', inplace=True)
        primary_df.set_index(primary_col_names[0], inplace=True)
        ref_df.set_index(ref_col_names[0], inplace=True)

        merged_df = datetimes_col.join(ref_df, how='left')
        merged_df = merged_df.join(primary_df, how='left', lsuffix='_primary', rsuffix='_reference')

        # Reset the index to get datetime column back as a regular column.
        merged_df.reset_index(inplace=True)

        # Reassign columns.
        ref_dt_col_name = merged_df.columns[0]
        ref_pwl_col_name = merged_df.columns[1]
        primary_pwl_col_name = merged_df.columns[2]

        # Get size of merged dataframe.
        size = len(merged_df)

        initial_nan_percentage = (len(merged_df[merged_df[primary_pwl_col_name].isna()]) / size) * 100

        correction_start_time = time.perf_counter()
        logger.info(f"Temporal correction for year ({common_years.index(year) + 1}) start")

        corrected_df = merged_df.copy()
        corrected_df = corrector.temporal_shift_corrector(corrected_df,
                                                          primary_data_column_name=primary_pwl_col_name,
                                                          reference_data_column_name=ref_pwl_col_name,
                                                          datetime_column_name=ref_dt_col_name)

        correction_end_time = time.perf_counter()
        correction_duration = correction_end_time - correction_start_time
        logger.info(f"Temporal correction algorithm for year ({common_years.index(year) + 1}) finished in "
                    f"{correction_duration:.2f} seconds")

        final_nan_percentage = (len(corrected_df[corrected_df[primary_pwl_col_name].isna()]) / size) * 100

        # Get the annotated raw series data for current year, concat each year in common years, the corresponding time
        # and vertical offsets per data point are also listed.
        series_data_annotated_current_year = corrector.get_time_shift_table()
        if year in annotated_raw_data_years:
            # Concats the current year dict and all-years dict by extending the values for each dict key.
            series_data_concat_dict = \
                {key: series_data_concat_dict[key] + series_data_annotated_current_year[key] for key in
                 series_data_concat_dict}

        # Get time-shifted percentage and error.
        series_data_df = pd.DataFrame(series_data_annotated_current_year)
        # Gets the time shifted cells that are not N/A or 0.
        num_time_shifted = (~series_data_df['temporal_shift'].isin(['N/A', 0])).sum()
        percent_time_shifted = num_time_shifted / len(series_data_df) * 100

        error = (series_data_df['temporal_shift'] == 'N/A').sum()
        error_percent = error / len(series_data_df) * 100

        # Add to all-years summary dataframe.
        shifts_summary_df = corrector.get_shifts_summary_df()[0]
        if year in temp_corr_summary_years:
            processed_year_row = pd.DataFrame({
                'Year': [year],
                'Temporal shifts': [shifts_summary_df['temporal_shift'].unique().tolist()],
                'Vertical offsets': [shifts_summary_df['vertical_offset'].unique().tolist()],
                'Time-shifted data %': [round(percent_time_shifted, 4)],
                'Positive error %': [round(error_percent, 4)],
                'Initial NaN %': [round(initial_nan_percentage, 4)],
                'Final NaN %': [round(final_nan_percentage, 4)],
                'Increased NaN %': [round(final_nan_percentage - initial_nan_percentage, 4)]
            })
            all_processed_years_df = all_processed_years_df.dropna(axis=1, how='all')
            all_processed_years_df = pd.concat([all_processed_years_df, processed_year_row],
                                               ignore_index=True)

        # If doing analysis on corrected data, update merged_df with corrected_df.
        if do_correction:
            merged_df = corrected_df.copy()

        # Get comparison table.
        stats_df = MetricsCalculator.get_comparison_stats(merged_df[primary_pwl_col_name],
                                                          merged_df[ref_pwl_col_name], size)

        # Get offset runs dataframe.
        # run_data_df = calculator.generate_runs_df(merged_df[primary_pwl_col_name],
        #                                           merged_df[ref_pwl_col_name],
        #                                           merged_df[ref_dt_col_name], size)

        # Set column names in calculator to match those in shifts_summary_df from TransformData.
        calculator.set_column_names(shifts_summary_df.columns[2], shifts_summary_df.columns[4],
                                    shifts_summary_df.columns[0], shifts_summary_df.columns[1])

        # Set the dataframe in calculator so metrics can be calculated.
        # Using shifts_summary_df from TransformDate is preferable to using runs_df from MetricsCalculator because
        # the latter assumes gaps are interruptions to vertical offsets while the former does not.
        shifts_summary_df.replace('N/A', np.nan, inplace=True)
        calculator.set_runs_dataframe(shifts_summary_df)

        # Calculate metrics.
        metrics = calculator.calculate_metrics()

        # Set metrics.
        # calculator.set_metrics(metrics)

        # Get table of offsets filtered by duration.
        offsets_duration_filtered_df = calculator.generate_duration_filtered_offsets_info()

        # Get table of offsets filtered by value.
        offsets_value_filtered_df = calculator.generate_value_filtered_offsets_info()

        # Append year info to metrics summary.
        if year in metrics_summary_years:
            summary[year] = {
                "% agree": stats_df.loc['total agreements', 'percent'],
                "% values disagree": stats_df.loc['value disagreements', 'percent'],
                "% missing": stats_df.loc['missing (primary)', 'percent'],
                "% total disagree": stats_df.loc['total disagreements', 'percent'],
                "% time-shifted": f'{round(percent_time_shifted, 2)} (+{round(error_percent, 2)})',
                "# DSs (FBD)": metrics['duration_filtered_offsets_count'],
                "# gaps (FBD)": metrics['duration_filtered_gaps_count'],
                "DSs list (FBD)": list(offsets_duration_filtered_df['offset'].unique()),
                "# DSs (FBV)": metrics['value_filtered_offsets_count'],
                "min DS": metrics['min_max_offsets'][1],
                "max DS": metrics['min_max_offsets'][0]
            }

        # Write vertical offsets info (FBD) report.
        reorder_columns = [shifts_summary_df.columns[4], shifts_summary_df.columns[0], shifts_summary_df.columns[1],
                           shifts_summary_df.columns[2]]
        if year in vert_offset_info_duration_filtered_years:
            offsets_duration_filtered_df = offsets_duration_filtered_df[reorder_columns]  # Automatically drops the
            # temporal_shift column from shifts_summary_df.
            offsets_duration_filtered_df.rename(columns={"vertical_offset": "vertical offset", "start_date": "start "
                                                         "date", "end_date": "end date"}, inplace=True)

            offsets_duration_filtered_df.to_csv(f"{write_path}/{filename}_{year}_"
                                                f"vertical_offset_info_duration_filtered.csv", index=False)

        # Write vertical offsets info (FBV) report.
        if year in vert_offset_info_value_filtered_years:
            offsets_value_filtered_df = offsets_value_filtered_df[reorder_columns]
            offsets_value_filtered_df.rename(columns={"vertical_offset": "vertical offset", "start_date": "start "
                                                      "date", "end_date": "end date"}, inplace=True)

            offsets_value_filtered_df.to_csv(f"{write_path}/{filename}_{year}_"
                                             f"vertical_offset_info_value_filtered.csv", index=False)

        process_year_end_time = time.perf_counter()
        process_year_duration = process_year_end_time - process_year_start_time
        logger.info(f"Year ({common_years.index(year) + 1}) processed in {process_year_duration:.2f} seconds")
    # End processing loop.
    processing_loop_end_time = time.perf_counter()
    processing_loop_duration = processing_loop_end_time - processing_loop_start_time
    logger.info(f"Processing loop finished in {processing_loop_duration:.2f} seconds")

    ''' ***********************************************************************************************************
        ************************************************* RESULTS *************************************************
        *********************************************************************************************************** '''
    logger.info("Writing results")
    # Write configs to file.
    with open(f'{write_path}/{filename}_configs.txt', 'w') as file:
        file.write(f"Configurations: {json.dumps(config, indent=4)}")

    if annotated_raw_data_years:
        # Get table of annotated series data.
        series_data_annotated_df = pd.DataFrame(series_data_concat_dict)

        reorder_columns = ['date_time', 'primary_water_level', 'reference_water_level', 'vertical_offset',
                           'temporal_shift']
        series_data_annotated_df = series_data_annotated_df[reorder_columns]
        series_data_annotated_df = (series_data_annotated_df.rename(columns={
                                    'date_time': 'Date Time', 'primary_water_level': 'Primary Water Level',
                                    'reference_water_level': 'Reference Water Level', 'vertical_offset': 'Vertical '
                                    'Offset', 'temporal_shift': 'Temporal Shift'}))

        # Write time shift table to CSV.
        series_data_annotated_df.to_csv(f"{write_path}/{filename}_annotated_raw_data.csv", index=False)

    # Write summary file.
    metrics_config = {
        "analysis_mode": config['analysis']['mode'],
        "filter_offsets_by_duration": config["filter_offsets_by_duration"],
        "filter_offsets_by_value": config["filter_offsets_by_value"],
        "filter_gaps_by_duration": config['filter_gaps_by_duration'],
        "temporal_shift_correction": config["temporal_shift_correction"]
    }
    if metrics_summary_years:
        with open(f'{write_path}/{filename}_metrics_summary.txt', 'w') as file:
            file.write(f"Configurations: {json.dumps(metrics_config, indent=4)}\n\n")
            file.write(f"% agree: Percentage of values that agree between datasets.\n"
                       f"% values disagree: Percentage of values that disagree (excluding NaNs) between datasets.\n"
                       f"% missing: Percentage of the primary data that is missing (NaN).\n"
                       f"% total disagree: Percentage of data that disagrees (including NaNs) between datasets.\n"
                       f"% time-shifted: Percentage of data (with positive error) that is time-shifted.\n"
                       f"# VOs (FBD): Number of vertical offsets (filtered by duration).\n"
                       f"# gaps (FBD): Number of gaps (filtered by duration).\n"
                       f"VOs list (FBD): List of unique vertical offset values (filtered by duration).\n"
                       f"# VOs (FBV): Number of vertical offsets (filtered by value).\n"
                       f"min VO: Minimum vertical offset value.\n"
                       f"max VO: Maximum vertical offset value.\n\n")
        helpers.write_table_from_nested_dict(summary, 'Year',
                                             f'{write_path}/{filename}_metrics_summary.txt')

    # Write temporal offset correction summary for all years.
    if temp_corr_summary_years:
        all_processed_years_df.to_csv(f"{write_path}/{filename}_temporal_shifts_summary.csv", index=False)

    program_end_time = time.perf_counter()
    program_duration = program_end_time - program_start_time
    logger.info("Program end")
    logger.info(f"Program finished in {program_duration:.2f} seconds")
# End main.


if __name__ == "__main__":
    main_args = helpers.parse_arguments()
    main(main_args)
