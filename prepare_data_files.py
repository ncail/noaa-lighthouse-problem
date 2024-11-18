# Script for preprocessing data files.
# Cleans files, and converts them to Pandas standards.
# Outputs preprocessed files to 'preprocessed' directory inside original data paths.

import helpers

import os
import sys
import glob
import pandas as pd


def main(args):
    # Get configs.
    config = helpers.load_configs(args.config)

    # Get paths to files.
    config_refdir = config['data']['paths']['refdir']
    config_primarydir = config['data']['paths']['primarydir']
    args_flag_ptr = [False]
    paths = helpers.get_data_paths(args, config_refdir, config_primarydir, flag=args_flag_ptr)

    # Check that get_data_paths succeeded.
    if args_flag_ptr[0] is True:
        ref_path = paths[0]
        primary_path = paths[1]
    else:
        print("args_flag_ptr is False. Data paths not entered, or paths do not exist. Exiting program.")
        sys.exit()

    # Output path.
    output_path_primary = f'{primary_path}/preprocessed'
    output_path_ref = f'{ref_path}/preprocessed'

    # Get all csv files from primary path.
    primary_csv_files = glob.glob(f"{primary_path}/*.csv")

    # Get all csv files from ref path.
    ref_csv_files = glob.glob(f"{ref_path}/*.csv")

    # Initialize a flag pointer to check if read_file() is successful.
    flag_ptr = [False]

    # Read and split up the primary files.
    primary_df_arr = []
    for primary_file in primary_csv_files:
        df = helpers.read_file_to_df(primary_file, flag=flag_ptr)

        # If read was successful, split into yearly data and append to primary_df_arr.
        if flag_ptr[0]:
            split_df = helpers.split_by_year(df, df.columns[0])
            primary_df_arr.extend(split_df)
    # End for.

    # Send ref data into dataframes. The files are already split by year.
    ref_df_arr = []
    for ref_file in ref_csv_files:
        df = helpers.read_file_to_df(ref_file, flag=flag_ptr)

        if flag_ptr[0] is True:
            ref_df_arr.append(df)
    # End for.

    # Assume position of datetime and water level columns.
    primary_dt_col_pos = 0
    primary_pwl_col_pos = 1
    ref_dt_col_pos = 0
    ref_pwl_col_pos = 1

    # Clean and output primary dataframes.
    if not os.path.exists(output_path_primary):
        os.makedirs(output_path_primary)
    for primary_df in primary_df_arr:
        cleaned_df = helpers.clean_dataframe(primary_df, primary_df.columns[primary_dt_col_pos],
                                             primary_df.columns[primary_pwl_col_pos])
        years = cleaned_df[cleaned_df.columns[primary_dt_col_pos]].dt.year

        if not pd.isna(years[0]):
            cleaned_df.to_csv(f'{output_path_primary}/{int(years[0])}.csv', index=False)
    # End for.

    # Clean and output ref dataframes.
    if not os.path.exists(output_path_ref):
        os.makedirs(output_path_ref)
    for ref_df in ref_df_arr:
        cleaned_df = helpers.clean_dataframe(ref_df, ref_df.columns[ref_dt_col_pos], ref_df.columns[ref_pwl_col_pos])
        years = cleaned_df[cleaned_df.columns[ref_dt_col_pos]].dt.year

        if not pd.isna(years[0]):
            cleaned_df.to_csv(f'{output_path_ref}/{int(years[0])}.csv', index=False)
    # End for.
# End main.


if __name__ == "__main__":
    main_args = helpers.parse_arguments()
    main(main_args)
