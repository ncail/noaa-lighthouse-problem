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
    args_flag_ptr = [True]
    paths = helpers.get_data_paths(args, config_refdir, config_primarydir, flag=args_flag_ptr)

    # Check that get_data_paths succeeded.
    if args_flag_ptr[0] is False:
        print("args_flag_ptr is False. Path(s) do not exist. Exiting program.")
        sys.exit()

    # Loop over paths if they were entered.
    for path in paths:
        if path:
            # Output path.
            output_path = f'{path}/preprocessed'

            # Get all csv files from primary path.
            csv_files = glob.glob(f"{path}/*.csv")

            # Initialize a flag pointer to check if read_file() is successful.
            flag_ptr = [False]

            # Read and split up the files.
            df_arr = []
            for file in csv_files:
                df = helpers.read_file_to_df(file, flag=flag_ptr)

                # If read was successful, split into yearly data and append to primary_df_arr.
                if flag_ptr[0]:
                    split_df = helpers.split_by_year(df, df.columns[0])
                    df_arr.extend(split_df)
            # End for.

            # Assume position of datetime and water level columns.
            dt_col_pos = 0
            pwl_col_pos = 1

            # Clean and output primary dataframes.
            if not os.path.exists(output_path):
                os.makedirs(output_path)
            for df in df_arr:
                cleaned_df = helpers.clean_dataframe(df, df.columns[dt_col_pos], df.columns[pwl_col_pos])
                years = cleaned_df[cleaned_df.columns[dt_col_pos]].dt.year

                if not pd.isna(years[0]):
                    cleaned_df.to_csv(f'{output_path}/{int(years[0])}.csv', index=False)
            # End for.
    # End for.
# End main.


if __name__ == "__main__":
    main_args = helpers.parse_arguments()
    main(main_args)
