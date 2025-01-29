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

    # Get positions of datetime and water level columns in order of: ref, primary.
    dt_col_pos = [config['data']['columns']['ref_dt_pos'], config['data']['columns']['primary_dt_pos']]
    pwl_col_pos = [config['data']['columns']['ref_wl_pos'], config['data']['columns']['primary_wl_pos']]

    # Loop over paths if they were entered. paths contains refdir, then primarydir, so uses the column positions in
    # this order.
    for loop in range(2):
        if paths[loop]:
            # Output path.
            output_path = f'{paths[loop]}/preprocessed'

            # Get all csv files from primary path.
            csv_files = glob.glob(f"{paths[loop]}/*.csv")

            # Initialize a flag pointer to check if read_file() is successful.
            flag_ptr = [False]

            # Read files to a concatd dataframe, then split by year.
            df = pd.DataFrame()
            for file in csv_files:
                df_from_file = helpers.read_file_to_df(file, flag=flag_ptr)

                if flag_ptr[0]:
                    df = pd.concat([df, df_from_file])
            # End for.

            split_df_arr = helpers.split_by_year(df, df.columns[0])

            # Clean and output primary dataframes.
            if not os.path.exists(output_path):
                os.makedirs(output_path)
            for df in split_df_arr:
                cleaned_df = helpers.clean_dataframe(df, df.columns[dt_col_pos[loop]], df.columns[pwl_col_pos[loop]])
                years = cleaned_df[cleaned_df.columns[dt_col_pos[loop]]].dt.year

                if not pd.isna(years[0]):
                    cleaned_df.to_csv(f'{output_path}/{int(years[0])}.csv', index=False)
            # End for.
        # End if.
    # End for.
# End main.


if __name__ == "__main__":
    main_args = helpers.parse_arguments()
    main(main_args)
