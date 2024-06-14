# Import file_data_functions.py as da (discrepancy analysis).
import file_data_functions as da

# Imports continued...
import os
import numpy as np
import pandas as pd
import glob
import matplotlib.pyplot as plt


# ***************************************************************************
# *************************** PROGRAM START *********************************
# ***************************************************************************

def main():

    # Assign paths to Bob Hall Pier data for Lighthouse and NOAA.
    bhp_lighthouse_path = 'data/lighthouse/Bob Hall Pier'
    bhp_noaa_path = 'data/NOAA/bobHallPier'

    # Get all csv files from Lighthouse path.
    lighthouse_csv_files = glob.glob(f"{bhp_lighthouse_path}/*.csv")

    # Ignore csv files for harmonic water level (harmwl) from NOAA path.
    pattern = f"{bhp_noaa_path}/bobHallPier_*_water_level.csv"
    noaa_csv_files = glob.glob(pattern)

    # Initialize dataframe arrays to hold the yearly Lighthouse and NOAA data.
    lh_df_arr = []
    noaa_df_arr = []

    # Initialize a flag pointer to check if da.read_file() was successful.
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
    # End for.

    # Send NOAA data into dataframes. The files are already split by year.
    for noaa_file in noaa_csv_files:

        noaa_df_arr.append(da.read_file(noaa_file, flag=flag_ptr))

        if flag_ptr[0] is False:
            print(f"failed to read file: {noaa_file}\n")
    # End for.

    # Get column names. Assumes all dataframes in the list have same column names.
    # Avoids repeated assignment in the loop which does not make this assumption.
    lh_dt_col_name = lh_df_arr[0].columns[0]
    lh_pwl_col_name = lh_df_arr[0].columns[1]
    noaa_dt_col_name = noaa_df_arr[0].columns[0]
    noaa_pwl_col_name = noaa_df_arr[0].columns[1]

    # Process dataframes for discrepancies between lighthouse and noaa.
    for lh_df, noaa_df in zip(lh_df_arr, noaa_df_arr):

        # Clean dataframe.
        da.clean_dataframe(lh_df, lh_dt_col_name, lh_pwl_col_name, flag=flag_ptr)
        da.clean_dataframe(noaa_df, noaa_dt_col_name, noaa_pwl_col_name, flag=flag_ptr)

        # If either read failed, skip this iteration.
        if flag_ptr is False:
            print("clean_dataframe failed.\n")
            continue

        # Get size of dataframes.
        lh_size = len(lh_df[lh_pwl_col_name])
        noaa_size = len(noaa_df[noaa_pwl_col_name])

        if lh_size != noaa_size:
            print("sizes are not equal: ", lh_size, " ", noaa_size, "\n")
            continue

        stats_df = da.get_comparison_stats(lh_df[lh_pwl_col_name], noaa_df[noaa_pwl_col_name], noaa_size)

        # Write stats_df to csv with year in file name.
        year = noaa_df[noaa_dt_col_name].dt.year
        stats_df.to_csv(f'generated_files/bobHallPier_{year[0]}_noaa_vs_lh_stats.csv', index=False)
        break



    # Loop for each station.
        # Loop for each year.
            # read file() into dataframe.
            # clean dataframe() into same dataframe.
            # write comparison stats() into .txt file.
            # do get run data() and filter by duration() for longer than one day.
            # count the number of rows in the filtered dataframe and write to .txt file.
            # write the min/max duration row (includes offset, and start/end date).
            # filter by value (larger than 5 cm).
            # count number of rows in this dataframe.
            # get max discrepancy and try to note the type of case (flatline, etc).
            # write these to .txt file.


if __name__ == "__main__":
    main()

# ***************************************************************************
# *************************** PROGRAM END ***********************************
# ***************************************************************************




