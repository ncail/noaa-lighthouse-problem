# Import file_data_functions.py as da (discrepancy analysis).
import file_data_functions as da

# Imports continued...
import argparse
import datetime
import glob
from datetime import timedelta


# parse_arguments will get command line arguments for the filename
# that main() will write results to.
# Use: python this_program.py --filename myFileName.txt
def parse_arguments():
    parser = argparse.ArgumentParser(description="Write to a specified file.")
    parser.add_argument('--filename', type=str,
                        help='Name of the file to write to', default=None)
    return parser.parse_args()
# End parse_arguments.


# get_filename will return the filename input from the user's command line
# argument, or return a default unique filename using timestamp.
def get_filename(args):
    if args.filename:
        return args.filename
    else:
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        return f"output_{timestamp}.txt"
# End get_filename.


# ***************************************************************************
# *************************** PROGRAM START *********************************
# ***************************************************************************
def main():

    # Parse command line arguments.
    args = parse_arguments()

    # Determine the filename results will be written to.
    filename = get_filename(args)

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

    # ***********************************************************************
    # ********************* CORRECT TIME SHIFT HERE *************************
    # ***********************************************************************

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

        # If either clean failed, skip this iteration.
        if flag_ptr[0] is False:
            print("clean_dataframe failed.\nskipping to next file pair...\n")
            continue

        # Get size of dataframes.
        # If dataframes are not same size, do not attempt getting discrepancy stats,
        # skip to next file pair.
        lh_size = len(lh_df)
        noaa_size = len(noaa_df)
        if lh_size != noaa_size:
            print("sizes are not equal: ", lh_size, " ", noaa_size, "\nskipping to next file pair...\n")
            continue

        # Get comparison table.
        stats_df = da.get_comparison_stats(lh_df[lh_pwl_col_name],
                                           noaa_df[noaa_pwl_col_name], noaa_size)

        # Do get_run_data() to get a dataframe that can be filtered for
        # duration and value.
        runs_df = da.get_run_data(lh_df[lh_pwl_col_name], noaa_df[noaa_pwl_col_name],
                                  noaa_df[noaa_dt_col_name], noaa_size)

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
        bool_mask = runs_df['offset (ref - primary, unit)'] == max_offset
        max_offset_durations = runs_df.loc[bool_mask, 'durations'].to_list()

        # Hold these metrics in metric_data.
        metric_data = [
            ("Number of offsets with duration >= one day", long_offsets_count),
            ("Maximum duration of an offset", max_duration),
            ("Offset value(s) with <" + str(max_duration) + "> duration", max_duration_offsets),
            ("Number of offsets with value >= 5 cm", large_offsets_count),
            ("Maximum offset value", max_offset),
            ("Duration(s) of offset with value <" + str(max_offset) + "> cm", max_offset_durations)
        ]

        year = noaa_df[noaa_dt_col_name].dt.year
        # Write all stats to a .txt file (in append mode).
        with open(f'generated_files/{filename}.txt', 'a') as file:
            file.write(f"Comparison Table for year {year[0]}:\n {stats_df.to_string(index=True)}\n\n")

            # Find the longest key length for key alignment.
            max_key_length = max(len(key) for key, value in metric_data)
            print("max_key_length: ", max_key_length, "\n")

            # Write each key-value pair aligned.
            for key, value in metric_data:
                print(f"Key: '{key}', Length: {len(key)}\n")
                file.write(f"{key:{max_key_length}}: {value}\n")
                # file.write("{:<{width}}: {}\n".format(key, value, width=max_key_length))
                # file.write(f"{key:<{max_key_length}}: {value}\n")
                # output = f'{key:>{max_key_length}}: {value}\n'
                # output = '{}{:>10}'.format(key, str(value))
                # file.write(output)
            # End for.
            file.write("\n\n\n")
            # file.write(f"\n\nNumber of offsets with duration >= one day: {long_offsets_count}")
            # file.write(f"\nMaximum duration of an offset: {max_duration}")
            # file.write(f"\nOffset value(s) with <{max_duration}> duration: {max_duration_offsets}")
            # file.write(f"\nNumber of offsets with value >= 5 cm: {large_offsets_count}")
            # file.write(f"\nMaximum offset value: {max_offset}")
            # file.write(f"\nDuration(s) of offset with value <{max_offset}> cm: {max_offset_durations}\n\n")
        # File closed.
    # End for.


if __name__ == "__main__":
    main()

# ***************************************************************************
# *************************** PROGRAM END ***********************************
# ***************************************************************************
