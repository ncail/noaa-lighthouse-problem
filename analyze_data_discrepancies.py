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

    # Get paths to Bob Hall Pier data for Lighthouse and NOAA.
    bhp_lighthouse_path = 'lighthouse/Bob Hall Pier'
    bhp_noaa_path = 'NOAA/bobHallPier'

    # Get all csv files from Lighthouse path.
    lighthouse_csv_files = glob.glob(f"{bhp_lighthouse_path}/*.csv")

    # Ignore csv files for harmonic water level (harmwl) from NOAA path.
    pattern = f"{bhp_noaa_path}/bobHallPier_*_water_level.csv"
    noaa_csv_files = glob.glob(pattern)

    # Read and split up the lighthouse files.
    for lh_file in lighthouse_csv_files:
        da.read_file(lh_file)

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




