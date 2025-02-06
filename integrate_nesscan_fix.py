# Imports.
import glob
import pandas as pd
import os
import helpers

# ***************************************************************************
# *************************** PROGRAM START *********************************
# ***************************************************************************

# Specify station (as given in data/lighthouse directory).
station = "portIsabel"

# Specify write path.
duplicates_path = f'generated_files/nesscan-fixed_duplicates'
if not os.path.exists(duplicates_path):
    os.makedirs(duplicates_path)

fixed_lh_file_path = f'data/lighthouse/{station}_nesscan_fixed'
if not os.path.exists(fixed_lh_file_path):
    os.makedirs(fixed_lh_file_path)

# Get lighthouse and nesscan_fixed data files.
lighthouse_files_path = f"data/lighthouse/{station}/preprocessed"
nesscan_files_path = f"data/lighthouse/nesscan_fix_full_history_12172024/{station}/{station}/preprocessed"
lighthouse_files = glob.glob(f"{lighthouse_files_path}/*.csv")
nesscan_files = glob.glob(f"{nesscan_files_path}/*.csv")

# Give everything the same header, so we can use pd.update() to merge the values.
cols = ['dt', 'wl']

# Put into dataframes dictionaries by year.
lh_dfs = {}
lh_df_arr = []
for file in lighthouse_files:
    df = pd.read_csv(file, parse_dates=[0])

    # Rename columns.
    df_cols = df.columns.tolist()
    df_cols[0] = cols[0]
    df_cols[1] = cols[1]
    df.columns = df_cols

    # Drop other columns.
    for col in df.columns:
        if col not in cols:
            df.drop(columns=col, inplace=True)

    lh_df_arr.append(df)
# End for.
lh_dfs = helpers.get_df_dictionary(lh_df_arr, 0)

nesscan_dfs = {}
nes_df_arr = []
for file in nesscan_files:
    df = pd.read_csv(file, parse_dates=[0])

    # Add label to the columns.
    df.columns = cols

    nes_df_arr.append(df)
# End for.
nesscan_dfs = helpers.get_df_dictionary(nes_df_arr, 0)

# Get common years.
common_years = set(lh_dfs.keys()) & set(nesscan_dfs.keys())

# Loop over common years.
for year in common_years:

    lh_df = lh_dfs[year]
    nesscan_df = nesscan_dfs[year]

    # Identify total and partial duplicates.
    # Total: same datetime and water level.
    # Partial: same datetime, different water level.
    total_duplicates = nesscan_df.duplicated(subset=[cols[0], cols[1]], keep=False)
    partial_duplicates = nesscan_df.duplicated(subset=cols[0], keep=False) & ~total_duplicates

    # Send partial duplicates to CSV.
    partial_duplicates_df = nesscan_df[partial_duplicates]
    partial_duplicates_df.to_csv(f"{duplicates_path}/{station}_partial_duplicates.csv", mode='a',
                                 header=False, index=False)

    # Ensure there are no duplicate datetime values in nesscan_df. Keep the first instance.
    nesscan_df = nesscan_df.drop_duplicates(subset=cols[0], keep='first')

    lh_df.set_index('dt', inplace=True)
    nesscan_df.set_index('dt', inplace=True)

    lh_df.update(nesscan_df)  # Updates df1 values with df2 where dt matches.

    lh_df.reset_index(inplace=True)

    lh_df.to_csv(f"{fixed_lh_file_path}/{year}.csv", index=False)
# ***************************************************************************
# *************************** PROGRAM END ***********************************
# ***************************************************************************
