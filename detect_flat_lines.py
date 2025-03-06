"""
Program detects the flat line regions in a Lighthouse station's data, and records these to a CSV.
Replaces the flat line records with NaN, and sends this flat-line-removed (cleaned) data to CSV.
Computes the 99% range of the cleaned data, and sends to text file.
This step guides whether outlier removal should be applied next (by running lighthouse_remove_outliers.py on the
cleaned data from detect_flat_lines.py).
"""


# Imports.
import os
import glob
import pandas as pd
import numpy as np
from scipy.ndimage import label

import helpers

''' ***********************************************************************************************************
    ************************************************* CONFIG **************************************************
    *********************************************************************************************************** '''
# Config station: bobHallPier, portIsabel, pier21, rockport (as in path).
station = 'rockport'
lighthouse_data_path = f'data/lighthouse/{station}_nesscan_fixed'

output_flat_lines = f'generated_files/detection_removal_processes'
output_cleaned_data = f'data/lighthouse/{station}_nesscan_fixed/1_removed_flat_lines'

''' ***********************************************************************************************************
    ******************************************* PROCESSING START **********************************************
    *********************************************************************************************************** '''
if not os.path.exists(output_cleaned_data):
    os.makedirs(output_cleaned_data)

data_files = glob.glob(f'{lighthouse_data_path}/*.csv')

station_df = pd.DataFrame()
for file in data_files:
    df = pd.read_csv(file, parse_dates=['dt'])

    station_df = pd.concat([station_df, df])
# End for.

val_col = station_df.columns[1]
dt_col = station_df.columns[0]

# Detect where values stay the same.
constant_mask = np.diff(station_df[val_col].to_numpy(), prepend=np.nan) == 0

# Label consecutive constant regions.
labels, num_features = label(constant_mask)

# Find labels where the streak is 10 or more.
# Count occurrences of each label.
label_counts = np.bincount(labels)

# Identify labels that meet the length threshold.
valid_labels = np.where(label_counts >= 10)[0]

# Extract only relevant sections.
flat_regions = {i: np.flatnonzero(labels == i) for i in valid_labels if i > 0}

# Save all flat lines to CSV.
df_flat = pd.concat([station_df.iloc[indices].assign(region=i) for i, indices in flat_regions.items()])
df_flat.to_csv(f"{output_flat_lines}/{station}_flat_regions.csv", index=False)

# Remove the flat lines.
for indices in flat_regions.values():
    station_df.loc[indices, 'value'] = np.nan

# Compute the 99% range. Send to text file.

# Re-split into yearly data.
df_dict = helpers.split_by_year(station_df, dt_col)

# Write cleaned data to CSV.
for year, df in df_dict.items():
    df.to_csv(f'{output_cleaned_data}/{year}.csv', index=False)











