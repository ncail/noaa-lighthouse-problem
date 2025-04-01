"""
Program detects the flat line regions in a Lighthouse station's data, and records these to a CSV.
Replaces the flat line records with NaN, and sends this flat-line-removed (cleaned) data to CSV.
Computes the yearly 99% range of the cleaned data, and sends year:range_99 to CSV file (named with 'post_FLR' ie
post-flat-line-removal).
This step guides whether outlier removal should be applied next (by running lighthouse_remove_outliers.py on the
cleaned data from detect_flat_lines.py).
"""


# Imports.
import os
import glob
import datetime
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy.ndimage import label

import helpers

''' ***********************************************************************************************************
    ************************************************* CONFIG **************************************************
    *********************************************************************************************************** '''
# Config station: bobHallPier, portIsabel, pier21, rockport (as in path).
station = 'bobHallPier'
lighthouse_data_path = f'data/lighthouse/{station}_nesscan_fixed/raw'

# Identical values method (step 1)
duration = 10  # one hour

# tolerance method (step 2)
window_size = 30
tolerance = 0.01
do_plots = False

output_flat_lines = f'generated_files/detection_removal_processes/flat_lines_records'
output_flat_lines_file_name = f'{output_flat_lines}/{station}_flat_line_records_03312025.csv'
output_flat_line_plots = f'generated_files/detection_removal_processes/flat_lines_plots/{station}/3_hr_1_cm'
output_range_99 = f'generated_files/detection_removal_processes/range_99s'
ninenine_filename = f'{output_range_99}/{station}_range_99_post_FLR_03312025.csv'
output_cleaned_data = f'data/lighthouse/nesscan_fixed_removed_flat_lines_03312025/{station}'

''' ***********************************************************************************************************
    ******************************************* PROCESSING START **********************************************
    *********************************************************************************************************** '''
current_timestamp = datetime.datetime.now().strftime('%H%M%S_%m%d%Y')

if not os.path.exists(output_flat_lines):
    os.makedirs(output_flat_lines)
if not os.path.exists(output_flat_line_plots):
    os.makedirs(output_flat_line_plots)
if not os.path.exists(output_range_99):
    os.makedirs(output_range_99)
if not os.path.exists(output_cleaned_data):
    os.makedirs(output_cleaned_data)

data_files = glob.glob(f'{lighthouse_data_path}/*.csv')

station_df = pd.DataFrame()
for file in data_files:
    df = pd.read_csv(file, parse_dates=['dt'])

    station_df = pd.concat([station_df, df])
# End for.
station_df = station_df.reset_index(drop=True)  # Reset index from 0. This allows flat_regions to index it properly
# later.

val_col = station_df.columns[1]
dt_col = station_df.columns[0]

""" ****************************** Identical value method for detecting flat lines ****************************** """
# Detect where values stay the same.
constant_mask = np.diff(station_df[val_col].to_numpy(), prepend=np.nan) == 0

# Label consecutive constant regions.
labels, num_features = label(constant_mask)

# Find labels where the streak is 10 or more.
# Count occurrences of each label.
label_counts = np.bincount(labels)

# Identify labels that meet the length threshold.
valid_labels = np.where(label_counts >= duration)[0]

# Extract only relevant sections.
flat_regions = {i: np.flatnonzero(labels == i) for i in valid_labels if i > 0}

# Save to df.
flat_df_identical_method = pd.concat([station_df.iloc[indices] for i, indices in flat_regions.items()])

# Save identical-value flat lines to CSV.
if not os.path.exists(output_flat_lines_file_name):
    flat_df_identical_method.to_csv(output_flat_lines_file_name, index=False)
# End if.

# Remove the flat lines.
for indices in flat_regions.values():
    station_df.loc[indices, val_col] = np.nan

""" ******************************  Tolerance method for detecting flat-ish lines ****************************** """
sliding_range = station_df[val_col].rolling(window=window_size).apply(lambda x: x.max() - x.min(), raw=True)

last_flat_indices = np.where(sliding_range < tolerance)[0]

all_flat_indices = set()
for index in last_flat_indices:
    all_flat_indices.update(range(index - window_size + 1, index + 1))

all_flat_indices = sorted(i for i in all_flat_indices if 0 <= i < len(station_df))

# Get start-end of flat lines.
if all_flat_indices:
    flats = {'start_index': [], 'end_index': []}
    start = all_flat_indices[0]
    flats['start_index'].append(start)
    for pos in range(0, len(all_flat_indices) - 1):
        if all_flat_indices[pos + 1] != all_flat_indices[pos] + 1:
            flats['end_index'].append(all_flat_indices[pos])
            flats['start_index'].append(all_flat_indices[pos + 1])
    flats['end_index'].append(all_flat_indices[-1])
    flat_plotting_df = pd.DataFrame(flats)

# Plot flat line examples with tolerance method.
if do_plots:
    figure = 0
    for row in range(0, len(flat_plotting_df), 9):
        fig, axs = plt.subplots(3, 3, figsize=(10, 10))
        for ax_idx, ax in enumerate(axs.flatten()):
            if row + ax_idx >= len(flat_plotting_df):
                ax.axis("off")  # Hide extra subplots.
                continue

            start_idx = flat_plotting_df.loc[row + ax_idx, 'start_index']
            end_idx = flat_plotting_df.loc[row + ax_idx, 'end_index']

            # Bound check indices.
            pre_start = max(0, start_idx - 10)
            post_end = min(len(station_df), end_idx + 10)

            pre_flat = np.arange(pre_start, start_idx + 1)
            flat_slice = np.arange(start_idx, end_idx + 1)
            post_flat = np.arange(end_idx, post_end)

            segment = station_df.loc[np.arange(pre_start, post_end), dt_col]
            x_tick_strings = segment.dt.strftime('%m/%d/%Y %H:%M')

            ax.plot(pre_flat, station_df.loc[pre_flat, val_col], color='b')
            ax.plot(flat_slice, station_df.loc[flat_slice, val_col], color='r')
            ax.plot(post_flat, station_df.loc[post_flat, val_col], color='b')

            # Make y_ticks range at least 20 cm.
            ax.set_ylim(station_df.loc[pre_flat, val_col].min() - 0.1 + tolerance,
                        station_df.loc[pre_flat, val_col].max() + 0.1 - tolerance)

            if len(segment) > 100:
                ax.set_xticks([segment.index[0], segment.index[-1]])
                ax.set_xticklabels([x_tick_strings[pre_start], x_tick_strings[post_end-1]], rotation=45, ha='right')
            else:
                ax.set_xticks(segment.index[::10])
                ax.set_xticklabels(x_tick_strings[::10], rotation=45, ha='right')

        fig.suptitle(f'{station} flat-ish regions\n'
                     f'sliding range interval: {window_size}, tolerance: {tolerance}')
        plt.tight_layout()
        plt.subplots_adjust(hspace=0.8)

        plt.savefig(f'{output_flat_line_plots}/{station}_{current_timestamp}_plots_{figure}.png', bbox_inches='tight')
        figure += 1

        # plt.show()
    # End for.
# End if do_plots.

# Store flat line data.
flat_df_tolerance_method = station_df.loc[all_flat_indices]

# Remove flat lines using tolerance method.
station_df.loc[all_flat_indices, val_col] = np.nan

""" **************************************************** Results *************************************************** """
# Compare lengths of dfs. Print data in tolerance method not in identical values method.
print(f'number of flat regions by tolerance method: {len(flat_plotting_df)}\n'
      f'number of flat regions by identical values method: {len(valid_labels)}')
# Number of flat regions removed by each method
# Average, median of length of flat regions, for each method
# Standard deviation of the dataset before and after removal
# Parameters of methods

# Re-split into yearly data.
df_dict = helpers.split_by_year(station_df, dt_col)

# Compute the 99% range. Send to file.
yearly_range_99 = {}
yearly_median = {}
for year, df in df_dict.items():
    sorted_data = np.sort(df[val_col].dropna())
    lower_percentile = int(0.005 * len(sorted_data))
    upper_percentile = int(0.995 * len(sorted_data))

    # 99% range (middle 99% data)
    range_99 = round(sorted_data[upper_percentile] - sorted_data[lower_percentile], 3)

    yearly_range_99[year] = range_99
# End for.

# Send ranges to CSV.
if not os.path.exists(ninenine_filename):
    yearly_range_99_df = pd.DataFrame.from_dict(yearly_range_99, orient='index')

    # Reset index to turn Year into a column.
    yearly_range_99_df.reset_index(inplace=True)
    yearly_range_99_df.columns = ['year', '99% range']
    yearly_range_99_df.to_csv(ninenine_filename, index=False)
# End if.

# Write cleaned data to CSV.
for year, df in df_dict.items():
    filename = f'{output_cleaned_data}/{year}.csv'
    if not os.path.exists(filename):
        df.to_csv(filename, index=False)
