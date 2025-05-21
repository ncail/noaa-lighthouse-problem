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
station = 'rockport'
lighthouse_data_path = (f'data/lighthouse/time_series_integrated_with_122024_nesscan_fix/'
                        f'rockport_with_122024_nesscan_fix_raw')

identical_method = True
tolerance_method = True

# Identical values method (step 1)
duration = 10  # data points

# tolerance method (step 2)
window_size = 20  # data points
tolerance = 0.003

current_timestamp = datetime.datetime.now().strftime('%H%M%S_%m%d%Y')

write_flats = False
output_flat_lines = f'generated_files/detection_removal_processes/flat_lines_records'
output_flat_lines_file_name = f'{output_flat_lines}/rockport_flat_line_records_{current_timestamp}.csv'
                               #{station}_flat_line_records_{current_timestamp}.csv'

do_plots = False
output_flat_line_plots = (f'generated_files/detection_removal_processes/flat_lines_plots/{station}/4_hr_05_cm'
                          f'_{current_timestamp}')

write_99s = False
output_range_99 = f'generated_files/detection_removal_processes/range_99s'
ninenine_filename = f'{output_range_99}/{station}_range_99_{current_timestamp}.csv'

write_cleaned_data = False
output_cleaned_data = f'data/lighthouse/nesscan_fixed_removed_flat_lines/{station}_{current_timestamp}'

write_log = True
output_stats_log = f'generated_files/detection_removal_processes/flat_line_removal_stats'
output_stats_log_filename = f'{output_stats_log}/stats_and_parameter_log.txt'

''' ***********************************************************************************************************
    ******************************************* PROCESSING START **********************************************
    *********************************************************************************************************** '''

if not os.path.exists(output_flat_lines) and write_flats:
    os.makedirs(output_flat_lines)
if not os.path.exists(output_flat_line_plots) and do_plots:
    os.makedirs(output_flat_line_plots)
if not os.path.exists(output_range_99) and write_99s:
    os.makedirs(output_range_99)
if not os.path.exists(output_cleaned_data) and write_cleaned_data:
    os.makedirs(output_cleaned_data)
if not os.path.exists(output_stats_log) and write_log:
    os.makedirs(output_stats_log)

data_files = glob.glob(f'{lighthouse_data_path}/*.csv')

station_df = pd.DataFrame()
for file in data_files:
    df = pd.read_csv(file, parse_dates=['dt'])

    station_df = pd.concat([station_df, df])
# End for.
station_df = station_df.reset_index(drop=True)  # Reset index from 0. This allows flat_regions to index it properly
# later.

station_df.rename(columns={station_df.columns[1]: 'final wl'}, inplace=True)
val_col = station_df.columns[1]
dt_col = station_df.columns[0]

# Make copy of complete dataset.
station_df['initial wl'] = station_df[val_col]

# Standard deviation.
stdev_pre = np.nanstd(station_df[val_col].to_numpy())

# NaN percentage.
initial_nan_percentage = (len(station_df[station_df[val_col].isna()]) / len(station_df)) * 100

""" ****************************** Identical value method for detecting flat lines ****************************** """
if identical_method:
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
    identically_flat_indices = np.concatenate(list(flat_regions.values()))
    # flat_df_identical_method = pd.concat([station_df.iloc[index] for index in identically_flat_indices])

    # Remove the flat lines.
    station_df.loc[identically_flat_indices, val_col] = np.nan

""" ******************************  Tolerance method for detecting flat-ish lines ****************************** """
if tolerance_method:
    sliding_range = station_df[val_col].rolling(window=window_size).apply(lambda x: x.max() - x.min(), raw=True)

    last_flat_indices = np.where(sliding_range < tolerance)[0]

    tolerance_flat_indices = set()
    for index in last_flat_indices:
        tolerance_flat_indices.update(range(index - window_size + 1, index + 1))

    tolerance_flat_indices = sorted(i for i in tolerance_flat_indices if 0 <= i < len(station_df))

    # Get start-end of flat lines.
    if tolerance_flat_indices:
        flats = {'start_index': [], 'end_index': []}
        start = tolerance_flat_indices[0]
        flats['start_index'].append(start)
        for pos in range(0, len(tolerance_flat_indices) - 1):
            if tolerance_flat_indices[pos + 1] != tolerance_flat_indices[pos] + 1:
                flats['end_index'].append(tolerance_flat_indices[pos])
                flats['start_index'].append(tolerance_flat_indices[pos + 1])
        flats['end_index'].append(tolerance_flat_indices[-1])
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
    # End if(do_plots).

    # Store flat line data from tolerance method.
    # flat_df_tolerance_method = station_df.loc[all_flat_indices]

    # Remove flat lines using tolerance method.
    station_df.loc[tolerance_flat_indices, val_col] = np.nan

# Save identical-value and tolerance-method flat lines to CSV.
if write_flats:
    station_df['flats'] = np.nan
    if identical_method:
        station_df.loc[identically_flat_indices, 'flats'] = station_df.loc[identically_flat_indices, 'initial wl']
    if tolerance_method:
        station_df.loc[tolerance_flat_indices, 'flats'] = station_df.loc[tolerance_flat_indices, 'initial wl']
    station_df.to_csv(output_flat_lines_file_name, index=False)

    # flat_regions_df = pd.concat([flat_df_tolerance_method, flat_df_identical_method]).sort_values(by=[dt_col])
    # flat_regions_df.to_csv(output_flat_lines_file_name, index=False)
# End if.

""" **************************************************** Results *************************************************** """
# Get avg and median length of flat regions.
if identical_method:
    identical_flat_lengths = [len(indices) for indices in flat_regions.values()]
    average_length_id = np.mean(identical_flat_lengths)
    median_length_id = np.median(identical_flat_lengths)

if tolerance_method:
    tolerance_flat_lengths = pd.Series(flat_plotting_df['end_index'] - flat_plotting_df['start_index']).to_numpy()
    average_length_tol = np.mean(tolerance_flat_lengths)
    median_length_tol = np.median(tolerance_flat_lengths)

# Standard deviation.
stdev_post = np.nanstd(station_df[val_col].to_numpy())

# Final NaN percentage.
final_nan_percentage = (len(station_df[station_df[val_col].isna()]) / len(station_df)) * 100

# Increased NaN percentage.
increased_nan_percentage = final_nan_percentage - initial_nan_percentage

# Write stats and parameter log to file.
if write_log:
    with open(f'{output_stats_log_filename}', 'a') as file:
        file.write(f'Time: {current_timestamp}\n'
                   f'Data path: {lighthouse_data_path}\n'
                   f'Parameters:\n'
                   f'\tIdentical value method:\n'
                   f'\t\tDuration: {duration} points\n'
                   f'\tTolerance method:\n'
                   f'\t\tDuration: {window_size} points\n'
                   f'\t\tTolerance: {tolerance} m\n'
                   f'Number of flat regions removed by identical values method: {len(flat_regions.keys())}\n'
                   f'Remaining flat regions removed by tolerance method:        {len(flat_plotting_df)}\n'
                   f'Increased NaN percentage after flat line removal:          {increased_nan_percentage:.2f}%\n'
                   f'Average flat region length by identical values method:     {average_length_id:.2f} points\n'
                   f'Median flat region length by identical values method:      {median_length_id:.2f} points\n'
                   f'Average flat region length by tolerance method:            {average_length_tol:.2f} points\n'
                   f'Median flat region length by tolerance method:             {median_length_tol:.2f} points\n'
                   f'Standard deviation before flat line removal:               {stdev_pre:.2f}\n'
                   f'Standard deviation after flat line removal:                {stdev_post:.2f}\n'
                   f'\n\n')
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
if write_99s:
    yearly_range_99_df = pd.DataFrame.from_dict(yearly_range_99, orient='index')

    # Reset index to turn Year into a column.
    yearly_range_99_df.reset_index(inplace=True)
    yearly_range_99_df.columns = ['year', '99% range']
    yearly_range_99_df.to_csv(ninenine_filename, index=False)
# End if.

# Write cleaned data to CSV.
if write_cleaned_data:
    for year, df in df_dict.items():
        filename = f'{output_cleaned_data}/{year}.csv'
        df.to_csv(filename, index=False)
