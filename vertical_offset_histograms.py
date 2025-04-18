# Imports
import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import datetime


''' ***********************************************************************************************************
    ************************************************* CONFIG **************************************************
    *********************************************************************************************************** '''
# Config mode: 1 - offset value hist, 2 - offset duration hist.
mode = 1

# Mode 1 configs.
is_abs = True
high_pass_filter = 0

# Config station: bhp, pIsabel, pier21, rockport (as in path)
station = 'pier21'
years = [2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]

# Config vertical offsets data path.
offsets_tables_path = (f'generated_files/nesscan_fixed_12172024/eval_vertical_offsets_nesscan-fixed/vertical_offsets'
                       f'/pier21_v0.7.5/all_offsets')
station_fig_title = 'Pier 21 (december nesscan fix)'
year_str = '2006-2023'

# Output path.
output = f'generated_files/pier21_reprocessed_03312025/histograms'
if not os.path.exists(output):
    os.makedirs(output)

# Config MODE 1 step size for VO value (step_size_val), MODE 2 duration (num_bins, because its log scale) histogram.
step_size_val = 0.02
num_bins = 9


''' ***********************************************************************************************************
    ******************************************* PROCESSING START **********************************************
    *********************************************************************************************************** '''
# Read vertical offset csv into dfs.
offsets_files = glob.glob(f'{offsets_tables_path}/*.csv')
current_timestamp = datetime.datetime.now().strftime('%H%M%S_%m%d%Y')  # For output filename.

station_df = pd.DataFrame()
full_dataset = []
for file in offsets_files:
    df = pd.read_csv(file, parse_dates=['start date'])

    # If years is not empty (if empty, do all years), check if current year is in years, else skip
    current_year = df['start date'].dt.year[0]
    if years:
        if current_year not in years:
            continue

    df['duration'] = pd.to_timedelta(df['duration'])

    station_df = pd.concat([station_df, df])

    datetime_list_current_year = pd.date_range(
            start=datetime.datetime(year=current_year, month=1, day=1),
            end=datetime.datetime(year=current_year, month=12, day=31, hour=23, minute=54),
            freq='6min'  # Frequency of 6 minutes
            ).tolist()
    full_dataset.extend(datetime_list_current_year)
# End for.

# Apply vertical offsets configs.
if is_abs:
    station_df['offset'] = np.abs(station_df['offset'])

if high_pass_filter:
    station_df = station_df[station_df['offset'] > high_pass_filter]

if station == 'rockport':
    station_df = station_df[station_df['offset'] < 6.000]

# Get size of dataset and percent offset.
dataset_size = pd.to_timedelta(len(full_dataset)*6, unit='minutes')
offset_info_size = station_df['duration'].sum()
percent_offset = offset_info_size / dataset_size * 100

# Convert columns to numpy series.
offsets_arr = station_df['offset'].to_numpy()

if not is_abs:
    offsets_arr = -offsets_arr  # Get negative of offsets (so the values represent LH - NOAA).

# Durations array.
durations_arr = station_df['duration'].to_numpy()

# Generate the offset values histogram.
if mode == 1:
    # Define bin edges with precision.
    bin_edges = np.arange(offsets_arr.min() - step_size_val, offsets_arr.max() + step_size_val, step_size_val)

    # Compute histogram values.
    counts, bin_edges = np.histogram(offsets_arr, bins=bin_edges)

    # Create the histogram.
    fig, ax = plt.subplots(figsize=(10, 6))
    n, bins, patches = ax.hist(offsets_arr, bins=bin_edges, edgecolor='black')

    # Label each bar with its value.
    # for i in range(len(bin_edges) - 1):
    #     bin_center = (bin_edges[i] + bin_edges[i + 1]) / 2
    #     if counts[i] > 0:
    #         ax.text(bin_center, counts[i], f'{bin_center:.3f}', ha='center', va='bottom')

    # x-axis rounds to millimeters.
    ax.xaxis.set_major_formatter(ticker.FormatStrFormatter('%.3f'))

    # y-axis only uses integers.
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    # Add labels and titles.
    if is_abs:
        ax.set_xlabel('Vertical Offset Absolute Value, m', fontsize=18)
    else:
        ax.set_xlabel('Vertical Offset Value (LH - NOAA, m)', fontsize=18)
    if high_pass_filter:
        plt.suptitle(f"{station_fig_title}: Frequency of Vertical Offsets (>{high_pass_filter} m)", fontsize=22)
    else:
        plt.suptitle(f"{station_fig_title}: Frequency of Vertical Offsets", fontsize=22)
    ax.set_ylabel('Number of Occurrences', fontsize=18)
    # ax.set_title(f'For offsets with duration >= 1 hour', fontsize=18)  # Dataset is filtered by 6-minutes.
    ax.set_title(f'(Excluding missing values. Step size={step_size_val}). Years: {year_str}', fontsize=18)
    ax.tick_params(axis='both', which='major', labelsize=14)
# End mode 1.

if mode == 2:
    # Convert to minutes for convenience.
    durations_arr_minutes = durations_arr.astype('timedelta64[m]').astype(int)  # arange() in next line uses int.

    # Define bin edges with precision.
    # bin_edges = np.arange(durations_arr_minutes.min() - step_size_dur, durations_arr_minutes.max() + step_size_dur,
    # step_size_dur)

    # Compute histogram values.
    # counts, bin_edges = np.histogram(durations_arr_minutes, bins=bin_edges)

    # Create the histogram.
    fig, ax = plt.subplots(figsize=(10, 6))
    # n, bins, patches = ax.hist(durations_arr_minutes, bins=bin_edges, edgecolor='black')

    # Label each bar with its value.
    # for i in range(len(bin_edges) - 1):
    #     bin_center = (bin_edges[i] + bin_edges[i + 1]) / 2
    #     if counts[i] > 0:
    #         ax.text(bin_center, counts[i], f'{bin_center:.3f}', ha='center', va='bottom')

    # Define the bin edges using logspace.
    min_val = np.log10(durations_arr_minutes.min())  # log(min duration)
    max_val = np.log10(durations_arr_minutes.max())  # log(max duration)

    bin_edges = np.logspace(min_val, max_val, num=num_bins)

    # Plot histogram with log bins.
    ax.hist(durations_arr_minutes, bins=bin_edges, edgecolor='black', log=True)
    ax.set_xscale('log')

    benchmarks = {
        60: '1 hour',
        1440: '1 day',
        10080: '1 week',
        43200: '1 month',
        525600: '1 year'
    }

    for minute_val, time_descriptor in benchmarks.items():
        plt.axvline(x=minute_val, label=time_descriptor, color='green', linestyle='--')
        plt.text(minute_val, 100, time_descriptor, ha='right', va='bottom')

    # Add labels and titles.
    if high_pass_filter:
        plt.suptitle(f"{station_fig_title}: Frequency of Vertical Offsets (>{high_pass_filter} m)", fontsize=22)
    else:
        plt.suptitle(f"{station_fig_title}: Frequency of Vertical Offsets", fontsize=22)
    ax.set_title(f'(Excluding missing values)', fontsize=18)
    ax.set_xlabel('Duration Value (minutes)', fontsize=18)
    ax.set_ylabel('Number of Occurrences', fontsize=18)
    ax.tick_params(axis='both', which='major', labelsize=14)
# End mode 2.

# Add note about the total datum-shifted percentage.
longest_offset_index = np.argmax(durations_arr)
longest_offset = offsets_arr[longest_offset_index]
longest_duration = durations_arr[longest_offset_index]

# Convert to days, hours, and minutes.
days = longest_duration.astype('timedelta64[D]').astype(int)
hours = (longest_duration - np.timedelta64(days, 'D')).astype('timedelta64[h]').astype(int)
minutes = (longest_duration - np.timedelta64(days, 'D') -
           np.timedelta64(hours, 'h')).astype('timedelta64[m]').astype(int)

formatted_longest_duration = f"{days} days {hours} hours {minutes} minutes"

long_shift_note = (f'Longest vertical offset is\n'
                   f'{str(longest_offset)} m, {formatted_longest_duration} long')

xcoord = 0.05
ycoord = 0.95

# Add note about the longest shift.
ax.text(xcoord, ycoord - 0.1, long_shift_note, transform=ax.transAxes, fontsize=14,
        verticalalignment='top', horizontalalignment='left',
        bbox=dict(facecolor='white', alpha=0.6))

# Add note about percent offset data.
note_text = f'Percentage of vertically-offset data: {round(percent_offset, 2)}%'
ax.text(xcoord, ycoord, note_text, transform=ax.transAxes, fontsize=14,
        verticalalignment='top', horizontalalignment='left',
        bbox=dict(facecolor='white', alpha=0.6))

if mode == 1:
    mode_str = 'offset_value'
elif mode == 2:
    mode_str = 'offset_duration'
plt.savefig(f'{output}/{station}_{current_timestamp}_{mode_str}_plot.png', bbox_inches='tight')

plt.show()

