# Imports
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import datetime

# Config mode: 1 - offset value hist, 2 - offset duration hist.
mode = 1

# Config station: bhp, pIsabel, pier21, rockport (as in path)
station = 'bhp'
station_fig_title = 'Bob Hall Pier'

# Read vertical offset csv into dfs.
offsets_tables_path = f'generated_files/eval_vertical_offsets_nesscan-fixed/{station}_v0.7.5'
offsets_files = glob.glob(f'{offsets_tables_path}/*.csv')

datetimes_list = pd.date_range(
            start=datetime.datetime(year=year, month=1, day=1),
            end=datetime.datetime(year=year, month=12, day=31, hour=23, minute=54),
            freq=datetime.timedelta(minutes=6)  # Frequency of 6 minutes
            ).tolist()

station_offsets = pd.Series()
station_durations = pd.Series()
full_dataset = []
for file in offsets_files:
    df = pd.read_csv(file, parse_dates='start date')
    df['duration'] = pd.to_timedelta(df['duration'])

    station_offsets.append(df['offset'])
    station_durations.append(df['duration'])

    current_year = df['start date'].dt.year[0]
    datetime_list_current_year = pd.date_range(
            start=datetime.datetime(year=current_year, month=1, day=1),
            end=datetime.datetime(year=current_year, month=12, day=31, hour=23, minute=54),
            freq=datetime.timedelta(minutes=6)  # Frequency of 6 minutes
            ).tolist()
    full_dataset.append(datetime_list_current_year)
# End for.

# Get size of dataset and percent offset.
dataset_size = len(full_dataset)
percent_offset = len(station_offsets) / dataset_size * 100

# Convert columns to numpy series.
offsets_arr = station_offsets.to_numpy()
durations_arr = station_durations.to_numpy()

# Generate the offset values histogram.
if mode == 1:
    # Get negative of offsets (so the values represent LH - NOAA).
    offsets_arr = -offsets_arr

    # Define bin edges with precision.
    step_size = 0.001
    bin_edges = np.arange(offsets_arr.min() - step_size, offsets_arr.max() + step_size, step_size)

    # Compute histogram values.
    counts, bin_edges = np.histogram(offsets_arr, bins=bin_edges)

    # Create the histogram.
    fig, ax = plt.subplots()
    n, bins, patches = ax.hist(offsets_arr, bins=bin_edges, edgecolor='black')

    # Label each bar with its value.
    for i in range(len(bin_edges) - 1):
        bin_center = (bin_edges[i] + bin_edges[i + 1]) / 2
        if counts[i] > 0:
            ax.text(bin_center, counts[i], f'{bin_center:.3f}', ha='center', va='bottom')

    # x-axis rounds to millimeters.
    ax.xaxis.set_major_formatter(ticker.FormatStrFormatter('%.3f'))

    # y-axis only uses integers.
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    # Add labels and titles.
    ax.set_xlabel('Vertical Offset Value (LH - NOAA, m)', fontsize=18)
    plt.suptitle(f"{station_fig_title}: Frequency of Datum Shifts", fontsize=22)
    ax.set_ylabel('Number of Occurrences', fontsize=18)
    ax.set_title(f'For shifts with duration >= 1 hour', fontsize=18)
    ax.tick_params(axis='both', which='major', labelsize=14)
# End mode 1.

if mode == 2:
    
# End mode 2.

# Add note about the total datum-shifted percentage.
longest_duration = durations_arr.max()
days = longest_duration.days
hours = longest_duration.seconds // 3600
minutes = (longest_duration.seconds % 3600) // 60
formatted_longest_duration = f"{days} days {hours} hours {minutes} minutes"

longest_datum_shift = filtered_nonzero_df['vertical_offset'][filtered_nonzero_df['duration'] ==
                                                             longest_duration].values[0]
long_shift_note = (f'Longest datum shift is\n'
                   f'{str(longest_datum_shift)} m, {formatted_longest_duration} long')

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

plt.show()

