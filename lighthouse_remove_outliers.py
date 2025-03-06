# Imports.
import os
import glob
import pandas as pd
import numpy as np

''' ***********************************************************************************************************
    ************************************************* CONFIG **************************************************
    *********************************************************************************************************** '''

station = 'portIsabel'
abs_cutoff = 3

lighthouse_files_path = f"data/lighthouse/{station}_nesscan_fixed"

cleaned_data_path = f'data/lighthouse/{station}_nesscan_fixed/outliers_removed_{abs_cutoff}m'
if not os.path.exists(cleaned_data_path):
    os.makedirs(cleaned_data_path)

write_medians = True
medians_output_path = f'generated_files/medians/'
if not os.path.exists(medians_output_path):
    os.makedirs(medians_output_path)

''' ***********************************************************************************************************
    ******************************************* PROCESSING START **********************************************
    *********************************************************************************************************** '''
# Read preprocessed nesscan-fixed yearly files into dfs.
lighthouse_files = glob.glob(f"{lighthouse_files_path}/*.csv")

yearly_median = {}
for file in lighthouse_files:
    df = pd.read_csv(file, parse_dates=[0])

    # Convert wl col to numpy.
    data = df['wl'].to_numpy()

    # Get year.
    year = df['dt'].dt.year[0]

    # Drop nans, get median.
    clean_data = data[~np.isnan(data)]
    median = np.median(clean_data)

    # Add to dictionary.
    yearly_median[year] = median

    # Create filter for outliers.
    outlier_upper_mask = df['wl'] > median+3
    outlier_lower_mask = df['wl'] < median-3

    # Fill outliers with nan.
    df.loc[outlier_upper_mask, 'wl'] = np.nan
    df.loc[outlier_lower_mask, 'wl'] = np.nan

    # Write cleaned df to data path.
    df.to_csv(f'{cleaned_data_path}/{year}.csv', index=False)
# End for.

# Write medians to file.
if write_medians:
    # Convert dict to df.
    median_df = pd.DataFrame.from_dict(yearly_median, orient='index')

    # Reset index to turn Year into a column.
    median_df.reset_index(inplace=True)

    median_df.columns = ['year', 'median']
    median_df.to_csv(f'{medians_output_path}/{station}_medians.csv', index=False)
# End if.













