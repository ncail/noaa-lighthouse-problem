# Imports.
import glob
import pandas as pd
import numpy as np
from scipy.ndimage import label

''' ***********************************************************************************************************
    ************************************************* CONFIG **************************************************
    *********************************************************************************************************** '''
# Config station: bobHallPier, portIsabel, pier21, rockport (as in path).
station = 'rockport'
lighthouse_data_path = f'data/lighthouse/{station}_nesscan_fixed'

output = f'generated_files/detection_removal_processes'

''' ***********************************************************************************************************
    ******************************************* PROCESSING START **********************************************
    *********************************************************************************************************** '''

data_files = glob.glob(f'{lighthouse_data_path}/*.csv')

station_df = pd.DataFrame()
for file in data_files:
    df = pd.read_csv(file)

    station_df = pd.concat([station_df, df])
# End for.

val_col = station_df.columns[1]

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
df_flat.to_csv(f"{output}/{station}_flat_regions.csv", index=False)










