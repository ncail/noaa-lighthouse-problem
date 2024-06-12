import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ***************************************************************************
# *************************** PROGRAM START *********************************
# ***************************************************************************

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

# ***************************************************************************
# *************************** PROGRAM END ***********************************
# ***************************************************************************


""" ******** Function implementation below. Main program above. ******** """


# ***************************************************************************
# ******************* FUNCTION READ_FILE ************************************
# ***************************************************************************

# Reads csv file into a dataframe.
# Only reads a year's worth of 6-minute data (size 87840).
def read_file(file, index_limit=87840):

    # Initialize df and error to None.
    df = None
    error = None

    # Read the file into a dataframe. If error reading or finding file,
    # append the error.
    if os.path.isfile(file):
        try:
            df = pd.read_csv(file, nrows=index_limit)
        except Exception as e:
            error = (file, str(e))
    else:
        error = (file, "File not found.")
    # End for.

    return df, error
# End read_file.


# ***************************************************************************
# ******************* FUNCTION CLEAN_DATAFRAME ******************************
# ***************************************************************************

# Prepare dataframe for analysis by standardizing datetimes and numerical values.
# Option to clean backup water level (bwl) and harmonic water level (harmwl) columns.
def clean_dataframe(df, datetime_col_name, pwl_col_name, harmwl_col_name=None, bwl_col_name=None):

    # Replace missing values with NaN.
    df.replace([-999, -99, 99, 'NA', 'RM'], np.nan, inplace=True)

    # Convert to datetime.
    df[datetime_col_name] = pd.to_datetime(df[datetime_col_name])

    # Convert primary water level and backup water level to numeric.
    # coerce changes invalid values to NaN.
    df[pwl_col_name] = pd.to_numeric(df[pwl_col_name], errors='coerce')

    # Convert bwl and harmwl to numeric if they are not None.
    if bwl_col_name is not None:
        df[bwl_col_name] = pd.to_numeric(df[bwl_col_name], errors='coerce')
    if harmwl_col_name is not None:
        df[harmwl_col_name] = pd.to_numeric(df[harmwl_col_name], errors='coerce')
# End clean_dataframe.


# ***************************************************************************
# ******************* FUNCTION GET_COMPARISON_STATS *************************
# ***************************************************************************

# Compares two dataframes and returns statistics about their discrepancies
# including number of values that agree, disagree (including and excluding gaps),
# and the percentage of the dataset for these metrics.
def get_comparison_stats(primary_df_col, reference_df_col, size):

    # Initialize incremented stats.
    values_disagree = 0
    values_agree = 0
    primary_gaps = 0
    ref_gaps = 0
    shared_gaps = 0

    for index in range(size):
        # If neither value is NaN, compare the values and increment disagreements
        # if not equal.
        # Round values to millimeters.
        if not pd.isna(primary_df_col.iloc[index]) and not pd.isna(reference_df_col.iloc[index]):
            if round(primary_df_col.iloc[index], 4) != round(reference_df_col.iloc[index], 4):
                values_disagree += 1
            else:
                values_agree += 1

        if pd.isna(primary_df_col.iloc[index]) and pd.isna(reference_df_col.iloc[index]):
            shared_gaps += 1
            continue

        if pd.isna(primary_df_col.iloc[index]):
            primary_gaps += 1

        if pd.isna(reference_df_col.iloc[index]):
            ref_gaps += 1
    # End for.

    total_disagree = values_disagree + primary_gaps + ref_gaps
    total_agree = values_agree + shared_gaps
    primary_missing = shared_gaps + primary_gaps
    ref_missing = shared_gaps + ref_gaps

    percent_agree = round(total_agree/size * 100, 4)
    percent_agree_gaps = round(shared_gaps/total_agree * 100, 4)
    percent_vals_disagree = round(values_disagree/size * 100, 4)
    percent_primary_missing = round(primary_missing/size * 100, 4)
    percent_ref_missing = round(ref_missing/size * 100, 4)

    table = {
        'total points': [total_agree, shared_gaps, total_disagree, primary_missing,
                         ref_missing],
        'percent': [percent_agree, percent_agree_gaps, percent_vals_disagree,
                    percent_primary_missing, percent_ref_missing]
    }

    row_labels = ['agreements', 'shared gaps (of agreements)', 'disagreements',
                  'missing (primary)', 'missing (reference)']

    stats_table = pd.DataFrame(table, index=row_labels)

    return stats_table
# End get_comparison_stats.


# ***************************************************************************
# ******************* FUNCTION PROCESS_OFFSETS ******************************
# ***************************************************************************

# Correct offsets in a data set, considering that the offset value may change throughout,
# and that some discrepancies cannot be considered offsets.
# Includes option to append an array with offset values including NaNs.
# Modifies passed columns. Returns nothing.
def process_offsets(offset_column, reference_column, size, offset_arr=None):

    index = 0
    while index < size:
        # Skip NaNs.
        if pd.isna(offset_column.iloc[index]) or pd.isna(reference_column.iloc[index]):
            index += 1
            offset_arr.append(np.nan)
            continue

        # Get offset.
        offset = identify_offset(offset_column, reference_column, index, size)

        # Skip if no verified offset.
        if pd.isna(offset):
            index += 1
            offset_arr.append(np.nan)
            continue

        # Correct offset until the correction stops working.
        while index < size:
            if round(offset_column.iloc[index] + offset, 4) == round(reference_column.iloc[index], 4):
                offset_column.iloc[index] = offset_column.iloc[index] + offset
                offset_arr.append(offset)
                index += 1
                # print("fixing offset at index: ", index, " with difference: ", offset)

            else:
                break
        # End inner while.
    # End outer while.
# End process_offsets.


# ***************************************************************************
# ******************* FUNCTION IDENTIFY_OFFSET ******************************
# ***************************************************************************

# If there is an offset identified as a constant difference between the
# reference and suspected offset data, for [duration] number of intervals
# (default at 240 intervals or one day for 6 minute intervals), then return
# the offset value, else return NaN.
# Called by process_offsets.
def identify_offset(offset_column, reference_column, index, size, duration=240):

    is_offset = False
    offset_value = offset_column.iloc[index]
    ref_value = reference_column.iloc[index]
    difference = round(ref_value - offset_value, 4)

    for loop in range(index, index + duration):
        if loop + 1 >= size:
            is_offset = False
            break

        if pd.isna(offset_column.iloc[loop]):
            continue

        current_diff = round(reference_column.iloc[loop + 1] - offset_column.iloc[loop + 1], 4)

        if current_diff == 0.000:
            is_offset = False
            break

        if current_diff == difference:
            is_offset = True
        else:
            is_offset = False
            break
    # End for.

    if is_offset:
        return difference
    else:
        return np.nan
# End identify_offset.


# ***************************************************************************
# ******************* FUNCTION GET_RUN_DATA *********************************
# ***************************************************************************

# Return dataframe of discrepancies with start and end dates, and durations.
# Calls get_discrepancies.
# ref_dates need to be time deltas (use pd.to_datetime) that correspond to
# the offset_column and reference_column values (same size).
def get_run_data(offset_column, reference_column, ref_dates, size, create_table=True):

    offsets = get_discrepancies(offset_column, reference_column, size)

    start_indices = []
    end_indices = []
    run_values = []

    previous_value = offsets[0]
    start_index = 0

    for index in range(1, size):
        current_value = offsets[index]

        # Check if current value is different than previous value.
        if current_value != previous_value and not (pd.isna(current_value) and pd.isna(previous_value)):
            end_index = index - 1
            start_indices.append(start_index)
            end_indices.append(end_index)
            run_values.append(previous_value)
            start_index = end_index

        previous_value = current_value
    # End for.

    # Append the last run.
    start_indices.append(start_index)
    end_indices.append(size - 1)
    run_values.append(previous_value)

    if create_table is True:
        # Create the dataframe.
        summary_df = pd.DataFrame()
        summary_df['discrepancy value (ref - primary, unit)'] = run_values
        summary_df['start date'] = ref_dates.iloc[start_indices].tolist()
        summary_df['end date'] = ref_dates.iloc[end_indices].tolist()
        summary_df['durations'] = summary_df['end date'] - summary_df['start date']

        return summary_df

    else:
        return start_indices, end_indices, run_values
# End get_run_data.


# ***************************************************************************
# ******************* FUNCTION GET_DISCREPANCIES ****************************
# ***************************************************************************

# Get array of all discrepancies.
def get_discrepancies(offset_column, reference_column, size):

    discrepancies = []

    for index in range(size):

        if pd.isna(offset_column.iloc[index]):
            discrepancies.append(np.nan)
            continue

        difference = round(reference_column.iloc[index] - offset_column.iloc[index], 4)
        discrepancies.append(difference)
    # End for.

    return discrepancies
# End get_discrepancies.


# ***************************************************************************
# ******************* FUNCTION FILTER_DURATION ******************************
# ***************************************************************************

# Return a dataframe filtered for a threshold duration of the discrepancy.
# For example, returns all discrepancies that persisted for no less than 1 day.
# Passed dataframe must be generated by get_run_data.
# Threshold must be a time delta so that units passed into function are flexible.
def filter_duration(dataframe, threshold, is_max=False, is_min=True):

    filtered_df = dataframe.copy()

    # Default filter.
    filter_series = pd.Series([True] * len(dataframe))

    if is_min is True:
        filter_series = filtered_df['durations'] >= threshold
    else:
        if is_max is True:
            filter_series = filtered_df['durations'] <= threshold

    filtered_df = filtered_df[filter_series]

    return filtered_df
# End filter_duration.


# ***************************************************************************
# ******************* FUNCTION FILTER_VALUE *********************************
# ***************************************************************************

# Return a dataframe filtered for a threshold value of the discrepancy.
# For example, returns all discrepancies greater than 5 cm.
# Passed dataframe must be generated by get_run_data.
def filter_value(dataframe, threshold, is_max=False, is_min=True):

    filtered_df = dataframe.copy()

    # Default filter.
    filter_series = pd.Series([True] * len(dataframe))

    if is_min is True:
        filter_series = abs(filtered_df['discrepancy value (ref - primary, unit)']) >= threshold
    else:
        if is_max is True:
            filter_series = abs(filtered_df['discrepancy value (ref - primary, unit)']) <= threshold

    filtered_df = filtered_df[filter_series]

    return filtered_df
# End filter_value.







