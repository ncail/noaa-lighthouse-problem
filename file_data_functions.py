import os
import pandas as pd
import numpy as np

# Opt into future behavior for pandas. Encouraged by FutureWarning message
# for pd.replace(): "Downcasting behavior in 'replace' is deprecated and
# will be removed in a future version."
pd.set_option('future.no_silent_downcasting', True)


""" ******************* Function implementation below ******************* """


# ***************************************************************************
# **************************** FUNCTION READ_FILE ***************************
# ***************************************************************************

# Reads csv file into a dataframe.
# Pass an index limit to only read file until a certain index.
# Uses end_file_index to stop reading file when valid data stops.
# If index_limit is None and assigned None by end_file_index(), then
# pd.read_csv will read the entire csv file.
# Option to pass a flag and error message to be retrieved from main() if
# user wants to verify that read was successful.
def read_file(file, index_limit=None, flag=[False], error=[""]):

    # Initialize df to None.
    df = None

    # If index_limit is None, find where valid data stops using
    # end_file_index() so that the nrows parameter can be assigned in
    # pd.read_csv().
    if index_limit is None:
        index_limit = end_file_index(file)

    # Read the file into a dataframe. If error reading or finding file,
    # append the error.
    if os.path.isfile(file):
        try:
            df = pd.read_csv(file, nrows=index_limit)
            flag[0] = True
        except Exception as e:
            error[0] = str(e)
            flag[0] = False
    else:
        error[0] = "File not found."
        flag[0] = False
    # End for.

    return df
# End read_file.


# ***************************************************************************
# ******************* FUNCTION END_FILE_INDEX VERSION 1 *********************
# ***************************************************************************
''' 
# Get the index where valid data ends. This is returned to read_file()
# which uses the index for limiting how many rows to read into a dataframe.
# The Lighthouse data stops when lines at the bottom begin with '#' so this
# is the default criteria_char.
# Option to specify if the criteria_char means iterate UNTIL criteria_char
# or UNTIL_NOT criteria_char.

# Note: this version returns the index of the file pointer which contains
# all characters, not row number. So the function was rewritten to
# read the lines correctly and calculate the row number in Version 2.
def end_file_index(filename, criteria_char='#', until=False, until_not=True):

    with open(filename, 'r') as file:
        # Move the file pointer to the end of the file: start from the
        # bottom for better efficiency.
        file.seek(0, 2)

        lines = file.readlines()

        # Get current position (end of file).
        index = file.tell()

        if until:
            while index >= 0:
                file.seek(index)
                current_char = file.read(1)
                if current_char == criteria_char:
                    return index
                index -= 1
            # End while.

        if until_not:
            while index >= 0:
                file.seek(index)
                current_char = file.read(1)
                # Skip blank lines.
                if current_char == "":
                    index -= 1
                    continue
                if current_char != criteria_char:
                    return index
                index -= 1
            # End while.

        # Forward traversal.
        # for index, line in enumerate(file):
        #     if line.startswith(criteria_char):
        #         return index
    # File closed.

    return None
# End end_file_index.'''


# ***************************************************************************
# ******************* FUNCTION END_FILE_INDEX VERSION 2 *********************
# ***************************************************************************
'''
# Get the number of the last row of valid data.
# In Lighthouse files, there are junk lines that begin with '#', so
# the last valid row number is returned to read_file so that nrows
# parameter in pd.read_csv() can be specified, to limit the number of
# rows read into the dataframe.
# This is the more efficient byte-sized approach than loading the entire
# file into memory at once using readlines().
def end_file_index(filename):

    # Read file in binary mode to get byte objects instead of unicode
    # strings.
    with open(filename, 'rb') as file:

        file.seek(0, 2)
        end_position = file.tell()

        buffer_size = 1024
        total_lines = 0
        trailing_lines = 0
        buffer = b"" # Bytes literal, instead of default unicode characters.
        # found_valid_data = False

        while end_position > 0:
            read_position = max(0, end_position - buffer_size)
            file.seek(read_position)
            new_buffer = file.read(end_position - read_position)
            buffer = new_buffer + buffer
            end_position = read_position

            lines = buffer.split(b'\n')

            if read_position > 0:
                buffer = lines[0]
                lines = lines[1:]
            else:
                buffer = b""

            for line in reversed(lines):
                total_lines += 1
                if not found_valid_data:
                    if line.strip() == b'':
                        trailing_lines += 1
                    elif line.startswith(b'#'):
                        trailing_lines += 1
                    else:
                        found_valid_data = True
            # End for.

        # if not found_valid_data:
        #     trailing_lines = total_lines
    # End while.

    valid_data_lines = total_lines - trailing_lines
    return valid_data_lines
# End end_file_index().'''


# ***************************************************************************
# ******************* FUNCTION END_FILE_INDEX VERSION 3 *********************
# ***************************************************************************

# Simply count the lines in the file (Python's iteration over file does
# not load the entire file into memory). Then, subtract the number of lines
# that begin with '# '. This assumes all of them are at the end.
def end_file_index(filename):

    line_count = 0
    trailing_lines = 0

    with open(filename, 'r', encoding='utf-8') as file:
        for line in file:
            line_count += 1
            if line.startswith("# "):
                trailing_lines += 1
    # Close file.

    valid_data_lines = line_count - trailing_lines - 1 # -1 for last new line.

    return valid_data_lines
# End end_file_index().


# ***************************************************************************
# ******************* FUNCTION SPLIT_BY_YEAR ********************************
# ***************************************************************************

# Splits a dataframe into yearly dataframes. Returns a list of
# yearly dataframes.
# Lighthouse data files are provided in multi-year csv files but need
# to be processed year by year so splitting them is necessary.
def split_by_year(df, datetime_col_name):
    # Case if column name is not assigned.
    if datetime_col_name is None:
        return df

    df[datetime_col_name] = pd.to_datetime(df[datetime_col_name])

    # Get an array of unique years from dt.year which extracts the year
    # components from the datetime objects in the datetime column of
    # the dataframe.
    years = df[datetime_col_name].dt.year.unique()

    # Split dataframe by year. For each year, filter df using boolean
    # mask [df[datetime_col_name].dt.year == year] to create a new
    # dataframe containing only the rows where the year (.dt.year) matches
    # the current 'year'. Append these dataframes to a list.
    data_by_year = []
    for year in years:
        data_by_year.append(df[df[datetime_col_name].dt.year == year].reset_index(drop=True))
    # End for.

    # Or use dictionary comprehension and return a dictionary where the
    # keys are the years and the items are the corresponding dataframes.
    # data_by_year = {year: df[df[date_col_name].dt.year == year] for year in years}

    # Return the list.
    return data_by_year
# End split_by_year.


# ***************************************************************************
# ******************* FUNCTION CLEAN_DATAFRAME ******************************
# ***************************************************************************

# Prepare dataframe for analysis by standardizing datetimes and numerical values.
# Option to clean backup water level (bwl) and harmonic water level (harmwl) columns.
# Option to pass flag pointer so that status of if clean_dataframe succeeded or failed
# can be checked by user in main program.
def clean_dataframe(df, datetime_col_name, pwl_col_name, harmwl_col_name=None,
                    bwl_col_name=None, error=[""]):

    # Replace missing values with NaN.
    df.replace([-999, -99, 99, 'NA', 'RM'], np.nan, inplace=True)

    # Convert to datetime and numeric. coerce changes invalid values
    # to NaT/NaN.
    try:
        df[datetime_col_name] = pd.to_datetime(df[datetime_col_name], errors='coerce')
        df[pwl_col_name] = pd.to_numeric(df[pwl_col_name], errors='coerce')
    except Exception as e:
        error[0] += str(e)

    if bwl_col_name is not None:
        df[bwl_col_name] = pd.to_numeric(df[bwl_col_name], errors='coerce')
    if harmwl_col_name is not None:
        df[harmwl_col_name] = pd.to_numeric(df[harmwl_col_name], errors='coerce')

    if df[datetime_col_name].isna().all() or df[pwl_col_name].isna().all():
        error[0] += "dataframe has column with only NaT or NaN. "
# End clean_dataframe.


# ***************************************************************************
# ******************* FUNCTION GET_DF_DICTIONARY ****************************
# ***************************************************************************

# Convert list of dataframes to a dictionary with the year as the keys and
# corresponding dataframes as values. Then, comparing the NOAA and Lighthouse
# dictionaries ensures data from the same year will be compared.

def get_df_dictionary(df_list, dt_col_name):

    dfs_dict = {}
    for df in df_list:

        year = df[dt_col_name].dt.year
        dfs_dict[year[0]] = df
    # End for.

    return dfs_dict
# End get_df_dictionary.


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
    # percent_agree_gaps = round(shared_gaps/total_agree * 100, 4)
    percent_val_disagree = round(values_disagree/size * 100, 4)
    percent_total_disagree = round(total_disagree/size * 100, 4)
    percent_primary_missing = round(primary_missing/size * 100, 4)
    percent_ref_missing = round(ref_missing/size * 100, 4)

    table = {
        'total points': [total_agree, values_disagree, total_disagree, primary_missing,
                         ref_missing],
        'percent': [percent_agree, percent_val_disagree, percent_total_disagree,
                    percent_primary_missing, percent_ref_missing]
    }

    row_labels = ['total agreements', 'value disagreements', 'total disagreements',
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

        # Check if current value is different from previous value.
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
        summary_df['offset (ref - primary, unit)'] = run_values
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

# Get array of all discrepancies. Called by get_run_data().
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
        filter_series = abs(filtered_df['offset (ref - primary, unit)']) >= threshold
    else:
        if is_max is True:
            filter_series = abs(filtered_df['offset (ref - primary, unit)']) <= threshold

    filtered_df = filtered_df[filter_series]

    return filtered_df
# End filter_value.



