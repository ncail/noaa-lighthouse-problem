import numpy as np
import pandas as pd
import json


class TransformData:

    # ******************************************************************************
    # ****************************** CONSTRUCTOR ***********************************
    # ******************************************************************************
    def __init__(self, user_config=None):
        default_config = {
            'number_of_intervals': 0
        }

        if user_config is None:
            self.config = default_config
        else:
            self.set_configs(user_config)

    # ******************************************************************************
    # ******************************** SETTERS *************************************
    # ******************************************************************************
    def set_configs(self, user_config: dict) -> None:
        for section, settings in user_config.items():
            if section in self.config:
                self.config[section].update(settings)
    # End set_configs.

    # ******************************************************************************
    # ***************************** DATA PROCESSING ********************************
    # ******************************************************************************
    def temporal_deshifter(self, merged_df, primary_col_name, ref_col_name, size):

        """ Loop over 7 temporal shifts: from -3 to 3
                Create copy of merged dataframe.
                Shift lighthouse by temporal shift loop variable value.
                Create dataframe to hold the de-shifted data.
                Call identify_offset.
                    If returns nan, then continue to trying next temporal offset value.
                    If offset,
                        find index where offset stops.
                        Save this index, only loop for index < size.
                Save corrections to de-shifted dataframe.
                Restart loop, beginning at last index, and first temporal offset value.
        """

        # Initialize dataframes. df_copy will be shifted to find offsets.
        # deshifted_df will hold the temporally corrected values.
        df_copy = merged_df.copy()
        deshifted_df = merged_df.copy()

        index = 0
        temporal_shifts = range(-3, 4)
        shift_val_index = 0

        # While indices of dataframe are valid, correct temporal shifts if possible.
        while index < size:

            # If all temporal shifts have been tried, consider this segment
            # of data impossible to automatically correct, possibly because of a
            # changing vertical offset or skipped values, etc.
            # Restart process at +10 indices from current.
            if shift_val_index > 6:
                shift_val_index = 0
                while index < index + 10:
                    if index >= size:
                        break
                    deshifted_df[primary_col_name].iloc[index] = df_copy[primary_col_name].iloc[index]
                    index += 1

            # Current shift value.
            try_shift = temporal_shifts[shift_val_index]

            # Temporally shift the dataframe.
            df_copy[primary_col_name] = merged_df[primary_col_name].shift(try_shift)

            # Get the vertical offset. Note that identify_offset does not let missing
            # values contribute to the detection of an offset, but does include them in the
            # duration count.
            vert_offset = self.identify_offset(df_copy[primary_col_name], df_copy[ref_col_name],
                                               index, size, duration=10)

            # If there is no consistent vertical offset, try again for next temporal shift value.
            if pd.isna(vert_offset):
                shift_val_index += 1
                continue

            # If an offset is found, record df_copy values into deshifted_df while the vertical
            # offset is valid. Record the index where the offset stops.
            # When offset stops, undo the shift.
            while index < size:
                if (round(df_copy[primary_col_name].iloc[index] + vert_offset, 4) ==
                        round(df_copy[ref_col_name].iloc[index], 4)):
                    index += 1
                    deshifted_df[primary_col_name].iloc[index] = df_copy[primary_col_name].iloc[index]
                else:
                    df_copy[primary_col_name] = merged_df[primary_col_name]
                    # df_copy.drop(index, inplace=True)
                    break
            # End inner while.

            shift_val_index = 0
        # End outer while.

        return deshifted_df
    # End temporal_deshifter.

    def process_offsets(self, offset_column, reference_column, size, index=0, criteria=None, offset_arr=None):

        # index = 0
        while index < size:
            # Skip NaNs.
            if pd.isna(offset_column.iloc[index]) or pd.isna(reference_column.iloc[index]):
                index += 1
                offset_arr.append(np.nan)
                continue

            # Get offset.
            offset = self.identify_offset(offset_column, reference_column, index, size, duration=criteria)

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

    def identify_offset(self, offset_column, reference_column, index, size, duration=None):
        if duration is None:
            duration = self.config['number_of_intervals']

        offset_value = offset_column.iloc[index]
        ref_value = reference_column.iloc[index]
        difference = round(ref_value - offset_value, 4)

        f_loop = index
        # for loop in range(index, index + duration):
        while f_loop < (index + duration):
            if f_loop + 1 >= size:
                return np.nan

            # Skips over NaNs. Considers that the offset remains valid even if
            # some values are missing due to sensor failure or whatever.
            # This prevents invalidating an offset that is consistent otherwise.
            if pd.isna(offset_column.iloc[f_loop]) or pd.isna(reference_column.iloc[f_loop]):
                continue

            current_diff = round(reference_column.iloc[f_loop + 1] - offset_column.iloc[f_loop + 1], 4)

            if current_diff != difference:
                return np.nan

            # Increment index.
            f_loop += 1
        # End while.

        return difference
    # End identify_offset.

    # ******************************************************************************
    # ***************************** FILE HANDLING **********************************
    # ******************************************************************************
    @staticmethod
    def load_configs(file_path: str) -> dict:
        try:
            # Update config dictionary only if section exists.
            with open(file_path, 'r') as file:
                user_config = json.load(file)
        except FileNotFoundError:
            print(f"Error: Config file '{file_path}' not found. Using default "
                  f"MetricsCalculator configuration.")
        return user_config
    # End load_configs.



