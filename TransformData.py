import numpy as np
import pandas as pd
import json
import datetime
from collections import namedtuple


class TransformData:

    # ******************************************************************************
    # ****************************** CONSTRUCTOR ***********************************
    # ******************************************************************************
    def __init__(self, user_config=None, df=None, col_names=None):
        default_config = {
            'vertical_offset_correction': {
                'number_of_intervals': 0
            },
            'temporal_shift_correction': {
                'number_of_intervals': 10,
                'replace_with_nans': True,
                'enable_write': False
            }
        }

        default_col_config = {
            'primary_data_column_name': 'primary_col',
            'reference_data_column_name': 'ref_col',
            'datetime_column_name': 'dt_col'
        }

        self.config = default_config.copy()
        if user_config is not None:
            self.set_configs(user_config.copy())

        self.dataframe = pd.DataFrame()
        if df is not None:
            self.set_dataframe(df)

        self.col_config = default_col_config.copy()
        if col_names is not None:
            self.set_column_names(col_names)

    # ******************************************************************************
    # ******************************** SETTERS *************************************
    # ******************************************************************************
    def set_configs(self, user_config: dict) -> None:
        for section, settings in user_config.items():
            if section in self.config:
                self.config[section].update(settings)
    # End set_configs.

    def set_data(self, primary_column: pd.Series, reference_column: pd.Series,
                 datetime_column: pd.Series = None, is_set=[False]) -> None:
        is_set[0] = True

        if len(primary_column) != len(reference_column):
            is_set[0] = False

        if is_set[0]:
            self.dataframe[self.col_config['primary_data_column_name']] = primary_column.copy()
            self.dataframe[self.col_config['reference_data_column_name']] = reference_column.copy()

        if datetime_column is not None:
            self.dataframe[self.col_config['datetime_data_column_name']] = datetime_column.copy()
    # End set_data.

    def set_dataframe(self, dataframe: pd.DataFrame, user_col_config: dict = None, **kwargs) -> None:
        self.dataframe = dataframe.copy()
        self.set_column_names(user_col_config, **kwargs)
    # End set_dataframe.

    def set_column_names(self, user_col_config: dict = None, **kwargs) -> None:
        if user_col_config is None:
            for key, name in kwargs:
                if key in self.col_config:
                    self.col_config[key] = name
        else:
            for key, name in user_col_config:
                if key in self.col_config:
                    self.col_config[key] = name
    # End set_column_names.

    # ******************************************************************************
    # ******************************** GETTERS *************************************
    # ******************************************************************************
    def get_configs(self) -> dict:
        return self.config.copy()
    # End get_configs.

    def get_col_config(self) -> dict:
        return self.col_config.copy()
    # End get_col_configs.

    def get_dataframe(self) -> pd.DataFrame:
        return self.dataframe.copy()
    # End get_dataframe.

    def get_data_column(self, column_name: str):
        if column_name in self.dataframe.columns:
            return self.dataframe[column_name].copy()
        return None
    # End get_data_column.

    # ******************************************************************************
    # ***************************** DATA PROCESSING ********************************
    # ******************************************************************************
    def temporal_shift_corrector(self, df=None, primary_col=None, reference_col=None, index=0, summary_df=None,
                                 write_path="", **kwargs):
        # Get column names.
        default_col_names = self.col_config
        names = {**default_col_names, **kwargs}
        primary_col_name = names['primary_data_column_name']
        reference_col_name = names['reference_data_column_name']

        # Get dataframe, with columns corresponding to the column names assigned above.
        if df is None:
            if any(item is None for item in [primary_col, reference_col]):
                if self.dataframe.empty:
                    raise ValueError("Incorrect data provided to temporal_shift_corrector, and no pre-set data found. "
                                     "Must either be passed a dataframe, or a primary and reference data Series.")
            else:
                df = self.dataframe.copy()
                if len(primary_col) == len(reference_col):
                    df[primary_col_name] = primary_col.copy()
                    df[reference_col_name] = reference_col.copy()
                else:
                    raise ValueError(f"Length mismatch: primary_col Series has {len(primary_col)} rows and "
                                     f"reference_col Series has {len(reference_col)} rows.")
        else:
            if not all(col in df.columns for col in [primary_col_name, reference_col_name]):
                raise KeyError("Passed DataFrame is invalid: required columns not found. Be sure to either "
                               "use TransformData.set_column_names() to set the names of the primary and reference "
                               "data columns in the DataFrame, or also pass in key word arguments "
                               "primary_data_column_name and reference_data_column_name to "
                               "TransformData.temporal_shift_corrector.")

        # Get configuration parameters for _temporal_deshifter.
        default_params = self.config['temporal_shift_correction']
        params = {**default_params, **kwargs}
        offset_criteria = params['number_of_intervals']
        insert_nans = params['replace_with_nans']
        enable_write = params['enable_write']

        return self._temporal_deshifter(df, primary_col_name, reference_col_name, index, summary_tuples,
                                        offset_criteria, insert_nans, enable_write, write_path)
    # End temporal_shift_corrector.

    def _temporal_deshifter(self, merged_df, primary_col_name, ref_col_name, index=0, summary_df=None,
                            offset_criteria=10, insert_nans=True, enable_write=False, write_path=""):
        size = len(merged_df)

        # Set write path if generating temporal correction reports is enabled.
        if enable_write is True:
            if not write_path:
                timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                write_path = f"generated_files/correction_reports/temporal_correction_report_{timestamp}.txt"

        # Initialize report string. And define dataframe to store metrics about the
        # offset correction.
        execution_writes = ""
        summary_df = pd.DataFrame(columns=['start_index', 'end_index', 'temporal_shift', 'vertical_offset'])

        # Initialize dataframes. df_copy will be shifted to find offsets.
        # corrected_df will hold the temporally corrected values. No vertical offset correction is done.
        df_copy = merged_df.copy()
        corrected_df = merged_df.copy()

        temporal_shifts = [0, -1, -2, -3, 1, 2, 3]  # A temporal shift of 0 or -1 is most likely.
        shift_val_index = 0

        # While indices of dataframe are valid, correct temporal shifts if possible.
        while index < size:

            # Store starting index.
            start_index = index

            # If all temporal shifts have been tried, consider this segment
            # of data impossible to automatically correct, possibly because of a
            # changing vertical offset or skipped values, etc.
            # Restart process at +10 indices from current.
            if shift_val_index > 6:
                df_copy[primary_col_name] = merged_df[primary_col_name].copy()
                shift_val_index = 0
                while index < start_index + offset_criteria:
                    if index >= size:
                        break
                    if insert_nans:
                        corrected_df.loc[index, primary_col_name] = np.nan
                    else:
                        corrected_df.loc[index, primary_col_name] = df_copy.loc[index, primary_col_name]
                    index += 1
                execution_writes += (f"SEGMENT COULD NOT BE CORRECTED:\n"
                                     f"{merged_df.iloc[start_index:index]}\n")
                execution_writes += (f"Corrected dataframe holds:\n"
                                     f"{corrected_df.iloc[start_index:index]}\n")
                summary_df = summary_df.append({
                    'start_index': start_index,
                    'end_index': index,
                    'temporal_shift': np.nan,
                    'vertical_offset': np.nan
                }, ignore_index=True)

            # Current shift value.
            try_shift = temporal_shifts[shift_val_index]

            # Temporally shift the dataframe.
            df_copy[primary_col_name] = merged_df[primary_col_name].shift(try_shift).copy()

            # Get the vertical offset. Note that identify_offset does not let missing
            # values contribute to the detection of an offset, but does include them in the
            # duration count.
            vert_offset = self.identify_offset(df_copy[primary_col_name], df_copy[ref_col_name],
                                               index, size, duration=offset_criteria)

            # If there is no consistent vertical offset, try again for next temporal shift value.
            if pd.isna(vert_offset):
                shift_val_index += 1
                continue

            # If an offset is found, record df_copy values into corrected_df while the vertical
            # offset is valid. Record the index where the offset stops.
            # When offset stops, undo the shift.
            while index < size:
                if (round(df_copy[primary_col_name].iloc[index] + vert_offset, 4) ==
                        round(df_copy[ref_col_name].iloc[index], 4)):
                    corrected_df.loc[index, primary_col_name] = df_copy.loc[index, primary_col_name]
                    index += 1
                else:
                    df_copy[primary_col_name] = merged_df[primary_col_name].copy()
                    # df_copy.drop(index, inplace=True)
                    break
            # End inner while.
            execution_writes += (f"Vertical offset found: {vert_offset} using temporal shift {try_shift}.\n"
                                 f"Corrected temporal shift from indices {start_index} : {index}\n")
            execution_writes += f"{corrected_df.iloc[start_index:index]}\n"
            summary_df = summary_df.append({
                'start_index': start_index,
                'end_index': index,
                'temporal_shift': try_shift,
                'vertical_offset': vert_offset
            }, ignore_index=True)

            shift_val_index = 0
        # End outer while.

        # Write report to file.
        self._report_correction(execution_writes, write_path)

        # Return temporally corrected dataframe.
        return corrected_df
    # End temporal_deshifter.

    def process_offsets(self, offset_column, reference_column, size, index=0, criteria=10, offset_arr=None):
        if criteria is None:
            criteria = self.config['vertical_offset_correction']['number_of_intervals']

        # index = 0
        while index < size:
            # Skip NaNs.
            if pd.isna(offset_column.iloc[index]) or pd.isna(reference_column.iloc[index]):
                index += 1
                offset_arr.append(np.nan)
                continue

            # Get offset.
            offset = self.identify_offset(offset_column, reference_column, index, size, duration=criteria)

            # If offset is zero, no corrections needed. Skip over indices.
            if offset == 0.0:
                index += criteria
                continue

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

    def identify_offset(self, offset_column, reference_column, index, size, duration=0):
        if duration is None:
            duration = self.config['vertical_offset_correction']['number_of_intervals']

        if index >= size:
            return np.nan

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
                f_loop += 1
                index += 1
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
                  f"TransformData configuration.")
        return user_config
    # End load_configs.

    @staticmethod
    def _report_correction(msg, write_path=""):
        if not write_path:
            return

        with open(write_path, 'a') as file:
            file.write(f"{msg}")




