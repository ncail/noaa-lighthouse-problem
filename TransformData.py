import numpy as np
import pandas as pd
import json
import datetime


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
                'replace_with_nans': True
            }
        }

        default_col_config = {
            'primary_data_column_name': 'primary_col',
            'reference_data_column_name': 'ref_col',
            'datetime_column_name': 'dt_col'
        }

        self.config = default_config
        if user_config is not None:
            self.set_configs(user_config)

        self.dataframe = pd.DataFrame()
        if df is not None:
            self.set_dataframe(df)

        self.col_config = default_col_config
        if col_names is not None:
            self.set_column_names(col_names)

        self.time_shift_table = {
            'temporal_shift': [],
            'vertical_offset': [],
            'date_time': [],
            'primary_water_level': [],
            'reference_water_level': []
        }

        self.shifts_summary_df = [pd.DataFrame(columns=['start_date', 'end_date', 'duration',
                                               'temporal_shift', 'vertical_offset'])]

        self.document_corrected_data_entries = False

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
            self.dataframe[self.col_config['primary_data_column_name']] = primary_column
            self.dataframe[self.col_config['reference_data_column_name']] = reference_column

        if datetime_column is not None:
            self.dataframe[self.col_config['datetime_data_column_name']] = datetime_column
    # End set_data.

    def set_dataframe(self, dataframe: pd.DataFrame, user_col_config: dict = None, **kwargs) -> None:
        self.dataframe = dataframe
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

    def set_document_corrected_time_shift_series_data(self, document_corrected_data_entries=False):
        self.document_corrected_data_entries = document_corrected_data_entries

    # ******************************************************************************
    # ******************************** GETTERS *************************************
    # ******************************************************************************
    def get_configs(self) -> dict:
        return self.config.copy()
    # End get_configs.

    def get_col_config(self) -> dict:
        return self.col_config
    # End get_col_configs.

    def get_dataframe(self) -> pd.DataFrame:
        return self.dataframe
    # End get_dataframe.

    def get_data_column(self, column_name: str):
        if column_name in self.dataframe.columns:
            return self.dataframe[column_name]
        return None
    # End get_data_column.

    def get_shifts_summary_df(self):
        return self.shifts_summary_df

    def get_time_shift_table(self):
        return self.time_shift_table

    def get_temporal_processing_string(self):
        return self.temporal_processing_string

    @staticmethod
    def get_temporal_processing_summary_dataframe():
        return pd.DataFrame(columns=['start_index', 'end_index', 'temporal_shift', 'vertical_offset'])

    # ******************************************************************************
    # ***************************** DATA PROCESSING ********************************
    # ******************************************************************************
    def clear_temporal_processing_string(self):
        self.temporal_processing_string = ""

    def temporal_shift_corrector(self, df=None, primary_col=None, reference_col=None, datetime_col=None, index=0,
                                 **kwargs):
        # Get column names.
        default_col_names = self.col_config
        names = {**default_col_names, **kwargs}
        primary_col_name = names['primary_data_column_name']
        reference_col_name = names['reference_data_column_name']
        ref_dt_col_name = names['datetime_column_name']

        # Get dataframe, with columns corresponding to the column names assigned above.
        if df is None:
            if any(item is None for item in [primary_col, reference_col]):
                if self.dataframe.empty:
                    raise ValueError("Incorrect data provided to temporal_shift_corrector, and no pre-set data found. "
                                     "Must either be passed a dataframe, or a primary and reference data Series.")
            else:
                df = self.dataframe
                if len(primary_col) == len(reference_col) == len(datetime_col):
                    df[primary_col_name] = primary_col
                    df[reference_col_name] = reference_col
                    df[ref_dt_col_name] = datetime_col
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

        return self._temporal_deshifter(df, primary_col_name, reference_col_name, ref_dt_col_name, index,
                                        offset_criteria, insert_nans)
    # End temporal_shift_corrector.

    def _temporal_deshifter(self, merged_df, primary_col_name, ref_col_name, ref_dt_col_name, index=0,
                            offset_criteria=10, insert_nans=True):
        size = len(merged_df)

        # Initialize dataframes. df_copy will be shifted to find offsets.
        # corrected_df will hold the temporally corrected values. No vertical offset correction is done.
        df_copy, corrected_df = self._initialize_dataframes(merged_df)

        temporal_shifts = [0, -1, -2, -3, 1, 2, 3]  # A temporal shift of 0 or -1 is most likely.
        shift_val_index = 0

        # While indices of dataframe are valid, correct temporal shifts if possible.
        is_end = [False]
        start_index = index
        while index < size:

            if not is_end[0]:
                start_index = index
            
            # Handle uncorrectable segment.
            if shift_val_index > 6 or is_end[0]:
                df_copy, shift_val_index = self._reset_shift(df_copy, merged_df, primary_col_name)

                corrected_df, index = self._handle_uncorrectable_segment(corrected_df, start_index,
                                                                         primary_col_name, df_copy,
                                                                         offset_criteria, size, insert_nans)
                if index >= size:
                    func_index = index - 1
                else:
                    func_index = index

                self.append_summary_df(start_index, func_index, try_shift, vert_offset, merged_df, ref_dt_col_name,
                                       uncorrected=True)
                self.append_shifts_table(start_index, func_index, try_shift, vert_offset,
                                         merged_df, ref_dt_col_name,
                                         primary_col_name, ref_col_name, uncorrected=True)

                # If end of dataframe was reached with no identifiable offset, break after filling
                # and documenting last segment.
                if is_end[0]:
                    break

                continue
            # End handle uncorrectable segment.

            # Current shift value.
            try_shift = temporal_shifts[shift_val_index]

            # Temporally shift the dataframe.
            df_copy = self._apply_temporal_shift(df_copy, merged_df, primary_col_name, try_shift)

            # Get the vertical offset. Note that identify_offset does not let missing
            # values contribute to the detection of an offset, but does include them in the
            # duration count.
            vert_offset = self.identify_offset(df_copy[primary_col_name], df_copy[ref_col_name], index, size,
                                               duration=offset_criteria, end_reached=is_end)[0]

            if pd.isna(vert_offset) and is_end[0]:
                index = size - 1
                continue

            # If there is no consistent vertical offset, try again for next temporal shift value.
            if pd.isna(vert_offset):
                shift_val_index += 1
                continue

            # If an offset is found, record df_copy values into corrected_df while the vertical
            # offset is valid. Record the index where the offset stops.
            # When offset stops, undo the shift.
            corrected_df, index = self._record_corrected_values(df_copy, corrected_df,
                                                                primary_col_name, ref_col_name,
                                                                vert_offset, index, size)
            
            df_copy, shift_val_index = self._reset_shift(df_copy, merged_df, primary_col_name)

            if index >= size:
                func_index = index - 1
            else:
                func_index = index

            self.append_summary_df(start_index, func_index, try_shift,
                                   vert_offset, merged_df, ref_dt_col_name)

            if self.document_corrected_data_entries:
                self.append_shifts_table(start_index, func_index, try_shift, vert_offset,
                                         corrected_df, ref_dt_col_name, primary_col_name, ref_col_name)
            else:
                self.append_shifts_table(start_index, func_index, try_shift, vert_offset,
                                         merged_df, ref_dt_col_name, primary_col_name, ref_col_name)

            # index += 1
        # End while.

        # Return temporally corrected dataframe.
        return corrected_df
    # End temporal_deshifter.
    
    def _handle_uncorrectable_segment(self, corrected_df, start_index, primary_col_name, df_copy,
                                      offset_criteria, size, insert_nans=True):
        # Case I: determine if a run of nans begins.
        # If so, return index where run ends if <= stopping index,
        # else return stopping index.
        case_one_index, is_nan_run = self._check_nan_runs(df_copy[primary_col_name], start_index,
                                                          offset_criteria, size)

        # Case II: determine if a flat line begins.
        # If so, return index where flat line ends.
        case_two_index, is_flat_line = self._check_flat_line(df_copy[primary_col_name], start_index,
                                                             offset_criteria, size)

        # Case III: likely that there is some intermediate problem like a duplicate value,
        # or the previous segment was also Case III so an offset exists but is interrupted
        # by something within the length of offset_criteria.
        # Return index + offset criteria as the stopping index.
        case_three_index = start_index + offset_criteria - 1

        if is_nan_run:
            end_fill_index = case_one_index
        elif is_flat_line:
            end_fill_index = case_two_index
        else:
            if case_three_index >= size:
                case_three_index = size - 1
            end_fill_index = case_three_index

        if insert_nans:
            corrected_df.loc[start_index:end_fill_index, primary_col_name] = np.nan
        else:
            corrected_df.loc[start_index:end_fill_index, primary_col_name] = \
                df_copy.loc[start_index:end_fill_index, primary_col_name].copy()

        return corrected_df, end_fill_index + 1
    # End uncorrectable case.

    def _check_nan_runs(self, series, starting_index, offset_criteria, size):
        index = starting_index
        nan_run_found = False
        valid_value_count = 0
        while index + offset_criteria < size:
            if valid_value_count >= offset_criteria:
                break
            if pd.isna(series[index]):
                if not nan_run_found:
                    if series.loc[index:index + offset_criteria].isna().all():
                        nan_run_found = True
            elif nan_run_found:
                break
            else:
                valid_value_count += 1
            index += 1
        # End while.

        return index, nan_run_found
    # End _check_nan_runs.

    def _check_flat_line(self, series, starting_index, offset_criteria, size):
        index = starting_index
        valid_value_count = 0
        flat_line_found = False
        while index + 1 < size:
            current_val = series[index]
            next_val = series[index + 1]
            if valid_value_count >= offset_criteria and not flat_line_found:
                break
            if pd.isna(current_val):
                index += 1
                continue
            if current_val == next_val:
                if not flat_line_found:
                    if len(series.loc[index:index + offset_criteria].unique()) == 1:
                        flat_line_found = True
            elif flat_line_found:
                break
            else:
                valid_value_count += 1
            index += 1
        # End while.

        return index, flat_line_found
    # End _check_flat_line.

    def _initialize_write_path(self, enable_write, write_path):
        if enable_write and not write_path:
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            write_path = f"generated_files/correction_reports/temporal_correction_report_{timestamp}.txt"
        return write_path
    
    def _initialize_dataframes(self, merged_df):
        return merged_df.copy(), merged_df.copy()
    
    def _reset_shift(self, df_copy, merged_df, primary_col_name):
        df_copy[primary_col_name] = merged_df[primary_col_name].copy()
        shift_val_index = 0
        return df_copy, shift_val_index
    
    def _apply_temporal_shift(self, df_copy, merged_df, primary_col_name, try_shift):
        df_copy[primary_col_name] = merged_df[primary_col_name].shift(try_shift)
        return df_copy
    
    def _record_corrected_values(self, df_copy, corrected_df, primary_col_name, ref_col_name,
                                 vert_offset, index, size):
        while index < size:
            if pd.isna(df_copy[primary_col_name].iloc[index]):
                corrected_df.loc[index, primary_col_name] = np.nan
            elif pd.isna(df_copy[ref_col_name].iloc[index]):
                corrected_df.loc[index, primary_col_name] = df_copy[primary_col_name].iloc[index]
            elif round(df_copy[primary_col_name].iloc[index] + vert_offset, 4) == round(
                    df_copy[ref_col_name].iloc[index], 4):
                corrected_df.loc[index, primary_col_name] = df_copy.loc[index, primary_col_name]
            else:
                break
            index += 1
        return corrected_df, index

    def append_shifts_table(self, start_index, index, try_shift, vert_offset, df, ref_dt_col_name,
                            primary_wl_col_name, ref_wl_col_name, uncorrected=False):
        if uncorrected:
            try_shift = 'N/A'
            # vert_offset = 'N/A'

        func_index = start_index
        while func_index < index:

            vert_offset_current = vert_offset

            if uncorrected:
                vert_offset_current = round(df[ref_wl_col_name].iloc[func_index] -
                                            df[primary_wl_col_name].iloc[func_index], 3)

            if pd.isna(df[ref_wl_col_name].iloc[func_index]) or \
                    pd.isna(df[primary_wl_col_name].iloc[func_index]):
                vert_offset_current = 'N/A'

            self.time_shift_table['temporal_shift'].append(try_shift)
            self.time_shift_table['vertical_offset'].append(vert_offset_current)
            self.time_shift_table['date_time'].append(df[ref_dt_col_name].iloc[func_index])
            self.time_shift_table['primary_water_level'].append(str(df[primary_wl_col_name].iloc[func_index]))
            self.time_shift_table['reference_water_level'].append(df[ref_wl_col_name].iloc[func_index])

            func_index += 1

    def append_summary_df(self, start_index, index, try_shift, vert_offset,
                          original_df, ref_dt_col_name, uncorrected=False):
        if uncorrected:
            try_shift = 'N/A'
            vert_offset = 'N/A'

        summary_row = pd.DataFrame({
            'start_date': [original_df[ref_dt_col_name].iloc[start_index]],
            'end_date': [original_df[ref_dt_col_name].iloc[index]],
            'duration': [original_df[ref_dt_col_name].iloc[index] -
                         original_df[ref_dt_col_name].iloc[start_index]],
            'temporal_shift': [try_shift],
            'vertical_offset': [vert_offset]
        })
        self.shifts_summary_df[0] = self.shifts_summary_df[0].dropna(axis=1, how='all')
        self.shifts_summary_df[0] = pd.concat([self.shifts_summary_df[0], summary_row], ignore_index=True)

        # return self.shifts_summary_df.copy()

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

    def identify_offset(self, offset_column, reference_column, index, size, duration=0, end_reached=[False]):
        if duration is None:
            duration = self.config['vertical_offset_correction']['number_of_intervals']

        if index >= size:
            return np.nan, index

        end_reached[0] = False

        while index < size:
            offset_value = offset_column.iloc[index]
            ref_value = reference_column.iloc[index]
            difference = round(ref_value - offset_value, 4)

            if pd.isna(difference):
                index += 1
            else:
                break

        f_loop = index
        # for loop in range(index, index + duration):
        while f_loop < (index + duration):
            if f_loop + 1 >= size:
                end_reached[0] = True
                return np.nan, f_loop

            # Skips over NaNs. Considers that the offset remains valid even if
            # some values are missing due to sensor failure or whatever.
            # This prevents invalidating an offset that is consistent otherwise.
            if pd.isna(offset_column.iloc[f_loop]) or pd.isna(reference_column.iloc[f_loop]):
                f_loop += 1
                index += 1
                continue

            current_diff = round(reference_column.iloc[f_loop + 1] - offset_column.iloc[f_loop + 1], 4)
            if current_diff != difference:
                return np.nan, f_loop
            # Increment index.
            f_loop += 1
        # End while.
        return difference, f_loop
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