import json
import pandas as pd
import numpy as np
from datetime import timedelta
import re


class MetricsCalculator:

    # ******************************************************************************
    # ****************************** CONSTRUCTOR ***********************************
    # ******************************************************************************
    def __init__(self, col_config=None):
        if col_config is None:
            self.col_config = {
                'offset_column': 'offset',
                'start_date_column': 'start date',
                'end_date_column': 'end date',
                'duration_column': 'duration'
            }
        else:
            self.col_config = col_config

        self.default_config = {
            'filter_offsets_by_duration': {
                'threshold': '0 days',
                'type': 'min',
                'is_strict': False
            },
            'filter_offsets_by_value': {
                'threshold': 0.0,
                'use_abs': True,
                'type': 'min',
                'is_strict': False
            },
            'filter_gaps_by_duration': {
                'threshold': '0 days',
                'type': 'min',
                'is_strict': False
            },
            'offset_correction_parameters': {
                'number_of_intervals': 0
            }
        }

        self.config = self.default_config.copy()
        self.run_data_df = None
        self.metrics = None
    # End constructor.

    # ******************************************************************************
    # ******************************** SETTERS *************************************
    # ******************************************************************************
    def set_column_names(self, duration_column_name: str = 'duration_column',
                         offsets_column_name: str = 'offset_column',
                         start_date_column_name: str = 'start_date_column',
                         end_date_column_name: str = 'end_date_column') -> None:
        self.col_config['duration_column'] = duration_column_name
        self.col_config['offset_column'] = offsets_column_name
        self.col_config['start_date_column'] = start_date_column_name
        self.col_config['end_date_column'] = end_date_column_name
    # End set_column_names.

    def set_configs(self, user_config: dict) -> None:
        for section, settings in user_config.items():
            if section in self.config:
                self.config[section].update(settings)
    # End set_configs.

    def set_runs_dataframe(self, df: pd.DataFrame) -> None:
        self._validate_dataframe(df)
        self.run_data_df = df.copy()
    # End set_dataframe.

    def set_metrics(self, metrics: dict, is_set=[False]) -> None:
        is_set[0] = self._validate_metrics(metrics)

        if is_set[0]:
            self.metrics = metrics.copy()
    # End set_metrics.

    # ******************************************************************************
    # ******************************** GETTERS *************************************
    # ******************************************************************************
    def get_column_names(self):
        return self.col_config.copy()
    # End get_column_names

    def get_configs(self) -> dict:
        return self.config.copy()
    # End get_configs.

    def get_runs_dataframe(self):
        if self.run_data_df is None:
            return None
        else:
            return self.run_data_df.copy()

    def get_metrics(self):
        if self.metrics is None:
            return None
        else:
            return self.metrics.copy()
    # End get_metrics.

    # ******************************************************************************
    # *************************** CALCULATING METHODS ******************************
    # ******************************************************************************
    def generate_runs_df(self, offset_column: pd.Series, reference_column: pd.Series,
                         ref_dates: pd.Series, size: int, create_df: bool = True):
        offsets = self.get_discrepancies(offset_column, reference_column, size)

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

        if create_df is True:
            # Create the dataframe.
            summary_df = pd.DataFrame()
            summary_df['offset'] = run_values
            summary_df['start date'] = ref_dates.iloc[start_indices].tolist()
            summary_df['end date'] = ref_dates.iloc[end_indices].tolist()
            summary_df['duration'] = summary_df['end date'] - summary_df['start date']

            return summary_df

        else:
            return start_indices, end_indices, run_values
    # End get_run_data.

    def calculate_metrics(self, df: pd.DataFrame = None, calc_all=True, **kwargs):

        if df is not None:
            self._validate_dataframe(df)
        elif self.run_data_df is not None:
            df = self.run_data_df
        else:
            raise ValueError("No DataFrame provided and no pre-set DataFrame found.")

        available_metrics = {
            "long_offsets_count": lambda df: self.count_long_offsets(df),
            "long_gaps_count": lambda df: self.count_long_gaps(df),
            "max_gap_duration": lambda df: self.get_max_gap_duration(df),
            "max_gap_dates": lambda df: self.get_max_gap_dates(self.get_max_gap_duration(df)),
            "max_offset_duration": lambda df: self.get_max_offset_duration(df),
            "longest_offsets": lambda df: self.get_longest_offsets(self.get_max_offset_duration(df)),
            "large_offsets_count": lambda df: self.count_large_offsets(df),
            "min_max_offsets": lambda df: self.get_min_max_offsets(df),
            "max_offset_dates": lambda df: self.get_offset_dates(self.get_min_max_offsets(df)[0]),
            "min_offset_dates": lambda df: self.get_offset_dates(self.get_min_max_offsets(df)[1])
        }

        metrics = {}
        for key, func in available_metrics.items():
            if calc_all:
                metrics[key] = func(df)
            elif key in kwargs:
                metrics[key] = func(df)

        return metrics
    # End calculate_metrics.

    def format_metrics(self, metrics: dict = None) -> list[tuple[str, str]]:
        if metrics is None:
            if self.metrics is not None:
                metrics = self.metrics
            else:
                raise ValueError("No Metrics provided, and no pre-set Metrics found.")
        elif not self._validate_metrics(metrics):
            raise ValueError("Invalid Metrics provided. Metrics must be a Dictionary"
                             "and must only contain keys found in the list of valid metrics."
                             "Use MetricsCalculator.get_valid_metrics_list().")

        metric_strings = self._get_metric_key_strings()

        available_metric_data = {
            'long_offsets_count': (f"{metric_strings['offset_dur']}",
                                   "{long_offsets_count}"),
            'long_gaps_count': (f"{metric_strings['gap_dur']}",
                                "{long_gaps_count}"),
            'max_gap_duration': (f"Duration of longest gap",
                                 "{max_gap_duration}"),
            'max_gap_dates': ("Start/end date(s) of <{max_gap_duration}> gap",
                              "{max_gap_dates}"),
            'max_offset_duration': (f"Maximum duration of an offset",
                                    "{max_offset_duration}"),
            'longest_offsets': ("Offset value(s) with <{max_offset_duration}> duration",
                                "{longest_offsets}"),
            'large_offsets_count': (f"{metric_strings['offset_val']}",
                                    "{large_offsets_count}"),
            'min_max_offsets': (f"Maximum/minimum offset value",
                                "{min_max_offsets[0]} / {min_max_offsets[1]}"),
            'max_offset_dates': ("Start/end date(s) of offset with value <{min_max_offsets[0]}>",
                                 "{max_offset_dates[0]} / {max_offset_dates[1]}"),
            'min_offset_dates': ("Start/end date(s) of offset with value <{min_max_offsets[1]}>",
                                 "{min_offset_dates[0]} / {min_offset_dates[1]}")
        }

        formatted_metrics = []
        for key, (description, value) in available_metric_data.items():
            if key in metrics:
                formatted_description = description.format(**metrics)
                formatted_value = value.format(**metrics)
                formatted_metrics.append((formatted_description, formatted_value))

        return formatted_metrics
    # End format_metrics.

    def count_long_gaps(self, df: pd.DataFrame = None, duration_column_name: str = None,
                        offsets_column_name: str = None, **kwargs) -> int:
        if df is None:
            df = self.run_data_df
        if duration_column_name is None:
            duration_column_name = self.col_config['duration_column']
        if offsets_column_name is None:
            offsets_column_name = self.col_config['offset_column']

        nan_df = df[df[offsets_column_name].isna()]
        long_gaps_count = len(self.filter_gaps_by_duration(nan_df, duration_column_name, **kwargs))
        return long_gaps_count
    # End count_long_gaps.

    def count_long_offsets(self, df: pd.DataFrame = None, duration_column_name: str = None,
                           offsets_column_name: str = None, **kwargs) -> int:
        if df is None:
            df = self.run_data_df
        if duration_column_name is None:
            duration_column_name = self.col_config['duration_column']
        if offsets_column_name is None:
            offsets_column_name = self.col_config['offset_column']

        long_offsets_df = self.filter_offsets_by_duration(df, duration_column_name, **kwargs)
        long_non_nan_df = long_offsets_df[long_offsets_df[offsets_column_name].notna()]
        long_offsets_count = len(long_non_nan_df)
        return long_offsets_count
    # End count_long_offsets.

    def count_large_offsets(self, df: pd.DataFrame = None, offsets_column_name: str = None,
                            **kwargs) -> int:
        if df is None:
            df = self.run_data_df
        if offsets_column_name is None:
            offsets_column_name = self.col_config['offset_column']

        large_offsets_df = self.filter_offsets_by_value(df, offsets_column_name, **kwargs)
        large_offsets_count = len(large_offsets_df)
        return large_offsets_count
    # End count_large_offsets.

    def get_max_gap_duration(self, df: pd.DataFrame = None, duration_column_name: str = None,
                             offsets_column_name: str = None):
        if df is None:
            df = self.run_data_df
        if duration_column_name is None:
            duration_column_name = self.col_config['duration_column']
        if offsets_column_name is None:
            offsets_column_name = self.col_config['offset_column']

        nan_df = df[df[offsets_column_name].isna()]
        max_gap_duration = nan_df[duration_column_name].max()
        return max_gap_duration
    # End get_max_gap_duration.

    def get_max_gap_dates(self, max_gap_duration=None, df: pd.DataFrame = None,
                          duration_column_name: str = None, offsets_column_name: str = None,
                          start_date_column_name: str = None, end_date_column_name: str = None):
        if max_gap_duration is None:
            max_gap_duration = self.get_max_gap_duration(df, duration_column_name, offsets_column_name)
        if df is None:
            df = self.run_data_df
        if duration_column_name is None:
            duration_column_name = self.col_config['duration_column']
        if offsets_column_name is None:
            offsets_column_name = self.col_config['offset_column']
        if start_date_column_name is None:
            start_date_column_name = self.col_config['start_date_column']
        if end_date_column_name is None:
            end_date_column_name = self.col_config['end_date_column']

        nan_df = df[df[offsets_column_name].isna()]
        gap_start_date = nan_df[nan_df[duration_column_name]
                                == max_gap_duration][start_date_column_name].tolist()
        gap_end_date = nan_df[nan_df[duration_column_name]
                              == max_gap_duration][end_date_column_name].tolist()
        return gap_start_date, gap_end_date
    # End get_max_gap_dates.

    def get_max_offset_duration(self, df: pd.DataFrame = None, offsets_column_name: str = None,
                                duration_column_name: str = None):
        if df is None:
            df = self.run_data_df
        if duration_column_name is None:
            duration_column_name = self.col_config['duration_column']
        if offsets_column_name is None:
            offsets_column_name = self.col_config['offset_column']

        non_nan_runs = df[df[offsets_column_name].notna()]
        max_offset_duration = non_nan_runs[duration_column_name].max()
        return max_offset_duration
    # End get_max_offset_duration.

    def get_longest_offsets(self, max_offset_duration=None, df: pd.DataFrame = None,
                            offsets_column_name: str = None, duration_column_name: str = None) -> list[float]:
        if max_offset_duration is None:
            max_offset_duration = self.get_max_offset_duration(df, offsets_column_name, duration_column_name)
        if df is None:
            df = self.run_data_df
        if duration_column_name is None:
            duration_column_name = self.col_config['duration_column']
        if offsets_column_name is None:
            offsets_column_name = self.col_config['offset_column']

        non_nan_runs = df[df[offsets_column_name].notna()]
        longest_offsets = non_nan_runs[non_nan_runs[duration_column_name]
                                       == max_offset_duration][offsets_column_name].tolist()
        return longest_offsets
    # End get_longest_offsets.

    def get_min_max_offsets(self, df: pd.DataFrame = None, offsets_column_name: str = None) -> tuple[float, float]:
        if df is None:
            df = self.run_data_df
        if offsets_column_name is None:
            offsets_column_name = self.col_config['offset_column']

        max_offset = df[offsets_column_name].max()
        min_offset = df[offsets_column_name].min()
        return max_offset, min_offset
    # End get_min_max_offsets.

    def get_offset_dates(self, offset_value, df: pd.DataFrame = None, offsets_column_name: str = None,
                         start_date_column_name: str = None, end_date_column_name: str = None):
        if df is None:
            df = self.run_data_df
        if offsets_column_name is None:
            offsets_column_name = self.col_config['offset_column']
        if start_date_column_name is None:
            start_date_column_name = self.col_config['start_date_column']
        if end_date_column_name is None:
            end_date_column_name = self.col_config['end_date_column']

        bool_mask = df[offsets_column_name] == offset_value
        start_dates = df.loc[bool_mask, start_date_column_name].tolist()
        end_dates = df.loc[bool_mask, end_date_column_name].tolist()
        return start_dates, end_dates
    # End get_offset_dates.

    def get_long_offsets_info(self, df: pd.DataFrame = None, duration_column_name: str = None):

        # Filter for offsets (runs) by duration.
        long_offsets_df = self.filter_offsets_by_duration(df, duration_column_name)

        # Get dictionary of info about the unique offsets in long_offsets_df.
        long_offsets_dict = self.get_unique_offsets_info(long_offsets_df)

        return long_offsets_dict
    # End get_long_offsets_dict.

    def get_unique_offsets_info(self, df: pd.DataFrame = None):
        if df is not None:
            self._validate_dataframe(df)
        elif self.run_data_df is not None:
            df = self.run_data_df
        else:
            raise ValueError("No valid DataFrame provided, and no pre-set DataFrame found.")

        # Get the unique offset values from long offsets.
        unique_offsets = df[self.col_config['offset_column']].unique()

        # Get all the durations, start dates, and end dates that correspond
        # to each offset value. Organize into a dictionary.
        unique_offsets_dict = {}
        for offset in unique_offsets:

            # Skip NaNs.
            if pd.isna(offset):
                continue

            # Add offset (key) and corresponding info from get_run_data df structure
            # to dictionary.
            offset_data = df[df[self.col_config['offset_column']] == offset]
            unique_offsets_dict[offset] = {
                'duration': offset_data[self.col_config['duration_column']].tolist(),
                'start_date': offset_data[self.col_config['start_date_column']].tolist(),
                'end_date': offset_data[self.col_config['end_date_column']].tolist()
            }
        # End for.

        return unique_offsets_dict
    # End get_unique_offsets_dict.

    def filter_offsets_by_duration(self, df: pd.DataFrame = None, duration_column_name: str = None,
                                   **kwargs) -> pd.DataFrame:
        df, duration_column_name = self._get_valid_dataframe(df, duration_column_name, 'duration_column')

        # Read in configurations.
        default_params = self.config['filter_offsets_by_duration']
        params = {**default_params, **kwargs}
        threshold = self._parse_timedelta(params['threshold'])
        type = params['type']
        is_strict = params['is_strict']

        return self.filter_by_duration(df, duration_column_name, threshold, type, is_strict)
    # End filter_offsets_by_duration.

    def filter_gaps_by_duration(self, df: pd.DataFrame = None, duration_column_name: str = None,
                                **kwargs) -> pd.DataFrame:
        df, duration_column_name = self._get_valid_dataframe(df, duration_column_name, 'duration_column')

        # Read in configurations. Allow user's kwargs to override set configurations.
        default_params = self.config['filter_gaps_by_duration']
        params = {**default_params, **kwargs}
        threshold = self._parse_timedelta(params['threshold'])
        type = params['type']
        is_strict = params['is_strict']

        return self.filter_by_duration(df, duration_column_name, threshold, type, is_strict)
    # End filter_gaps_by_duration.

    def filter_offsets_by_value(self, df: pd.DataFrame = None, offset_column_name: str = None,
                                **kwargs) -> pd.DataFrame:
        df, offset_column_name = self._get_valid_dataframe(df, offset_column_name, 'offset_column')

        # Read in configurations. Allow user's kwargs to override set configurations.
        default_params = self.config['filter_offsets_by_value']
        params = {**default_params, **kwargs}
        threshold = params['threshold']
        type = params['type']
        use_abs = params['use_abs']
        is_strict = params['is_strict']

        return self.filter_by_value(df, offset_column_name, threshold, type, use_abs, is_strict)
    # End filter_offsets_by_value.

    # ******************************************************************************
    # ***************************** HELPER METHODS *********************************
    # ******************************************************************************
    def _validate_dataframe(self, df):
        required_columns = [
            self.col_config['duration_column'],
            self.col_config['offset_column'],
            self.col_config['start_date_column'],
            self.col_config['end_date_column']
        ]
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"DataFrame must contain column: {col}")
    # End _validate_dataframe.

    def _get_valid_dataframe(self, df, col_name, config_col_name):
        if df is not None:
            if col_name is not None:
                if col_name in df.columns:
                    return df, col_name
            elif self.col_config[config_col_name] in df.columns:
                return df, self.col_config[config_col_name]
            else:
                raise ValueError(f"Column '{col_name}' could not be found in DataFrame.")
        elif self.run_data_df is not None:
            return self.run_data_df,  self.col_config[config_col_name]
        else:
            raise ValueError("No DataFrame provided and no pre-set DataFrame found.")
    # End _get_valid_dataframe.

    def _validate_metrics(self, metrics):
        valid_metrics_keys = self.get_valid_metrics_list()

        if type(metrics) is dict:
            for key in metrics.keys():
                if key not in valid_metrics_keys:
                    return False
        else:
            return False

        return True
    # End _validate_metrics.

    def _get_metric_key_strings(self):
        # Initialize strings.
        key_str_dict = {
            'offset_dur': "Number of offsets (non-NaN) with duration ",
            'gap_dur': "Number of gaps with duration ",
            'offset_val': "Number of offsets with "
        }

        # Get string for offset duration.
        key_str_dict['offset_dur'] += self._generate_duration_string(self.config['filter_offsets_by_duration'])

        # Get string for gap duration.
        key_str_dict['gap_dur'] += self._generate_duration_string(self.config['filter_gaps_by_duration'])

        # Get string for offset value.
        key_str_dict['offset_val'] += self._generate_value_string(self.config['filter_offsets_by_value'])

        return key_str_dict
    # End _get_metric_key_strings.

    def _generate_duration_string(self, params):
        if 'type' in params:
            if params['type'] == 'min':
                return f"> {'' if params['is_strict'] else '='} {params['threshold']}"
            elif params['type'] == 'max':
                return f"< {'' if params['is_strict'] else '='} {params['threshold']}"
        return "[Type of threshold not specified]"
    # End _generate_duration_string.

    def _generate_value_string(self, params):
        result = ""
        if params.get('use_abs'):
            result += "absolute value "

        if 'type' in params:
            if params['type'] == 'min':
                result += f"> {'' if params['is_strict'] else '='} {params['threshold']}"
            elif params['type'] == 'max':
                result += f"< {'' if params['is_strict'] else '='} {params['threshold']}"
        else:
            result += "[Type of threshold not specified]"

        return result
    # End _generate_value_string.

    def _parse_timedelta(self, time_str):
        pattern = r'(\d+)\s*(week|weeks|day|days|hour|hours|minute|minutes|second|seconds|millisecond|milliseconds)'
        matches = re.findall(pattern, time_str.lower())

        weeks = days = hours = minutes = seconds = milliseconds = 0

        for value, unit in matches:
            value = int(value)
            if 'week' in unit:
                weeks += value
            elif 'day' in unit:
                days += value
            elif 'hour' in unit:
                hours += value
            elif 'minute' in unit:
                minutes += value
            elif 'second' in unit:
                seconds += value
            elif 'millisecond' in unit:
                milliseconds += value

        return timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds,
                         milliseconds=milliseconds)
    # End _parse_timedelta.

    # ******************************************************************************
    # ***************************** STATIC METHODS *********************************
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

    @staticmethod
    def get_discrepancies(offset_column: pd.Series, reference_column: pd.Series,
                          size: int) -> list[float]:
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

    @staticmethod
    def get_valid_metrics_list() -> list:
        valid_metrics_keys = [
            "long_offsets_count",
            "long_gaps_count",
            "max_gap_duration",
            "max_gap_dates",
            "max_offset_duration",
            "longest_offsets",
            "large_offsets_count",
            "min_max_offsets",
            "max_offset_dates",
            "min_offset_dates"
        ]
        return valid_metrics_keys.copy()
    # End get_valid_metrics_list.

    @staticmethod
    def filter_by_duration(dataframe: pd.DataFrame, duration_column_name: str, threshold=timedelta(0),
                           type='min', is_strict=False) -> pd.DataFrame:
        filtered_df = dataframe.copy()

        series = filtered_df[duration_column_name]

        if type == 'min':
            if is_strict:
                filter_series = series > threshold
            else:
                filter_series = series >= threshold
        elif type == 'max':
            if is_strict:
                filter_series = series < threshold
            else:
                filter_series = series <= threshold
        else:
            filter_series = pd.Series([True] * len(dataframe))  # Default case: no filtering

        filtered_df = filtered_df[filter_series]

        return filtered_df
    # End filter_by_duration.

    @staticmethod
    def filter_by_value(dataframe: pd.DataFrame, offset_column_name: str, threshold=0.0, type='min',
                        use_abs=True, is_strict=False) -> pd.DataFrame:
        # Copy dataframe.
        filtered_df = dataframe.copy()

        # Apply absolute value if needed.
        series = abs(filtered_df[offset_column_name]) if use_abs else filtered_df[offset_column_name]

        # Define the filtering logic.
        if type == 'min':
            if is_strict:
                filter_series = series > threshold
            else:
                filter_series = series >= threshold
        elif type == 'max':
            if is_strict:
                filter_series = series < threshold
            else:
                filter_series = series <= threshold
        else:
            filter_series = pd.Series([True] * len(dataframe))  # Default case: no filtering

        filtered_df = filtered_df[filter_series]

        return filtered_df
    # End filter_by_value.


