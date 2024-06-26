import json
import pandas as pd
import numpy as np
from datetime import timedelta
import re


class MetricsCalculator:

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
            'filter_by_duration_parameters': {
                'threshold': '0 days',
                'type': 'min',
                'is_strict': False
            },
            'filter_by_value_parameters': {
                'threshold': 0.0,
                'use_abs': True,
                'type': 'min',
                'is_strict': False
            },
            'filter_gaps_parameters': {
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

    def set_column_names(self, duration_column, offset_column,
                         start_date_column, end_date_column):
        self.col_config['duration_column'] = duration_column
        self.col_config['offset_column'] = offset_column
        self.col_config['start_date_column'] = start_date_column
        self.col_config['end_date_column'] = end_date_column
    # End set_column_names.

    def set_configs(self, file_path):
        try:
            # Update config dictionary only if section exists.
            with open(file_path, 'r') as file:
                user_config = json.load(file)
                for section, settings in user_config.items():
                    if section in self.config:
                        self.config[section].update(settings)
        except FileNotFoundError:
            print(f"Error: Config file '{file_path}' not found. Using default configuration.")
    # End set_configs.

    def validate_dataframe(self, df):
        required_columns = [
            self.col_config['duration_column'],
            self.col_config['offset_column'],
            self.col_config['start_date_column'],
            self.col_config['end_date_column']
        ]
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"DataFrame must contain column: {col}")
    # End validate_dataframe.

    def _get_valid_dataframe(self, df, col_name, config_col_name):
        if None not in (df, col_name):
            if col_name in df.columns:
                # self.validate_dataframe(df)
                return df, col_name
        elif self.run_data_df is not None:
            return self.run_data_df,  self.col_config[config_col_name]
        else:
            raise ValueError("No DataFrame provided, or column not in DataFrame, "
                             "and no pre-set DataFrame found.")

    def set_dataframe(self, df):
        self.validate_dataframe(df)
        self.run_data_df = df
    # End set_dataframe.

    def get_run_data(self, offset_column, reference_column, ref_dates, size, create_df=True):
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

    def get_discrepancies(self, offset_column, reference_column, size):
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

    def calculate_metrics(self, df=None, **kwargs):

        if df is not None:
            self.validate_dataframe(df)
        elif self.run_data_df is not None:
            df = self.run_data_df
        else:
            raise ValueError("No DataFrame provided and no pre-set DataFrame found.")

        available_metrics = {
            "long_offsets_count": self.count_long_offsets,
            "long_gaps_count": self.count_long_gaps,
            "max_gap_duration": self.get_max_gap_duration,
            "max_gap_dates": lambda: self.get_max_gap_dates(self.get_max_gap_duration()),
            "max_offset_duration": self.get_max_offset_duration(),
            "longest_offsets": lambda: self.get_longest_offsets(self.get_max_offset_duration()),
            "large_offsets_count": self.count_large_offsets(),
            "min_max_offsets": self.get_min_max_offsets(),
            "max_offset_dates": lambda: self.get_offset_dates(self.get_min_max_offsets()[0]),
            "min_offset_dates": lambda: self.get_offset_dates(self.get_min_max_offsets()[1])
        }

        metrics = {}
        for key, func in available_metrics.items():
            if key in kwargs:
                metrics[key] = func()

        return metrics

    def get_valid_metrics_list(self):
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
        return valid_metrics_keys

    def validate_metrics(self, metrics):
        valid_metrics_keys = self.get_valid_metrics_list()

        if type(metrics) is dict:
            for key in metrics.keys():
                if key not in valid_metrics_keys:
                    return False
        else:
            return False

        return True

    def set_metrics(self, metrics, is_set=[False]):
        is_set[0] = self.validate_metrics(metrics)

        if is_set[0]:
            self.metrics = metrics

    def get_metrics(self):
        return self.metrics

    def format_metrics(self, metrics=None):
        if metrics is None:
            if self.metrics is not None:
                metrics = self.metrics
            else:
                raise ValueError("No Metrics provided, and no pre-set Metrics found.")
        else:
            self.validate_metrics(metrics)

        metric_strings = self.get_metric_key_strings()

        # Aggregate results.
        metric_data = [
            (f"{metric_strings['offset_dur']}", self.count_long_offsets()),
            (f"{metric_strings['gap_dur']}", self.count_long_gaps()),
            (f"Duration of longest gap", f"{max_gap_duration}"),
            (f"Start/end date(s) of <{max_gap_duration}> gap", f"{gap_start_date} / {gap_end_date}"),
            (f"Maximum duration of an offset", f"{max_offset_duration}"),
            (f"Offset value(s) with <{max_offset_duration}> duration", f"{longest_offsets}"),
            (f"{metric_strings['offset_val']} (unit)", f"{self.count_large_offsets()}"),
            (f"Maximum/minimum offset value", f"{max_offset}/{min_offset}"),
            (f"Start/end date(s) of offset with value <{max_offset}>", f"{max_offset_start_date} "
                                                                       f"/ {max_offset_end_date}"),
            (f"Start/end date(s) of offset with value <{min_offset}>", f"{min_offset_start_date} "
                                                                       f"/ {min_offset_end_date}")
        ]

        return metric_data

    def count_long_gaps(self):
        long_gaps_count = len(self.filter_gaps_by_duration())
        return long_gaps_count
    # End count_long_gaps.

    def count_long_offsets(self):
        long_offsets_df = self.filter_offsets_by_duration()
        long_non_nan_df = long_offsets_df[long_offsets_df[self.col_config['offset_column']].notna()]
        long_offsets_count = len(long_non_nan_df)
        return long_offsets_count

    def count_large_offsets(self):
        large_offsets_df = self.filter_offsets_by_value()
        large_offsets_count = len(large_offsets_df)
        return large_offsets_count

    def get_max_gap_duration(self):
        nan_df = self.run_data_df[self.run_data_df[self.col_config['offset_column']].isna()]
        max_gap_duration = nan_df[self.col_config['duration_column']].max()
        return max_gap_duration

    def get_max_gap_dates(self, max_gap_duration):
        nan_df = self.run_data_df[self.run_data_df[self.col_config['offset_column']].isna()]
        gap_start_date = nan_df[nan_df[self.col_config['duration_column']]
                                == max_gap_duration][self.col_config['start_date_column']].tolist()
        gap_end_date = nan_df[nan_df[self.col_config['duration_column']]
                              == max_gap_duration][self.col_config['end_date_column']].tolist()
        return gap_start_date, gap_end_date

    def get_max_offset_duration(self):
        non_nan_runs = self.run_data_df[self.run_data_df[self.col_config['offset_column']].notna()]
        max_offset_duration = non_nan_runs[self.col_config['duration_column']].max()
        return max_offset_duration

    def get_longest_offsets(self, max_offset_duration):
        non_nan_runs = self.run_data_df[self.run_data_df[self.col_config['offset_column']].notna()]
        longest_offsets = non_nan_runs[non_nan_runs[self.col_config['duration_column']]
                                       == max_offset_duration][self.col_config['offset_column']].tolist()
        return longest_offsets
    # End get_max_offset_duration_info.

    def get_min_max_offsets(self):
        max_offset = self.run_data_df[self.col_config['offset_column']].max()
        min_offset = self.run_data_df[self.col_config['offset_column']].min()
        return max_offset, min_offset
    # End get_min_max_offsets.

    def get_offset_dates(self, offset_value):
        bool_mask = self.run_data_df[self.col_config['offset_column']] == offset_value
        start_dates = self.run_data_df.loc[bool_mask, self.col_config['start_date_column']].tolist()
        end_dates = self.run_data_df.loc[bool_mask, self.col_config['end_date_column']].tolist()
        return start_dates, end_dates
    # End get_offset_dates.

    def get_metric_key_strings(self):
        # Initialize strings.
        key_str_dict = {
            'offset_dur': "Number of offsets (non-NaN) with duration ",
            'gap_dur': "Number of gaps with duration ",
            'offset_val': "Number of offsets with "
        }

        # Get string for offset duration.
        key_str_dict['offset_dur'] += self.generate_duration_string(self.config['filter_by_duration_parameters'])

        # Get string for gap duration.
        key_str_dict['gap_dur'] += self.generate_duration_string(self.config['filter_gaps_parameters'])

        # Get string for offset value.
        key_str_dict['offset_val'] += self.generate_value_string(self.config['filter_by_value_parameters'])

        return key_str_dict
    # End get_metric_key_strings.

    def generate_duration_string(self, params):
        if 'type' in params:
            if params['type'] == 'min':
                return f"> {'' if params['is_strict'] else '='} {params['threshold']}"
            elif params['type'] == 'max':
                return f"< {'' if params['is_strict'] else '='} {params['threshold']}"
        return "[Type of threshold not specified]"
    # End generate_duration_string.

    def generate_value_string(self, params):
        result = ""
        if params.get('use_abs'):
            result += "absolute "

        if 'type' in params:
            if params['type'] == 'min':
                result += f"> {'' if params['is_strict'] else '='} {params['threshold']}"
            elif params['type'] == 'max':
                result += f"< {'' if params['is_strict'] else '='} {params['threshold']}"
        else:
            result += "[Type of threshold not specified]"

        return result
    # End generate_value_string.

    def filter_offsets_by_duration(self, df=None, duration_col=None, **kwargs):
        df, duration_col = self._get_valid_dataframe(df, duration_col, 'duration_column')

        # Read in configurations.
        default_params = self.config['filter_by_duration_parameters']
        params = {**default_params, **kwargs}
        threshold = self.parse_timedelta(params['threshold'])
        type = params['type']
        is_strict = params['is_strict']

        return self.filter_by_duration(df, duration_col, threshold, type, is_strict)
    # End filter_offsets_by_duration.

    def filter_gaps_by_duration(self, df=None, duration_col=None, **kwargs):
        df, duration_col = self._get_valid_dataframe(df, duration_col, 'duration_column')

        # Read in configurations. Allow user's kwargs to override set configurations.
        default_params = self.config['filter_gaps_parameters']
        params = {**default_params, **kwargs}
        threshold = self.parse_timedelta(params['threshold'])
        type = params['type']
        is_strict = params['is_strict']

        return self.filter_by_duration(df, duration_col, threshold, type, is_strict)
    # End filter_gaps_by_duration.

    def filter_by_duration(self, dataframe, duration_col, threshold=timedelta(0),
                           type='min', is_strict=False):
        filtered_df = dataframe.copy()

        series = filtered_df[duration_col]

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

    def filter_offsets_by_value(self, df=None, offset_col=None, **kwargs):
        df, offset_col = self._get_valid_dataframe(df, offset_col, 'offset_column')

        # Read in configurations. Allow user's kwargs to override set configurations.
        default_params = self.config['filter_by_value_parameters']
        params = {**default_params, **kwargs}
        threshold = params['threshold']
        type = params['type']
        use_abs = params['use_abs']
        is_strict = params['is_strict']

        return self.filter_by_value(df, offset_col, threshold, type, use_abs, is_strict)
    # End filter_offsets_by_value.

    def filter_by_value(self, dataframe, offset_col, threshold=0.0, type='min',
                        use_abs=True, is_strict=False):
        # Copy dataframe.
        filtered_df = dataframe.copy()

        # Apply absolute value if needed.
        series = abs(filtered_df[offset_col]) if use_abs else filtered_df[offset_col]

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

    def parse_timedelta(self, time_str):
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
    # End parse_timedelta.












