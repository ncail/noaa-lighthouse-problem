# Table of Contents

1. [Introduction](#introduction)
2. [Diagrams](#diagrams)
   - [Program processing loop](#program-processing-loop)
   - [Temporal correction algorithm](#temporal-correction-algorithm)
3. [Running the Program](#running-the-program)
   - [Overview](#overview)
   - [Command Line Arguments](#command-line-arguments)
4. [Output](#output)
5. [Requirements](#requirements)
6. [Installation](#installation)
7. [Usage Example](#usage-example)
8. [Configuration](#configuration)
   - [Overview](#overview-1)
   - [File Location](#file-location)
   - [Configuration Sections](#configuration-sections)
   - [Configuration Values](#configuration-values)
   - [Default Values](#default-values)
   - [Example Configuration](#example-configuration)
9. [Technical Details and Limitations](#technical-details-and-limitations)
   - [Data Requirements](#data-requirements)
   - [Dependencies](#dependencies)
10. [Downloading Data](#downloading-data)
    - [NOAA Data](#noaa-data)
    - [Lighthouse Data](#lighthouse-data)

## Introduction

**noaa-lighthouse-problem** is a project aimed at assessing the discrepancies between time series water level data from tide gauge stations shared by NOAA and Lighthouse. In coastal Texas, some stations provide data to both organizations, but we have found significant discrepancies in the data available for download from these. Discrepancies include vertical and temporal offsets, missing values, flatlines, and spikes. Since NOAA sets the standard for data quality, it is crucial to understand why Lighthouse data differs to ensure the quality of water level data from Lighthouse stations not shared by NOAA.

The main program, `analyze_data.py`, uses class implementation from `MetricsCalculator.py` and `TransformData.py` to compare annual water level data between NOAA and Lighthouse. It can be configured to report on temporal offsets and datum shifts.

These processes are intended to be a starting point for diagnosing any underlying issues that prevent Lighthouse from meeting the standards of NOAA.

**Note:** Not all Lighthouse stations are also NOAA stations. Additionally, Lighthouse stations are only located in coastal Texas, whereas NOAA stations are distributed across the United States. Ensure that you select comparable stations when running the analysis.

## Diagrams

### Program processing loop
```mermaid
%%{
    init: {
            'theme':'base',
            'themeVariables': {
            'primaryColor':'#fff',
            'secondaryColor':'#445',
            'tertiaryColor':'#445',
            'primaryTextColor': '#000',
            'primaryBorderColor': '#36f'
            }   
          }
}%%


flowchart LR

    classDef decisions fill:#afa, stroke:#171;
    classDef outsideLoop fill:#69d; 
    classDef exit fill:#e77, stroke:#922;
    classDef insideLoop fill:#fad, stroke:#c8a

    Start:::exit
    Exit:::exit
    End:::exit

    FileDecision:::decisions
    AnalysisType:::decisions
    EndLoop:::decisions

    Config:::outsideLoop
    Files:::outsideLoop
    DataFrames:::outsideLoop
    SplitData:::outsideLoop
    Preprocess:::outsideLoop
    ExtractYrs:::outsideLoop
    Summary:::outsideLoop

    MergeData:::insideLoop
    Report:::insideLoop
    TemporalCorr:::insideLoop

    linkStyle default color:white

    subgraph main

        direction LR
    
        subgraph setup
            Start[start] --> Config[load <br> config]
            Config --> Files[load <br> files]
            Files --> FileDecision{files <br> good?}

            %% File decision.
            FileDecision --> |no|Exit[exit]
            FileDecision --> |yes|DataFrames[data <br> frames]

            DataFrames --> SplitData[split <br> data]
            SplitData --> Preprocess[preprocess <br> data]
            Preprocess --> ExtractYrs[extract <br> years]
        end

            %% Main program loop.
            ExtractYrs --> |loop over <br> years|MergeData[merge <br> datasets]

        subgraph yearly analysis

            direction RL

            MergeData --> AnalysisType{analysis <br> type?}

            %% Analysis type decision.
            AnalysisType --> |corrected|TemporalCorr[temporal <br> correction]
            AnalysisType --> |raw|Report[generate <br> report]

            TemporalCorr --> Report

            Report --> EndLoop{more <br> years?}

            EndLoop --> |yes|MergeData
        end

        EndLoop --> |no|Summary[summary <br> report]
        Summary --> End[end]
    
    end
```

### Temporal correction algorithm
```mermaid
%%{
    init: {
            'theme':'base',
            'themeVariables': {
            'primaryColor':'#fff',
            'secondaryColor':'#445',
            'tertiaryColor':'#445',
            'primaryTextColor': '#000',
            'primaryBorderColor': '#36f'
            }   
          }
}%%


flowchart RL

    classDef decisions fill:#afa, stroke:#171;
    classDef outsideLoop fill:#69d; 
    classDef exit fill:#e77, stroke:#922;
    classDef insideLoop fill:#fad, stroke:#c8a

    SetConfig:::outsideLoop

    Validate:::decisions
    IsFound:::decisions
    IsValid:::decisions
    TryNext:::decisions
    DataEnd:::decisions

    Exit:::exit
    End:::exit
    Start:::exit

    TryShift:::insideLoop
    GetDS:::insideLoop
    StoreCorr:::insideLoop
    HandleSegment:::insideLoop
    DocSegment:::insideLoop
    NextIndex:::insideLoop

    linkStyle default color:white

    subgraph algorithm
        direction LR

        subgraph initialize

            Start[start] --> Validate{valid <br> structure?}

            %% Valid structure decision.
            Validate --> |yes|SetConfig[set <br> configs]
            
            Validate --> |no|Exit[exit]
        end

        %% Enter loop.
        SetConfig --> |loop over <br> indices|TryShift[try shift]

        subgraph correct data
            
            TryShift --> GetDS[get valid <br> datum shift]
            GetDS --> IsFound{found?}

            %% Datum shift found decision.
            IsFound --> |yes|StoreCorr[store <br> correction]
            IsFound --> |no|TryNext{all shifts <br> tried?}

            %% Try next temporal shift decision.
            TryNext --> |yes|HandleSegment[handle <br> segment]
            TryNext --> |no|TryShift

            StoreCorr --> |next <br> index|IsValid{datum shift <br> still valid?}

            %% Datum shift still valid/continue storing corrections decision.
            IsValid --> |yes|StoreCorr
            IsValid --> |no|DocSegment[document <br> segment]

            HandleSegment --> DocSegment
            DocSegment --> NextIndex[index after <br> segment]

            NextIndex --> DataEnd{end of <br> data?}

            %% Exit loop decision.
            DataEnd --> |no|TryShift

        end

        DataEnd --> |yes|End[end]

    end
```

## Running the program

### Overview

Running the main program, `analyze_data.py`, will generate text file and CSV reports for the comparison analysis done on the reference (NOAA) and primary (Lighthouse) data from two respective specified directories.

The `data` directory included in the repository has example CSV files that the program can handle. The program will process all of the files in the specified directories at once (e.g. `data/lighthouse/[station]` and `data/NOAA/[station]`) and then sort the data into years for yearly comparison. 

Importantly, the program offers two modes, or types of analysis: *raw* and *corrected*. Raw mode will run the analysis on the provided data. Corrected mode will first perform a temporal correction (with configurable parameters) to align the primary data with the reference data, and proceed with the comparison analysis on the corrected data. In either mode, the program can still report on the existing, or fixed, temporal shifts.

### Command line arguments

```shell
--config myConfig.json
```
- Specifies the file used to entirely configure the program. By default, the program will load `config.json`. See [Configuration](#configuration) to learn how to fill the fields in `config.json` to have the most control over the program execution. Some values from the configuration file are available to be overridden by command line arguments, listed below.

```shell
--filename myFileName
```
- Specifies the base file name of output files. The provided file name will be appended with the name of the type of report being generated (detailed in [Output](#output)).

```shell
--primarydir path/to/Lighthouse/data/files --refdir path/to/NOAA/data/files
```
- Specifies the path to the primary (Lighthouse) and reference (NOAA) data sources. The program will read all CSV files in these directories and then proceed with a yearly comparison analysis of the data. 

```shell
--years 2007 2008 2012 2021
```
- Stores the years desired for comparison. Defaults to `all_years`. Although all CSV files will be read in from the primary and reference directories, only data with years matching this argument will be processed in the comparison analysis.

```shell
--output path/to/output/files
```
- Specifies the output files location. Defaults to the `generated_files` directory.

```shell
--logging-off
```
- Opts out of logging the program execution. Logging is on by default.

```shell
--mode corrected
```
- Configures the analysis type for options `raw` and `corrected`. Defaults to `raw`.


## Output

The program can generate four different types of reports based on the data analysis:

- **Metrics summary**
   - Contains a table of metrics and statitsics per year of the data, and lists the parameters of the configuration file used.
   - Text file with file name `[base file name]_metrics_summary.txt`.

- **Metrics detailed**
   - Contains a comparison table between the primary and reference data and details about the metrics such as the start/end dates of the minimum/maximum offsets and offsets meeting the configured duration threshold.
   - Text file with filename `[base_file_name]_metrics_detailed.txt`

- **Temporal shifts summary**
   - Contains a table listing the unique temporal shifts and datum shifts found per year of data

- **Annotated raw data**
## Requirements

Python 3.x<br>
Required packages (listed in `requirements.txt`)

## Installation

1. Clone the repository:
```shell
git clone https://github.com/ncail/noaa-lighthouse-problem.git
cd noaa-lighthouse-problem
```
2. Install the required packages:
```shell
pip install -r requirements.txt
```

## Usage example

1. Navigate to the project directory:
```shell
cd path/to/noaa-lighthouse-problem
```
2. Run the program:
```shell
python analyze_data.py --config config.json
```

## Configuration

### Overview

`config.json` is used to configure:
- Paths to the data, and for output files.
- Mode of data analysis.
- 

### File location

Place `config.json` in the root directory of your project.

### Configuration sections

- **Primary data column names**
    - `datetime`: Name of the datetime column in the primary data CSV files.
    - `water_level`: Name of the water level column in the primary data CSV files.

- **Reference data column names**
    - `datetime`: Name of the datetime column in the reference data CSV files.
    - `water_level`: Name of the water level column in the reference data CSV files.

- **Filter offsets by duration parameters**
    - `threshold`: The duration required for an offset to persist for it to be quantified in the results file. 
    - `type`: Specifies if the threshold is a minimum or maximum cutoff.
    - `is_strict`: Specifies if the threshold is exclusive (strict) or inclusive.

- **Filter gaps by duration parameters**
    - `threshold`: The duration required for a gap (missing values) to persist for it to be quantified in the results file (the total missing values are also provided in the results). 
    - `type`: Specifies if the threshold is a minimum or maximum cutoff.
    - `is_strict`: Specifies if the threshold is exclusive (strict) or inclusive.

- **Filter offsets by value parameters**
    - `threshold`: The value of an offset required for it to be quantified in the results file.
    - `use_abs`: Specifies to use the absolute values of offsets to determine if they meet the threshold criteria.
    - `type`: Specifies if the threshold is a minimum or maximum cutoff.
    - `is_strict`: Specifies if the threshold is exclusive (strict) or inclusive.

- **Offset correction parameters**
    - `number_of_intervals`: The number of intervals required for a discrepancy to persist for it to be identified as an offset. This is used to determine temporal and vertical offset corrections, and is unrelated to the filter by duration processes.

### Configuration values

- `datetime`: Examples include: "Date Time", "myDateTimeColumn".
- `water_level`: Examples include: "Water Level", "myWaterLevelColumn".
- `threshold` (duration): Examples include:  "1 week", "2 days, 12 hours", "30 minutes".
- `threshold` (numeric): Must be numeric. Examples include: 0.05, 10.0.
- `type`: Must be either "min" or "max".
- `use_abs`: Must be `true` or `false`.
- `is_strict`: Must be `true` or `false`.

### Default values

Default values are used if a parameter is not specified in `config.json`:

- `primary_data_column_names`: `datetime` = "", `water_level` = "".
- `reference_data_column_names`: `datetime` = "", `water_level` = "".
- `filter_offsets_by_duration`: `threshold` = "0 days", `type` = "min", `is_strict` = `false`.
- `filter_gaps_by_duration`: `threshold` = "0 days", `type` = "min", `is_strict` = `false`.
- `filter_offsets_by_value`: `threshold` = 0.0, `type` = "min", `use_abs` = `true`, `is_strict` = `false`.
- `offset_correction_parameters`: `number_of_intervals` = 0.

<br>

**Note**: For data column names that are left as default values, the program will assume the positions of the datetime and/or water level columns as the first and second column in the CSV files, respectively. Check the data files to verify the order of the columns, or to copy the names of the columns into `config.json`.

### Example configuration
```elixir
{
   "data": {
      "paths": {
         "refdir": "data/NOAA/rockport",
         "primarydir": "data/lighthouse/Rockport"
      },

      "primary_data_column_names": {
         "datetime": "",
         "water_level": ""
      },
   
      "reference_data_column_names": {
         "datetime": "",
         "water_level": ""
      }
  },

  "analysis": {
      "mode": "corrected",
      "years": [1999, 2000]
  },

   "output": {
      "base_filename": "Rockport_1999_2000",
      "path": "generated_files",
      "execution_msgs": false,

      "generate_reports_for_years": {
         "metrics_summary": [],
         "metrics_detailed": [1999, 2000],
         "temporal_corrections_summary": [],
         "temporal_correction_processing": []
     }
   },

  "filter_offsets_by_duration": {
      "threshold": "1 hour",
      "type": "min",
      "is_strict": false,
      "nonzero": true
  },

  "filter_offsets_by_value": {
      "threshold": 0.001,
      "use_abs": true,
      "type": "min",
      "is_strict": false,
      "nonzero": true
  },

  "filter_gaps_by_duration": {
      "threshold": "1 day",
      "type": "min",
      "is_strict": false
  },

  "temporal_shift_correction": {
      "number_of_intervals": 10,
      "replace_with_nans": true
  }
}
```

## Technical details and limitations

### Data requirements

- **Data format**: Since the purpose of the program is to analyze the discrepancies between time series water level datasets, the data processed by the program should contain water level measurements with the corresponding timestamps over some time period. The data files must be in CSV format which will provide the data in columns.
- **Column order**: If the names of the necessary columns (datetime and water level) are left as default in `config.json`, the program will assume that the datetime and water level columns are the first and second columns in the data files, respectively.

### Dependencies

- **Libraries**: The program cleans the data by replacing corrupt or missing values with null values using the `numpy` library so that this does not have to be done by the user beforehand. Additionally, the program relies heavily on the `pandas` library to process the data as dataframes, with the assumed positioning of columns outlined above (in the default case that the necessary column names have not been configured in `config.json`).

## Downloading data

- **NOAA data**

   1. Visit the NOAA Tides & Currents website: [NOAA Tides & Currents](https://tidesandcurrents.noaa.gov/)
   2. Use the search bar or map to select a tide gauge station.
   3. Specify the date range, units, and datum for the data you want to download.
   4. Download the data in CSV format and save it to a directory, e.g., path/to/NOAA/files.

- **Lighthouse data**

   1. Access the Lighthouse data portal: [Lighthouse Data Portal]"(link?)"
   2. Locate the relevant tide gauge station and specify the date range, etc. for the data.
   3. Be sure to download the data in CSV format by selecting to download as "Comma-separated values."
