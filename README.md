# NOAA-Lighthouse-Problem

<b>noaa-lighthouse-problem</b> is a project aimed at generating useful metrics for assessing the nature and severity of discrepancies between NOAA and Lighthouse data from tide gauge stations.

The main program `analyze_data_discrepancies.py` uses function implementation from `file_data_functions.py` to process annual water level data from both NOAA and Lighthouse (downloaded as .csv), gets their statistical differences, and directly compares the datasets using discrepancy analysis.

These processes are meant to be a starting point for diagnosing any underlying issues that prevent Lighthouse from meeting the standard of NOAA.

## Running program

Running the main program `analyze_data_discrepancies.py` will write statistics and metrics about the compared datasets for a tide gauge station to a text file in the `generated_files` directory.

## Command line arguments

- To specify the name of the file to be written to, use command line argument
```shell
--filename myFileName
```

- The reference data (NOAA) path and primary data (Lighthouse) path are provided by the user. To specify these paths, use command line arguments
```shell
--refDir path/to/NOAA/files --primaryDir path/to/Lighthouse/files
```

These paths should be to water level CSV files for a specific tide gauge station. Ideally, the station and years chosen for both NOAA and Lighthouse should be the same. However, the program can compare any two stations but will not write results if it cannot find data for common years.

## Output

The program generates a text file in the `generated_files` directory containing the statistics and metrics of the compared datasets. The output filename is either user-specified or generated based on the current timestamp.

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
python analyze_data_discrepancies.py --refDir path/to/NOAA/files --primaryDir path/to/Lighthouse/files --filename results
```