README

noaa-lighthouse-problem is a project aimed at generating useful metrics for assessing the 
nature and severity of discrepancies between NOAA and Lighthouse data from tide gauge
stations.

The main program analyze_data_discrepancies.py uses function implementation from
file_data_functions.py to process annual water level data from both NOAA and 
Lighthouse, gets their statistical differences, and directly compares the data sets
using discrepancy analysis.

These processes are meant to be a starting point for diagnosing any underlying issues
that prevent Lighthouse from meeting the standard of NOAA.

