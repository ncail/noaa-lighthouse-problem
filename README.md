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


RUNNING PROGRAM

Running the main program analyze_data_discrepancies.py will write stats and metrics about
the compared data sets for a tide gauge station to a text file in the generated_files
directory. 

To specify the name of the file to be written to, use command line argument

   --filename myFileName

with no extension (.txt) when running the file. If not specified, the program will
write to output_{timestamp}.txt.

The datasets being compared must be specified using command line arguments. The
reference data (NOAA) path and primary data (Lighthouse) path are provided by the user 
with command line arguments

   --refDir path/to/NOAA/files --primaryDir path/to/Lighthouse/files 

These paths should be to folders of water level csv files for a specific tide gauge
station. Typically, doing discrepancy analysis would require that the station chosen
for both NOAA and Lighthouse be the same. However, any two stations can be compared. 


