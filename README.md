<h3>README</h3>

<b>noaa-lighthouse-problem</b> is a project aimed at generating useful metrics for assessing the<br> 
nature and severity of discrepancies between NOAA and Lighthouse data from tide gauge<br>
stations.

The main program `analyze_data_discrepancies.py` uses function implementation from<br>
`file_data_functions.py` to process annual water level data from both NOAA and <br>
Lighthouse, gets their statistical differences, and directly compares the data sets<br>
using discrepancy analysis.

These processes are meant to be a starting point for diagnosing any underlying issues<br>
that prevent Lighthouse from meeting the standard of NOAA.<br>
<br><br>

<h3>RUNNING PROGRAM</h3>

Running the main program `analyze_data_discrepancies.py` will write stats and metrics about<br>
the compared data sets for a tide gauge station to a text file in the `generated_files`<br>
directory. 

To specify the name of the file to be written to, use command line argument

`--filename myFileName`

with no extension (.txt). If not specified, the program will write to `output_{timestamp}.txt.`

The datasets being compared must be specified using command line arguments. The<br>
reference data (NOAA) path and primary data (Lighthouse) path are provided by the user <br>
with command line arguments

`--refDir path/to/NOAA/files --primaryDir path/to/Lighthouse/files`

These paths should be to folders of water level csv files for a specific tide gauge<br>
station. Typically, doing discrepancy analysis would require that the station chosen<br>
for both NOAA and Lighthouse be the same. However, any two stations can be compared. 


