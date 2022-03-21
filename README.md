# get_DNAnexus_URLs


Clinical Scietists reported that they could not view the mappped files in the IGV browser easily. 

Therefore, get_DNAnexus_URLs was created to speed up the process

This code is written to generate .csv containing URLs for index and BAM files located on the DNAnexus for use in the IGV browser. 

The code is to be run every day to refresh the .csv file with udated information. 

The URLs are generated for files created within the last 1 week. This length can be easily modified within the script. 

The URL links are active for 23 hours.

## Running the script

Run this script in python 3. 

python script_name length output_name

### Example:

python path/get_DNANexus_URLs.py -12w path/hg19_dnanexus.csv

-12w = 12 weeks from now
hg19_dnanexus.csv = output file name


## Author
Igor Malashchuk, 
STP in Clinical Boinformatics
Guys and St Thomas's NHS Foundation Trust
Updated: 21st March 2022 
